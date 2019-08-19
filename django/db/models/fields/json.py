import json

from django import forms
from django.core import exceptions
from django.db import connection as builtin_connection
from django.db.models import Func, Value, lookups
from django.db.models.lookups import (
    FieldGetDbPrepValueMixin, Lookup, Transform,
)
from django.db.utils import NotSupportedError
from django.utils.translation import gettext_lazy as _

from . import Field, TextField
from .mixins import CheckFieldDefaultMixin


class JSONField(CheckFieldDefaultMixin, Field):
    description = _('A JSON object')
    default_error_messages = {
        'invalid': _("Value must be valid JSON."),
    }
    _default_hint = ('dict', '{}')

    def __init__(self, encoder=None, decoder=None, default=dict, *args, **kwargs):
        if not builtin_connection.features.supports_json:
            raise NotSupportedError(_('JSONField is not supported by this database backend.'))
        self.encoder, self.decoder = encoder, decoder
        super().__init__(default=default, *args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        if self.default is dict:
            del kwargs['default']
        if self.encoder is not None:
            kwargs['encoder'] = self.encoder
        if self.decoder is not None:
            kwargs['decoder'] = self.decoder
        return name, path, args, kwargs

    def from_db_value(self, value, expression, connection):
        if value is None:
            return value
        elif connection.vendor == 'oracle' and value == '':
            return None
        else:
            try:
                return json.loads(value, cls=self.decoder)
            except json.decoder.JSONDecodeError:
                return value

    def get_internal_type(self):
        return 'JSONField'

    def get_prep_value(self, value):
        if value is None:
            return value
        return json.dumps(value, cls=self.encoder)

    def get_transform(self, name):
        transform = super().get_transform(name)
        if transform:
            return transform
        return KeyTransformFactory(name)

    def select_format(self, compiler, sql, params):
        if compiler.connection.vendor == 'postgresql':
            # Avoid psycopg2's automatic decoding to allow custom decoder
            return '%s::text' % sql, params
        return super().select_format(compiler, sql, params)

    def validate(self, value, model_instance):
        super().validate(value, model_instance)
        try:
            json.dumps(value, cls=self.encoder)
        except TypeError:
            raise exceptions.ValidationError(
                self.error_messages['invalid'],
                code='invalid',
                params={'value': value},
            )

    def value_to_string(self, obj):
        return self.value_from_object(obj)

    def formfield(self, **kwargs):
        return super().formfield(**{
            'form_class': forms.JSONField,
            'encoder': self.encoder,
            'decoder': self.decoder,
            **kwargs,
        })


class SimpleFunctionOperatorMixin(FieldGetDbPrepValueMixin):
    def as_sql_function(self, compiler, connection, template, flipped=False):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        params = lhs_params + rhs_params
        return template % ((lhs, rhs) if not flipped else (rhs, lhs)), params

    def as_sql_operator(self, compiler, connection, operator):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        params = lhs_params + rhs_params
        return '%s %s %s' % (lhs, operator, rhs), params

    def as_postgresql(self, compiler, connection):
        return self.as_sql_operator(compiler, connection, self.postgres_operator)

    def as_sql(self, compiler, connection):
        raise NotSupportedError(
            _('%s lookup is not supported by this database backend.' % self.lookup_name)
        )


class HasKeyLookup(SimpleFunctionOperatorMixin, Lookup):
    logical_operator = None

    def as_sql(self, compiler, connection, template=None):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs = [self.rhs] if not isinstance(self.rhs, (list, tuple)) else self.rhs
        rhs_params = ['$.%s' % json.dumps(key) for key in rhs]
        sql = template % lhs
        if self.logical_operator:
            # Add condition for each key.
            sql = self.logical_operator.join([sql] * len(rhs_params))
        return sql, lhs_params + rhs_params

    def as_mysql(self, compiler, connection):
        return self.as_sql(compiler, connection, template="JSON_CONTAINS_PATH(%s, 'one', %%s)")

    def as_oracle(self, compiler, connection):
        sql, params = self.as_sql(compiler, connection, template="JSON_EXISTS(%s, '%%s')")
        # Add paths directly into SQL because path expressions cannot be passed
        # as bind variables on Oracle.
        return sql % tuple(params), []

    def as_sqlite(self, compiler, connection):
        return self.as_sql(compiler, connection, template='JSON_TYPE(%s, %%s) IS NOT NULL')


@JSONField.register_lookup
class HasKey(HasKeyLookup):
    lookup_name = 'has_key'
    postgres_operator = '?'

    prepare_rhs = False


@JSONField.register_lookup
class HasAnyKeys(HasKeyLookup):
    lookup_name = 'has_any_keys'
    postgres_operator = '?|'
    logical_operator = ' OR '

    def get_prep_lookup(self):
        return [str(item) for item in self.rhs]


@JSONField.register_lookup
class HasKeys(HasAnyKeys):
    lookup_name = 'has_keys'
    postgres_operator = '?&'
    logical_operator = ' AND '


@JSONField.register_lookup
class DataContains(SimpleFunctionOperatorMixin, Lookup):
    lookup_name = 'contains'
    postgres_operator = '@>'

    def as_mysql(self, compiler, connection, flipped=False):
        return super().as_sql_function(
            compiler, connection, template="JSON_CONTAINS(%s, %s, '$')", flipped=flipped
        )

    def as_oracle(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs = json.loads(self.rhs)
        if isinstance(rhs, dict):
            if not rhs:
                return "DBMS_LOB.SUBSTR(%s) LIKE '{%%%%}'" % lhs, []
            conditions = []
            for key, value in rhs.items():
                k = json.dumps(key)
                if isinstance(value, (list, dict)):
                    func = 'JSON_QUERY'
                else:
                    func = 'JSON_VALUE'
                conditions.append(
                    "%s(%s, '$.%s') = %s('{\"val\": %s}', '$.val')" % (
                        func, lhs, k, func, json.dumps(value)
                    )
                )
            return ' AND '.join(conditions), []
        else:
            return 'DBMS_LOB.SUBSTR(%s) = %%s' % lhs, [self.rhs]

    def as_sqlite(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs = json.loads(self.rhs)
        if isinstance(rhs, dict):
            if not rhs:
                return "JSON_TYPE(%s, '$') = %%s" % lhs, ['object']
            conditions = []
            params = []
            for key, value in rhs.items():
                if value is None:
                    template = 'JSON_TYPE(%s, %%s) = %%s'
                    val = 'null'
                elif isinstance(value, (list, dict)):
                    template = 'JSON_EXTRACT(%s, %%s) = JSON(%%s)'
                    val = json.dumps(value)
                else:
                    template = 'JSON_EXTRACT(%s, %%s) = %%s'
                    val = value
                conditions.append(template % lhs)
                params.append('$.' + json.dumps(key))
                params.append(val)
            return ' AND '.join(conditions), params
        else:
            return '%s = %%s' % lhs, [self.rhs]


@JSONField.register_lookup
class ContainedBy(DataContains):
    lookup_name = 'contained_by'
    postgres_operator = '<@'

    def as_mysql(self, compiler, connection):
        return super().as_mysql(compiler, connection, flipped=True)


class JSONValue(Func):
    function = 'CAST'
    template = '%(function)s(%(expressions)s AS JSON)'

    def __init__(self, expression):
        super().__init__(Value(expression))


@JSONField.register_lookup
class JSONExact(lookups.Exact):
    can_use_none_as_rhs = True

    def process_lhs(self, compiler, connection):
        lhs, lhs_params = super().process_lhs(compiler, connection)
        if connection.vendor == 'sqlite':
            rhs, rhs_params = super().process_rhs(compiler, connection)
            if (rhs, rhs_params) == ('%s', [None]):
                # Need to use JSON_TYPE instead of JSON_EXTRACT
                # to determine JSON null values.
                previous = self.lhs
                while isinstance(previous, KeyTransform):
                    previous = previous.lhs
                lhs, params = compiler.compile(previous)
                if previous == self.lhs:  # No KeyTransform was applied
                    lhs = "JSON_TYPE(%s, '$')" % lhs
                else:
                    lhs = 'JSON_TYPE(%s, %%s)' % lhs
        return lhs, lhs_params

    def process_rhs(self, compiler, connection):
        rhs, rhs_params = super().process_rhs(compiler, connection)
        # Treat None lookup values as null.
        if (rhs, rhs_params) == ('%s', [None]):
            rhs, rhs_params = ('%s', ['null'])
        if connection.vendor == 'mysql':
            func = []
            for value in rhs_params:
                obj = json.loads(value)
                if connection.mysql_is_mariadb and isinstance(obj, str):
                    func.append("JSON_UNQUOTE(JSON_EXTRACT(%s, '$'))")
                else:
                    func.append("JSON_EXTRACT(%s, '$')")
            rhs = rhs % tuple(func)
        return rhs, rhs_params


class KeyTransform(Transform):
    postgres_operator = '->'
    postgres_nested_operator = '#>'

    def __init__(self, key_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key_name = key_name

    def _preprocess_lhs(self, compiler, connection):
        key_transforms = [self.key_name]
        previous = self.lhs
        while isinstance(previous, KeyTransform):
            key_transforms.insert(0, previous.key_name)
            previous = previous.lhs

        lhs, params = compiler.compile(previous)
        return lhs, params, key_transforms

    def as_mysql(self, compiler, connection):
        lhs, params, key_transforms = self._preprocess_lhs(compiler, connection)
        json_path = self.compile_json_path(key_transforms)
        return 'JSON_EXTRACT(%s, %%s)' % lhs, params + [json_path]

    def compile_json_path(self, key_transforms):
        path = ['$']
        for key_transform in key_transforms:
            try:
                num = int(key_transform)
                path.append('[{}]'.format(num))
            except ValueError:  # non-integer
                path.append('.')
                path.append(json.dumps(key_transform))
        return ''.join(path)

    def as_oracle(self, compiler, connection):
        lhs, params, key_transforms = self._preprocess_lhs(compiler, connection)
        json_path = self.compile_json_path(key_transforms)
        return "COALESCE(JSON_QUERY(%s, '%s'), JSON_VALUE(%s, '%s'))" % ((lhs, json_path) * 2), params

    def as_postgresql(self, compiler, connection):
        lhs, params, key_transforms = self._preprocess_lhs(compiler, connection)
        if len(key_transforms) > 1:
            return "(%s %s %%s)" % (lhs, self.postgres_nested_operator), [key_transforms] + params
        try:
            int(self.key_name)
        except ValueError:
            lookup = "'%s'" % self.key_name
        else:
            lookup = "%s" % self.key_name
        return "(%s %s %s)" % (lhs, self.postgres_operator, lookup), params

    def as_sqlite(self, compiler, connection):
        return self.as_mysql(compiler, connection)


class KeyTextTransform(KeyTransform):
    postgres_operator = '->>'
    postgres_nested_operator = '#>>'


class KeyTransformTextLookupMixin:
    """
    Mixin for combining with a lookup expecting a text lhs from a JSONField
    key lookup. Make use of the ->> operator instead of casting key values to
    text and performing the lookup on the resulting representation.
    """
    def __init__(self, key_transform, *args, **kwargs):
        assert isinstance(key_transform, KeyTransform)
        if builtin_connection.vendor == 'postgresql':
            KeyTextTransform.output_field = TextField()

        key_text_transform = KeyTextTransform(
            key_transform.key_name, *key_transform.source_expressions, **key_transform.extra
        )
        super().__init__(key_text_transform, *args, **kwargs)

    def process_lhs(self, compiler, connection):
        lhs, lhs_params = super().process_lhs(compiler, connection)
        if connection.vendor == 'mysql':
            return 'JSON_UNQUOTE(%s)' % lhs, lhs_params
        return lhs, lhs_params


class CaseInsensitiveMixin:
    def process_lhs(self, compiler, connection):
        lhs, lhs_params = super().process_lhs(compiler, connection)
        if connection.vendor == 'mysql':
            return 'LOWER(%s)' % lhs, lhs_params
        return lhs, lhs_params

    def process_rhs(self, compiler, connection):
        rhs, rhs_params = super().process_rhs(compiler, connection)
        if connection.vendor == 'mysql':
            return 'LOWER(%s)' % rhs, rhs_params
        return rhs, rhs_params


@KeyTransform.register_lookup
class KeyTransformExact(JSONExact):
    def process_rhs(self, compiler, connection):
        rhs, rhs_params = super().process_rhs(compiler, connection)
        if connection.vendor == 'oracle':
            func = []
            for value in rhs_params:
                val = json.loads(value)
                if isinstance(val, (list, dict)):
                    func.append("JSON_QUERY('{\"val\": %s}', '$.val')" % value)
                else:
                    func.append("JSON_VALUE('{\"val\": %s}', '$.val')" % value)
            rhs = rhs % tuple(func)
            rhs_params = []
        elif connection.vendor == 'sqlite':
            func = ["JSON_EXTRACT(%s, '$')" if value != 'null' else '%s' for value in rhs_params]
            rhs = rhs % tuple(func)
        return rhs, rhs_params


@KeyTransform.register_lookup
class KeyTransformIExact(CaseInsensitiveMixin, KeyTransformTextLookupMixin, lookups.IExact):
    pass


@KeyTransform.register_lookup
class KeyTransformIContains(CaseInsensitiveMixin, KeyTransformTextLookupMixin, lookups.IContains):
    pass


@KeyTransform.register_lookup
class KeyTransformContains(KeyTransformTextLookupMixin, lookups.Contains):
    pass


@KeyTransform.register_lookup
class KeyTransformStartsWith(KeyTransformTextLookupMixin, lookups.StartsWith):
    pass


@KeyTransform.register_lookup
class KeyTransformIStartsWith(CaseInsensitiveMixin, KeyTransformTextLookupMixin, lookups.IStartsWith):
    pass


@KeyTransform.register_lookup
class KeyTransformEndsWith(KeyTransformTextLookupMixin, lookups.EndsWith):
    pass


@KeyTransform.register_lookup
class KeyTransformIEndsWith(CaseInsensitiveMixin, KeyTransformTextLookupMixin, lookups.IEndsWith):
    pass


@KeyTransform.register_lookup
class KeyTransformRegex(KeyTransformTextLookupMixin, lookups.Regex):
    pass


@KeyTransform.register_lookup
class KeyTransformIRegex(CaseInsensitiveMixin, KeyTransformTextLookupMixin, lookups.IRegex):
    pass


@KeyTransform.register_lookup
class KeyTransformLte(KeyTransformTextLookupMixin, lookups.LessThanOrEqual):
    pass


@KeyTransform.register_lookup
class KeyTransformLt(KeyTransformTextLookupMixin, lookups.LessThan):
    pass


@KeyTransform.register_lookup
class KeyTransformGte(KeyTransformTextLookupMixin, lookups.GreaterThanOrEqual):
    pass


@KeyTransform.register_lookup
class KeyTransformGt(KeyTransformTextLookupMixin, lookups.GreaterThan):
    pass


class KeyTransformFactory:

    def __init__(self, key_name):
        self.key_name = key_name

    def __call__(self, *args, **kwargs):
        return KeyTransform(self.key_name, *args, **kwargs)
