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
        if connection.vendor == 'oracle' and value == '':
            return None
        return json.loads(value, cls=self.decoder)

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


class JSONLookup(FieldGetDbPrepValueMixin, Lookup):
    def as_postgresql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        params = lhs_params + rhs_params
        return '%s %s %s' % (lhs, self.postgresql_operator, rhs), params

    def as_sql(self, compiler, connection):
        raise NotSupportedError(
            _('%s lookup is not supported by this database backend.' % self.lookup_name)
        )


class HasKeyMixin(JSONLookup):
    mysql_template = "JSON_CONTAINS_PATH({}, '%s', {})"
    oracle_template = "JSON_EXISTS({}, '{}')"
    sqlite_template = "JSON_TYPE({}, %s) IS NOT NULL"
    _one_or_all = 'one'
    _logical_operator = ''

    def _process_paths(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        if isinstance(self.rhs, str):
            self.rhs = [self.rhs]
        paths = [
            '$.{}'.format(json.dumps(key_name))
            for key_name in self.rhs
        ]
        return lhs, lhs_params, paths

    def as_mysql(self, compiler, connection):
        lhs, lhs_params, paths = self._process_paths(compiler, connection)
        sql = (self.mysql_template % self._one_or_all).format(lhs, ', '.join('%s' for _ in paths))
        return sql, lhs_params + paths

    def as_oracle(self, compiler, connection):
        lhs, lhs_params, paths = self._process_paths(compiler, connection)
        template = (self.oracle_template.format(lhs, path) for path in paths)
        sql = '(%s)' % self._logical_operator.join(template)
        return sql, lhs_params

    def as_sqlite(self, compiler, connection):
        lhs, lhs_params, paths = self._process_paths(compiler, connection)
        template = (self.sqlite_template.format(lhs) for _ in paths)
        sql = '(%s)' % self._logical_operator.join(template)
        return sql, lhs_params + paths


@JSONField.register_lookup
class HasKey(HasKeyMixin):
    lookup_name = 'has_key'
    postgresql_operator = '?'

    prepare_rhs = False


@JSONField.register_lookup
class HasAnyKeys(HasKeyMixin):
    lookup_name = 'has_any_keys'
    postgresql_operator = '?|'
    _logical_operator = ' OR '

    def get_prep_lookup(self):
        return [str(item) for item in self.rhs]


@JSONField.register_lookup
class HasKeys(HasAnyKeys):
    lookup_name = 'has_keys'
    postgresql_operator = '?&'
    _logical_operator = ' AND '
    _one_or_all = 'all'


@JSONField.register_lookup
class DataContains(JSONLookup):
    lookup_name = 'contains'
    postgresql_operator = '@>'

    def as_mysql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        sql = "JSON_CONTAINS(%s, %s, '$')"
        params = lhs_params + rhs_params
        return sql % (lhs, rhs), params


@JSONField.register_lookup
class ContainedBy(JSONLookup):
    lookup_name = 'contained_by'
    postgresql_operator = '<@'

    def as_mysql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        sql = "JSON_CONTAINS(%s, %s, '$')"
        params = rhs_params + lhs_params
        return sql % (rhs, lhs), params


class JSONValue(Func):
    function = 'CAST'
    template = '%(function)s(%(expressions)s AS JSON)'

    def __init__(self, expression):
        super().__init__(Value(expression))


@JSONField.register_lookup
class JSONExact(lookups.Exact):
    can_use_none_as_rhs = True

    def process_rhs(self, compiler, connection):
        rhs, rhs_params = super().process_rhs(compiler, connection)
        # Treat None lookup values as null.
        if (rhs, rhs_params) == ('%s', [None]):
            rhs, rhs_params = ('%s', ['null'])
        elif connection.vendor == 'mysql':
            func = []
            for value in rhs_params:
                val = json.loads(value)
                if isinstance(val, (list, dict)):
                    func.append("JSON_EXTRACT(%s, '$')")
                else:
                    func.append("JSON_UNQUOTE(JSON_EXTRACT(%s, '$'))")
            rhs = rhs % tuple(func)
        return rhs, rhs_params


class KeyTransform(Transform):
    postgresql_operator = '->'
    postgresql_nested_operator = '#>'

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

    def as_postgresql(self, compiler, connection):
        lhs, params, key_transforms = self._preprocess_lhs(compiler, connection)
        if len(key_transforms) > 1:
            return "(%s %s %%s)" % (lhs, self.postgresql_nested_operator), [key_transforms] + params
        try:
            int(self.key_name)
        except ValueError:
            lookup = "'%s'" % self.key_name
        else:
            lookup = "%s" % self.key_name
        return "(%s %s %s)" % (lhs, self.postgresql_operator, lookup), params

    def as_mysql(self, compiler, connection):
        lhs, params, key_transforms = self._preprocess_lhs(compiler, connection)
        json_path = self.mysql_compile_json_path(key_transforms)
        return 'JSON_UNQUOTE(JSON_EXTRACT(%s, %%s))' % lhs, params + [json_path]

    def mysql_compile_json_path(self, key_transforms):
        path = ['$']
        for key_transform in key_transforms:
            try:
                num = int(key_transform)
                path.append('[{}]'.format(num))
            except ValueError:  # non-integer
                path.append('.')
                path.append(key_transform)
        return ''.join(path)


class KeyTextTransform(KeyTransform):
    postgresql_operator = '->>'
    postgresql_nested_operator = '#>>'
    output_field = TextField()


class KeyTransformTextLookupMixin:
    """
    Mixin for combining with a lookup expecting a text lhs from a JSONField
    key lookup. Make use of the ->> operator instead of casting key values to
    text and performing the lookup on the resulting representation.
    """
    def __init__(self, key_transform, *args, **kwargs):
        assert isinstance(key_transform, KeyTransform)
        key_text_transform = KeyTextTransform(
            key_transform.key_name, *key_transform.source_expressions, **key_transform.extra
        )
        super().__init__(key_text_transform, *args, **kwargs)


class CaseInsensitiveMixin:
    def process_lhs(self, compiler, connection):
        if connection.vendor == 'mysql':
            lhs, lhs_params = super().process_lhs(compiler, connection, lhs=None)
            return 'LOWER(%s)' % lhs, lhs_params
        return super().process_lhs(compiler, connection)

    def process_rhs(self, compiler, connection):
        if connection.vendor == 'mysql':
            rhs, rhs_params = super().process_rhs(compiler, connection)
            return 'LOWER(%s)' % rhs, rhs_params
        return super().process_rhs(compiler, connection)


@KeyTransform.register_lookup
class KeyTransformExact(JSONExact):
    def process_rhs(self, compiler, connection):
        rhs, rhs_params = super().process_rhs(compiler, connection)
        if connection.vendor == 'mysql':
            func_params = []
            new_params = []

            for param in rhs_params:
                val = json.loads(param)
                if isinstance(val, (list, dict)):
                    if not connection.mysql_is_mariadb:
                        func, this_func_param = JSONValue(param).as_sql(compiler, connection)
                        func_params.append(func)
                        new_params += this_func_param
                    else:
                        func_params.append('%s')
                        new_params.append(param)
                else:
                    if not connection.mysql_is_mariadb or val is None:
                        val = param
                    func_params.append('%s')
                    new_params.append(val)
            rhs, rhs_params = rhs % tuple(func_params), new_params
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
