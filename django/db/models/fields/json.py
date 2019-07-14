import json

from django import forms
from django.core import exceptions
from django.db import connection as builtin_connection
from django.db.models.lookups import FieldGetDbPrepValueMixin, Lookup
from django.db.utils import NotSupportedError
from django.utils.translation import gettext_lazy as _

from . import Field
from .mixins import CheckFieldDefaultMixin


class JSONField(CheckFieldDefaultMixin, Field):
    description = _('A JSON object')
    default_error_messages = {
        'invalid': _("Value must be valid JSON."),
    }
    _default_hint = ('dict', '{}')

    def __init__(self, encoder=None, decoder=None, default=dict, *args, **kwargs):
        if not builtin_connection.features.supports_json:
            raise NotSupportedError(_('This database backend does not support JSONField.'))
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
            _('%s lookup is not supported on this database backend.' % self.lookup_name)
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
