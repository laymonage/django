import json

from django import forms
from django.core import exceptions
from django.db import connection as builtin_connection
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

    def to_python(self, value):
        if value is None or isinstance(
            value, (bool, int, float, dict, list)
        ):
            return value
        try:
            return json.loads(value, cls=self.decoder)
        except TypeError:
            raise exceptions.ValidationError(
                self.error_messages['invalid'],
                code='invalid',
                params={'value': value},
            )

    def validate(self, value, model_instance):
        super().validate(value, model_instance)
        self.to_python(value)

    def value_to_string(self, obj):
        value = self.value_from_object(obj)
        return self.get_prep_value(value)

    def formfield(self, **kwargs):
        return super().formfield(**{
            'form_class': forms.JSONField,
            'encoder': self.encoder,
            'decoder': self.decoder,
            **kwargs,
        })
