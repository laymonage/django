import uuid

from tests.test_utils.json import CustomDecoder, StrEncoder

from django import forms
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection, models, transaction
from django.db.utils import DataError, IntegrityError, OperationalError
from django.test import TestCase

from .models import JSONModel


class JSONFieldTests(TestCase):

    def test_scalar_value(self):
        values = [
            True, False, 123456, 1234.56, 'A string'
        ]
        for value in values:
            with self.subTest(value=value):
                obj = JSONModel(value=value)
                # Oracle Database doesn't allow scalar values
                if connection.vendor == 'oracle':
                    with transaction.atomic():
                        self.assertRaises(IntegrityError, obj.save)
                else:
                    obj.save()
                    obj = JSONModel.objects.get(id=obj.id)
                    self.assertEqual(obj.value, value)

    def test_dict_value(self):
        value = {'name': 'John', 'age': 20, 'height': 180.3}
        obj = JSONModel.objects.create(value=value)
        obj = JSONModel.objects.get(id=obj.id)
        self.assertEqual(obj.value, value)

    def test_list_value(self):
        value = ['John', 20, 180.3]
        obj = JSONModel.objects.create(value=value)
        obj = JSONModel.objects.get(id=obj.id)
        self.assertEqual(obj.value, value)

    def test_nested_value(self):
        value = {
            'name': 'John',
            'age': 20,
            'pets': [
                {'name': 'Kit', 'type': 'cat', 'age': 2},
                {'name': 'Max', 'type': 'dog', 'age': 1}
            ],
            'courses': [
                ['A1', 'A2', 'A3'], ['B1', 'B2'], ['C1']
            ]
        }
        obj = JSONModel.objects.create(value=value)
        obj = JSONModel.objects.get(id=obj.id)
        self.assertEqual(obj.value, value)

    def test_mutable_default_value(self):
        mutable_values = [
            ['foo', 'bar', 123], {'foo': 'bar'},
        ]
        for value in mutable_values:
            with self.subTest(value=value):
                field = JSONModel._meta.get_field('value')
                field.default = value
                errors = field.check()
                self.assertEqual(len(errors), 1)
                self.assertIn('default should not be a mutable object', errors[0].msg)

    def test_valid_default_value(self):
        def callable_obj():
            return {'it': 'works'}

        valid_values = [
            callable_obj, True, 123, 123.45, 'foo bar', ('foo', 'bar', 123)
        ]
        for value in valid_values:
            with self.subTest(value=value):
                field = JSONModel._meta.get_field('value')
                field.default = value
                errors = field.check()
                self.assertEqual(len(errors), 0)

    def test_custom_encoder_decoder(self):
        value = {'uuid': uuid.UUID('{12345678-1234-5678-1234-567812345678}')}
        obj = JSONModel(value=value)
        with transaction.atomic():
            self.assertRaises(TypeError, obj.save)
        field = JSONModel._meta.get_field('value')
        field.encoder, field.decoder = DjangoJSONEncoder, CustomDecoder
        obj = JSONModel.objects.create(value=value)
        obj = JSONModel.objects.get(id=obj.id)
        self.assertEqual(obj.value, value)
        field.encoder, field.decoder = None, None

    def test_db_check_constraints(self):
        value = '{@!invalid json value 123 $!@#'
        field = JSONModel._meta.get_field('value')
        field.encoder = StrEncoder
        obj = JSONModel(value=value)
        with transaction.atomic():
            self.assertRaises((DataError, IntegrityError, OperationalError), obj.save)
        field.encoder, field.decoder = None, None

    def test_formfield(self):
        model_field = models.JSONField()
        form_field = model_field.formfield()
        self.assertIsInstance(form_field, forms.JSONField)

    def test_formfield_custom_encoder_decoder(self):
        model_field = models.JSONField(encoder=DjangoJSONEncoder, decoder=CustomDecoder)
        form_field = model_field.formfield()
        self.assertIs(form_field.encoder, DjangoJSONEncoder)
        self.assertIs(form_field.decoder, CustomDecoder)
