import uuid

from tests.test_utils.json import CustomDecoder, StrEncoder

from django import forms
from django.core.exceptions import ValidationError
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection, models, transaction
from django.db.utils import DatabaseError, IntegrityError
from django.test import TestCase

from .models import JSONModel, NullableJSONModel, OrderedJSONModel


class TestModelMetaOrdering(TestCase):
    def test_ordering_by_json_field_value(self):
        OrderedJSONModel.objects.create(value={'b': 2})
        OrderedJSONModel.objects.create(value={'a': 3})
        objects = OrderedJSONModel.objects.all()

        if connection.vendor == 'oracle':
            with transaction.atomic(), self.assertRaises(DatabaseError):
                objects[0].value
        else:
            self.assertEqual(objects[0].value, {'a': 3})
            self.assertEqual(objects[1].value, {'b': 2})


class TestDefaultValue(TestCase):
    def _set_default(self, value):
        field = JSONModel._meta.get_field('value')
        field.default = value
        return field.check()

    def tearDown(self):
        self._set_default(dict)
        return super().tearDown()

    def test_mutable_default_value(self):
        mutable_values = [
            ['foo', 'bar', 123], {'foo': 'bar'},
        ]
        for value in mutable_values:
            with self.subTest(value=value):
                errors = self._set_default(value)
                self.assertEqual(len(errors), 1)
                self.assertIn('default should not be a mutable object', errors[0].msg)

    def test_valid_default_value(self):
        def callable_obj():
            return {'it': 'works'}

        valid_values = [
            None, True, 123, 123.45, 'foo bar', ('foo', 'bar', 123), callable_obj,
        ]
        for value in valid_values:
            with self.subTest(value=value):
                errors = self._set_default(value)
                self.assertEqual(len(errors), 0)


class TestValidation(TestCase):
    def _set_encoder_decoder(self, encoder, decoder):
        field = JSONModel._meta.get_field('value')
        field.encoder, field.decoder = encoder, decoder
        return field.check()

    @classmethod
    def setUpTestData(cls):
        cls.uuid_value = {'uuid': uuid.UUID('{12345678-1234-5678-1234-567812345678}')}

    def tearDown(self):
        self._set_encoder_decoder(None, None)
        return super().tearDown()

    def test_validation_error(self):
        field = models.JSONField()
        with self.assertRaises(ValidationError) as err:
            field.clean(self.uuid_value, None)
        self.assertEqual(err.exception.code, 'invalid')
        self.assertEqual(err.exception.message % err.exception.params, 'Value must be valid JSON.')

    def test_not_serializable(self):
        obj = JSONModel(value=self.uuid_value)
        with transaction.atomic():
            self.assertRaises(TypeError, obj.save)

    def test_custom_encoder_decoder(self):
        self._set_encoder_decoder(DjangoJSONEncoder, CustomDecoder)
        obj = JSONModel(value=self.uuid_value)
        obj.clean_fields()
        obj.save()
        obj = JSONModel.objects.get(id=obj.id)
        self.assertEqual(obj.value, self.uuid_value)

    def test_db_check_constraints(self):
        value = '{@!invalid json value 123 $!@#'
        self._set_encoder_decoder(StrEncoder, None)
        obj = JSONModel(value=value)
        with transaction.atomic():
            self.assertRaises(DatabaseError, obj.save)


class TestModelFormField(TestCase):
    def test_formfield(self):
        model_field = models.JSONField()
        form_field = model_field.formfield()
        self.assertIsInstance(form_field, forms.JSONField)

    def test_formfield_custom_encoder_decoder(self):
        model_field = models.JSONField(encoder=DjangoJSONEncoder, decoder=CustomDecoder)
        form_field = model_field.formfield()
        self.assertIs(form_field.encoder, DjangoJSONEncoder)
        self.assertIs(form_field.decoder, CustomDecoder)


class TestSaveLoad(TestCase):
    def test_none_value(self):
        obj = JSONModel(value=None)
        # Oracle backend uses empty string instead of null
        if connection.vendor == 'oracle':
            obj.save()
            obj = JSONModel.objects.get(id=obj.id)
            self.assertIsNone(obj.value)
        else:
            with transaction.atomic():
                self.assertRaises(IntegrityError, obj.save)
        obj = NullableJSONModel.objects.create(value=None)
        obj = NullableJSONModel.objects.get(id=obj.id)
        self.assertIsNone(obj.value)

    def test_scalar_value(self):
        values = [
            True, False, 123456, 1234.56, 'A string', '',
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
        values = [
            {},
            {'name': 'John', 'age': 20, 'height': 180.3},
            {'a': True, 'b': {'b1': False, 'b2': None}},
        ]
        for value in values:
            with self.subTest(value=value):
                obj = JSONModel.objects.create(value=value)
                obj = JSONModel.objects.get(id=obj.id)
                self.assertEqual(obj.value, value)

    def test_list_value(self):
        values = [
            [],
            ['John', 20, 180.3],
            [True, [False, None]],
        ]
        for value in values:
            with self.subTest(value=value):
                obj = JSONModel.objects.create(value=value)
                obj = JSONModel.objects.get(id=obj.id)
                self.assertEqual(obj.value, value)

    def test_realistic_object_value(self):
        value = {
            'name': 'John',
            'age': 20,
            'pets': [
                {'name': 'Kit', 'type': 'cat', 'age': 2},
                {'name': 'Max', 'type': 'dog', 'age': 1}
            ],
            'courses': [
                ['A1', 'A2', 'A3'], ['B1', 'B2'], ['C1']
            ],
        }
        obj = JSONModel.objects.create(value=value)
        obj = JSONModel.objects.get(id=obj.id)
        self.assertEqual(obj.value, value)
