import operator
import uuid

from tests.test_utils.json import CustomDecoder, StrEncoder

from django import forms
from django.core import serializers
from django.core.exceptions import ValidationError
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection, models, transaction
from django.db.models import Count, Q
from django.db.models.fields.json import KeyTextTransform, KeyTransform
from django.db.utils import DatabaseError, IntegrityError, NotSupportedError
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


class TestSerialization(TestCase):
    test_data = (
        '[{"fields": {"value": %s}, '
        '"model": "model_fields.jsonmodel", "pk": null}]'
    )
    test_values = (
        # (Python value, serialized value),
        ({'a': 'b', 'c': None}, '{"a": "b", "c": null}'),
        ('abc', '"abc"'),
        ('{"a": "a"}', '"{\\"a\\": \\"a\\"}"'),
    )

    def test_dumping(self):
        for value, serialized in self.test_values:
            with self.subTest(value=value):
                instance = JSONModel(value=value)
                data = serializers.serialize('json', [instance])
                self.assertJSONEqual(data, self.test_data % serialized)

    def test_loading(self):
        for value, serialized in self.test_values:
            with self.subTest(value=value):
                instance = list(
                    serializers.deserialize('json', self.test_data % serialized)
                )[0].object
                self.assertEqual(instance.value, value)


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


class TestQuerying(TestCase):
    @classmethod
    def setUpTestData(cls):
        scalar_values = [None] if connection.vendor == 'oracle' else [
            None, True, False, 'yes', 7,
        ]
        object_values = [
            [], {},
            {'a': 'b', 'c': 1},
            {
                'a': 'b',
                'c': 1,
                'd': ['e', {'f': 'g'}],
                'h': True,
                'i': False,
                'j': None,
                'k': {'l': 'm'},
            },
            [1, [2]],
            {'k': True, 'l': False},
            {'foo': 'bar'},
        ]
        cls.scalar_data = [
            NullableJSONModel.objects.create(value=value)
            for value in scalar_values
        ]
        cls.object_data = [
            NullableJSONModel.objects.create(value=value)
            for value in object_values
        ]

    def test_has_key_with_null_value(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__has_key='j'),
            [self.object_data[3]]
        )

    def test_has_key(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__has_key='a'),
            [self.object_data[2], self.object_data[3]]
        )

    def test_has_keys(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__has_keys=['a', 'c', 'h']),
            [self.object_data[3]]
        )

    def test_has_any_keys(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__has_any_keys=['c', 'l']),
            [self.object_data[2], self.object_data[3], self.object_data[5]]
        )

    def test_contains(self):
        query = NullableJSONModel.objects.filter(value__contains={'a': 'b'})
        if connection.vendor in ['oracle', 'sqlite']:
            self.assertRaises(NotSupportedError, query.exists)
        else:
            self.assertSequenceEqual(
                query,
                [self.object_data[2], self.object_data[3]]
            )

    def test_contained_by(self):
        query = NullableJSONModel.objects.filter(value__contained_by={'a': 'b', 'c': 1, 'h': True})
        if connection.vendor in ['oracle', 'sqlite']:
            self.assertRaises(NotSupportedError, query.exists)
        else:
            self.assertSequenceEqual(
                query,
                [self.object_data[1], self.object_data[2]]
            )

    def test_exact(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__exact={}),
            [self.object_data[1]]
        )

    def test_exact_complex(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__exact={'a': 'b', 'c': 1}),
            [self.object_data[2]]
        )

    def test_isnull(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__isnull=True),
            [self.scalar_data[0]]
        )

    def test_ordering_by_transform(self):
        objs = [
            NullableJSONModel.objects.create(value={'ord': 93, 'name': 'bar'}),
            NullableJSONModel.objects.create(value={'ord': 22.1, 'name': 'foo'}),
            NullableJSONModel.objects.create(value={'ord': -1, 'name': 'baz'}),
            NullableJSONModel.objects.create(value={'ord': 21.931902, 'name': 'spam'}),
            NullableJSONModel.objects.create(value={'ord': -100291029, 'name': 'eggs'}),
        ]
        query = NullableJSONModel.objects.filter(value__name__isnull=False).order_by('value__ord')
        self.assertSequenceEqual(query, [objs[4], objs[2], objs[3], objs[1], objs[0]])

    def test_ordering_grouping_by_key_transform(self):
        base_qs = NullableJSONModel.objects.filter(value__d__0__isnull=False)
        for qs in (
            base_qs.order_by('value__d__0'),
            base_qs.annotate(key=KeyTransform('0', KeyTransform('d', 'value'))).order_by('key'),
        ):
            self.assertSequenceEqual(qs, [self.object_data[3]])
        qs = NullableJSONModel.objects.filter(value__isnull=False)
        self.assertQuerysetEqual(
            qs.values('value__d__0').annotate(count=Count('value__d__0')).order_by('count'),
            [1, 10],
            operator.itemgetter('count'),
        )
        self.assertQuerysetEqual(
            qs.filter(value__isnull=False).annotate(
                key=KeyTextTransform('f', KeyTransform('1', KeyTransform('d', 'value'))),
            ).values('key').annotate(count=Count('key')).order_by('count'),
            [(None, 0), ('g', 1)],
            operator.itemgetter('key', 'count'),
        )

    def test_deep_values(self):
        query = NullableJSONModel.objects.values_list('value__k__l')
        self.assertSequenceEqual(
            query,
            [
                (None,), (None,), (None,), (None,), (None,), (None,),
                (None,), (None,), ('m',), (None,), (None,), (None,),
            ]
        )

    def test_deep_distinct(self):
        query = NullableJSONModel.objects.distinct('value__k__l').values_list('value__k__l')
        self.assertSequenceEqual(query, [('m',), (None,)])

    def test_isnull_key(self):
        # key__isnull works the same as has_key='key'.
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__a__isnull=True),
            self.scalar_data + self.object_data[:2] + self.object_data[4:]
        )
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__a__isnull=False),
            [self.object_data[2], self.object_data[3]]
        )

    def test_none_key(self):
        self.assertSequenceEqual(NullableJSONModel.objects.filter(value__j=None), [self.object_data[3]])

    def test_none_key_exclude(self):
        obj = NullableJSONModel.objects.create(value={'j': 1})
        self.assertSequenceEqual(NullableJSONModel.objects.exclude(value__j=None), [obj])

    def test_isnull_key_or_none(self):
        obj = NullableJSONModel.objects.create(value={'a': None})
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(Q(value__a__isnull=True) | Q(value__a=None)),
            self.scalar_data + self.object_data[:2] + self.object_data[4:] + [obj]
        )

    def test_shallow_list_lookup(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__0=1),
            [self.object_data[4]]
        )

    def test_shallow_obj_lookup(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__a='b'),
            [self.object_data[2], self.object_data[3]]
        )

    def test_deep_lookup_objs(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__k__l='m'),
            [self.object_data[3]]
        )

    def test_shallow_lookup_obj_target(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__k={'l': 'm'}),
            [self.object_data[3]]
        )

    def test_deep_lookup_array(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__1__0=2),
            [self.object_data[4]]
        )

    def test_deep_lookup_mixed(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__d__1__f='g'),
            [self.object_data[3]]
        )

    def test_deep_lookup_transform(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__c__gt=1),
            []
        )
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__c__lt=5),
            [self.object_data[2], self.object_data[3]]
        )

    def test_usage_in_subquery(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(id__in=NullableJSONModel.objects.filter(value__c=1)),
            self.object_data[2:4]
        )

    def test_iexact(self):
        self.assertTrue(NullableJSONModel.objects.filter(value__foo__iexact='BaR').exists())
        self.assertFalse(NullableJSONModel.objects.filter(value__foo__iexact='"BaR"').exists())

    def test_icontains(self):
        self.assertFalse(NullableJSONModel.objects.filter(value__foo__icontains='"bar"').exists())

    def test_startswith(self):
        self.assertTrue(NullableJSONModel.objects.filter(value__foo__startswith='b').exists())

    def test_istartswith(self):
        self.assertTrue(NullableJSONModel.objects.filter(value__foo__istartswith='B').exists())

    def test_endswith(self):
        self.assertTrue(NullableJSONModel.objects.filter(value__foo__endswith='r').exists())

    def test_iendswith(self):
        self.assertTrue(NullableJSONModel.objects.filter(value__foo__iendswith='R').exists())

    def test_regex(self):
        self.assertTrue(NullableJSONModel.objects.filter(value__foo__regex=r'^bar$').exists())

    def test_iregex(self):
        self.assertTrue(NullableJSONModel.objects.filter(value__foo__iregex=r'^bAr$').exists())

    def test_key_sql_injection(self):
        with CaptureQueriesContext(connection) as queries:
            self.assertFalse(
                JSONModel.objects.filter(**{
                    """field__test' = '"a"') OR 1 = 1 OR ('d""": 'x',
                }).exists()
            )
        self.assertIn(
            """."field" -> 'test'' = ''"a"'') OR 1 = 1 OR (''d') = '"x"' """,
            queries[0]['sql'],
        )
