import operator
import uuid
from unittest import skipIf

from tests.test_utils.json import CustomDecoder, StrEncoder

from django import forms
from django.core import serializers
from django.core.exceptions import ValidationError
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection, models, transaction
from django.db.models import Count, F, Q, Value
from django.db.models.expressions import RawSQL
from django.db.models.functions import Cast
from django.db.models.fields.json import KeyTextTransform, KeyTransform
from django.db.utils import DatabaseError, IntegrityError
from django.test import SimpleTestCase, TestCase, skipUnlessDBFeature
from django.test.utils import CaptureQueriesContext

from .models import JSONModel, NullableJSONModel, OrderedJSONModel


@skipIf(connection.vendor == 'oracle', 'Oracle does not support meta ordering.')
class TestModelMetaOrdering(TestCase):
    def test_ordering_by_json_field_value(self):
        OrderedJSONModel.objects.create(value={'b': 2})
        OrderedJSONModel.objects.create(value={'a': 3})
        objects = OrderedJSONModel.objects.all()
        self.assertEqual(objects[0].value, {'a': 3})
        self.assertEqual(objects[1].value, {'b': 2})


class TestDefaultValue(SimpleTestCase):
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
        with transaction.atomic(), self.assertRaises(TypeError) as ex:
            obj.save()
        self.assertIn('UUID', str(ex.exception))
        self.assertIn('is not JSON serializable', str(ex.exception))

    def test_custom_encoder_decoder(self):
        self._set_encoder_decoder(DjangoJSONEncoder, CustomDecoder)
        obj = JSONModel(value=self.uuid_value)
        obj.clean_fields()
        obj.save()
        obj.refresh_from_db()
        self.assertEqual(obj.value, self.uuid_value)

    def test_db_check_constraints(self):
        value = '{@!invalid json value 123 $!@#'
        self._set_encoder_decoder(StrEncoder, None)
        obj = JSONModel(value=value)
        with transaction.atomic(), self.assertRaises(DatabaseError):
            obj.save()


class TestModelFormField(SimpleTestCase):
    def test_formfield(self):
        model_field = models.JSONField()
        form_field = model_field.formfield()
        self.assertIsInstance(form_field, forms.JSONField)

    def test_formfield_custom_encoder_decoder(self):
        model_field = models.JSONField(encoder=DjangoJSONEncoder, decoder=CustomDecoder)
        form_field = model_field.formfield()
        self.assertIs(form_field.encoder, DjangoJSONEncoder)
        self.assertIs(form_field.decoder, CustomDecoder)


class TestSerialization(SimpleTestCase):
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
        # Oracle backend uses empty string instead of SQL NULL,
        # so it doesn't violate IS NOT NULL constraint.
        if connection.vendor == 'oracle':
            obj.save()
            obj.refresh_from_db()
            self.assertIsNone(obj.value)
        else:
            with transaction.atomic(), self.assertRaises(IntegrityError):
                obj.save()
        obj = NullableJSONModel.objects.create(value=None)
        obj.refresh_from_db()
        self.assertIsNone(obj.value)

    @skipIf(connection.vendor == 'oracle', 'Oracle does not support scalar values.')
    def test_json_null_different_from_sql_null(self):
        json_null = NullableJSONModel.objects.create(value=Value('null'))
        json_null.refresh_from_db()
        sql_null = NullableJSONModel.objects.create(value=None)
        sql_null.refresh_from_db()

        # They are different in the database ('null' vs NULL)
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value=Value('null')),
            [json_null]
        )
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value=None),
            [json_null]
        )
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__isnull=True),
            [sql_null]
        )
        # They are equal in Python (None)
        self.assertEqual(json_null.value, sql_null.value)

    @skipIf(connection.vendor == 'oracle', 'Oracle does not support scalar values.')
    def test_scalar_value(self):
        values = [
            Value('null'), True, False, 123456, 1234.56, 'A string', '',
        ]
        for value in values:
            with self.subTest(value=value):
                obj = JSONModel(value=value)
                obj.save()
                obj.refresh_from_db()
                if value == Value('null'):
                    value = None
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
                obj.refresh_from_db()
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
                obj.refresh_from_db()
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
        obj.refresh_from_db()
        self.assertEqual(obj.value, value)


class TestQuerying(TestCase):
    @classmethod
    def setUpTestData(cls):
        scalar_values = [None]
        if connection.vendor != 'oracle':
            scalar_values += [True, False, 'yes', 7, 9.6]
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

    @skipIf(connection.vendor == 'oracle', "Oracle does not support scalar values.")
    def test_contains_scalar(self):
        for data in self.scalar_data[1:]:
            with self.subTest(data=data):
                self.assertSequenceEqual(
                    NullableJSONModel.objects.filter(value__contains=data.value),
                    [data]
                )

    def test_contains_empty_dict(self):
        query = NullableJSONModel.objects.filter(value__contains={})
        self.assertSequenceEqual(
            query,
            self.object_data[1:4] + self.object_data[5:]
        )

    def test_contains_multiple(self):
        query = NullableJSONModel.objects.filter(value__contains={'k': True, 'l': False})
        self.assertSequenceEqual(
            query,
            [self.object_data[5]]
        )

    def test_contains_complex(self):
        query = NullableJSONModel.objects.filter(value__contains={'d': ['e', {'f': 'g'}]})
        self.assertSequenceEqual(
            query,
            [self.object_data[3]]
        )

    def test_contains_array(self):
        query = NullableJSONModel.objects.filter(value__contains=[1, [2]])
        self.assertSequenceEqual(
            query,
            [self.object_data[4]]
        )

    @skipIf(
        connection.vendor in ['oracle', 'sqlite'],
        "Oracle and SQLite do not support 'contained_by' lookup."
    )
    def test_contained_by(self):
        query = NullableJSONModel.objects.filter(value__contained_by={'a': 'b', 'c': 1, 'h': True})
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
        if connection.vendor == 'mysql' and connection.mysql_is_mariadb or connection.vendor == 'oracle':
            # MariaDB and Oracle use string representation of the JSON values to sort the objects.
            self.assertSequenceEqual(query, [objs[2], objs[4], objs[3], objs[1], objs[0]])
        else:
            self.assertSequenceEqual(query, [objs[4], objs[2], objs[3], objs[1], objs[0]])

    def test_ordering_grouping_by_key_transform(self):
        base_qs = NullableJSONModel.objects.filter(value__d__0__isnull=False)
        for qs in (
            base_qs.order_by('value__d__0'),
            base_qs.annotate(key=KeyTransform('0', KeyTransform('d', 'value'))).order_by('key'),
        ):
            self.assertSequenceEqual(qs, [self.object_data[3]])
        qs = NullableJSONModel.objects.filter(value__isnull=False)
        if connection.vendor != 'oracle':
            # Oracle doesn't support direct COUNT on LOB fields.
            self.assertQuerysetEqual(
                qs.values('value__d__0').annotate(count=Count('value__d__0')).order_by('count'),
                [1, 11],
                operator.itemgetter('count'),
            )
        self.assertQuerysetEqual(
            qs.filter(value__isnull=False).annotate(
                key=KeyTextTransform('f', KeyTransform('1', KeyTransform('d', 'value'))),
            ).values('key').annotate(count=Count('key')).order_by('count'),
            [(None, 0), ('g', 1)],
            operator.itemgetter('key', 'count'),
        )

    def test_key_transform_raw_expression(self):
        expr = RawSQL('%s::jsonb', ['{"x": "bar"}'])
        self.assertSequenceEqual(
            JSONModel.objects.filter(field__foo=KeyTransform('x', expr)),
            [self.objs[-1]],
        )

    def test_key_transform_expression(self):
        self.assertSequenceEqual(
            JSONModel.objects.filter(field__d__0__isnull=False).annotate(
                key=KeyTransform('d', 'field'),
                chain=KeyTransform('0', 'key'),
                expr=KeyTransform('0', Cast('key', models.JSONField())),
            ).filter(chain=F('expr')),
            [self.objs[8]],
        )

    def test_nested_key_transform_raw_expression(self):
        expr = RawSQL('%s::jsonb', ['{"x": {"y": "bar"}}'])
        self.assertSequenceEqual(
            JSONModel.objects.filter(field__foo=KeyTransform('y', KeyTransform('x', expr))),
            [self.objs[-1]],
        )

    def test_nested_key_transform_expression(self):
        self.assertSequenceEqual(
            JSONModel.objects.filter(field__d__0__isnull=False).annotate(
                key=KeyTransform('d', 'field'),
                chain=KeyTransform('f', KeyTransform('1', 'key')),
                expr=KeyTransform('f', KeyTransform('1', Cast('key', models.JSONField()))),
            ).filter(chain=F('expr')),
            [self.objs[8]],
        )

    def test_deep_values(self):
        query = NullableJSONModel.objects.values_list('value__k__l')
        if connection.vendor == 'oracle':
            self.assertSequenceEqual(
                query,
                [
                    (None,),
                    (None,), (None,), (None,), ('m',), (None,), (None,), (None,),
                ]
            )
        else:
            self.assertSequenceEqual(
                query,
                [
                    (None,), (None,), (None,), (None,), (None,), (None,),
                    (None,), (None,), (None,), ('m',), (None,), (None,), (None,),
                ]
            )

    @skipUnlessDBFeature('can_distinct_on_fields')
    def test_deep_distinct(self):
        query = NullableJSONModel.objects.distinct('value__k__l').values_list('value__k__l')
        self.assertSequenceEqual(query, [('m',), (None,)])

    def test_isnull_key(self):
        # key__isnull=False works the same as has_key='key'.
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__a__isnull=True),
            self.scalar_data + self.object_data[:2] + self.object_data[4:]
        )
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__a__isnull=False),
            [self.object_data[2], self.object_data[3]]
        )
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__j__isnull=False),
            [self.object_data[3]]
        )

    def test_none_key(self):
        self.assertSequenceEqual(NullableJSONModel.objects.filter(value__j=None), [self.object_data[3]])

    def test_none_key_exclude(self):
        obj = NullableJSONModel.objects.create(value={'j': 1})
        if connection.vendor == 'oracle':
            # On Oracle, the query returns JSON objects and arrays that do not have a 'null' value
            # at the specified path, including those that do not have the key.
            self.assertSequenceEqual(
                NullableJSONModel.objects.exclude(value__j=None),
                self.object_data[:3] + self.object_data[4:] + [obj]
            )
        else:
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
        self.assertIs(NullableJSONModel.objects.filter(value__foo__iexact='BaR').exists(), True)
        self.assertIs(NullableJSONModel.objects.filter(value__foo__iexact='"BaR"').exists(), False)

    def test_icontains(self):
        self.assertIs(NullableJSONModel.objects.filter(value__foo__icontains='"bar"').exists(), False)

    def test_startswith(self):
        self.assertIs(NullableJSONModel.objects.filter(value__foo__startswith='b').exists(), True)

    def test_istartswith(self):
        self.assertIs(NullableJSONModel.objects.filter(value__foo__istartswith='B').exists(), True)

    def test_endswith(self):
        self.assertIs(NullableJSONModel.objects.filter(value__foo__endswith='r').exists(), True)

    def test_iendswith(self):
        self.assertIs(NullableJSONModel.objects.filter(value__foo__iendswith='R').exists(), True)

    def test_regex(self):
        self.assertIs(NullableJSONModel.objects.filter(value__foo__regex=r'^bar$').exists(), True)

    def test_iregex(self):
        self.assertIs(NullableJSONModel.objects.filter(value__foo__iregex=r'^bAr$').exists(), True)

    def test_key_sql_injection(self):
        with CaptureQueriesContext(connection) as queries:
            query = NullableJSONModel.objects.filter(**{"""value__test' = '"a"') OR 1 = 1 OR ('d""": 'x', })
            if connection.vendor == 'oracle':
                with self.assertRaises(DatabaseError):
                    query.exists()
            else:
                self.assertIs(query.exists(), False)
        if connection.vendor == 'postgresql':
            self.assertIn(
                """."value" -> 'test'' = ''"a"'') OR 1 = 1 OR (''d') = '"x"' """,
                queries[0]['sql'],
            )
