import operator
import uuid
from unittest import mock, skipIf, skipUnless

from django import forms
from django.core import checks, serializers
from django.core.exceptions import ValidationError
from django.core.serializers.json import DjangoJSONEncoder
from django.db import (
    DataError, IntegrityError, NotSupportedError, OperationalError, connection,
    models,
)
from django.db.models import Count, F, OuterRef, Q, Subquery, Transform, Value
from django.db.models.expressions import RawSQL
from django.db.models.fields.json import (
    KeyTextTransform, KeyTransform, KeyTransformFactory,
    KeyTransformTextLookupMixin,
)
from django.db.models.functions import Cast
from django.db.utils import DatabaseError
from django.test import SimpleTestCase, TestCase, skipUnlessDBFeature
from django.test.utils import CaptureQueriesContext, isolate_apps

from .models import CustomJSONDecoder, JSONModel, NullableJSONModel


@skipUnlessDBFeature('supports_json_field')
class JSONFieldTests(TestCase):
    def test_invalid_value(self):
        msg = 'is not JSON serializable'
        with self.assertRaisesMessage(TypeError, msg):
            NullableJSONModel.objects.create(value={
                'uuid': uuid.UUID('d85e2076-b67c-4ee7-8c3a-2bf5a2cc2475'),
            })

    def test_custom_encoder_decoder(self):
        value = {'uuid': uuid.UUID('{d85e2076-b67c-4ee7-8c3a-2bf5a2cc2475}')}
        obj = NullableJSONModel(value_custom=value)
        obj.clean_fields()
        obj.save()
        obj.refresh_from_db()
        self.assertEqual(obj.value_custom, value)

    def test_db_check_constraints(self):
        value = '{@!invalid json value 123 $!@#'
        with mock.patch.object(DjangoJSONEncoder, 'encode', return_value=value):
            with self.assertRaises((IntegrityError, DataError, OperationalError)):
                NullableJSONModel.objects.create(value_custom=value)


class TestMethods(SimpleTestCase):
    def test_deconstruct(self):
        field = models.JSONField()
        name, path, args, kwargs = field.deconstruct()
        self.assertEqual(path, 'django.db.models.JSONField')
        self.assertEqual(args, [])
        self.assertEqual(kwargs, {})

    def test_deconstruct_custom_encoder_decoder(self):
        field = models.JSONField(encoder=DjangoJSONEncoder, decoder=CustomJSONDecoder)
        name, path, args, kwargs = field.deconstruct()
        self.assertEqual(kwargs['encoder'], DjangoJSONEncoder)
        self.assertEqual(kwargs['decoder'], CustomJSONDecoder)

    def test_get_transforms(self):
        @models.JSONField.register_lookup
        class MyTransform(Transform):
            lookup_name = 'my_transform'
        field = models.JSONField()
        transform = field.get_transform('my_transform')
        self.assertIs(transform, MyTransform)
        models.JSONField._unregister_lookup(MyTransform)
        models.JSONField._clear_cached_lookups()
        transform = field.get_transform('my_transform')
        self.assertIsInstance(transform, KeyTransformFactory)

    def test_key_transform_text_lookup_mixin_non_key_transform(self):
        transform = Transform('test')
        with self.assertRaisesMessage(
            TypeError,
            'Transform should be an instance of KeyTransform in order to use this lookup.'
        ):
            KeyTransformTextLookupMixin(transform)


class TestValidation(SimpleTestCase):
    def test_invalid_encoder(self):
        msg = 'The encoder parameter must be a callable object.'
        with self.assertRaisesMessage(ValueError, msg):
            models.JSONField(encoder=DjangoJSONEncoder())

    def test_invalid_decoder(self):
        msg = 'The decoder parameter must be a callable object.'
        with self.assertRaisesMessage(ValueError, msg):
            models.JSONField(decoder=CustomJSONDecoder())

    def test_validation_error(self):
        field = models.JSONField()
        msg = 'Value must be valid JSON.'
        value = uuid.UUID('{d85e2076-b67c-4ee7-8c3a-2bf5a2cc2475}')
        with self.assertRaisesMessage(ValidationError, msg):
            field.clean({'uuid': value}, None)

    def test_custom_encoder(self):
        field = models.JSONField(encoder=DjangoJSONEncoder)
        value = uuid.UUID('{d85e2076-b67c-4ee7-8c3a-2bf5a2cc2475}')
        field.clean({'uuid': value}, None)


class TestFormField(SimpleTestCase):
    def test_formfield(self):
        model_field = models.JSONField()
        form_field = model_field.formfield()
        self.assertIsInstance(form_field, forms.JSONField)

    def test_formfield_custom_encoder_decoder(self):
        model_field = models.JSONField(encoder=DjangoJSONEncoder, decoder=CustomJSONDecoder)
        form_field = model_field.formfield()
        self.assertIs(form_field.encoder, DjangoJSONEncoder)
        self.assertIs(form_field.decoder, CustomJSONDecoder)


@isolate_apps('model_fields')
@skipUnlessDBFeature('supports_json_field')
class TestChecks(TestCase):
    def test_invalid_default(self):
        class MyModel(models.Model):
            field = models.JSONField(default={})

        model = MyModel()
        self.assertEqual(model.check(), [
            checks.Warning(
                msg=(
                    "JSONField default should be a callable instead of an "
                    "instance so that it's not shared between all field "
                    "instances."
                ),
                hint='Use a callable instead, e.g., use `dict` instead of `{}`.',
                obj=MyModel._meta.get_field('field'),
                id='fields.E010',
            )
        ])

    def test_valid_default(self):
        class MyModel(models.Model):
            field = models.JSONField(default=dict)

        model = MyModel()
        self.assertEqual(model.check(), [])

    def test_valid_default_none(self):
        class MyModel(models.Model):
            field = models.JSONField(default=None)

        model = MyModel()
        self.assertEqual(model.check(), [])

    def test_valid_callable_default(self):
        def callable_default():
            return {'it': 'works'}

        class MyModel(models.Model):
            field = models.JSONField(default=callable_default)

        model = MyModel()
        self.assertEqual(model.check(), [])


class TestDatabaseChecks(TestCase):
    @isolate_apps('model_fields')
    def test_check_databases(self):
        class Model(models.Model):
            field = models.JSONField()

        error = checks.Error(
            '%s does not support JSONFields.' % connection.display_name,
            obj=Model,
            id='fields.E180',
        )
        expected = [] if connection.features.supports_json_field else [error]
        self.assertEqual(Model.check(databases=self.databases), expected)

    def test_required_db_features(self):
        self.assertEqual(JSONModel.check(databases=self.databases), [])


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


@skipUnlessDBFeature('supports_json_field')
class TestSaveLoad(TestCase):
    def test_null(self):
        obj = NullableJSONModel(value=None)
        obj.save()
        obj.refresh_from_db()
        self.assertIsNone(obj.value)

    @skipUnlessDBFeature('supports_primitives_in_json_field')
    def test_json_null_different_from_sql_null(self):
        json_null = NullableJSONModel.objects.create(value=Value('null'))
        json_null.refresh_from_db()
        sql_null = NullableJSONModel.objects.create(value=None)
        sql_null.refresh_from_db()

        # They are different in the database ('null' vs NULL).
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
        # They are equal in Python (None).
        self.assertEqual(json_null.value, sql_null.value)

    @skipUnlessDBFeature('supports_primitives_in_json_field')
    def test_primitives(self):
        values = [
            True,
            1,
            1.45,
            'String',
            '',
        ]
        for value in values:
            with self.subTest(value=value):
                obj = JSONModel(value=value)
                obj.save()
                obj.refresh_from_db()
                if value == Value('null'):
                    value = None
                self.assertEqual(obj.value, value)

    def test_dict(self):
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

    def test_list(self):
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

    def test_realistic_object(self):
        value = {
            'name': 'John',
            'age': 20,
            'pets': [
                {'name': 'Kit', 'type': 'cat', 'age': 2},
                {'name': 'Max', 'type': 'dog', 'age': 1},
            ],
            'courses': [
                ['A1', 'A2', 'A3'],
                ['B1', 'B2'],
                ['C1'],
            ],
        }
        obj = JSONModel.objects.create(value=value)
        obj.refresh_from_db()
        self.assertEqual(obj.value, value)


@skipUnlessDBFeature('supports_json_field')
class TestQuerying(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.primitives = [True, False, 'yes', 7, 9.6]
        values = [
            None,
            [],
            {},
            {'a': 'b', 'c': 14},
            {
                'a': 'b',
                'c': 14,
                'd': ['e', {'f': 'g'}],
                'h': True,
                'i': False,
                'j': None,
                'k': {'l': 'm'},
                'n': [None],
            },
            [1, [2]],
            {'k': True, 'l': False},
            {
                'foo': 'bar',
                'baz': {'a': 'b', 'c': 'd'},
                'bar': ['foo', 'bar'],
                'bax': {'foo': 'bar'},
            },
        ]
        cls.objs = [
            NullableJSONModel.objects.create(value=value)
            for value in values
        ]
        if connection.features.supports_primitives_in_json_field:
            cls.objs.extend([
                NullableJSONModel.objects.create(value=value)
                for value in cls.primitives
            ])

    def test_has_key_with_null_value(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__has_key='j'),
            [self.objs[4]]
        )

    def test_has_key(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__has_key='a'),
            [self.objs[3], self.objs[4]]
        )

    def test_has_keys(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__has_keys=['a', 'c', 'h']),
            [self.objs[4]]
        )

    def test_has_any_keys(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__has_any_keys=['c', 'l']),
            [self.objs[3], self.objs[4], self.objs[6]],
        )

    @skipUnlessDBFeature('supports_primitives_in_json_field')
    def test_contains_primitives(self):
        for value in self.primitives:
            with self.subTest(value=value):
                self.assertTrue(
                    NullableJSONModel.objects.filter(value__contains=value).exists()
                )

    def test_contains_dict(self):
        query = NullableJSONModel.objects.filter(
            value__contains={'baz': {'a': 'b', 'c': 'd'}},
        )
        self.assertSequenceEqual(query, [self.objs[7]])

    def test_contains_empty_dict(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__contains={}),
            self.objs[2:5] + self.objs[6:8],
        )

    def test_contains_multiple(self):
        query = NullableJSONModel.objects.filter(value__contains={'k': True, 'l': False})
        self.assertSequenceEqual(
            query,
            [self.objs[6]]
        )

    def test_contains_complex(self):
        query = NullableJSONModel.objects.filter(value__contains={'d': ['e', {'f': 'g'}]})
        self.assertSequenceEqual(
            query,
            [self.objs[4]]
        )

    def test_contains_array(self):
        query = NullableJSONModel.objects.filter(value__contains=[1, [2]])
        self.assertSequenceEqual(
            query,
            [self.objs[5]]
        )

    def test_contains_null_array(self):
        query = NullableJSONModel.objects.filter(value__contains={'n': [None]})
        self.assertSequenceEqual(query, [self.objs[4]])

    def test_contains_null(self):
        query = NullableJSONModel.objects.filter(value__contains={'j': None})
        self.assertSequenceEqual(query, [self.objs[4]])

    @skipIf(
        connection.vendor == 'oracle',
        "Oracle doesn't support contained_by lookup.",
    )
    def test_contained_by(self):
        query = NullableJSONModel.objects.filter(value__contained_by={'a': 'b', 'c': 14, 'h': True})
        self.assertSequenceEqual(query, self.objs[2:4])

    @skipUnless(
        connection.vendor == 'oracle',
        "Oracle doesn't support contained_by lookup.",
    )
    def test_contained_by_unsupported(self):
        msg = 'contained_by lookup is not supported on Oracle.'
        with self.assertRaisesMessage(NotSupportedError, msg):
            NullableJSONModel.objects.filter(value__contained_by={'a': 'b'}).get()

    def test_exact(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__exact={}),
            [self.objs[2]]
        )

    def test_exact_complex(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__exact={'a': 'b', 'c': 14}),
            [self.objs[3]]
        )

    def test_isnull(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__isnull=True),
            [self.objs[0]]
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
            self.assertSequenceEqual(qs, [self.objs[4]])
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
        if connection.vendor == 'postgresql':
            expr = RawSQL('%s::jsonb', ['{"x": "bar"}'])
        else:
            expr = RawSQL('%s', ['{"x": "bar"}'])
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__foo=KeyTransform('x', expr)),
            [self.objs[7]],
        )

    def test_key_transform_expression(self):
        if connection.vendor == 'oracle' or connection.vendor == 'mysql' and connection.mysql_is_mariadb:
            expr = 'key'
        else:
            expr = Cast('key', models.JSONField())
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__d__0__isnull=False).annotate(
                key=KeyTransform('d', 'value'),
                chain=KeyTransform('0', 'key'),
                expr=KeyTransform('0', expr),
            ).filter(chain=F('expr')),
            [self.objs[4]],
        )

    def test_nested_key_transform_raw_expression(self):
        if connection.vendor == 'postgresql':
            expr = RawSQL('%s::jsonb', ['{"x": {"y": "bar"}}'])
        else:
            expr = RawSQL('%s', ['{"x": {"y": "bar"}}'])
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__foo=KeyTransform('y', KeyTransform('x', expr))),
            [self.objs[7]],
        )

    def test_nested_key_transform_expression(self):
        if connection.vendor == 'oracle' or connection.vendor == 'mysql' and connection.mysql_is_mariadb:
            expr = 'key'
        else:
            expr = Cast('key', models.JSONField())
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__d__0__isnull=False).annotate(
                key=KeyTransform('d', 'value'),
                chain=KeyTransform('f', KeyTransform('1', 'key')),
                expr=KeyTransform('f', KeyTransform('1', expr)),
            ).filter(chain=F('expr')),
            [self.objs[4]],
        )

    def test_deep_values(self):
        qs = NullableJSONModel.objects.values_list('value__k__l')
        expected_objs = [(None,)] * len(self.objs)
        expected_objs[4] = ('m',)
        self.assertSequenceEqual(qs, expected_objs)

    @skipUnlessDBFeature('can_distinct_on_fields')
    def test_deep_distinct(self):
        query = NullableJSONModel.objects.distinct('value__k__l').values_list('value__k__l')
        self.assertSequenceEqual(query, [('m',), (None,)])

    def test_isnull_key(self):
        # key__isnull=False works the same as has_key='key'.
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__a__isnull=True),
            self.objs[:3] + self.objs[5:]
        )
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__a__isnull=False),
            [self.objs[3], self.objs[4]]
        )
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__j__isnull=False),
            [self.objs[4]]
        )

    def test_none_key(self):
        self.assertSequenceEqual(NullableJSONModel.objects.filter(value__j=None), [self.objs[4]])

    def test_none_key_exclude(self):
        obj = NullableJSONModel.objects.create(value={'j': 1})
        if connection.vendor == 'oracle':
            # On Oracle, the query returns JSON objects and arrays that do not have a 'null' value
            # at the specified path, including those that do not have the key.
            self.assertSequenceEqual(
                NullableJSONModel.objects.exclude(value__j=None),
                self.objs[1:4] + self.objs[5:] + [obj]
            )
        else:
            self.assertSequenceEqual(NullableJSONModel.objects.exclude(value__j=None), [obj])

    def test_isnull_key_or_none(self):
        obj = NullableJSONModel.objects.create(value={'a': None})
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(Q(value__a__isnull=True) | Q(value__a=None)),
            self.objs[:3] + self.objs[5:] + [obj]
        )

    def test_shallow_list_lookup(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__0=1),
            [self.objs[5]]
        )

    def test_shallow_obj_lookup(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__a='b'),
            [self.objs[3], self.objs[4]]
        )

    def test_obj_subquery_lookup(self):
        qs = NullableJSONModel.objects.annotate(
            field=Subquery(NullableJSONModel.objects.filter(pk=OuterRef('pk')).values('value')),
        ).filter(field__a='b')
        self.assertSequenceEqual(qs, [self.objs[3], self.objs[4]])

    def test_deep_lookup_objs(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__k__l='m'),
            [self.objs[4]]
        )

    def test_shallow_lookup_obj_target(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__k={'l': 'm'}),
            [self.objs[4]]
        )

    def test_deep_lookup_array(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__1__0=2),
            [self.objs[5]]
        )

    def test_deep_lookup_mixed(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__d__1__f='g'),
            [self.objs[4]]
        )

    def test_deep_lookup_transform(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__c__gt=2),
            [self.objs[3], self.objs[4]]
        )
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__c__lt=5),
            []
        )

    def test_usage_in_subquery(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(id__in=NullableJSONModel.objects.filter(value__c=14)),
            self.objs[3:5]
        )

    def test_iexact(self):
        self.assertTrue(NullableJSONModel.objects.filter(value__foo__iexact='BaR').exists())
        self.assertFalse(NullableJSONModel.objects.filter(value__foo__iexact='"BaR"').exists())

    def test_contains(self):
        self.assertTrue(NullableJSONModel.objects.filter(value__foo__contains='ar').exists())

    def test_icontains(self):
        self.assertTrue(NullableJSONModel.objects.filter(value__foo__icontains='A').exists())

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
            query = NullableJSONModel.objects.filter(**{"""value__test' = '"a"') OR 1 = 1 OR ('d""": 'x', })
            if connection.vendor == 'oracle':
                with self.assertRaises(DatabaseError):
                    query.exists()
            else:
                self.assertFalse(query.exists())
        if connection.vendor == 'postgresql':
            self.assertIn(
                """."value" -> 'test'' = ''"a"'') OR 1 = 1 OR (''d') = '"x"' """,
                queries[0]['sql'],
            )

    def test_key_escape(self):
        obj = NullableJSONModel.objects.create(value={'%total': 10})
        self.assertEqual(NullableJSONModel.objects.filter(**{'value__%total': 10}).get(), obj)

    def test_none_key_and_exact_lookup(self):
        self.assertSequenceEqual(
            NullableJSONModel.objects.filter(value__a='b', value__j=None), [self.objs[4]]
        )

    def test_lookups_with_key_transform(self):
        tests = (
            ('value__d__contains', 'e'),
            ('value__baz__has_key', 'c'),
            ('value__baz__has_keys', ['a', 'c']),
            ('value__baz__has_any_keys', ['a', 'x']),
            ('value__contains', KeyTransform('bax', 'value')),
            ('value__has_key', KeyTextTransform('foo', 'value')),
        )
        # contained_by lookup is not supported on Oracle.
        if connection.vendor != 'oracle':
            sql = '%s::jsonb' if connection.vendor == 'postgresql' else '%s'
            tests += (
                ('value__baz__contained_by', {'a': 'b', 'c': 'd', 'e': 'f'}),
                (
                    'value__contained_by',
                    KeyTransform('x', RawSQL(
                        sql,
                        ['{"x": {"a": "b", "c": 1, "d": "e"}}'],
                    )),
                ),
            )
        for lookup, value in tests:
            with self.subTest(lookup=lookup):
                self.assertTrue(NullableJSONModel.objects.filter(
                    **{lookup: value},
                ).exists())
