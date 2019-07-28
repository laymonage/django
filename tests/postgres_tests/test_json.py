from django.contrib.postgres.fields import JSONField as ModelJSONField
from django.contrib.postgres.forms import JSONField as FormJSONField
from django.utils.deprecation import RemovedInDjango40Warning

from . import PostgreSQLSimpleTestCase


class JSONFieldTests(PostgreSQLSimpleTestCase):
    def test_model_field_deprecation_message(self):
        msg = (
            'django.contrib.postgres.fields.JSONField is deprecated in favor of '
            'django.db.models.JSONField'
        )
        with self.assertWarnsMessage(RemovedInDjango40Warning, msg):
            ModelJSONField()

    def test_form_field_deprecation_message(self):
        msg = (
            'django.contrib.postgres.forms.JSONField is deprecated in favor of '
            'django.forms.JSONField'
        )
        with self.assertWarnsMessage(RemovedInDjango40Warning, msg):
            FormJSONField()
