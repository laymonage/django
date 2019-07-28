from django.contrib.postgres.fields import JSONField as ModelJSONField
from django.contrib.postgres.forms import JSONField as FormJSONField
from django.test import SimpleTestCase
from django.utils.deprecation import RemovedInDjango40Warning


class JSONFieldTests(SimpleTestCase):
    def test_model_field_deprecation_message(self):
        msg = (
            'django.contrib.postgres.fields.JSONField is deprecated in favour of '
            'django.db.models.JSONField'
        )
        with self.assertWarnsMessage(RemovedInDjango40Warning, msg):
            ModelJSONField()

    def test_form_field_deprecation_message(self):
        msg = (
            'django.contrib.postgres.forms.JSONField is deprecated in favour of '
            'django.forms.JSONField'
        )
        with self.assertWarnsMessage(RemovedInDjango40Warning, msg):
            FormJSONField()
