import warnings

from django.db.models import JSONField as BuiltinJSONField
from django.utils.deprecation import RemovedInDjango40Warning

__all__ = ['JSONField']


class JSONField(BuiltinJSONField):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            'django.contrib.postgres.fields.JSONField is deprecated in favour of '
            'django.db.models.JSONField',
            RemovedInDjango40Warning, stacklevel=2
        )
        super().__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        path = 'django.contrib.postgres.fields.JSONField'
        return name, path, args, kwargs
