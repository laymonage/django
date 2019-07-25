from django.db.models import JSONField as BuiltinJSONField

__all__ = ['JSONField']


class JSONField(BuiltinJSONField):
    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        path = 'django.contrib.postgres.fields.JSONField'
        return name, path, args, kwargs
