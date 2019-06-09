import json
import uuid


class StrEncoder(json.JSONEncoder):
    def encode(self, obj):
        return str(obj)


class CustomDecoder(json.JSONDecoder):
    def __init__(self, object_hook=None, *args, **kwargs):
        return super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, dct):
        try:
            dct['uuid'] = uuid.UUID(dct['uuid'])
        except KeyError:
            pass
        return dct
