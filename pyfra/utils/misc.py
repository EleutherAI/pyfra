import hashlib
import json
import os
import pyfra.remote


class _ObjectEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pyfra.remote.RemotePath):
            return obj.sha256sum()
        if hasattr(obj, "_to_json"):
            return obj._to_json()
        
        return super().default(obj)


def hash_obs(*args):
    jsonobj = json.dumps(args, sort_keys=True, cls=_ObjectEncoder)
    arghash = hashlib.sha256(jsonobj.encode()).hexdigest()
    return arghash
