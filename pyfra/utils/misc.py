from sqlitedict import SqliteDict
import hashlib
import json
import os


os.makedirs('state', exist_ok=True)
state = SqliteDict("state/main.db", autocommit=True)


def once(fn, name=None):
    """ Only run a function once, saving its return value to disk. Args must be json-encodable. """

    fname = name if name is not None else fn.__name__

    def _fn(*args, **kwargs):
        # hash the arguments
        arghash = hashlib.sha256(json.dumps([args, kwargs], sort_keys=True).encode()).hexdigest()

        key = f"once-{fname}-{arghash}-seen"
        if key in state: return state[key]
        
        ret = fn(*args, **kwargs)
        state[key] = ret
        return ret

    return _fn