from functools import partial, wraps
from typing import Any, Callable, Dict, Type
import pyfra.remote
import abc
import os

try:
    import blobfile as bf
except ImportError:
    pass

import pickle
import inspect


class KVStoreProvider(abc.ABC):
    @abc.abstractmethod
    def get(self, key: str):
        """
        Get the value for a key.
        """
        pass

    @abc.abstractmethod
    def set(self, key: str, value):
        """
        Set the value for a key.
        """
        pass


class LocalKVStore(KVStoreProvider):
    def __init__(self):
        self.rem = pyfra.remote.Remote(wd=os.path.expanduser("~"))

    def get(self, key: str):
        return self.rem.get_kv(key)
    
    def set(self, key: str, value):
        self.rem.set_kv(key, value)


class BlobfileKVStore(KVStoreProvider):
    def __init__(self, prefix):
        if prefix[-1] == "/":
            prefix = prefix[:-1]
        self.prefix = prefix
    
    def get(self, key: str):
        try:
            return pickle.load(bf.BlobFile(self.prefix + "/" + key))
        except FileNotFoundError:
            raise KeyError(key)
    
    def set(self, key: str, value):
        with bf.BlobFile(self.prefix + "/" + key) as f:
            pickle.dump(value, f)


kvstore = LocalKVStore()
special_hashing: Dict[Type, Callable[[Any], str]] = {}

# some pyfra special hashing stuff
special_hashing[pyfra.remote.RemotePath] = lambda x: x.quick_hash()
special_hashing[pyfra.remote.Remote] = lambda x: x.hash


def set_kvstore(provider):
    global kvstore
    kvstore = provider


def _prepare_for_hash(x):
    for type, fn in special_hashing.items():
        if isinstance(x, type):
            return fn(x)

    return x


def update_source_cache(fname, lineno, new_name):
    with open(fname, "r") as f:
        file_lines = f.read().split("\n")

    # line numbering is 1-indexed
    lineno -= 1

    assert file_lines[lineno] in ["@cache", "@cache()"], "@cache can only be used as a decorator!"

    file_lines[lineno] = f"@cache('{new_name}')"

    with open(fname, "w") as f:
        f.write("\n".join(file_lines))


def cache(name=None):
    def wrapper(fn, name):
        if name is None:
            name = pyfra.remote._hash_obs(fn.__module__, fn.__name__, inspect.getsource(fn))[:16] + "_v0"
            # the decorator part of the stack is always the same size because we only get here if name is None
            stack_original_function = inspect.stack()[2]
            update_source_cache(stack_original_function.filename, stack_original_function.lineno - 1, name)

        @wraps(fn)
        def _fn(*args, **kwargs):
            overall_input_hash = pyfra.remote._hash_obs(
                name,
                [_prepare_for_hash(i) for i in range(len(args))],
                [_prepare_for_hash(k) for k in sorted(kwargs.keys())],
            )

            try:
                ret, awaitable = kvstore.get(overall_input_hash)
                ## ASYNC HANDLING, resume from file
                if awaitable:
                    async def _wrapper(ret):
                        # wrap ret in a dummy async function
                        return ret
                    
                    return _wrapper(ret)
                else:
                    return ret
            except KeyError:
                ret = fn(*args, **kwargs)

                ## ASYNC HANDLING, first run
                if inspect.isawaitable(ret):
                    # WARNING: using the same env across multiple async stages is not supported, and not possible to support!
                    # TODO: detect if two async stages are using the same env and throw an error if so

                    async def _wrapper(ret):
                        # turn the original async function into a synchronous one and return a new async function
                        ret = await ret
                        kvstore.set(overall_input_hash, (ret, True))
                        return ret
                    
                    return _wrapper(ret)
                else:
                    kvstore.set(overall_input_hash, (ret, False))
                    return ret
        return _fn

    if callable(name):
        return wrapper(name, None)

    return partial(wrapper, name=name)


# TODO: make it so Envs and cached Remotes cannot be used in both global and cached fn
# TODO: add registry system for special object hashing