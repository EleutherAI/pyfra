
# EXPERIMENTAL

from functools import partial, wraps
import types
from typing import Any, Callable, Dict, Type
import pyfra.remote
import abc
import os
import re
import time
import dataclasses

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

    def cache(self, key=None):
        return cache(key, kvstore=self, _callstackoffset=3)


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
            return pickle.load(bf.BlobFile(self.prefix + "/" + key, "rb"))
        except (FileNotFoundError, EOFError):
            raise KeyError(key)
    
    def set(self, key: str, value):
        with bf.BlobFile(self.prefix + "/" + key, "wb") as f:
            pickle.dump(value, f)


default_kvstore = LocalKVStore()
special_hashing: Dict[Type, Callable[[Any], str]] = {}

# some pyfra special hashing stuff
special_hashing[pyfra.remote.RemotePath] = lambda x: x.quick_hash()
special_hashing[pyfra.remote.Remote] = lambda x: x.hash
special_hashing[list] = lambda x: list(map(_prepare_for_hash, x))
special_hashing[dict] = lambda x: {_prepare_for_hash(k): _prepare_for_hash(v) for k, v in x.items()}
special_hashing[tuple] = lambda x: tuple(map(_prepare_for_hash, x))
special_hashing[types.FunctionType] = lambda x: x.__name__
special_hashing[type] = lambda x: x.__name__

try:
    from pandas import DataFrame
    special_hashing[DataFrame] = lambda x: x.to_json()
except ImportError:
    pass

def set_kvstore(provider):
    global default_kvstore
    default_kvstore = provider


def _prepare_for_hash(x):
    if dataclasses.is_dataclass(x):
        return dataclasses.asdict(x)

    for type_, fn in special_hashing.items():
        if isinstance(x, type_):
            return fn(x)

    return x


def update_source_cache(fname, lineno, new_key):
    with open(fname, "r") as f:
        file_lines = f.read().split("\n")

    # line numbering is 1-indexed
    lineno -= 1

    s = re.match(r"@((?:[^\W0-9]\w*\.)?)cache(\(\))?", file_lines[lineno].lstrip())
    assert s, "@cache can only be used as a decorator!"
    leading_whitespace = re.match(r"^\s*", file_lines[lineno]).group(0)

    file_lines[lineno] = f"{leading_whitespace}@{s.group(1)}cache(\"{new_key}\")"

    with open(fname, "w") as f:
        f.write("\n".join(file_lines))


def cache(key=None, kvstore=None, *, _callstackoffset=2):
    def wrapper(fn, key, kvstore, _callstackoffset):
        # execution always gets here, before the function is called

        if key is None:
            key = pyfra.remote._hash_obs(fn.__module__, fn.__name__, inspect.getsource(fn))[:8] + "_v0"
            # the decorator part of the stack is always the same size because we only get here if key is None
            stack_original_function = inspect.stack()[_callstackoffset]
            update_source_cache(stack_original_function.filename, stack_original_function.lineno - 1, key)
    
        if kvstore is None:
            global default_kvstore
            kvstore = default_kvstore

        @wraps(fn)
        def _fn(*args, **kwargs):
            # execution gets here only after the function is called

            arg_hash = pyfra.remote._hash_obs(
                [_prepare_for_hash(i) for i in args],
                [(_prepare_for_hash(k), _prepare_for_hash(v)) for k, v in list(sorted(kwargs.items()))],
            )

            kwargs.pop(kwargs.pop("_pyfra_nonce_kwarg", "v"), None)

            overall_input_hash = key + "_" + arg_hash

            try:
                ob = kvstore.get(overall_input_hash)
                ret = ob['ret']
                original_awaitable = ob['awaitable']
                original_was_coroutine = ob['iscoroutine']
                current_is_coroutine = inspect.iscoroutinefunction(fn)

                ## ASYNC HANDLING, resume from file

                if original_was_coroutine and current_is_coroutine:
                    return_awaitable = True # coroutine -> coroutine
                elif original_was_coroutine and not current_is_coroutine:
                    return_awaitable = False # coroutine -> normal
                elif not original_was_coroutine and not original_awaitable and current_is_coroutine:
                    return_awaitable = True # normal -> coroutine
                elif not original_was_coroutine and not original_awaitable and not current_is_coroutine:
                    return_awaitable = False # normal -> normal
                elif not original_was_coroutine and original_awaitable and current_is_coroutine:
                    return_awaitable = True # normal_returning_awaitable -> coroutine
                elif not original_was_coroutine and original_awaitable and not current_is_coroutine:
                    # this case is ambiguous! we can't know if the modifier function returns an awaitable or not
                    # without actually running the function, so we just assume it's an awaitable,
                    # since probably nothing changed.
                    return_awaitable = True # normal_returning_awaitable -> normal/normal_returning_awaitable
                else:
                    return_awaitable = False # fallback - most likely this is a bug
                    print(f"WARNING: unknown change in async situation for {fn._name__}")

                if return_awaitable:
                    async def _wrapper(ret):
                        # wrap ret in a dummy async function
                        return ret
                    
                    return _wrapper(ret)
                else:
                    return ret
            except KeyError:
                start_time = time.time()
                ret = fn(*args, **kwargs)
                end_time = time.time()

                ## ASYNC HANDLING, first run
                if inspect.isawaitable(ret):
                    # WARNING: using the same env across multiple async stages is not supported, and not possible to support!
                    # TODO: detect if two async stages are using the same env and throw an error if so

                    async def _wrapper(ret):
                        # turn the original async function into a synchronous one and return a new async function
                        ret = await ret
                        kvstore.set(overall_input_hash, {
                            "ret": ret,
                            "awaitable": True,
                            "iscoroutine": inspect.iscoroutinefunction(fn),
                            "start_time": start_time,
                            "end_time": end_time,
                        })
                        return ret
                    
                    return _wrapper(ret)
                else:
                    kvstore.set(overall_input_hash, {
                        "ret": ret,
                        "awaitable": False,
                        "iscoroutine": inspect.iscoroutinefunction(fn),
                        "start_time": start_time,
                        "end_time": end_time,
                    })
                    return ret
        return _fn

    if callable(key):
        return wrapper(key, None, kvstore=kvstore, _callstackoffset=_callstackoffset)

    return partial(wrapper, key=key, kvstore=kvstore, _callstackoffset=_callstackoffset)


# TODO: make it so Envs and cached Remotes cannot be used in both global and cached fn
# TODO: make sure Envs/Remotes serialize and deserialize properly
