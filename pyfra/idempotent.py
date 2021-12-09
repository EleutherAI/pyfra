from functools import partial, wraps
import pyfra.remote
import abc
import os
import blobfile as bf
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


def set_kvstore(provider):
    global kvstore
    kvstore = provider


def _prepare_for_hash(x):
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
            name = pyfra.remote._hash_obs(fn.__module__, fn.__name__)[:16] + "-v0"
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
                r = kvstore.get(overall_input_hash)
                return r
            except KeyError:
                r = fn(*args, **kwargs)
                kvstore.set(overall_input_hash, r)
                return r
        return _fn

    if callable(name):
        return wrapper(name, None)

    return partial(wrapper, name=name)
