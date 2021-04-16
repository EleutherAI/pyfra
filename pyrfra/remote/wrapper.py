import pyrfra.utils as _utils
import pickle
import codecs
import sys
import contextlib
import os


# from https://stackoverflow.com/a/28321717
def suppress_stdout(func):
    def wrapper(*a, **ka):
        with open(os.devnull, 'w') as devnull:
            with contextlib.redirect_stdout(devnull):
                func(*a, **ka)
    return wrapper


@suppress_stdout
def execute_utils(packed):
    cmd, args, kwargs = pickle.loads(codecs.decode(packed.encode(), "base64"))
    ret = getattr(_utils, cmd)(*args, **kwargs)
    ret = codecs.encode(pickle.dumps(ret), "base64").decode()
    return ret


if __name__ == '__main__':
    print(execute_utils(sys.argv[1]))