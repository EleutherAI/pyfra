import pyfra.utils as _utils
import pickle
import codecs
import sys
import contextlib
import os


def execute_utils(packed):
    cmd, args, kwargs = pickle.loads(codecs.decode(packed.encode(), "base64"))
    ret = getattr(_utils, cmd)(*args, **kwargs)
    ret = codecs.encode(pickle.dumps(ret), "base64").decode()
    return ret


if __name__ == '__main__':
    _utils.fwrite(".pyfra.result", execute_utils(sys.argv[1]))