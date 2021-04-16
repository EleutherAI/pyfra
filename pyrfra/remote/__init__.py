from ..utils import *
import pyrfra.utils as _utils
from collections import namedtuple
import codecs
import pickle
import shlex


class RemoteFile:
    def __init__(self, remote, fname):
        if remote == '127.0.0.1': remote = None

        self.remote = remote
        self.fname = fname
    
    def __repr__(self):
        return f"{self.remote}:{self.fname}" if self.remote.ip is not None else self.fname


class Remote:
    def __init__(self, ip=None):
        self.ip = ip

        # TODO: set up remote

    def sh(self, x):
        if self.ip is None:
            return sh(x)
        else:
            return rsh(self.ip, x)
    
    def file(self, fname):
        return RemoteFile(self, fname)

    def __repr__(self):
        return self.ip if self.ip is not None else "127.0.0.1"

    def _run_utils_cmd(self, cmd, args, kwargs):
        if self.ip is None:
            getattr(_utils, cmd)(*args, **kwargs)
        else:
            packed = codecs.encode(pickle.dumps((cmd, args, kwargs)), "base64").decode()
            ret = self.sh(f"python -m pyrfra.remote.wrapper {shlex.quote(packed)}")
            return pickle.loads(codecs.decode(ret.encode(), "base64"))


# set up command methods
methods = ['ls', 'rm', 'mv', 'curl', 'wget']

def command_method(cls, name):
    def _cmd(self, *args, **kwargs):
        self._run_utils_cmd(name, args, kwargs)

    setattr(cls, name, _cmd)

for method in methods: command_method(Remote, method)