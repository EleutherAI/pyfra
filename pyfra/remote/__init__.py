import pyfra.utils as _utils
from collections import namedtuple
import codecs
import pickle
import shlex

# set up command methods
methods = ['ls', 'rm', 'mv', 'curl', 'wget',
           'fwrite', 'fread', 'jread', 'jwrite', 'csvread', 'csvwrite']


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

    def sh(self, x, quiet=False):
        if self.ip is None:
            return _utils.sh(x, quiet=quiet)
        else:
            return _utils.rsh(self.ip, x, quiet=quiet)
    
    def file(self, fname):
        return RemoteFile(self, fname)

    def __repr__(self):
        return self.ip if self.ip is not None else "127.0.0.1"

    def _run_utils_cmd(self, cmd, args, kwargs):
        if self.ip is None:
            return getattr(_utils, cmd)(*args, **kwargs)
        else:
            packed = codecs.encode(pickle.dumps((cmd, args, kwargs)), "base64").decode()
            self.sh(f"python3 -m pyfra.remote.wrapper {shlex.quote(packed)}")
            ret = self.sh("cat .pyfra.result; rm .pyfra.result", quiet=True)
            return pickle.loads(codecs.decode(ret.encode(), "base64"))

    # dummy methods
    def ls(self, *a, **v): pass
    def rm(self, *a, **v): pass
    def mv(self, *a, **v): pass
    def curl(self, *a, **v): pass
    def wget(self, *a, **v): pass

    def fwrite(self, *a, **v): pass
    def fread(self, *a, **v): pass
    def jread(self, *a, **v): pass
    def jwrite(self, *a, **v): pass
    def csvread(self, *a, **v): pass
    def csvwrite(self, *a, **v): pass


class MultiRemote:
    def __init__(self, ips=None):
        self.remotes = [Remote(ip) for ip in ips]

    def sh(self, x, quiet=False):
        # TODO: run in parallel
        return [
            remote.sh(x, quiet=quiet)
            for remote in self.remotes
        ]
    
    def file(self, fname):
        return [RemoteFile(rem, fname) for rem in self.remotes]

    def _run_utils_cmd(self, cmd, args, kwargs):
        # TODO: run in parallel
        return [
            remote._run_utils_cmd(cmd, args, kwargs)
            for remote in self.remotes
        ]

    # dummy methods
    def ls(self, *a, **v): pass
    def rm(self, *a, **v): pass
    def mv(self, *a, **v): pass
    def curl(self, *a, **v): pass
    def wget(self, *a, **v): pass

    def fwrite(self, *a, **v): pass
    def fread(self, *a, **v): pass
    def jread(self, *a, **v): pass
    def jwrite(self, *a, **v): pass
    def csvread(self, *a, **v): pass
    def csvwrite(self, *a, **v): pass


def command_method(cls, name):
    def _cmd(self, *args, **kwargs):
        return self._run_utils_cmd(name, args, kwargs)

    setattr(cls, name, _cmd)


for method in methods:
    command_method(Remote, method)
    command_method(MultiRemote, method)