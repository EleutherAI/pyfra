from pyfra.utils import shell as _shell
from .setup import install_pyenv
from collections import namedtuple
import codecs
import pickle
import shlex
import os
import random
from natsort import natsorted


def normalize_homedir(x):

    if x[:2] == './':
        x = x[2:]
    if x[-2:] == '/.':
        x = x[:-2]
    
    x = x.replace('/./', '/')

    if '~/' in x:
        x = x.split('~/')[-1]
    
    # some special cases
    if x == '': return '~'
    if x == '.': return '~'
    if x == '~': return '~'
    if x == '/': return '/'
    
    if x[0] != '/' and x[:2] != '~/':
        x = '~/' + x
    
    if x[-1] == '/' and len(x) > 1: x = x[:-1]
    
    return x


class RemoteFile:
    def __init__(self, remote, fname):
        if remote == '127.0.0.1': remote = None

        self.remote = remote
        self.fname = fname
    
    def __repr__(self):
        return f"{self.remote}:{self.fname}" if self.remote.ip is not None else self.fname
    
    def to_json(self):
        return {
            'remote': self.remote.ip,
            'fname': self.fname,
        }


class Remote:
    def __init__(self, ip=None, wd=None, python_version="3.9.4"):
        self.ip = ip

        self.wd = normalize_homedir(wd) if wd is not None else "~"
        
        self.pyenv_version = python_version

        # self.sh("pip install -U git+https://github.com/EleutherAI/pyfra/")

    def env(self, wd=None, git=None, python_version=None):
        if python_version is None: python_version = self.pyenv_version
        if wd is None:
            return Remote(self.ip, None, self.pyenv_version)

        if wd[-1] == '/': wd = wd[:-1]
        wd = self.file(wd).fname

        newrem = Remote(self.ip, wd, self.pyenv_version if python_version is None else python_version)

        # set up remote python version
        if python_version is not None:
            install_pyenv(self, python_version)

        # install venv
        if wd is not None:
            pyenv_cmds = f"[ -d env/lib/python{python_version.rsplit('.')[0]} ] || rm -rf env ; pyenv shell {python_version} ;" if python_version is not None else ""
            self.sh(f"mkdir -p {wd | _shell.quote}; cd {wd | _shell.quote}; {pyenv_cmds} [ -f env/bin/activate ] || virtualenv env", no_venv=True)

        # pull git
        if git is not None:
            # TODO: make this usable
            nonce = str(random.randint(0, 99999))
            newrem.sh(f"{{ rm -rf .tmp_git_repo ; git clone {git} .tmp_git_repo.{nonce} ; rsync -ar .tmp_git_repo.{nonce}/ ~/{wd}/ ; rm -rf .tmp_git_repo.{nonce} ; cd ~/{wd} && {{ pip install -e . || pip install -r requirements.txt; }} }}")

        return newrem

    def sh(self, x, quiet=False, wrap=True, maxbuflen=1000000000, ignore_errors=False, no_venv=False):
        if self.ip is None:
            return _shell.sh(x, quiet=quiet, wd=self.wd, wrap=wrap, maxbuflen=maxbuflen, ignore_errors=ignore_errors, no_venv=no_venv, pyenv_version=self.pyenv_version)
        else:
            return _shell.rsh(self.ip, x, quiet=quiet, wd=self.wd, wrap=wrap, maxbuflen=maxbuflen, ignore_errors=ignore_errors, no_venv=no_venv, pyenv_version=self.pyenv_version)
    
    def file(self, fname):
        if isinstance(fname, RemoteFile):
            assert fname.remote == self
            return fname

        return RemoteFile(self, normalize_homedir(os.path.join(self.wd, fname) if self.wd is not None else fname))

    def __repr__(self):
        return self.ip if self.ip is not None else "127.0.0.1"

    def _run_utils_cmd(self, cmd, args, kwargs):
        # TODO: use this to do something useful for experiment scheduling
        packed = codecs.encode(pickle.dumps((cmd, args, kwargs)), "base64").decode()
        self.sh(f"python3 -m pyfra.remote.wrapper {shlex.quote(packed)}")

        tmpname = normalize_homedir(".pyfra.result." + str(random.randint(0, 99999)))
        _shell.rsync(self.file(".pyfra.result"), tmpname)
        ret = _shell.fread(tmpname)
        _shell.rm(tmpname)

        self.sh("rm .pyfra.result", quiet=True, wrap=False)
        return pickle.loads(codecs.decode(ret.encode(), "base64"))

    @property
    def fingerprint(self):
        self.sh("if [ ! -f ~/.pyfra.fingerprint ]; then cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1 > ~/.pyfra.fingerprint; fi")
        tmpname = ".fingerprint." + str(random.randint(0, 99999))
        _shell.rsync(self.file("~/.pyfra.fingerprint"), tmpname)
        ret = _shell.fread(tmpname)
        _shell.rm(tmpname)
        return ret.strip()
    
    def to_json(self):
        return {
            'ip': self.ip,
            'wd': self.wd,
            'python_version': self.pyenv_version,
        }

    def ls(self, x='.'):
        return list(natsorted(self.sh(f"ls {x} | cat").strip().split("\n")))

    def rm(self, x, no_exists_ok=True):
        self.sh(f"cd ~; rm -rf {self.file(x).fname}", ignore_errors=no_exists_ok)


local = Remote()
