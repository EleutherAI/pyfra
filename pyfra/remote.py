from __future__ import annotations
from typing import *

import pyfra.shell
from .setup import install_pyenv
from collections import namedtuple
import codecs
import pickle
import shlex
import os
import random
import json
import csv
from natsort import natsorted
import io
import pathlib
from yaspin import yaspin
import uuid


sentinel = object()


def _normalize_homedir(x):
    """ Essentially expanduser(path.join("~", x)) but remote-agnostic """
    if x[:2] == './':
        x = x[2:]
    if x[-2:] == '/.':
        x = x[:-2]
    
    x = x.replace('/./', '/')

    if '~/' in x:
        x = x.split('~/')[-1]
    
    if x[-2:] == '/~':
        return '~'
    
    # some special cases
    if x == '': return '~'
    if x == '.': return '~'
    if x == '~': return '~'
    if x == '/': return '/'
    
    if x[0] != '/' and x[:2] != '~/':
        x = '~/' + x
    
    if x[-1] == '/' and len(x) > 1: x = x[:-1]
    
    return x


class RemotePath:
    """
    A RemotePath represents a path somewhere on some Remote. The RemotePath object can be used to manipulate the file.

    Example usage: ::

        # write text
        rem.path("goose.txt").write("honk")

        # read text
        print(rem.path("goose.txt").read())

        # write json
        rem.path("goose.json").jwrite({"honk": 1})

        # read json
        print(rem.path("goose.json").jread())

        # write csv
        rem.path("goose.csv").csvwrite([{"col1": 1, "col2": "duck"}, {"col1": 2, "col2": "goose"}])

        # read csv
        print(rem.path("goose.csv").csvread())

        # copy stuff to/from remotes
        copy(rem1.path('goose.txt'), 'test1.txt')
        copy('test1.txt', rem2.path('goose.txt'))
        copy(rem2.path('goose.txt'), rem1.path('testing123.txt'))
    """
    def __init__(self, remote, fname):
        if remote is None or remote.ip == '127.0.0.1' or remote.ip is None: remote = None

        self.remote = remote
        self.fname = fname
    
    def rsyncstr(self):
        return f"{self.remote}:{self.fname}" if self.remote is not None and self.remote.ip is not None else self.fname

    def _to_json(self):
        return {
            'remote': self.remote.ip if self.remote is not None else None,
            'fname': self.fname,
        }

    def __repr__(self):
        return f"RemotePath({json.dumps(self._to_json())})"
    
    def read(self) -> str:
        """
        Read the contents of this file into a string
        """
        if self.remote is None:
            with open(os.path.expanduser(self.fname)) as fh:
                return fh.read()
        else:
            nonce = random.randint(0, 99999)
            pyfra.shell.copy(self, f".tmp.{nonce}", quiet=True)
            with open(f".tmp.{nonce}") as fh:
                ret = fh.read()
                pyfra.shell.rm(f".tmp.{nonce}")
                return ret
    
    def write(self, content, append=False) -> str:
        """
        Write text to this file.

        Args:
            content (str): The text to write
            append (bool): Whether to append or overwrite the file contents

        """
        if self.remote is None:
            with open(os.path.expanduser(self.fname), 'a' if append else 'w') as fh:
                fh.write(content)
        else:
            nonce = random.randint(0, 99999)
            with open(f".tmp.{nonce}", 'w') as fh:
                fh.write(content)
            if append:
                pyfra.shell.copy(f".tmp.{nonce}", self.remote.path(f".tmp.{nonce}"), quiet=True)
                self.remote.sh(f"cat .tmp.{nonce} >> {self.fname} && rm .tmp.{nonce}", quiet=True)
            else:
                pyfra.shell.copy(f".tmp.{nonce}", self, quiet=True)
            pyfra.shell.rm(f".tmp.{nonce}")
    
    def jread(self):
        """
        Read the contents of this json file and parses it. Equivalent to :code:`json.loads(self.read())`
        """
        return json.loads(self.read())

    def jwrite(self, content):
        """
        Write a json object to this file. Equivalent to :code:`self.write(json.dumps(content))`

        Args:
            content (json): The json object to write

        """
        self.write(json.dumps(content))

    def csvread(self, colnames=None) -> List[dict]:
        """
        Read the contents of this csv file and parses it into an array of dictionaries where the keys are column names.

        Args:
            colnames (list): Optionally specify the names of the columns for csvs without a header row.
        """
        fh = io.StringIO(self.read())
        if self.fname[-4:] == ".tsv":
            rdr = csv.reader(fh, delimiter="\t")
        else:
            rdr = csv.reader(fh)

        if colnames:
            cols = colnames
        else:
            cols = list(next(rdr))
        
        for ob in rdr:
            yield {
                k: v for k, v in zip(cols, [*ob, *[None for _ in range(len(cols) - len(ob))]])
            }
    
    def csvwrite(self, data, colnames=None):
        """
        Write a list of dicts object to this csv file.

        Args:
            content (List[dict]): A list of dicts where the keys are column names. Every dicts should have the exact same keys.

        """
        fh = io.StringIO()
        if colnames is None:
            colnames = data[0].keys()

        wtr = csv.writer(fh)
        wtr.writerow(colnames)

        for dat in data:
            assert dat.keys() == colnames

            wtr.writerow([dat[k] for k in colnames])
        
        fh.seek(0)
        self.write(fh.read())
    
    def _remote_payload(self, name, *args, **kwargs):
        """
        Run an arbitrary Path.* function remotely and return the result.
        Restricted to os rather than arbitrary eval for security reasons.
        """
        assert all(x in "_.abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" for x in name)

        if self.remote is None:
            fn = pathlib.Path(self.fname)
            for k in name.split("."):
                fn = getattr(fn, k)
            return fn(*args, **kwargs)
        else:
            payload = f"import pathlib,json; print(json.dumps(pathlib.Path({repr(self.fname)}).expanduser().{name}(*{args}, **{kwargs})))"
            ret = self.remote.sh(f"python -c {payload | pyfra.shell.quote}", quiet=True)
            return json.loads(ret)

    def stat(self) -> os.stat_result:
        """
        Stat a remote file
        """
        return os.stat_result(self._remote_payload("stat"))
    
    def exists(self) -> bool:
        """
        Check if this file exists
        """
        try:
            return self._remote_payload("exists")
        except pyfra.shell.ShellException:
            # if we can't connect to the remote, the file does not exist
            return False
    
    def is_dir(self) -> bool:
        """
        Check if this file exists
        """
        return self._remote_payload("is_dir")

    def expanduser(self) -> RemotePath:
        """
        Return a copy of this path with the home directory expanded.
        """
        if self.remote is None:
            return RemotePath(None, os.path.expanduser(self.fname))
        else:
            homedir = self.remote.home()

            # todo: be more careful with this replace
            return RemotePath(self.remote, os.path.expanduser(self.fname).replace("~", homedir))
    
    def __div__(self, other):
        return RemotePath(self.remote, os.path.join(self.fname, other))


class Remote:
    def __init__(self, ip=None, wd=None):
        """
        Args:
            ip (str): The host to ssh to. This looks something like :code:`12.34.56.78` or :code:`goose.com` or :code:`someuser@12.34.56.78` or :code:`someuser@goose.com`. You must enable passwordless ssh and have your ssh key added to the server first. If None, the Remote represents localhost.
            wd (str): The working directory on the server to start out on.
            python_version (str): The version of python to use (i.e running :code:`Remote("goose.com", python_version="3.8.10").sh("python --version")` will use python 3.8.10). If this version is not already installed, pyfra will install it.
        """
        if ip in ["127.0.0.1", "localhost"]: ip = None

        self.ip = ip

        self.wd = _normalize_homedir(wd) if wd is not None else "~"

        self._home = None

    def env(self, envname, git=None, branch=None, python_version="3.9.4") -> Remote:
        """
        Args:
            envname (str): The name for the environment.
            git (str): A git repo to clone for the environment. If the repo already exists it (and any local changes) will be reset and overwritten with a fresh clone. If None, nothing will be cloned. The requirements.txt will be automatically installed in the virtualenv.
            branch (str): Check out a particular branch. Defaults to whatever the repo default is.
            python_version (str): The python version for this environment. Defaults to the python version of this Remote.
        """

        wd = f"~/pyfra_envs/{envname}"

        return Env(ip=self.ip, wd=wd, git=git, branch=branch, python_version=python_version)

    def sh(self, x, quiet=False, wrap=True, maxbuflen=1000000000, ignore_errors=False, no_venv=False, pyenv_version=None):
        """
        Run a series of bash commands on this remote. This command shares the same arguments as :func:`pyfra.shell.sh`.
        """
        if self.ip is None:
            return pyfra.shell.sh(x, quiet=quiet, wd=self.wd, wrap=wrap, maxbuflen=maxbuflen, ignore_errors=ignore_errors, no_venv=no_venv, pyenv_version=pyenv_version)
        else:
            return pyfra.shell._rsh(self.ip, x, quiet=quiet, wd=self.wd, wrap=wrap, maxbuflen=maxbuflen, ignore_errors=ignore_errors, no_venv=no_venv, pyenv_version=pyenv_version)
    
    def path(self, fname=None) -> RemotePath:
        """
        This is the main way to make a :class:`RemotePath` object; see RemotePath docs for more info on what they're used for.

        If fname is not specified, this command allocates a temporary path
        """
        if fname is None:
            return self.path(f"pyfra_tmp_{uuid.uuid4().hex}")

        if isinstance(fname, RemotePath):
            assert fname.remote == self
            return fname

        return RemotePath(self, _normalize_homedir(os.path.join(self.wd, fname) if self.wd is not None else fname))

    def __repr__(self):
        return self.ip if self.ip is not None else "127.0.0.1"

    @property
    def fingerprint(self):
        """
        A unique string for the server that this Remote is pointing to. 
        """
        self.sh("if [ ! -f ~/.pyfra.fingerprint ]; then cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1 > ~/.pyfra.fingerprint; fi", quiet=True)
        tmpname = ".fingerprint." + str(random.randint(0, 99999))
        pyfra.shell.copy(self.path("~/.pyfra.fingerprint"), tmpname)
        ret = pyfra.shell.fread(tmpname)
        pyfra.shell.rm(tmpname)
        return ret.strip()
    
    def _to_json(self):
        return {
            'ip': self.ip,
            'wd': self.wd,
        }

    def ls(self, x='.'):
        return list(natsorted(self.sh(f"ls {x} | cat").strip().split("\n")))

    def rm(self, x, no_exists_ok=True):
        self.sh(f"cd ~; rm -rf {self.path(x).fname}", ignore_errors=no_exists_ok)
     
    def home(self):
        if self._home is None:
            self._home = self.sh("echo $HOME", quiet=True).strip()
        
        return self._home


class Env(Remote):
    """
    An environment is a Remote pointing to a directory that has a virtualenv and a specific version version of python installed, optionally initialized from a git repo. Since environments are also just Remotes, all methods on Remotes work on environments too (including env itself, which makes a nested environment within that invironment with no problems whatsoever).

    A typical design pattern sees functions accepting remotes as argument and immediately turning it into an env that's used for the rest of the function. 

    Example usage: ::

        def train_model(rem, ...):
            e = rem.env("neo_experiment", "https://github.com/EleutherAI/gpt-neo", python_version="3.8.10")
            e.sh("do something")
            e.sh("do something else")
            f = some_other_thing(e, ...)
            e.path("goose.txt").write(f.jread()["honk"])
        
        def some_other_thing(rem, ...):
            # this makes an env inside the other env
            e = rem.env("other_thing", "https://github.com/EleutherAI/thing-doer-5000", python_version="3.8.10")
            e.sh("do something")
            e.sh("do something else")

            return e.path("output.json")
    """
    def __init__(self, ip=None, wd=None, git=None, branch=None, python_version="3.9.4"):
        with yaspin(text="Loading", color="white") as spinner:
            spinner.text = f"[{ip}:{wd}] Creating Env" 
            super().__init__(ip, wd)
            
            self.pyenv_version = python_version
            self.wd = wd

            spinner.text = f"[{ip}:{wd}] Installing python in env" 
            # install python/pyenv
            with spinner.hidden():
                self._install(python_version)

            self.sh(f"mkdir -p {wd}", no_venv=True, quiet=True)

            # pull git
            if git is not None:
                spinner.text = f"[{ip}:{wd}] Cloning from git repo" 
                # TODO: make this usable
                nonce = str(random.randint(0, 99999))

                if branch is None:
                    branch_cmds = ""
                else:
                    branch_cmds = f"git checkout {branch}; git pull origin {branch}; "

                self.sh(f"{{ rm -rf ~/.tmp_git_repo.{nonce} ; git clone {git} ~/.tmp_git_repo.{nonce} ; rsync -ar --delete ~/.tmp_git_repo.{nonce}/ {wd}/ ; rm -rf ~/.tmp_git_repo.{nonce} ; cd {wd}; {branch_cmds} }}", ignore_errors=True, quiet=True)

            # install venv
            if wd is not None:
                spinner.text = f"[{ip}:{wd}] Creating virtualenv" 
                pyenv_cmds = f"[ -d env/lib/python{python_version.rsplit('.')[0]} ] || rm -rf env ; python --version ; pyenv shell {python_version} ; python --version;" if python_version is not None else ""
                self.sh(f"mkdir -p {wd}; cd {wd}; {pyenv_cmds} [ -f env/bin/activate ] || python -m virtualenv env || ( python -m pip install virtualenv; python -m virtualenv env )", no_venv=True, quiet=True)
                spinner.text = f"[{ip}:{wd}] Installing requirements" 
                self.sh("pip install -e . ; pip install -r requirements.txt", ignore_errors=True, quiet=True)
            
            spinner.text = f"[{ip}:{wd}] Env created" 
            spinner.color = "green"
            spinner.ok("OK ")
            

    def sh(self, x, quiet=False, wrap=True, maxbuflen=1000000000, ignore_errors=False, no_venv=False, pyenv_version=sentinel):
        """
        Run a series of bash commands on this remote. This command shares the same arguments as :func:`pyfra.shell.sh`.
        """
    
        return super().sh(x, quiet=quiet, wrap=wrap, maxbuflen=maxbuflen, ignore_errors=ignore_errors, no_venv=no_venv, pyenv_version=pyenv_version if pyenv_version is not sentinel else self.pyenv_version)

    def _install(self, python_version):   
        # set up remote python version
        if python_version is not None: install_pyenv(self, python_version)

        # install rsync for copying files
        self.sh("rsync --help > /dev/null || ( sudo apt-get update && sudo apt-get install -y rsync )", quiet=True)

    def _to_json(self):
        return {
            'ip': self.ip,
            'wd': self.wd,
            'pyenv_version': self.pyenv_version,
        }


local = Remote(wd=os.getcwd())
