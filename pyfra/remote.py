from __future__ import annotations
from typing import *

from pyfra import shell as _shell
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


def _normalize_homedir(x):

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


class RemoteFile:
    """
    A RemoteFile represents a file somewhere on some Remote. The RemoteFile object can be used to manipulate the file.

    Example usage: ::

        # write text
        rem.file("goose.txt").write("honk")

        # read text
        print(rem.file("goose.txt").read())

        # write json
        rem.file("goose.json").jwrite({"honk": 1})

        # read json
        print(rem.file("goose.json").jread())

        # write csv
        rem.file("goose.csv").csvwrite([{"col1": 1, "col2": "duck"}, {"col1": 2, "col2": "goose"}])

        # read csv
        print(rem.file("goose.csv").csvread())

        # copy stuff to/from remotes
        rsync(rem1.file('goose.txt'), 'test1.txt')
        rsync('test1.txt', rem2.file('goose.txt'))
        rsync(rem2.file('goose.txt'), rem1.file('testing123.txt'))
    """
    def __init__(self, remote, fname):
        if remote.ip == '127.0.0.1' or remote.ip is None: remote = None

        self.remote = remote
        self.fname = fname
    
    def __repr__(self):
        return f"{self.remote}:{self.fname}" if self.remote is not None and self.remote.ip is not None else self.fname
    
    def _to_json(self):
        return {
            'remote': self.remote.ip,
            'fname': self.fname,
        }
    
    def read(self) -> str:
        """
        Read the contents of this file into a string
        """
        if self.remote is None:
            with open(os.path.expanduser(self.fname)) as fh:
                return fh.read()
        else:
            nonce = random.randint(0, 99999)
            _shell.rsync(self, f".tmp.{nonce}", quiet=True)
            with open(f".tmp.{nonce}") as fh:
                ret = fh.read()
                _shell.rm(f".tmp.{nonce}")
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
                _shell.rsync(f".tmp.{nonce}", self.remote.file(f".tmp.{nonce}"), quiet=True)
                self.remote.sh(f"cat .tmp.{nonce} >> {self.fname} && rm .tmp.{nonce}")
            else:
                _shell.rsync(f".tmp.{nonce}", self, quiet=True)
            _shell.rm(f".tmp.{nonce}")
    
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


class Remote:
    def __init__(self, ip=None, wd=None, python_version="3.9.4"):
        """
        Args:
            ip (str): The host to ssh to. This looks something like :code:`12.34.56.78` or :code:`goose.com` or :code:`someuser@12.34.56.78` or :code:`someuser@goose.com`. You must enable passwordless ssh and have your ssh key added to the server first. If None, the Remote represents localhost.
            wd (str): The working directory on the server to start out on.
            python_version (str): The version of python to use (i.e running :code:`Remote("goose.com", python_version="3.8.10").sh("python --version")` will use python 3.8.10). If this version is not already installed, pyfra will install it.
        """
        if ip in ["127.0.0.1", "localhost"]: ip = None

        self.ip = ip

        self.wd = _normalize_homedir(wd) if wd is not None else "~"
        
        self.pyenv_version = python_version

        self.installed = False

    def env(self, wd, git=None, branch=None, python_version=None) -> Remote:
        """
        An environment is a Remote pointing to a directory that has a virtualenv and a specific version version of python installed, optionally initialized from a git repo. Since environments are also just Remotes, all methods on Remotes work on environments too (including env itself, which makes a nested environment within that invironment with no problems whatsoever).

        A typical design pattern sees functions accepting remotes as argument and immediately turning it into an env that's used for the rest of the function. 

        Example usage: ::

            def train_model(rem, ...):
                e = rem.env("neo_experiment", "https://github.com/EleutherAI/gpt-neo", python_version="3.8.10")
                e.sh("do something")
                e.sh("do something else")
                f = some_other_thing(e, ...)
                e.file("goose.txt").write(f.jread()["honk"])
            
            def some_other_thing(rem, ...):
                # this makes an env inside the other env
                e = rem.env("other_thing", "https://github.com/EleutherAI/thing-doer-5000", python_version="3.8.10")
                e.sh("do something")
                e.sh("do something else")

                return e.file("output.json")

        Args:
            wd (str): The working directory for the environment.
            git (str): A git repo to clone for the environment. If the repo already exists it (and any local changes) will be reset and overwritten with a fresh clone. If None, nothing will be cloned. The requirements.txt will be automatically installed in the virtualenv.
            branch (str): Check out a particular branch. Defaults to whatever the repo default is.
            python_version (str): The python version for this environment. Defaults to the python version of this Remote.
        """
        if python_version is None: python_version = self.pyenv_version
        if wd is None:
            return Remote(self.ip, None, self.pyenv_version)

        if wd[-1] == '/': wd = wd[:-1]
        wd = self.file(wd).fname

        newrem = Remote(self.ip, wd, self.pyenv_version if python_version is None else python_version)

        # install venv
        if wd is not None:
            pyenv_cmds = f"[ -d env/lib/python{python_version.rsplit('.')[0]} ] || rm -rf env ; pyenv shell {python_version} ;" if python_version is not None else ""
            self.sh(f"mkdir -p {wd}; cd {wd}; {pyenv_cmds} [ -f env/bin/activate ] || virtualenv env", no_venv=True)

        # pull git
        if git is not None:
            # TODO: make this usable
            nonce = str(random.randint(0, 99999))

            if branch is None:
                branch_cmds = ""
            else:
                branch_cmds = f"git checkout {branch}; git pull origin {branch}; "

            newrem.sh(f"{{ rm -rf .tmp_git_repo ; git clone {git} .tmp_git_repo.{nonce} ; rsync -ar --delete .tmp_git_repo.{nonce}/ {wd}/ ; rm -rf .tmp_git_repo.{nonce} ; cd {wd}; {branch_cmds} {{ pip install -e . ; pip install -r requirements.txt; }} }}")

        return newrem
    
    def _install(self):
        if not self.installed:
            # set up remote python version
            if self.pyenv_version is not None: install_pyenv(self, self.pyenv_version)
            self.installed = True    

    def sh(self, x, quiet=False, wrap=True, maxbuflen=1000000000, ignore_errors=False, no_venv=False):
        """
        Run a series of bash commands on this remote. This command shares the same arguments as :func:`pyfra.shell.sh`.
        """
        self._install()
    
        if self.ip is None:
            return _shell.sh(x, quiet=quiet, wd=self.wd, wrap=wrap, maxbuflen=maxbuflen, ignore_errors=ignore_errors, no_venv=no_venv, pyenv_version=self.pyenv_version)
        else:
            return _shell._rsh(self.ip, x, quiet=quiet, wd=self.wd, wrap=wrap, maxbuflen=maxbuflen, ignore_errors=ignore_errors, no_venv=no_venv, pyenv_version=self.pyenv_version)
    
    def file(self, fname) -> RemoteFile:
        """
        This is the main way to make a :class:`RemoteFile` object; see RemoteFile docs for more info on what they're used for.
        """
        if isinstance(fname, RemoteFile):
            assert fname.remote == self
            return fname

        return RemoteFile(self, _normalize_homedir(os.path.join(self.wd, fname) if self.wd is not None else fname))

    def __repr__(self):
        return self.ip if self.ip is not None else "127.0.0.1"

    def _run_utils_cmd(self, cmd, args, kwargs):
        # DEPRECATED
        # TODO: use this to do something useful for experiment scheduling
        packed = codecs.encode(pickle.dumps((cmd, args, kwargs)), "base64").decode()
        self.sh(f"python3 -m pyfra.remote.wrapper {shlex.quote(packed)}")

        tmpname = _normalize_homedir(".pyfra.result." + str(random.randint(0, 99999)))
        _shell.rsync(self.file(".pyfra.result"), tmpname)
        ret = _shell.fread(tmpname)
        _shell.rm(tmpname)

        self.sh("rm .pyfra.result", quiet=True, wrap=False)
        return pickle.loads(codecs.decode(ret.encode(), "base64"))

    @property
    def fingerprint(self):
        """
        A unique string for the server that this Remote is pointing to. 
        """
        self.sh("if [ ! -f ~/.pyfra.fingerprint ]; then cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1 > ~/.pyfra.fingerprint; fi")
        tmpname = ".fingerprint." + str(random.randint(0, 99999))
        _shell.rsync(self.file("~/.pyfra.fingerprint"), tmpname)
        ret = _shell.fread(tmpname)
        _shell.rm(tmpname)
        return ret.strip()
    
    def _to_json(self):
        return {
            'ip': self.ip,
            'wd': self.wd,
            'python_version': self.pyenv_version,
        }

    def ls(self, x='.'):
        return list(natsorted(self.sh(f"ls {x} | cat").strip().split("\n")))

    def rm(self, x, no_exists_ok=True):
        self.sh(f"cd ~; rm -rf {self.file(x).fname}", ignore_errors=no_exists_ok)


local = Remote(wd=os.getcwd())

file = local.file