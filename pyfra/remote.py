from __future__ import annotations
from contextlib import contextmanager
from hashlib import new
from typing import *

from best_download import handler

import pyfra.shell
from pyfra.setup import install_pyenv
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
import abc
from functools import partial, wraps
from colorama import Fore, Style
import imohash
import hashlib

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


class _ObjectEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pyfra.remote.RemotePath):
            return obj.sha256sum()
        if hasattr(obj, "_to_json"):
            return obj._to_json()
        
        return super().default(obj)


def hash_obs(*args):
    jsonobj = json.dumps(args, sort_keys=True, cls=_ObjectEncoder)
    arghash = hashlib.sha256(jsonobj.encode()).hexdigest()
    return arghash


def _print_skip_msg(envname, fn, hash):
    print(f"{Style.BRIGHT}[{envname.ljust(15)} {Style.DIM}§{Style.RESET_ALL}{Style.BRIGHT}{fn.rjust(10)}]{Style.RESET_ALL} Skipping {hash}")


def mutates_state(fn, hash_key=None):
    """
    Decorator that marks a function as mutating the state of the underlying environment.
    """
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        if self._no_hash: return fn(self, *args, **kwargs)
        new_hash = self.update_hash(fn.__name__, *args, **kwargs) if hash_key is None else self.update_hash(*hash_key(fn, *args, **kwargs))
        try:
            # if hash is in the state, then we can just return that
            ret = self.get_kv(new_hash)
            _print_skip_msg(self.envname, fn.__name__, new_hash)
            
            return ret
        except KeyError:
            # otherwise, we need to run the function and save the result
            ret = fn(self, *args, **kwargs)
            self.set_kv(new_hash, ret)
            return ret
    return wrapper


# remote stuff

# global cache
_remotepath_cache = {}
_remotepath_modified_time = {}
def cache(fn):
    """
    Use as an annotation. Caches the response of the function and
    check modification time on the file on every call.
    """
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        modified_time = self.stat().st_mtime
        hash = hash_obs(fn.__name__, args, kwargs)
        if hash not in _remotepath_cache or modified_time != _remotepath_modified_time[hash]:
            ret = fn(self, *args, **kwargs)
            _remotepath_cache[(self.remote.ip, self.fname, hash)] = ret
            _remotepath_modified_time[(self.remote.ip, self.fname, hash)] = modified_time
            return ret
        else:
            return _remotepath_cache[hash]
    return wrapper


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
        if remote is None: remote = local

        self.remote = remote
        self.fname = fname
    
    def rsyncstr(self) -> str:
        return f"{self.remote.ip}:{self.fname}" if self.remote is not None and self.remote.ip is not None else self.fname

    def _to_json(self) -> Dict[str, str]:
        return {
            'remote': self.remote.ip if self.remote is not None else None,
            'fname': self.fname,
        }

    def __repr__(self) -> str:
        return f"RemotePath({json.dumps(self._to_json())})"
    
    def _set_cache(self, fn_name, value, *args, **kwargs):
        modified_time = self.stat().st_mtime
        hash = hash_obs(fn_name, args, kwargs)
        _remotepath_modified_time[(self.remote.ip, self.fname, hash)] = modified_time
        _remotepath_cache[(self.remote.ip, self.fname, hash)] = value

    def read(self) -> str:
        """
        Read the contents of this file into a string
        """
        if self.remote.is_local():
            with open(os.path.expanduser(self.fname)) as fh:
                return fh.read()
        else:
            # TODO: replace with paramiko
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
        self.remote.fwrite(self.fname, content, append)
    
    def jread(self) -> Dict[str, Any]:
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

        if self.remote.is_local():
            fn = pathlib.Path(self.fname)
            for k in name.split("."):
                fn = getattr(fn, k)
            return fn(*args, **kwargs)
        else:
            payload = f"import pathlib,json; print(json.dumps(pathlib.Path({repr(self.fname)}).expanduser().{name}(*{args}, **{kwargs})))"
            ret = self.remote.sh(f"python -c {payload | pyfra.shell.quote}", quiet=True, no_venv=True, pyenv_version=None)
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
    
    def unlink(self) -> None:
        """
        Delete this file
        """
        self._remote_payload("unlink")

    def expanduser(self) -> RemotePath:
        """
        Return a copy of this path with the home directory expanded.
        """
        if self.remote.is_local():
            return RemotePath(None, os.path.expanduser(self.fname))
        else:
            homedir = self.remote.home()

            # todo: be more careful with this replace
            return RemotePath(self.remote, os.path.expanduser(self.fname).replace("~", homedir))
    
    def __div__(self, other):
        return RemotePath(self.remote, os.path.join(self.fname, other))

    @cache
    def sha256sum(self) -> str:
        """
        Return the sha256sum of this file.
        """
        return self.remote.sh(f"sha256sum {self.fname}", quiet=True).split(" ")[0]

    @cache
    def quick_hash(self) -> str:
        """
        Get a hash of this file that catches file changes most of the time
        by hashing blocks from the file at th beginning, middle, and end.
        Really useful for getting a quick hash of a really big file, but obviously
        unsuitable for guaranteeing file integrity.

        Uses imohash under the hood.
        """
        params = {
            "hexdigest": True,
            "sample_size": 4 * 1024**2, # 4 MB
            "sample_threshhold": 16 * 1024**2, # 16 MB
        }
        if self.remote.is_local():
            return imohash.hashfile(self.fname, **params)
        else:
            # TODO: use paramiko
            # TODO: make faster by not trying to install every time
            payload = f"import imohash,json,os; print(json.dumps(imohash.hashfile(os.path.expanduser({repr(self.fname)}), **{params})))"
            ret = self.remote.sh(f"[ -f ~/.imohash ] || ( python -m pip --help > /dev/null 2>&1 || sudo apt-get install python3-pip -y > /dev/null 2>&1; python -m pip install imohash > /dev/null 2>&1; touch ~/.imohash ); python -c {payload | pyfra.shell.quote}", no_venv=True, pyenv_version=None, quiet=True)
            return json.loads(ret)
            

class Remote:
    def __init__(self, ip=None, wd=None, experiment=None):
        """
        Args:
            ip (str): The host to ssh to. This looks something like :code:`12.34.56.78` or :code:`goose.com` or :code:`someuser@12.34.56.78` or :code:`someuser@goose.com`. You must enable passwordless ssh and have your ssh key added to the server first. If None, the Remote represents localhost.
            wd (str): The working directory on the server to start out on.
            python_version (str): The version of python to use (i.e running :code:`Remote("goose.com", python_version="3.8.10").sh("python --version")` will use python 3.8.10). If this version is not already installed, pyfra will install it.
        """
        if ip in ["127.0.0.1", "localhost"]: ip = None

        self.ip = ip
        self.wd = _normalize_homedir(wd) if wd is not None else "~"
        self.experiment = experiment

        self._home = None
        self._no_hash = True

    def env(self, envname, git=None, branch=None, python_version="3.9.4") -> Remote:
        """
        Args:
            envname (str): The name for the environment.
            git (str): A git repo to clone for the environment. If the repo already exists it (and any local changes) will be reset and overwritten with a fresh clone. If None, nothing will be cloned. The requirements.txt will be automatically installed in the virtualenv.
            branch (str): Check out a particular branch. Defaults to whatever the repo default is.
            python_version (str): The python version for this environment. Defaults to the python version of this Remote.
        """

        return Env(ip=self.ip, git=git, branch=branch, python_version=python_version, envname=envname)

    def sh(self, x, quiet=False, wrap=True, maxbuflen=1000000000, ignore_errors=False, no_venv=False, pyenv_version=None, forward_keys=False):
        """
        Run a series of bash commands on this remote. This command shares the same arguments as :func:`pyfra.shell.sh`.
        """
        if self.ip is None:
            return pyfra.shell.sh(x, quiet=quiet, wd=self.wd, wrap=wrap, maxbuflen=maxbuflen, ignore_errors=ignore_errors, no_venv=no_venv, pyenv_version=pyenv_version)
        else:
            return pyfra.shell._rsh(self.ip, x, quiet=quiet, wd=self.wd, wrap=wrap, maxbuflen=maxbuflen, ignore_errors=ignore_errors, no_venv=no_venv, pyenv_version=pyenv_version, forward_keys=forward_keys)

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
        return (self.ip if self.ip is not None else "127.0.0.1") + ":" + self.wd

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
     
    def home(self):
        if self._home is None:
            self._home = self.sh("echo $HOME", quiet=True).strip()
        
        return self._home

    def fwrite(self, fname, content, append=False):
        if self.ip is None:
            with open(os.path.expanduser(fname), 'a' if append else 'w') as fh:
                fh.write(content)
        else:
            nonce = random.randint(0, 99999)
            with open(f".tmp.{nonce}", 'w') as fh:
                fh.write(content)
            if append:
                pyfra.shell.copy(f".tmp.{nonce}", self.path(f".tmp.{nonce}"), quiet=True)
                self.sh(f"cat .tmp.{nonce} >> {fname} && rm .tmp.{nonce}", quiet=True)
            else:
                pyfra.shell.copy(f".tmp.{nonce}", self.path(fname), quiet=True)
            pyfra.shell.rm(f".tmp.{nonce}")

    # key-value store for convienence

    def set_kv(self, key: str, value: Any) -> None:
        """
        A key value store to keep track of stuff in this env. The data is stored in the env
        on the remote. Current implementation is inefficient (linear time, and the entire json 
        file is moved each time) but there shouldn't be a lot of data stored in it anyways
        (premature optimization is bad), and if we need to store a lot of data in the future 
        we can always make this more efficient without changing the interface.
        """
        with self.no_hash():
            # TODO: make more efficient
            statefile = self.path(".pyfra_env_state.json")
            if statefile.exists():
                ob = statefile.jread()
            else:
                ob = {}
            pickled_value = pickle.dumps(value, 0).decode()
            ob[key] = pickled_value
            statefile.jwrite(ob)

    def get_kv(self, key: str) -> Any:
        """
        Retrieve a value from the state file.
        """
        with self.no_hash():
            # TODO: make more efficient
            statefile = self.path(".pyfra_env_state.json")
            if statefile.exists():
                ob = statefile.jread()
            else:
                ob = {}

            return pickle.loads(ob[key].encode())

    def update_hash(self, *args, **kwargs):
        pass
    
    @contextmanager
    def no_hash(self):
        """ Context manager to turn off hashing temporarily """
        if self._no_hash:
            yield
            return

        self._no_hash = True
        try:
            yield
        finally:
            self._no_hash = False

    def is_local(self):
        return self.ip is None
    
    def __hash__(self):
        # this hash is not the Env hash, which represents the state inside the Env,
        # but rather is supposed to represent the particular location this 
        # Remote/Env is pointing to.
        return hash((self.ip, self.wd))

# env
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
    def __init__(self, ip=None, git=None, branch=None, force_rerun=False, python_version="3.9.4", envname=None):
        self.wd = f"~/pyfra_envs/{envname}"
        super().__init__(ip, self.wd)
        self.pyenv_version = python_version

        self.envname = envname

        self.hash = self._hash(None)
        self._no_hash = False

        if force_rerun:
            self.path(".pyfra_env_state.json").unlink()

        self._init_env(git, branch, python_version)

        global_env_registry.register(self)
    
    @classmethod
    def _hash(cls, *args, **kwargs):
        return hash_obs([args, kwargs])

    @mutates_state
    def _init_env(self, git, branch, python_version):
        with yaspin(text="Loading", color="white") as spinner, self.no_hash():
            ip = self.ip
            wd = self.wd

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

    @mutates_state
    def sh(self, x, quiet=False, wrap=True, maxbuflen=1000000000, ignore_errors=False, no_venv=False, pyenv_version=sentinel, forward_keys=False):
        """
        Run a series of bash commands on this remote. This command shares the same arguments as :func:`pyfra.shell.sh`.
        """
    
        return super().sh(x, quiet=quiet, wrap=wrap, maxbuflen=maxbuflen, ignore_errors=ignore_errors, no_venv=no_venv, pyenv_version=pyenv_version if pyenv_version is not sentinel else self.pyenv_version, forward_keys=forward_keys)

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

    def fwrite(self, fname, content, append=False):
        # wraps fwrite to make it keep track of state hashes
        # TODO: replace with paramiko
        # TODO: extract this statehash code and the one in shell.copy to a common function or something
        needs_set_kv = False
        if not self._no_hash:
            assert fname.startswith(self.wd)
            fname_suffix = fname[len(self.wd):]
            new_hash = self.update_hash("fwrite", fname_suffix, content, append)
            try:
                self.get_kv(new_hash)
                _print_skip_msg(self.envname, "fwrite", new_hash)
                return
            except KeyError:
                needs_set_kv = True
        
        with self.no_hash():
            super().fwrite(fname, content, append)

        if needs_set_kv:
            self.set_kv(new_hash, None)

    def update_hash(self, *args, **kwargs):
        self.hash = self._hash(self.hash, *args, **kwargs)
        return self.hash


def block(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        # get all Envs in args and kwargs
        envs = [(i, x) for i, x in enumerate(args) if isinstance(x, Env)] + \
            [(k, kwargs[k]) for k in sorted(kwargs.keys()) if isinstance(kwargs[k], Env)]
        inp_hashes = {
            k: v.hash for k, v in envs
        }

        def _prepare_for_hash(ind):
            # we want to handle Envs and RemotePaths specially:
            # for Envs, we only care about the Env hash
            # for RemotePaths, we only car about the quick_hash
            if ind in inp_hashes:
                return inp_hashes[ind]
            elif isinstance(ind, int) and isinstance(args[ind], RemotePath):
                return args[ind].quick_hash()
            elif isinstance(ind, str) and isinstance(kwargs[ind], RemotePath):
                return kwargs[ind].quick_hash()
            elif isinstance(ind, int): # normal arg type
                return args[ind]
            elif isinstance(ind, str):
                return kwargs[ind]
            else:
                raise Exception(f"Unknown ind type: {type(ind)}")

        # get a hash of all the inputs, except Env objects are counted as their hashes rather than the actual ip and wd
        overall_input_hash = hash_obs(
            [_prepare_for_hash(i) for i in range(len(args))],
            [_prepare_for_hash(k) for k in sorted(kwargs.keys())],
        )

        global_hashes_before = global_env_registry.hashes_by_env()

        try:
            # todo: detect RemotePaths in return value and substitute if broken
            # todo: handle Env objects that change the server they're on

            # the following different cases of envs passed are possible:
            # - created in block and not returned:
            #     as long as the Env is never 
            #     independently created again later, we don't need to do anything 
            #     special to track it; if it is created again, we need some kind
            #     of global tracking to tell it where to resume to
            # - created in block and returned
            #     comes for free because we save the return values. there is
            #     the problem that if the original Env disappears we might need to 
            #     rerun it, but we can figure that out later
            # - passed from outside block and not returned
            #     we Want to set the hash of the env, because it might be used 
            #     elsewhere by the caller. global tracking would also be useful here
            # - passed from outside block and returned
            #     same as last case, and we can basically ignore the return
            # - global from outside block and not returned
            #     it would be bad if we skip this block but don't update the hash of 
            #     the env, because it might be used elsewhere by the caller, and then 
            #     the hash will be all wrong.
            # - global from outside block and returned
            #     same as last case, except it's slightly easier to detect since we 
            #     can parse the output

            # set hashes for envs
            changed_hashes, ret = local.get_kv(overall_input_hash)

            envs_by_ip_envname = global_env_registry.envs_by_ip_envname()

            for ip, envname, orighash, newhash in changed_hashes:
                env = envs_by_ip_envname[(ip, envname)]
                if env.hash != orighash:
                    print(f"WARNING: expected env {ip}:{envname} to have hash {orighash} but got {env.hash}! Did the ip change?")
                env.hash = newhash
                
            print(f"Skipping block {fn.__name__}")
        except KeyError:
            ret = fn(*args, **kwargs)

            global_hashes_after = global_env_registry.hashes_by_env()

            # get the hashes that changed
            changed_hashes = [
                (k.ip, k.envname, global_hashes_before[k], global_hashes_after[k]) for k in global_hashes_after if global_hashes_before[k] != global_hashes_after[k]
            ]

            local.set_kv(overall_input_hash, (changed_hashes, ret))
        return ret
    return wrapper


class EnvRegistry:
    # everything here is O(n) but there shouldn't be a lot of envs so it's fine
    def __init__(self):
        self.envs = []
    
    def hashes_by_env(self):
        return {
            v: v.hash for v in self.envs
        }
    
    def envs_by_ip_envname(self):
        return {
            (v.ip, v.envname): v for v in self.envs
        }

    def register(self, env):
        self.envs.append(env)


local = Remote(wd=os.getcwd())
global_env_registry = EnvRegistry()