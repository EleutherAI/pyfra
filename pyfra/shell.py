import json
import pathlib
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
import urllib

from best_download import download_file
from colorama import Fore, Style
from natsort import natsorted

import imohash
import pyfra.remote


class ShellException(Exception): pass


__all__ = ['sh', 'copy', 'ls', 'curl', 'quote', 'ShellException']


def _wrap_command(x, no_venv=False, pyenv_version=None):
    bashrc_payload = r"""import sys,re; print(re.sub("If not running interactively.{,128}?esac", "", sys.stdin.read(), flags=re.DOTALL).replace('[ -z "$PS1" ] && return', ''))"""
    hdr = f"shopt -s expand_aliases; ctrlc() {{ echo Shell wrapper interrupted with C-c, raising error; exit 174; }}; trap ctrlc SIGINT; "
    hdr += f"eval \"$(cat ~/.bashrc | python3 -c {bashrc_payload | quote})\"  > /dev/null 2>&1; "
    hdr += "python() { python3 \"$@\"; };" # use python3 by default
    if pyenv_version is not None: hdr += f"pyenv shell {pyenv_version} || exit 1 > /dev/null 2>&1; "
    if not no_venv: hdr += "[ -f env/bin/activate ] && . env/bin/activate; "
    return hdr + x


def _process_remotepaths(host, cmd):
    candidates = re.findall(r"RemotePath\((.+?)\)", cmd)

    rempaths = []

    for c in candidates:
        ob = json.loads(c)
        rem = ob["remote"] if ob["remote"] is not None else "127.0.0.1"
        fname = ob["fname"]

        if rem != host:
            loc_fname = rem.replace(".", "_").replace("@", "_")+"_"+fname.split("/")[-1]
            if loc_fname.startswith("~/"): loc_fname = loc_fname[2:]
            if loc_fname.startswith("/"): loc_fname = loc_fname[1:]
            loc_fname = "~/.pyfra_remote_files/" + loc_fname

            copyerr = False
            try:
                frm = pyfra.remote.Remote(rem).path(fname)

                # we want to copy dirs into, but into doesnt work with files
                copy(frm, pyfra.remote.Remote(host).path(loc_fname), into=not frm.is_dir())
            except ShellException:
                # if this file doesn't exist, it's probably an implicit return
                copyerr = True

            rempaths.append((pyfra.remote.Remote(rem).path(fname), pyfra.remote.Remote(host).path(loc_fname), copyerr))

            cmd = cmd.replace(f"RemotePath({c})", loc_fname)
        else:
            cmd = cmd.replace(f"RemotePath({c})", fname)
    
    return cmd, rempaths


def _sh(cmd, quiet=False, wd=None, wrap=True, maxbuflen=1000000000, ignore_errors=False, no_venv=False, pyenv_version=None):
    if wrap: cmd = _wrap_command(cmd, no_venv=no_venv, pyenv_version=pyenv_version)

    if wd is None: wd = "~"

    cmd = f"cd {wd} > /dev/null 2>&1; {cmd}"

    p = subprocess.Popen(cmd, shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        executable="/bin/bash")
    
    ret = bytearray()
    while True:
        byte = p.stdout.read(1)

        if byte == b'':
            break
        if not quiet:
            sys.stdout.buffer.write(byte)
            sys.stdout.flush()

        if maxbuflen is None or len(ret) < maxbuflen:
            ret += bytearray(byte)
    
    p.communicate()
    if p.returncode == 174:
        raise KeyboardInterrupt()
    elif p.returncode != 0 and not ignore_errors:
        raise ShellException(p.returncode)

    return ret.decode("utf-8").replace("\r\n", "\n").strip()


def sh(cmd, quiet=False, wd=None, wrap=True, maxbuflen=1000000000, ignore_errors=False, no_venv=False, pyenv_version=None):
    """
    Runs commands as if it were in a local bash terminal.

    This function patches out the non-interactive detection in bashrc and sources it, activates virtualenvs and sets pyenv shell, handles interrupts correctly, and returns the text printed to stdout.

    Args:
        quiet (bool): If turned on, nothing is printed to stdout.
        wd (str): Working directory to run in. Defaults to ~
        wrap (bool): Magic for the bashrc, virtualenv, interrupt-handing, and pyenv stuff. Turn off to make this essentially os.system
        maxbuflen (int): Max number of bytes to save and return. Useful to prevent memory errors.
        ignore_errors (bool): If set, errors will be swallowed.
        no_venv (bool): If set, virtualenv will not be activated
        pyenv_version (str): Pyenv version to use. Will be silently ignored if not found.
    Returns:
        The standard output of the command, limited to maxbuflen bytes.
    """
    if wd is None: wd = os.getcwd()

    return _rsh("127.0.0.1", cmd, quiet, wd, wrap, maxbuflen, -1, ignore_errors, no_venv, pyenv_version)


def _rsh(host, cmd, quiet=False, wd=None, wrap=True, maxbuflen=1000000000, connection_timeout=10, ignore_errors=False, no_venv=False, pyenv_version=None, forward_keys=False):
    if host is None or host == "localhost": host = "127.0.0.1"

    # implicit-copy files from remote to local
    cmd, rempaths = _process_remotepaths(host, cmd)

    if not quiet:
        # display colored message
        host_style = Fore.GREEN+Style.BRIGHT
        sep_style = Style.NORMAL
        cmd_style = Fore.WHITE+Style.BRIGHT
        dir_style = Fore.BLUE+Style.BRIGHT
        hoststr = str(host)
        if wd is not None: 
            wd_display = wd
            if not wd.startswith("~/") and wd != '~':
                wd_display = os.path.join("~", wd)
        else:
            wd_display = "~"
        hoststr += f"{Style.RESET_ALL}:{dir_style}{wd_display}{Style.RESET_ALL}"
        cmd_fmt = cmd.strip().replace('\n', f'\n{ " " * (len(str(host)) + 3 + len(wd_display))}{sep_style}>{Style.RESET_ALL}{cmd_style} ')
        print(f"{Style.BRIGHT}{Fore.RED}*{Style.RESET_ALL} {host_style}{hoststr}{Style.RESET_ALL}{sep_style}$ {Style.RESET_ALL}{cmd_style}{cmd_fmt}{Style.RESET_ALL}")
    
    if host == "127.0.0.1":
        return _sh(cmd, quiet, wd, wrap, maxbuflen, ignore_errors, no_venv, pyenv_version)

    if wrap: cmd = _wrap_command(cmd, no_venv=no_venv, pyenv_version=pyenv_version)
    if wd: cmd = f"cd {wd}  > /dev/null 2>&1; {cmd}"
 
    ssh_cmd = "eval \"$(ssh-agent -s)\"; ssh-add ~/.ssh/id_rsa; ssh -A" if forward_keys else "ssh"
    ret = _sh(f"{ssh_cmd} -q -oConnectTimeout={connection_timeout} -oBatchMode=yes -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -t {host} {shlex.quote(cmd)}", quiet=quiet, wrap=False, maxbuflen=maxbuflen, ignore_errors=ignore_errors, no_venv=no_venv)

    # implicit-copy files from local to remote
    for remf, locf, copyerr in rempaths:
        try:
            copy(locf, remf)
        except ShellException:
            # if it errored both before and after, something is wrong
            if copyerr:
                raise ShellException(f"implicit-copy file {remf}/{locf} (remote/local) was neither written to nor read from!")

    return ret

def copy(frm, to, quiet=False, connection_timeout=10, symlink_ok=True, into=True, exclude=[]) -> None:
    """
    Copies things from one place to another.

    Args:
        frm (str or RemotePath): Can be a string indicating a local path, a :class:`pyfra.remote.RemotePath`, or a URL.
        to (str or RemotePath): Can be a string indicating a local path or a :class:`pyfra.remote.RemotePath`.
        quiet (bool): Disables logging.
        connection_timeout (int): How long in seconds to give up after
        symlink_ok (bool): If frm and to are on the same machine, symlinks will be created instead of actually copying. Set to false to force copying.
        into (bool): If frm is a file, this has no effect. If frm is a directory, then into=True for frm="src" and to="dst" means "src/a" will get copied to "dst/src/a", whereas into=False means "src/a" will get copied to "dst/a".
    """

    # copy from url
    if isinstance(frm, str) and (frm.startswith("http://") or frm.startswith("https://")):
        if ":" in to:
            to_host, to_path = to.split(":")
            _rsh(to_host, f"curl {frm} --create-dirs -o {to_path}")
        else:
            wget(frm, to)
        return

    # get rsync strs and make sure frm and to are RemotePaths
    if isinstance(frm, pyfra.remote.RemotePath): 
        frm_str = frm.rsyncstr()
    else:
        frm_str = frm
        assert ":" not in frm_str
        frm = pyfra.remote.local.path(frm)

    if isinstance(to, pyfra.remote.RemotePath):
        to_str = to.rsyncstr()
    else:
        to_str = to
        assert ":" not in to_str
        to = pyfra.remote.local.path(to)
    
    # state tracking
    needs_set_kv = False
    if not to.remote._no_hash:
        with to.remote.no_hash():
            checksum = frm.quick_hash()
            new_hash = to.remote.update_hash("copy", to.fname, checksum)
            try:
                to.remote.get_kv(new_hash)
                # if already copied, then return
                to._set_cache("quick_hash", checksum) # set the checksum of the target file to avoid needing to calculate it again
                pyfra.remote._print_skip_msg(to.remote.envname, "copy", new_hash)

                return
            except KeyError:
                needs_set_kv = True

    # print info
    if not quiet: print(f"{Style.BRIGHT}{Fore.RED}*{Style.RESET_ALL} Copying {Style.BRIGHT}{frm_str} {Style.RESET_ALL}to {Style.BRIGHT}{to_str}{Style.RESET_ALL}")

    if frm_str[-1] == '/' and len(frm_str) > 1: frm_str = frm_str[:-1]
    if not into: frm_str += '/'

    if quiet:
        opts = "-e \"ssh -o StrictHostKeyChecking=no\" -arqL"
    else:
        opts = "-e \"ssh -o StrictHostKeyChecking=no\" -arL --info=progress2"
    
    for ex in exclude:
        opts += f" --exclude {ex | pyfra.shell.quote}"
    
    def symlink_frm(frm_str):
        # rsync behavior is to_str copy the contents of frm_str into to_str if frm_str ends with a /
        if frm_str[-1] == '/': frm_str += '*'
        # ln -s can't handle relative paths well! make absolute if not already
        if frm_str[0] != '/' and frm_str[0] != '~': frm_str = "$PWD/" + frm_str

        return frm_str

    if ":" in frm_str and ":" in to_str:
        frm_host, frm_path = frm_str.split(":")
        to_host, to_path = to_str.split(":")

        par_target = to_path.rsplit('/', 1)[0] if "/" in to_path else ""

        if to_host == frm_host:
            if symlink_ok:
                assert not exclude, "Cannot use exclude symlink"
                _rsh(frm_host, f"[ -d {frm_path} ] && mkdir -p {to_path}; ln -sf {symlink_frm(frm_path)} {to_path}", quiet=True)
            else:

                if par_target: _rsh(to_host, f"mkdir -p {par_target}", quiet=True)
                _rsh(frm_host, f"rsync {opts} {frm_path} {to_path}", quiet=True)
        else:
            rsync_cmd = f"rsync {opts} {frm_path} {to_str}"
                
            # make parent dir in terget if not exists
            if par_target: _rsh(to_host, f"mkdir -p {par_target}", quiet=True)

            sh(f"eval \"$(ssh-agent -s)\"; ssh-add ~/.ssh/id_rsa; ssh -q -oConnectTimeout={connection_timeout} -oBatchMode=yes -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -A {frm_host} {rsync_cmd | quote}", wrap=False, quiet=True)
    else:
        # if to_str is host:path, then this gives us path; otherwise, it leaves it unchanged
        par_target = to_str.split(":")[-1]
        par_target = par_target.rsplit('/', 1)[0] if "/" in par_target else ""

        if symlink_ok and ":" not in frm_str and ":" not in to_str:
            assert not exclude, "Cannot use exclude symlink"
            sh(f"[ -d {frm_str} ] && mkdir -p {par_target}; ln -sf {symlink_frm(frm_str)} {to_str}", quiet=True)
        else:
            if ":" in to_str: _rsh(to_str.split(":")[0], f"mkdir -p {par_target}", quiet=True)
            sh((f"mkdir -p {par_target}; " if par_target and ":" in frm_str else "") + f"rsync {opts} {frm_str} {to_str}", wrap=False, quiet=True)
    
    # set value in key value store to flag as done
    if needs_set_kv:
        to.remote.set_kv(new_hash, None)
        to._set_cache("quick_hash", checksum) # set the checksum of the target file to avoid needing to calculate it again


def ls(x='.'):
    return list(natsorted([x + '/' + fn for fn in os.listdir(x)]))

def rm(x, no_exists_ok=True):
    # from https://stackoverflow.com/a/41789397
    if not os.path.exists(x) and no_exists_ok: return

    if os.path.isfile(x) or os.path.islink(x):
        os.remove(x)  # remove the file
    elif os.path.isdir(x):
        shutil.rmtree(x)  # remove dir and all contains
    else:
        raise ValueError("file {} is not a file or dir.".format(x))

def curl(url, max_tries=10, timeout=30): # TODO: add checksum option
    cooldown = 1
    for i in range(max_tries):
        try:
            response = urllib.request.urlopen(url, timeout=timeout)
            data = response.read()
        except urllib.error.URLError as e:
            if e.code == 429:
                time.sleep(cooldown)
                cooldown *= 1.5
                continue
            else:
                return None
        except:
            return None
        return data

def wget(url, to=None, checksum=None):
    # DEPRECATED
    # thin wrapper for best_download

    if to is None:
        to = os.path.basename(url)
        if not to: to = 'index'

    download_file(url, to, checksum)

def quick_hash(path):
    params = {
        "hexdigest": True,
        "sample_size": 4 * 1024**2, # 4 MB
        "sample_threshhold": 16 * 1024**2, # 16 MB
    }
    path = os.path.expanduser(path)
    if pathlib.Path(path).is_dir():
        files = list(sorted(pathlib.Path(path).glob('**/*')))
        res = pyfra.remote._hash_obs(*[(str(f.relative_to(pathlib.Path(path))), imohash.hashfile(str(f.resolve()), **params)) for f in files if f.is_file()])[:32]
        return res
    return imohash.hashfile(path, **params)

# convenience function for shlex.quote
class _quote:
    def __ror__(self, other):
        return shlex.quote(other)
    
    def __call__(self, other):
        return shlex.quote(other)

quote = _quote()
