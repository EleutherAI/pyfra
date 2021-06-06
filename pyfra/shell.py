import errno
import os
import random
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


class ShellException(Exception): pass


__all__ = ['sh', 'rsync', 'ls', 'rm', 'curl', 'wget', 'quote']


def _wrap_command(x, no_venv=False, pyenv_version=None):
    bashrc_payload = r"""import sys,re; print(re.sub("If not running interactively.{,128}?esac", "", sys.stdin.read(), flags=re.DOTALL).replace('[ -z "$PS1" ] && return', ''))"""
    hdr = f"ctrlc() {{ echo Shell wrapper interrupted with C-c, raising error; exit 174; }}; trap ctrlc SIGINT; "
    hdr += f"eval \"$(cat ~/.bashrc | python3 -c {bashrc_payload | quote})\"  > /dev/null 2>&1; "
    if pyenv_version is not None: hdr += f"pyenv shell {pyenv_version}  > /dev/null 2>&1; "
    if not no_venv: hdr += "[ -f env/bin/activate ] && . env/bin/activate; "
    return hdr + x


def _sh(cmd, quiet=False, wd=None, wrap=True, maxbuflen=1000000000, ignore_errors=False, no_venv=False, pyenv_version=None):
    if wrap: cmd = _wrap_command(cmd, no_venv=no_venv, pyenv_version=pyenv_version)

    if wd is None: wd = "~"

    cmd = f"cd {wd}; {cmd}"

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


def _rsh(host, cmd, quiet=False, wd=None, wrap=True, maxbuflen=1000000000, connection_timeout=10, ignore_errors=False, no_venv=False, pyenv_version=None):
    if host is None or host == "localhost": host = "127.0.0.1"

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
        cmd_fmt = cmd.strip().replace('\n', f'\n{ " " * (len(str(host)) + 1 + len(wd_display))}{sep_style}> {Style.RESET_ALL}{cmd_style} ')
        print(f"{host_style}{hoststr}{Style.RESET_ALL}{sep_style}$ {Style.RESET_ALL}{cmd_style}{cmd_fmt}{Style.RESET_ALL}")
    
    if host == "127.0.0.1":
        return _sh(cmd, quiet, wd, wrap, maxbuflen, ignore_errors, no_venv, pyenv_version)

    if wrap: cmd = _wrap_command(cmd, no_venv=no_venv, pyenv_version=pyenv_version)
    if wd: cmd = f"cd {wd}; {cmd}"

    return _sh(f"ssh -q -oConnectTimeout={connection_timeout} -oBatchMode=yes -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -t {host} {shlex.quote(cmd)}", quiet=quiet, wrap=False, maxbuflen=maxbuflen, ignore_errors=ignore_errors, no_venv=no_venv)

def rsync(frm, to, quiet=False, connection_timeout=10, symlink_ok=True, into=True):
    """
    Copies things from one place to another.

    Args:
        frm (str or RemoteFile): Can be a string indicating a local path, a :class:`pyfra.remote.RemoteFile`, or a URL.
        to (str or RemoteFile): Can be a string indicating a local path or a :class:`pyfra.remote.RemoteFile`.
        quiet (bool): Disables logging.
        connection_timeout (int): How long in seconds to give up after
        symlink_ok (bool): If frm and to are on the same machine, symlinks will be created instead of actually copying. Set to false to force copying.
        into (bool): If frm is a file, this has no effect. If frm is a directory, then into=True for frm="src" and to="dst" means "src/a" will get copied to "dst/src/a", whereas into=False means "src/a" will get copied to "dst/a".
    """
    frm = str(frm)
    to = str(to)

    # copy from url
    if frm.startswith("http://") or frm.startswith("https://"):
        if ":" in to:
            to_host, to_path = to.split(":")
            _rsh(to_host, f"curl {frm} --create-dirs -o {to}")
        else:
            wget(frm, to)
        return

    if frm[-1] == '/' and len(frm) > 1: frm = frm[:-1]
    if not into: frm += '/'

    if quiet:
        opts = "-e \"ssh -o StrictHostKeyChecking=no\" -arq"
    else:
        opts = "-e \"ssh -o StrictHostKeyChecking=no\" -ar --info=progress2"
    
    def symlink_frm(frm):
        # rsync behavior is to copy the contents of frm into to if frm ends with a /
        if frm[-1] == '/': frm += '*'
        # ln -s can't handle relative paths well! make absolute if not already
        if frm[0] != '/' and frm[0] != '~': frm = "$PWD/" + frm

        return frm

    if ":" in frm and ":" in to:
        frm_host, frm_path = frm.split(":")
        to_host, to_path = to.split(":")

        if to_host == frm_host:
            if symlink_ok:
                _rsh(frm_host, f"[ -d {frm_path} ] && mkdir -p {to_path}; ln -sf {symlink_frm(frm_path)} {to_path}")
            else:
                _rsh(frm_host, f"rsync {opts} {frm_path} {to_path}")
        else:
            rsync_cmd = f"rsync {opts} {frm_path} {to}"
                
            sh(f"eval \"$(ssh-agent -s)\"; ssh-add ~/.ssh/id_rsa; ssh -q -oConnectTimeout={connection_timeout} -oBatchMode=yes -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -A {frm_host} {rsync_cmd | quote}", wrap=False)
    else:
        if symlink_ok and ":" not in frm and ":" not in to:
            sh(f"[ -d {frm} ] && mkdir -p {to}; ln -sf {symlink_frm(frm)} {to}")
        else:
            sh(f"rsync {opts} {frm} {to}", wrap=False)

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

# convenience function for shlex.quote
class _quote:
    def __ror__(self, other):
        return shlex.quote(other)
    
    def __call__(self, other):
        return shlex.quote(other)

quote = _quote()
