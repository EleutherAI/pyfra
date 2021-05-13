import subprocess
import os
import sys
import urllib
import time
import shutil
import shlex
import errno
import re


from best_download import download_file


class ShellException(Exception): pass


__all__ = ['sh', 'rsh', 'rsync', 'ls', 'rm', 'mv', 'curl', 'wget', 'quote', 'columns']


def _wrap_command(x, no_venv=False, pyenv_version=None):
    bashrc_payload = r"""import sys,re; print(re.sub("If not running interactively.{,128}?esac", "", sys.stdin.read(), flags=re.DOTALL).replace('[ -z "$PS1" ] && return', ''))"""
    hdr = f"ctrlc() {{ echo Shell wrapper interrupted with C-c, raising error; exit 174; }}; trap ctrlc SIGINT; "
    hdr += f"eval \"$(cat ~/.bashrc | python3 -c {bashrc_payload | quote})\"  1>&2; "
    if pyenv_version is not None: hdr += f"pyenv shell {pyenv_version} ; "
    if not no_venv: hdr += "[ -f env/bin/activate ] && . env/bin/activate; "
    return hdr + x


def sh(cmd, quiet=False, wd=None, wrap=True, maxbuflen=1000000000, ignore_errors=False, no_venv=False, pyenv_version=None):

    if wrap: x = _wrap_command(cmd, no_venv=no_venv, pyenv_version=pyenv_version)
    if wd: x = f"cd {wd}; {cmd}"

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

def rsh(host, cmd, quiet=False, wd=None, wrap=True, maxbuflen=1000000000, connection_timeout=10, ignore_errors=False, no_venv=False, pyenv_version=None):
    if not quiet: print(f"Connecting to {host}.")

    if wrap: cmd = _wrap_command(cmd, no_venv=no_venv, pyenv_version=pyenv_version)
    if wd: cmd = f"cd {wd}; {cmd}"

    return sh(f"ssh -q -oConnectTimeout={connection_timeout} -oBatchMode=yes -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -t {host} {shlex.quote(cmd)}", quiet=quiet, wrap=False, maxbuflen=maxbuflen, ignore_errors=ignore_errors, no_venv=no_venv)

def rsync(frm, to, quiet=False, connection_timeout=10):
    frm = repr(frm)
    to = repr(to)

    if quiet:
        opts = "-arq"
    else:
        opts = "-ar --info=progress2"

    if ":" in frm and ":" in to:
        frm_host, frm_path = frm.split(":")
        to_host, to_path = to.split(":")

        if to_host == frm_host:
            rsh(frm_host, f"rsync {opts} {frm_path} {to_path}")
        else:
            sh(f"eval \"$(ssh-agent -s)\"; ssh-add ~/.ssh/id_rsa; ssh -q -oConnectTimeout={connection_timeout} -oBatchMode=yes -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -A {frm_host} rsync {opts} {frm_path} {to}", wrap=False)
    else:
        sh(f"rsync {opts} {frm} {to}", wrap=False)

def ls(x='.'):
    return [x + '/' + fn for fn in os.listdir(x)]

def rm(x, no_exists_ok=True):
    # from https://stackoverflow.com/a/41789397
    if not os.path.exists(x) and no_exists_ok: return

    if os.path.isfile(x) or os.path.islink(x):
        os.remove(x)  # remove the file
    elif os.path.isdir(x):
        shutil.rmtree(x)  # remove dir and all contains
    else:
        raise ValueError("file {} is not a file or dir.".format(x))

#DEPRECATED
# alias shutil function
mv = shutil.move

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
    # thin wrapper for best_download

    if to is None:
        to = os.path.basename(to)
        if not to: to = 'index'

    download_file(url, to, checksum)

def columns(x):
    return re.split((r'\s+'), x)

# convenience function for shlex.quote
class _quote:
    def __ror__(self, other):
        return shlex.quote(other)
    
    def __call__(self, other):
        return shlex.quote(other)

quote = _quote()
