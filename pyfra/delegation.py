import os
import pathlib
import sys
from shlex import quote

import pyfra.remote
import pyfra.shell

__all__ = ["delegate"]


def delegate(experiment_name, rem, artifacts=[]):
    tmux_name = f"pyfra_delegated_{experiment_name}"
    if isinstance(rem, str): rem = pyfra.remote.Remote(rem)
    if isinstance(artifacts, str): artifacts = [artifacts]

    if is_delegated():
        return
    def _attach_tmux():
        rem.sh(f"tmux a -t {quote(tmux_name)}", maxbuflen=0, forward_keys=True)
    
    try:
        _attach_tmux()
    except pyfra.shell.ShellException:
        # todo: figure out how to forward ssh keys securely
        # the problem with ssh -A is that your remote is screwed if your original client goes offline, which totally
        # defeats the purpose of doing this in the first place. also there's something extremely weird going on
        # with tmux that makes it so that ssh forward won't carry over to the tmux session, but you can
        # fix that with `eval $(tmux show-env -s |grep '^SSH_')` inside the tmux, but that doesn't work with send-keys, it only works with 
        # actually running it by hand in the tmux for some reason. I've tried running it multiple times using send-keys,
        # adding a delay, adding a dummy ssh 127.0.0.1 command in between just to get ssh to use the ssh-agent to auth, etc and it jsut won't work. 
        # I'm not sure why, and to save my sanity for now I'm just going to require adding the right ssh keys to the delegated server manually.

        env = rem.env(tmux_name)

        ignore = [] if not pathlib.Path(".pyfraignore").exists() else pathlib.Path(".pyfraignore").read_text().strip().splitlines()
        env.sh(f"sudo apt install tmux -y; pip install -U git+https://github.com/EleutherAI/pyfra; pip install -r requirements.txt", ignore_errors=True)

        try:
            # allow sshing remote into itself
            rem_key = rem.path("~/.ssh/id_rsa.pub").read()
            if rem_key not in rem.path("~/.ssh/authorized_keys").read():
                rem.path("~/.ssh/authorized_keys").write(rem_key, append=True)
        except pyfra.shell.ShellException:
            print("WARNING: couldn't add self-key to server")
        
        pyfra.shell.copy(pyfra.remote.local.path("."), env.path("."), into=False, exclude=ignore)
        
        with pyfra.remote.force_run():
            env.sh(f"tmux new-session -d -s {quote(tmux_name)}")

        # cmd = f"{cmd} || ( eval $(tmux show-env -s |grep '^SSH_'); {cmd} )"
        cmd = f"pyenv shell {env.pyenv_version}" if env.pyenv_version is not None else ""
        cmd += f";[ -f env/bin/activate ] && . env/bin/activate; "
        cmd += f"PYFRA_DELEGATED=1 PYFRA_DELEGATED_TO={quote(rem.ip)} python "+" ".join([quote(x) for x in sys.argv])
        
        with pyfra.remote.force_run():
            env.sh(f"tmux send-keys -t {quote(tmux_name)} {quote(cmd)} Enter")
        
        _attach_tmux()

    if artifacts: print("Copying artifacts")
    for pattern in artifacts:
        for path in env.path(".").glob(pattern):
            if path.fname.endswith(".pyfra_env_state.json"): continue
            pyfra.shell.copy(path, pyfra.remote.local.path("."), exclude=ignore)

    sys.exit(0)


def is_delegated():
    return "PYFRA_DELEGATED" in os.environ