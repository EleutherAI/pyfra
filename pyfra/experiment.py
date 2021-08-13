import os
from shlex import quote
import sys
import pathlib
import time

import pyfra.shell
import pyfra.remote

class Experiment:
    def __init__(self, experiment_name, experiment_server=None):
        self.experiment_name = experiment_name
        
        if experiment_server is not None:
            self.delegate(experiment_server)

    def delegate(self, rem):
        tmux_name = f"pyfra_delegated_{self.experiment_name}"
        if isinstance(rem, str): rem = self.remote(rem)
    
        if "PYFRA_DELEGATED" in os.environ:
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
            pyfra.shell.copy(pyfra.remote.local.path("."), env.path("."), into=False, exclude=ignore)

            env.sh(f"sudo apt install tmux -y; pip install -U git+https://github.com/EleutherAI/pyfra; pip install -r requirements.txt; tmux new-session -d -s {quote(tmux_name)}", ignore_errors=True)
            # cmd = f"{cmd} || ( eval $(tmux show-env -s |grep '^SSH_'); {cmd} )"
            cmd = f"pyenv shell {env.pyenv_version}" if env.pyenv_version is not None else ""
            cmd += f";[ -f env/bin/activate ] && . env/bin/activate; "
            cmd += "PYFRA_DELEGATED=1 python "+" ".join([quote(x) for x in sys.argv])

            env.sh(f"tmux send-keys -t {quote(tmux_name)} {quote(cmd)} Enter")
            
            _attach_tmux()

        sys.exit(0)


class CachedBlock:
