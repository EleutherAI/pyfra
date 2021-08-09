import os
from shlex import quote
import sys
import pathlib

import pyfra.shell
import pyfra.remote

class Experiment:
    def __init__(self, experiment_name, experiment_server=None):
        self.experiment_name = experiment_name
        self.remotes = []
        
        if experiment_server is not None:
            self.delegate(experiment_server)
    
    def remote(self, ip, wd=None):
        r = pyfra.remote.Remote(ip, wd, self)
        self.remotes.append(r)
        return r

    def delegate(self, rem):
        tmux_name = f"pyfra_delegated_{self.experiment_name}"
        if isinstance(rem, str): rem = self.remote(rem)
    
        if "PYFRA_DELEGATED" in os.environ:
            return
        
        env = rem.env(tmux_name)

        ignore = [] if not pathlib.Path(".pyfraignore").exists() else pathlib.Path(".pyfraignore").read_text().strip().splitlines()
        pyfra.shell.copy(pyfra.remote.local.path("."), env.path("."), into=False, exclude=ignore)

        def _attach_tmux():
            env.sh(f"tmux a -t {quote(tmux_name)}", maxbuflen=0, forward_keys=True)
        
        try:
            _attach_tmux()
        except pyfra.shell.ShellException:
            env.sh(f"sudo apt install tmux -y; pip install -U pyfra; pip install -r requirements.txt; tmux new-session -d -s {quote(tmux_name)}", ignore_errors=True)
            cmd = pyfra.shell._wrap_command("eval $(tmux show-env -s |grep '^SSH_'); PYFRA_DELEGATED=1 python "+" ".join([quote(x) for x in sys.argv]), pyenv_version=env.pyenv_version)
            env.sh(f"tmux send-keys -t {quote(tmux_name)} {quote(cmd)} Enter")
            _attach_tmux()

        sys.exit(0)
