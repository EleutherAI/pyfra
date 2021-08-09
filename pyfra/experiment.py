import os
from shlex import quote
import sys

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
        if isinstance(rem, str): rem = self.remote(rem)
    
        if "PYFRA_DELEGATED" in os.environ:
            return
        
        env = rem.env(self.experiment_name)

        pyfra.shell.copy(pyfra.remote.local.path("."), env.path("."), into=False)
        env.sh(f"sudo apt install tmux -y; pip install -U pyfra; pip install -r requirements.txt; tmux new-session -d -s {quote(self.experiment_name)}", ignore_errors=True)

        cmd = pyfra.shell._wrap_command("PYFRA_DELEGATED=1 python "+" ".join([quote(x) for x in sys.argv]), pyenv_version=env.pyenv_version)
        env.sh(f"tmux send-keys -t {quote(self.experiment_name)} {quote(cmd)} Enter")
        env.sh(f"tmux a -t {quote(self.experiment_name)}", maxbuflen=0, forward_keys=True)
        sys.exit(0)
