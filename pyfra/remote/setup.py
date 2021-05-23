from pyfra.utils.misc import once
from pyfra.utils.shell import *
from functools import wraps, partial


def apt(r, packages):
    # install sudo if it's not installed; this is the case in some docker containers
    r.sh(f"sudo echo hi || {{ apt-get update; apt-get install sudo; }}; sudo apt-get update; sudo apt-get install -y {' '.join(packages)}")


def ensure_supported(r):
    supported = [
        "Ubuntu 18", "Ubuntu 20", 
        "stretch"   # debian stretch
    ]
    def _f(r):
        print("Checking if", r, "is running a supported distro")

        assert any([
            ver in r.sh("lsb_release -d")
            for ver in supported
        ])

    once(partial(_f, r), name="ensure_supported;" + r.fingerprint)()

## things to install


def install_pyenv(r, version="3.9.4"):
    if r.sh(f"python --version", no_venv=True, ignore_errors=True).strip().split(" ")[-1] == version:
        return

    apt(r, [
        'build-essential',
        'curl',
        'git',
        'libbz2-dev',
        'libffi-dev',
        'liblzma-dev',
        'libncurses5-dev',
        'libncursesw5-dev',
        'libreadline-dev',
        'libsqlite3-dev',
        'libssl-dev',
        'make',
        'python3-openssl',
        'rsync',
        'tk-dev',
        'wget',
        'xz-utils',
        'zlib1g-dev',
    ])
    r.sh("curl https://pyenv.run | bash", ignore_errors=True)

    payload = """
# pyfra-managed: pyenv stuff
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init --path)"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
"""
    bashrc = r.sh("cat ~/.bashrc")

    if "# pyfra-managed: pyenv stuff" not in bashrc:
        r.sh(f"echo {payload | quote} >> ~/.bashrc")

    # install updater
    r.sh("git clone https://github.com/pyenv/pyenv-update.git $(pyenv root)/plugins/pyenv-update", ignore_errors=True)
    r.sh("pyenv update", ignore_errors=True)

    r.sh(f"pyenv install --verbose -s {version}")

    # make sure the versions all check out
    assert r.sh(f"python --version", no_venv=True).strip().split(" ")[-1] == version
    assert r.sh(f"python3 --version", no_venv=True).strip().split(" ")[-1] == version
    assert version.rsplit('.', 1)[0] in r.sh("pip --version", no_venv=True)
    assert version.rsplit('.', 1)[0] in r.sh("pip3 --version", no_venv=True)
    
    r.sh("pip install virtualenv")
    r.sh("virtualenv --version")


def setup_overall(r):
    pyenv(r)