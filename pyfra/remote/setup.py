from pyfra.utils.misc import once
from pyfra.utils.shell import *
from functools import wraps, partial


# DEPRECATED
def install(x=[]):
    deps = x
    if callable(deps):
        deps = []

    def _decorator(f):
        @wraps(f)
        def _f(rem):
            print(">>> Installing dependencies for", f.__name__)
            for dep in [ensure_supported] + deps:
                dep(rem)
        
            print()
            print(">>>")
            print(">>> Installing", f.__name__)
            print(">>>")
            print()

            return once(partial(f, rem), name=f.__name__ + ";" + rem.fingerprint)()
        
        return _f
    
    if callable(x):
        return _decorator(x)
    return _decorator


def apt(r, packages):
    r.sh(f"sudo apt-get install -y {' '.join(packages)}")


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
        'libbz2-dev',
        'libffi-dev',
        'libreadline-dev',
        'libsqlite3-dev',
        'libssl-dev',
        'make',
        'python3-openssl',
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