# pyfra

*The Python Research Framework.*


The objective of pyfra is to make it as *low-friction* as possible to write research code involving complex pipelines over many machines. You'll never have to have a nest of hard-to-maintain bash scripts again!

To make the interface intuitive, many functions are named after common shell commands.

**Pyfra is in its very early stages of development. The interface may change rapidly and without warning.**


Current features:

 - Spin up an internal webserver complete with a permissions system using only a few lines of code
 - Extremely elegant shell integration—run commands on any server seamlessly. All the best parts of bash and python combined
 - (Coming soon) Tools for painless functional programming in python
 - (Coming soon) Automated remote environment setup, so you never have to worry about provisioning machines by hand again
 - (Coming soon) High level API for experiment management/scheduling and resource provisioning

## Example code

```
from pyfra import *

loc = Remote()
rem = Remote("user@example.com")
nas = Remote("user@example2.com")

@page("Run experiment", dropdowns={'server': ['local', 'remote']})
def run_experiment(server: str, config_file: str):
    r = loc if server == 'local' else rem

    r.sh("git clone https://github.com/EleutherAI/gpt-neox")
    
    # rsync as a function can do local-local, local-remote, and remote-remote
    rsync(config_file, r.file("gpt-neox/configs/my-config.yml"))
    rsync(nas.file('some_data_file'), r.file('gpt-neox/data/whatever'))
    
    return r.sh('cd gpt-neox; python3 main.py')

@page("Write example file and copy")
def example():
    rem.fwrite("testing.txt", "hello world")
    
    # tlocal files can be specified as just a string
    rsync(rem.file('testing123.txt'), 'test1.txt')
    rsync(rem.file('testing123.txt'), loc.file('test2.txt'))

    loc.sh('cat test1.txt')
    
    assert fread('test1.txt') == fread('test2.txt')
    
    # fread, fwrite, etc can take a `rem.file` instead of a string filename.
    # you can also use all *read and *write functions directly on the remote too.
    assert fread('test1.txt') == fread(rem.file('testing123.txt'))
    assert fread('test1.txt') == rem.fread('testing123.txt')

    # ls as a function returns a list of files (with absolute paths) on the selected remote.
    # the returned value is displayed on the webpage.
    return '\n'.join(rem.ls('/'))

# start internal webserver
webserver()
```

## Installation

```pip3 install git+https://github.com/EleutherAI/pyfra/```

The version of PyPI is not up to date, do not use it.
