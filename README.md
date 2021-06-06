# pyfra

*The Python Framework for Research Applications.*

[![Documentation Status](https://readthedocs.org/projects/pyfra/badge/?version=latest)](https://pyfra.readthedocs.io/en/latest/?badge=latest)
      

## Design Philosophy

Research code has some of the fastest shifting requirements of any type of code. It's nearly impossible to plan ahead of time the proper abstractions, because it is exceedingly likely that in the course of the project what you originally thought was your main focus suddenly no longer is. Further, research code (especially in ML) often involves big and complicated pipelines, typically involving many different machines, which are either run by hand or using shell scripts that are far more complicated than any shell script ever should be. 

Therefore, the objective of pyfra is to make it as fast and *low-friction* as possible to write research code involving complex pipelines over many machines. This entails making it as easy as possible to implement a research idea in reality, at the cost of fine-grained control and the long-term maintainability of the system. In other words, pyfra expects that code will either be rapidly obsoleted by newer code, or rewritten using some other framework once it is no longer a research project and requirements have settled down.

**Pyfra is in its very early stages of development. The interface may change rapidly and without warning.**

Features:

 - Spin up an internal webserver complete with a permissions system using only a few lines of code
 - Extremely elegant shell integration—run commands on any server seamlessly. All the best parts of bash and python combined
 - Automated remote environment setup, so you never have to worry about provisioning machines by hand again
 - (WIP) Tools for painless functional programming in python
 - (Coming soon) High level API for experiment management/scheduling and resource provisioning
 - (Coming soon) Idempotent resumable data pipelines with no cognitive overhead

Want to dive in? See the [documentation](https://pyfra.readthedocs.io/en/latest/).

## Example code

```python
from pyfra import *

loc = Remote()
rem = Remote("user@example.com")
nas = Remote("user@example2.com")

@page("Run experiment", dropdowns={'server': ['local', 'remote']})
def run_experiment(server: str, config_file: str, some_numerical_value: int, some_checkbox: bool):
    r = loc if server == 'local' else rem

    env = r.env("neox", "https://github.com/EleutherAI/gpt-neox")
    
    # rsync as a function can do local-local, local-remote, and remote-remote
    rsync(config_file, env.file("configs/my-config.yml"))
    rsync(nas.file('some_data_file'), env.file('data/whatever'))
    
    return env.sh('python main.py')

@page("Write example file and copy")
def example():
    rem.file("testing.txt").fwrite("hello world")
    
    # tlocal files can be specified as just a string
    rsync(rem.file('testing123.txt'), 'test1.txt')
    rsync(rem.file('testing123.txt'), loc.file('test2.txt'))

    loc.sh('cat test1.txt')
    
    assert loc.file('test1.txt').read() == loc.file('test2.txt').read()
    assert loc.file('test1.txt').read() == rem.file('testing123.txt').read()

    # ls as a function returns a list of files (with absolute paths) on the selected remote.
    # the returned value is displayed on the webpage.
    return '\n'.join(rem.ls('/'))

@page("List files in some directory")
def list_files(directory):
    return sh(f"ls -la {directory | quote}")


# start internal webserver
webserver()
```

## Installation

```pip3 install pyfra```


## Webserver screenshots

![image](https://user-images.githubusercontent.com/54557097/119907788-4a2f6700-bf0e-11eb-9655-55e3317ba871.png)
![image](https://user-images.githubusercontent.com/54557097/115158135-fc3f5d80-a049-11eb-8310-a43b7b5c58e0.png)
