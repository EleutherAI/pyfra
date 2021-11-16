# pyfra

*The Python Framework for Research Applications.*

[![Documentation Status](https://readthedocs.org/projects/pyfra/badge/?version=latest)](https://pyfra.readthedocs.io/en/latest/?badge=latest)
      

## Design Philosophy

Research code has some of the fastest shifting requirements of any type of code. It's nearly impossible to plan ahead of time the proper abstractions, because it is exceedingly likely that in the course of the project what you originally thought was your main focus suddenly no longer is. Further, research code (especially in ML) often involves big and complicated pipelines, typically involving many different machines, which are either run by hand or using shell scripts that are far more complicated than any shell script ever should be. 

Therefore, the objective of pyfra is to make it as fast and *low-friction* as possible to write research code involving complex pipelines over many machines. This entails making it as easy as possible to implement a research idea in reality, at the cost of fine-grained control and the long-term maintainability of the system. In other words, pyfra expects that code will either be rapidly obsoleted by newer code, or rewritten using some other framework once it is no longer a research project and requirements have settled down.

As such, high quality interface design is a top priority of pyfra. The ultimate goal is to make interfaces that feel extremely natural, such that it is intuitively obvious what any piece of pyfra code should do, without requiring consultation of the docs. The core pyfra abstractions should be optimized, as much as possible, for _naturalness_; it should be easy to do the things that are frequently needed, but if making it so would require a sacrifice in generality, the abstraction should be decomposed into a maximally-general core and an application-specific layer on top. As is it expected that pyfra-based code is short lived, when trading off between API stability and cleaner/more intuitive interfaces, pyfra will attempt to choose the latter. That being said, core pyfra should become more and more stable as time progressese.

The `pyfra.contrib` package is intended for code that is built on top of pyfra and useful in conjunction with pyfra, but has fewer stability guarantees than core pyfra. Especially useful and well designed functions in contrib may be promoted to core pyfra.

**Pyfra is in its very early stages of development. The interface may change rapidly and without warning.**

Features:

 - Extremely elegant shell integration—run commands on any server seamlessly. All the best parts of bash and python combined
 - Handle files on remote servers with a pathlib-like interface the same way you would local files (WIP, partially implemented)
 - Automated remote environment setup, so you never have to worry about provisioning machines by hand again
 - Idempotent resumable data and training pipelines with no cognitive overhead
 - Spin up an internal webserver complete with a permissions system using only a few lines of code
 - (Coming soon) High level API for experiment management/scheduling and resource provisioning

Want to dive in? See the [documentation](https://pyfra.readthedocs.io/en/latest/).

## Example code

(This example obviously doesn't run as-is, it's just an illustrative example)

```python
from pyfra import *

rem1 = Remote("user@example.com")
rem2 = Remote("goose@8.8.8.8")

# env creates an environment object, which behaves very similarly to a Remote (in fact Env inherits from Remote), 
# but comes with a fresh python environment in a newly created directory (optionally initialized from a git repo)
# also, anything you run in an env will resume where it left off, with semantics similar to dockerfiles.
env1 = rem1.env("tokenization")
env2 = rem2.env("training", "https://github.com/some/repo")

# path creates a RemotePath object, which behaves similar to a pathlib Path.
raw_data = local.path("training_data.txt")
tokenized_data = env2.path("tokenized_data")

# tokenize
copy("https://goose.com/files/tokenize_script.py", env1.path("tokenize.py")) # copy can copy from local/remote/url to local/remote
env1.sh(f"python tokenize.py --input {raw_data} --output {tokenized_data}") # implicitly copy files just by using the path object in an f-string

# start training run
env2.path("config.json").jwrite({...})
env2.sh("python train.py --input tokenized_data --config config.json")
```

## Installation

```pip3 install pyfra```


## Webserver screenshots

![image](https://user-images.githubusercontent.com/54557097/119907788-4a2f6700-bf0e-11eb-9655-55e3317ba871.png)
![image](https://user-images.githubusercontent.com/54557097/115158135-fc3f5d80-a049-11eb-8310-a43b7b5c58e0.png)
