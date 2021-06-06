from colorama import init
init()

from .utils import *
from .functional import *
from .remote import *
from .web import *
from .shell import *

from .contrib import *

# fallback snippet from https://github.com/gruns/icecream
try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa