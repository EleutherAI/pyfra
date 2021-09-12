from colorama import init
init()

from .remote import *
from .web import *
from .shell import *
from .experiment import *

import pyfra.contrib as contrib


# fallback snippet from https://github.com/gruns/icecream
try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa