from sqlitedict import SqliteDict
import hashlib
import json
import os


os.makedirs('state', exist_ok=True)
state = SqliteDict("state/main.db", autocommit=True)


def once(fn, name=None):
    """ Only run a function once, saving its return value to disk. Args must be json-encodable. """

    fname = name if name is not None else fn.__name__

    def _fn(*args, **kwargs):
        # hash the arguments
        arghash = hashlib.sha256(json.dumps([args, kwargs], sort_keys=True).encode()).hexdigest()

        key = f"once-{fname}-{arghash}-seen"
        if key in state: return state[key]
        
        ret = fn(*args, **kwargs)
        print("FINISHED", ret, fn.__name__)
        state[key] = ret
        state.commit()
        return ret

    return _fn



# DEPRECATED

import multiprocessing.dummy
import multiprocessing.pool
import threading
import weakref


# BEGIN patched section

# this doesnt totally work yet, for some reason, you need to ^C multiple times,
# but it should be good enough because it makes sure everything grinds to a halt.

class Process(multiprocessing.dummy.DummyProcess):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}):
        # PATCHED FROM ORIGINAL: use daemon=True
        threading.Thread.__init__(self, group, target, name, args, kwargs, daemon=True)
        self._pid = None
        self._children = weakref.WeakKeyDictionary()
        self._start_called = False
        self._parent = multiprocessing.dummy.current_process()

class StoppingThreadPool(multiprocessing.pool.ThreadPool):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def Process(*args, **kwds):
        # PATCHED FROM ORIGINAL: use custom Process
        return Process(*args, **kwds)


# END patched section

# based on code from kindiana
def pipeline(*func_list):
    def _f(in_iter):
        pools = [StoppingThreadPool(1) for _ in func_list]
        
        iter = in_iter
        for pool, func in zip(pools, func_list):
            iter = pool.imap(func, iter)
        
        return iter
    return _f