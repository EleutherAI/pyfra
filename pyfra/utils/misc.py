from sqlitedict import SqliteDict
import hashlib
import json
import os
import signal


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
        return ret

    return _fn


import multiprocessing.dummy
import multiprocessing.pool
import threading
import weakref
import _thread
# BEGIN patched section

class Condition(threading.Condition):
    def wait(self, timeout=None):
        # PATCHED FROM ORIGINAL: doesn't swallow error
        if not self._is_owned():
            raise RuntimeError("cannot wait on un-acquired lock")
        waiter = threading._allocate_lock()
        waiter.acquire()
        self._waiters.append(waiter)
        saved_state = self._release_save()
        gotit = False
        try:
            if timeout is None:
                waiter.acquire()
                gotit = True
            else:
                if timeout > 0:
                    gotit = waiter.acquire(True, timeout)
                else:
                    gotit = waiter.acquire(False)
        except KeyboardInterrupt:
            import os
            print("!!!")
            os.kill(os.getpid(), signal.SIGINT)
        finally:
            self._acquire_restore(saved_state)
            if not gotit:
                try:
                    self._waiters.remove(waiter)
                except ValueError:
                    pass

            return gotit

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

    def imap(self, func, iterable, chunksize=1):
        '''
        Equivalent of `map()` -- can be MUCH slower than `Pool.map()`.
        '''
        # PATCHED FROM ORIGINAL: use custom Condition
        if self._state != multiprocessing.pool.RUN:
            raise ValueError("Pool not running")
        if chunksize == 1:
            result = multiprocessing.pool.IMapIterator(self._cache)
            result._cond = Condition(threading.Lock())
            self._taskqueue.put(
                (
                    self._guarded_task_generation(result._job, func, iterable),
                    result._set_length
                ))
            return result
        else:
            if chunksize < 1:
                raise ValueError(
                    "Chunksize must be 1+, not {0:n}".format(
                        chunksize))
            task_batches = Pool._get_tasks(func, iterable, chunksize)
            result = multiprocessing.pool.IMapIterator(self._cache)
            result._cond = Condition(threading.Lock())
            self._taskqueue.put(
                (
                    self._guarded_task_generation(result._job,
                                                  mapstar,
                                                  task_batches),
                    result._set_length
                ))
            return (item for chunk in result for item in chunk)

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