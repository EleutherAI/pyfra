from .iterators import *
from functools import partial


def identity(x):
    return x

def pointwise(*fs):
    def _fn(xs):
        return [f(x) for f, x in zip(fs, xs)]
    return _fn

# (b -> a -> b) -> b -> [a] -> b
def foldl(f, init, arr):
    curr = init
    for elem in arr:
        curr = f(curr, elem)
    return curr

# (a -> b -> b) -> b -> [a] -> b
def foldr(f, init, arr):
    curr = init
    for elem in arr[::-1]:
        curr = f(elem, curr)
    return curr

def mean(x):
    return sum(x) / len(x)

class SmartIter:
    def __init__(self, v):
        self.v = v
        self.clean = True
    def __next__(self):
        self.clean = False
        return next(self.v)
    def __iter__(self):
        return self
    def __getattr__(self, attr):
        return getattr(self.v, attr)
    def __repr__(self):
        self.v = listify(self.v)
        ret = repr(self.v)
        self.v = iter(self.v)
        return ret

class Pipeline:
    def __init__(self, *fs):
        self.fs = list(fs)

    def __rshift__(self, other):
        assert isinstance(other, Pipeline)
        return Pipeline(*(self.fs + other.fs))

    def __rrshift__(self, other):
        if isinstance(other, Pipeline):
            return Pipeline(*(other.fs + self.fs))
        else:
            return self(other)
    
    def __call__(self, x):
        for f in self.fs: x = f(x)

        return x

class each(Pipeline):
    def __init__(self, *f):
        self.fs = [lambda xs: SmartIter(Pipeline(*f)(x) for x in xs)]

class join(Pipeline):
    def __init__(self):
        self.fs = [do(self._join, SmartIter)]

    def _join(self, xs):
        for it in xs:
            for elem in it:
                yield elem

class filt(Pipeline):
    def __init__(self, f):
        self.fs = [do(partial(self._filter, f), SmartIter)]

    def _filter(self, f,xs):
        for elem in xs:
            if f(elem):
                yield elem
# aliases
do = Pipeline

builtin_filter = filter
def filter(*x):
    if len(x) == 1: return filt(*x)
    if len(x) == 2: return builtin_filter(*x)
    assert False