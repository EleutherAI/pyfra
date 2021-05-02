import json
import csv
import os
from pyfra.remote import RemoteFile, Remote

__all__ = ['fwrite', 'fread', 'jread', 'jwrite', 'csvread', 'csvwrite']

def fname_fn(fn):
    def _fn(fname, *a, **k):
        # use Remote impl if argument is RemoteFile
        if isinstance(fname, RemoteFile):
            remfile = fname
            return getattr(remfile.remote, fn.__name__)(remfile.fname, *a, **k)

        # map over list if fname is list
        elif isinstance(fname, list):
            fnames = fname
            return [
                fn(fname, *a, **k)
                for fname in fnames
            ]
        
        else:
            return fn(os.path.expanduser(fname), *a, **k)

    return _fn


@fname_fn
def fwrite(fname, content):
    with open(fname, 'w') as fh:
        fh.write(content)

@fname_fn
def fread(fname):
    with open(fname) as fh:
        return fh.read()

@fname_fn
def jread(fname):
    with open(fname) as fh:
        return json.load(fh)

@fname_fn
def jwrite(fname, content):
    with open(fname, 'w') as fh:
        json.dump(content, fh)

@fname_fn
def csvread(fname, colnames=None):
    fh = open(fname)
    if fname[-4:] == ".tsv":
        rdr = csv.reader(fh, delimiter="\t")
    else:
        rdr = csv.reader(fh)

    if colnames:
        cols = colnames
    else:
        cols = list(next(rdr))
    
    for ob in rdr:
        yield {
            k: v for k, v in zip(cols, [*ob, *[None for _ in range(len(cols) - len(ob))]])
        }

@fname_fn
def csvwrite(fname, data, colnames=None):
    fh = open(fname, 'w')
    if colnames is None:
        colnames = data[0].keys()

    wtr = csv.writer(fh)
    wtr.writerow(colnames)

    for dat in data:
        assert dat.keys() == colnames

        wtr.writerow([dat[k] for k in colnames])