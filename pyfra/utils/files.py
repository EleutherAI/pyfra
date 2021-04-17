import json
import csv
from pyfra.remote import RemoteFile

__all__ = ['fwrite', 'fread', 'jread', 'jwrite', 'csvread', 'csvwrite']

def fwrite(fname, content):
    if isinstance(fname, RemoteFile):
        remfile = fname
        return remfile.remote.fwrite(remfile.fname, content)

    with open(fname, 'w') as fh:
        fh.write(content)

def fread(fname):
    if isinstance(fname, RemoteFile):
        remfile = fname
        return remfile.remote.fread(remfile.fname)

    with open(fname) as fh:
        return fh.read()

def jread(fname):
    if isinstance(fname, RemoteFile):
        remfile = fname
        return remfile.remote.jread(remfile.fname)

    with open(fname) as fh:
        return json.load(fh)

def jwrite(fname, content):
    if isinstance(fname, RemoteFile):
        remfile = fname
        return remfile.remote.jwrite(remfile.fname, content)

    with open(fname, 'w') as fh:
        json.dump(content, fh)

def csvread(fname, colnames=None):
    if isinstance(fname, RemoteFile):
        remfile = fname
        return remfile.remote.csvread(remfile.fname, colnames)

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

def csvwrite(fname, data, colnames=None):
    if isinstance(fname, RemoteFile):
        remfile = fname
        return remfile.remote.csvwrite(remfile.fname, data, colnames)

    fh = open(fname, 'w')
    if colnames is None:
        colnames = data[0].keys()

    wtr = csv.writer(fh)
    wtr.writerow(colnames)

    for dat in data:
        assert dat.keys() == colnames

        wtr.writerow([dat[k] for k in colnames])