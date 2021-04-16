import collections


def chunks(iter, n):
    arr = []
    for x in iter:
        arr.append(x)
        if len(arr) == n:
            yield arr
            arr = []
    
    if arr: yield arr

def group(arr, fn):
    res = collections.defaultdict(list)

    for ob in arr:
        res[fn(ob)].append(ob)
    
    return list(res.values())


# orders by fn(x), allows computation, and then converts to original order
class Reorderer:
    def __init__(self, arr, fn):
        self.size = len(arr)
        arr = list(enumerate(arr))
        arr = group(arr, lambda x: fn(x[1]))
        arr = [
            ([y[0] for y in x], x[0][1]) for x in arr
        ]
        arr.sort(key=lambda x: fn(x[1]))

        self.arr = arr
        
    
    def get_reordered(self):
        return [x[1] for x in self.arr]
    
    def get_original(self, newarr):
        res = [None] * self.size
        cov = [False] * self.size

        for (inds, _), v in zip(self.arr, newarr):
            for ind in inds: 
                res[ind] = v
                cov[ind] = True
        
        assert all(cov)
        
        return res

