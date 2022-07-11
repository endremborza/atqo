from functools import partial, partialmethod
from operator import add, sub


def dic_merge(d1, d2, _def, fun):
    return {k: fun(d1.get(k, _def), d2.get(k, _def)) for k in [*d1, *d2]}


def partial_cls(cls: type, *args, **kwargs):
    _dict = {
        k: (v if k != "__init__" else partialmethod(v, *args, **kwargs))
        for k, v in cls.__dict__.items()
    }
    return type(cls.__name__, cls.__bases__, _dict)


sumdict = partial(dic_merge, _def=0, fun=add)
subdict = partial(dic_merge, _def=0, fun=sub)
