from functools import partial
from operator import add, sub


def dic_merge(d1: dict, d2: dict, _def, fun):
    return {k: fun(d1.get(k, _def), d2.get(k, _def)) for k in [*d1, *d2]}


def dic_val_filt(dic):
    return {k: v for k, v in dic.items() if v}


sumdict = partial(dic_merge, _def=0, fun=add)
subdict = partial(dic_merge, _def=0, fun=sub)
