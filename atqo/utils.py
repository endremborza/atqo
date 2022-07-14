from dataclasses import dataclass, field
from functools import partial
from operator import add, sub
from typing import Any


@dataclass
class ArgRunner:
    kls: type
    args: tuple = ()
    kwargs: dict = field(default_factory=dict)

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self.kls(*args, *self.args, **kwds, **self.kwargs)


def dic_merge(d1, d2, _def, fun):
    return {k: fun(d1.get(k, _def), d2.get(k, _def)) for k in [*d1, *d2]}


def partial_cls(cls: type, *args, **kwargs):
    return ArgRunner(cls, args, kwargs)


def dic_val_filt(dic):
    return {k: v for k, v in dic.items() if v}


sumdict = partial(dic_merge, _def=0, fun=add)
subdict = partial(dic_merge, _def=0, fun=sub)
