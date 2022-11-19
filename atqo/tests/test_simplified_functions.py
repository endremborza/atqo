import random
import time
from itertools import product

import pytest

from atqo import parallel_map
from atqo.distributed_apis import DIST_API_MAP


def add2(x):
    return x + 2


def extstr(x):
    return f"{x}-add"


def div(x):
    return 10 / x


@pytest.mark.parametrize(
    ["fun", "inl", "dapi"],
    product(
        [add2, extstr, div],
        [[1, 2, 3, 4, 5], ["a", 10, "b", 3], [None, 2, 0]],
        DIST_API_MAP.keys(),
    ),
)
def test_batch(fun, inl, dapi):
    res = []
    exs = []
    for x in inl:
        try:
            res.append(fun(x))
        except Exception as e:
            exs.append(e)

    map_outs = [
        list(parallel_map(fun, inl, dapi, raise_errors=False, verbose=True)),
        parallel_map(fun, iter(inl), dapi, raise_errors=False),
    ]

    for mout in map_outs:
        mres = []
        mex = []
        for o in mout:
            if isinstance(o, Exception):
                mex.append(o)
            else:
                mres.append(o)
        assert sorted(mres) == sorted(res)
        for _w in [str, type]:
            assert set(map(_w, mex)) == set(map(_w, exs))


def test_extras():

    with pytest.raises(TypeError):
        list(parallel_map(add2, [1, 2, 3, "X", "Y"], batch_size=3, pbar=True))


def slow_fun(x):  # pragma: no cover
    time.sleep(random.random() / 100)
    return x


def test_simple():
    out = sorted(parallel_map(slow_fun, range(100), pbar=True, verbose=True))
    assert out == list(range(100))

    def g():
        for i in range(10):
            yield i

    assert sorted(parallel_map(slow_fun, g(), pbar=True)) == list(range(10))
