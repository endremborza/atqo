from itertools import chain, combinations_with_replacement, product
from random import Random

import pytest

from atqo import Capability, CapabilitySet
from atqo.exceptions import NotEnoughResources
from atqo.exchange import CapsetExchange
from atqo.resource_handling import NumStore


@pytest.mark.parametrize(
    ["seed", "res_pop", "max_caps", "max_capsets"],
    product([10, 20, 42], ["ABCDEFGHIJKL", "XYZ"], [3, 4, 5], [6, 10, 20]),
)
def test_exchange(seed, res_pop, max_caps, max_capsets):
    rng = Random(seed)
    pop_n = len(res_pop)

    resources = rng.sample(res_pop, rng.randint(1, pop_n))
    res_n = len(resources)
    res_limits = [
        (r, int(round(rng.random() * rng.randint(10, 200), -1)) + 50)
        for r in resources
    ]
    capcount = rng.randint(1, max_caps)
    caps = [
        Capability(
            {
                r: int(round(rng.random() * rng.randint(10, 30), -1)) + 10
                for r in rng.sample(resources, rng.randint(1, res_n))
            }
        )
        for _ in range(capcount)
    ]
    capsets = set(
        [
            CapabilitySet(rng.sample(caps, rng.randint(1, capcount)))
            for _ in range(rng.randint(1, max_capsets))
        ]
    )

    cex = CapsetExchange(capsets, resources=res_limits)
    cex.set_values(NumStore({cs: rng.randint(0, 15) for cs in capsets}))


def capset_cex_factory(n_act, mul):
    capsets = [CapabilitySet([Capability({"A": 1})]) for _ in range(n_act)]
    cex = CapsetExchange(
        capsets,
        resources={"A": n_act * mul},
    )
    return capsets, cex


def assert_smooth(cex, capsets, mul, n_act, _):
    out = cex.set_values({c: 10 for c in capsets})
    assert sum(out.values()) == (mul * n_act)
    assert max(out.values()) == min(out.values())


def assert_lopside(cex, capsets, mul, n_act, param):
    out = cex.set_values(
        {c: n for c, n in zip(capsets, [param, *([1] * n_act)])}
    )

    first = [*out.values()][0]
    assert sum(out.values()) == (mul * n_act)
    assert max(out.values()) == first
    if (n_act > 1) and (mul > 1):
        assert any([first > v for v in out.values()])
    assert not any([first < v for v in out.values()])


def assert_lin(cex, capsets, mul, n_act, param):
    out = cex.set_values(
        {c: n for c, n in zip(capsets, range(0 + param, n_act + param))}
    )

    last = [*out.values()][-1]
    assert sum(out.values()) == (mul * n_act)
    assert max(out.values()) == last
    if (n_act > 2) and (mul > 1):
        assert any([last > v for v in out.values()])
    assert not any([last < v for v in out.values()])


fun_combs = [
    *chain(
        *[
            combinations_with_replacement(
                [assert_lin, assert_lopside, assert_smooth], r
            )
            for r in range(1, 5)
        ]
    )
]


@pytest.mark.parametrize(
    ["n_act", "mul", "param", "funlist"],
    product([1, 2, 10], [1, 3, 11], [3, 10], fun_combs),
)
def test_simple_cex(n_act, mul, param, funlist):
    capsets, cex = capset_cex_factory(n_act, mul)
    for fun in funlist:
        fun(cex, capsets, mul, n_act, param)
        # TODO: make it work without this reset
        cex.set_values({c: 0 for c in capsets})


def test_repr():

    cex = CapsetExchange([CapabilitySet([])], {})
    assert "0" in cex.__repr__()
    with pytest.raises(NotEnoughResources):
        print(cex._possible_trades)
