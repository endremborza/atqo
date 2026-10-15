import pytest

from atqo.exceptions import NotEnoughResources
from atqo.exchange import NumStore, ResourceExchange


def test_numstore_arithmetic():
    a = NumStore({"x": 10})
    b = NumStore({"x": 3, "y": 1})
    assert (a + b).base_dict == {"x": 13, "y": 1}
    assert (a - NumStore({"x": 4})).base_dict == {"x": 6}
    assert (a * 2).base_dict == {"x": 20}
    assert a >= NumStore({"x": 5})
    assert not (a >= NumStore({"x": 11}))
    assert hash(a) == hash(NumStore({"x": 10}))


def test_numstore_empty_min_value():
    assert NumStore().min_value == 0


def test_exchange_simple_growth():
    cex = ResourceExchange({"A": {"cpu": 1}}, {"cpu": 4})
    out = cex.set_values({"A": 10})
    assert out["A"] == 4


def test_exchange_idle_when_no_tasks():
    cex = ResourceExchange({"A": {"cpu": 1}}, {"cpu": 4})
    cex.set_values({"A": 0})
    assert cex.idle


def test_exchange_two_actors_distribute():
    cex = ResourceExchange({"A": {"cpu": 1}, "B": {"cpu": 2}}, {"cpu": 6})
    out = cex.set_values({"A": 2, "B": 100})
    used = out["A"] * 1 + out["B"] * 2
    assert used <= 6
    assert out["A"] == 2
    assert out["B"] >= 1


def test_exchange_barter():
    cex = ResourceExchange({"A": {"cpu": 1}, "B": {"cpu": 2}}, {"cpu": 4})
    cex.set_values({"B": 1})
    assert cex.actors_running["B"] == 1
    out = cex.set_values({"A": 10, "B": 0})
    assert out["A"] >= 2


def test_exchange_dead_end_raises():
    cex = ResourceExchange({"NeedsMore": {"cpu": 100}}, {"cpu": 1})
    with pytest.raises(NotEnoughResources):
        _ = cex._possible_trades


def test_exchange_multi_resource():
    cex = ResourceExchange({"AB": {"cpu": 1, "mem": 2}}, {"cpu": 4, "mem": 8})
    out = cex.set_values({"AB": 10})
    assert out["AB"] == min(4, 8 // 2)
