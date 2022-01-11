from atqo.resource_handling import NumStore


def test_num_store():

    assert NumStore({"A": 10}) + NumStore({"A": 5}) == NumStore({"A": 15})
