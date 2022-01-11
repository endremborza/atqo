from collections import defaultdict
from enum import Enum
from functools import cached_property, reduce
from math import gcd
from typing import Dict, Iterable, List, Tuple, Union

from .exceptions import NotEnoughResources
from .resource_handling import CapabilitySet, NumStore


class CapsetExchange:
    def __init__(
        self,
        actor_capsets: Iterable[CapabilitySet],
        resources: Union[Dict[Enum, int], Iterable[Tuple[Enum, int]]],
    ) -> None:
        """trade resources for running actors with capsets

        - find possible trades
        - when prompted, filter these to possible ones
        - select best valued one
        - execute trade, refresh possible ones, repeat
        """

        self._sources = (
            [*resources.items()] if isinstance(resources, dict) else resources
        )  # can be limited what can be used together, later
        self._capsets = [*actor_capsets]
        self._nc = len(self._capsets)
        self._ns = len(self._sources)

        self.tasks_under = {cs: 0 for cs in self._capsets}
        self.actors_running = {cs: 0 for cs in self._capsets}
        self.actors_over = {}
        self._set_actors_over()
        self.idle_sources = [l // self._grans[eid] for eid, l in self._sources]

    def __repr__(self) -> str:
        bases = [
            ("actors used", self.actors_running),
            ("tasks under", self.tasks_under),
            (
                "using resources",
                self._utilized_resources,
            ),
            ("available resources", self.idle_sources),
        ]
        vals = [
            (k, self._get_value(k)) for k in [*self._capsets, *range(self._ns)]
        ]
        descr = "\n".join(f"{k}:\t{v}" for k, v in bases)
        probdesc = "\n".join(f"{k}:\t{v}" for k, v in vals)
        return f"Capset Exchange: \n{descr}\n\nvaluations:\n{probdesc}"

    def set_values(self, new_values: Union[NumStore, Dict[Enum, int]]):
        self._update_tasks_under(NumStore(new_values))
        self._execute_positive_trades()
        return self.actors_running

    @property
    def idle(self):
        return not sum(self.actors_running.values())

    def _set_actors_over(self):
        for cs_base in self._capsets:
            self.actors_over[cs_base] = sum(
                [n for cs, n in self.actors_running.items() if cs >= cs_base]
            )

    def _update_tasks_under(self, new_values):
        for cs, val in new_values:
            self.tasks_under[cs] = val

    def _execute_positive_trades(self):
        while True:
            max_value = -float("inf")
            best_trade = None
            for trade in self._possible_trades:
                if not self._is_trade_possible(trade):
                    continue
                trade_value = self._get_trade_value(trade)
                if trade_value > max_value:
                    best_trade = trade
                    max_value = trade_value
            if max_value <= 0:
                break
            self._execute_trade(best_trade)
            self._set_actors_over()

    def _is_trade_possible(self, trade: NumStore):
        for rid, num in trade:
            if (num + self._any_tradeable[rid]) < 0:
                return False
        return True

    def _get_trade_value(self, trade: NumStore):
        return sum([n * self._get_value(rid) for rid, n in trade])

    def _get_value(self, rid: Union[CapabilitySet, int]):
        if isinstance(rid, int):
            return 1e-15
        val = (self.tasks_under[rid] - 1e-2) / (self.actors_over[rid] + 1e-3)
        return val

    def _get_prices(self, capset: CapabilitySet):

        prices = [NumStore({capset: 1})]
        for res_id, res_need in capset.total_resource_use:
            gran_need = res_need // self._grans[res_id]
            prices = [
                p + NumStore({sid: -gran_need})
                for p in prices
                for sid, limit in self._sources_by_res[res_id]
                if limit >= gran_need
            ]

        if (len(prices) < 2) and (len(prices[0]) == 1):
            raise NotEnoughResources(
                f"can't ever start {capset}: \n{capset.total_resource_use}"
            )

        return prices + [p * -1 for p in prices]

    def _execute_trade(self, trade: NumStore):
        for rid, num in trade:
            if isinstance(rid, int):
                self.idle_sources[rid] += num
            else:
                self.actors_running[rid] += num

    @property
    def _utilized_resources(self):
        return sum(
            [
                cs.total_resource_use * n
                for cs, n in self.actors_running.items()
            ],
            start=NumStore(),
        )

    @property
    def _any_tradeable(self):
        return {**self.actors_running, **dict(enumerate(self.idle_sources))}

    @cached_property
    def _grans(self):
        # granularities

        res_int_lists = defaultdict(list)
        for res_id, res_int in self._sources + [
            items
            for capset in self._capsets
            for items in capset.total_resource_use
        ]:
            res_int_lists[res_id].append(res_int)

        return {
            res_id: reduce(gcd, res_ints)
            for res_id, res_ints in res_int_lists.items()
        }

    @cached_property
    def _possible_trades(self) -> Iterable[NumStore]:
        """find all (so far most/useful) possible trades

        1. find all 1 actor -> idle resource trades
          - if one is not possible, raise/warn something
        2. find some between actor trades (TODO)
        """
        trades = []
        for capset in self._capsets:
            trades += self._get_prices(capset)
        return trades

    @cached_property
    def _sources_by_res(self) -> Dict[Enum, List[Tuple[int, int]]]:
        out = defaultdict(list)
        for sid, (res_id, limit) in enumerate(self._sources):
            out[res_id].append((sid, limit))
        return out
