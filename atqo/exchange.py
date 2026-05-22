from collections import defaultdict
from functools import cached_property
from itertools import product
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

from .exceptions import NotEnoughResources
from .utils import subdict, sumdict

if TYPE_CHECKING:
    from .bases import ActorBase  # pragma: no cover


class NumStore:
    def __init__(self, use: Optional[Dict[Any, float]] = None):
        if isinstance(use, type(self)):
            use = use.base_dict
        self.base_dict = use or {}
        self.get = self.base_dict.get

    def __eq__(self, other):
        assert isinstance(other, type(self))
        return self.base_dict == other.base_dict

    def __le__(self, other):
        assert isinstance(other, type(self))
        return all(
            v <= other.base_dict.get(k, -float("inf"))
            for k, v in self.base_dict.items()
        )

    def __ge__(self, other):
        return other <= self

    def __mul__(self, other: int):
        return type(self)({k: v * other for k, v in self.base_dict.items()})

    def __add__(self, other):
        assert isinstance(other, type(self))
        return type(self)(sumdict(self.base_dict, other.base_dict))

    def __sub__(self, other):
        assert isinstance(other, type(self))
        return type(self)(subdict(self.base_dict, other.base_dict))

    def __repr__(self):
        return str(self.base_dict)

    def __hash__(self) -> int:
        return frozenset(self.base_dict.items()).__hash__()

    def __iter__(self):
        yield from self.base_dict.items()

    @property
    def min_value(self):
        return min(self.base_dict.values()) if self.base_dict else 0

    @property
    def pos_part(self):
        return type(self)({k: v for k, v in self if v > 0})


def requirements_store(cls: "type[ActorBase]") -> NumStore:
    return NumStore(dict(cls.requirements))


class ResourceExchange:
    """Trade idle resources for running actors of registered classes.

    Trades are NumStore over keys that are either an int (index into
    ``self._sources``) or an actor class. Resources are integer pools held for
    the lifetime of each actor instance.
    """

    def __init__(
        self,
        actor_classes: Iterable["type[ActorBase]"],
        resources: Dict[str, int],
    ) -> None:
        self._sources: List[Tuple[str, int]] = list(resources.items())
        self._actor_classes = sorted(actor_classes, key=lambda c: c.__name__)
        self.actors_running: Dict[type, int] = {cls: 0 for cls in self._actor_classes}
        self.tasks_queued = NumStore()
        self.idle_sources: List[int] = [limit for _, limit in self._sources]

    def __repr__(self) -> str:
        bases = [
            ("actors used", self.actors_running),
            ("tasks queued", self.tasks_queued),
            ("available", self.idle_sources),
        ]
        descr = "\n".join(f"{k}:\t{v}" for k, v in bases)
        return f"ResourceExchange:\n{descr}\n"

    def set_values(self, new_values):
        self.tasks_queued = NumStore(new_values)
        self._execute_positive_trades()
        return self.actors_running

    @property
    def idle(self) -> bool:
        return not sum(self.actors_running.values())

    def _execute_positive_trades(self):
        while True:
            state = ExchangeState(self.tasks_queued, self.actors_running)
            max_value = 0
            best_trade = None
            for trade in self._possible_trades:
                if not self._is_possible(trade):
                    continue
                trade_value = state.valuation(trade)
                if trade_value > max_value:
                    best_trade = trade
                    max_value = trade_value
            if max_value <= 0:
                break
            self._execute_trade(best_trade)

    def _is_possible(self, trade: NumStore) -> bool:
        for rid, num in trade:
            rem = (
                self.idle_sources[rid]
                if isinstance(rid, int)
                else self.actors_running[rid]
            )
            if (num + rem) < 0:
                return False
        return True

    def _execute_trade(self, trade: NumStore):
        for rid, num in trade:
            if isinstance(rid, int):
                self.idle_sources[rid] += num
            else:
                self.actors_running[rid] += num

    def _get_source_prices(self, cls: "type[ActorBase]") -> List[NumStore]:
        reqs = requirements_store(cls)
        by_source = self._source_combs(reqs)
        if not by_source and reqs.base_dict:
            raise NotEnoughResources(
                f"can't ever start {cls.__name__}: requires {reqs}"
            )
        if not by_source:
            return [NumStore({cls: 1})]
        return [NumStore({cls: 1}) - p for p in by_source]

    def _get_barter_prices(self, cls: "type[ActorBase]") -> List[NumStore]:
        bf = BarterFinder(cls, self._actor_classes)
        barters: List[NumStore] = []
        for resources, barter_dict in bf.barter_pairs:
            if resources.min_value > 0 and resources.base_dict:
                source_combs = self._source_combs(resources.pos_part)
                barters += [barter_dict + sc for sc in source_combs]
            else:
                barters.append(barter_dict)
        return barters

    def _source_combs(self, resource_use: NumStore) -> List[NumStore]:
        combs = []
        for res_id, res_need in resource_use:
            poss_sources = [
                NumStore({sid: res_need})
                for sid, limit in self._sources_by_res[res_id]
                if limit >= res_need
            ]
            if not poss_sources:
                return []
            combs.append(poss_sources)
        if not combs:
            return []
        return [sum(poss, NumStore({})) for poss in product(*combs)]

    @cached_property
    def _possible_trades(self) -> List[NumStore]:
        all_trades: List[NumStore] = []
        for cls in self._actor_classes:
            all_trades += self._get_source_prices(cls)
            all_trades += self._get_barter_prices(cls)
        return all_trades + [t * -1 for t in all_trades]

    @cached_property
    def _sources_by_res(self) -> Dict[str, List[Tuple[int, int]]]:
        out: Dict[str, List[Tuple[int, int]]] = defaultdict(list)
        for sid, (res_id, limit) in enumerate(self._sources):
            out[res_id].append((sid, limit))
        return out


class BarterFinder:
    def __init__(
        self, cls: "type[ActorBase]", classes: List["type[ActorBase]"]
    ) -> None:
        self._cls = cls
        self._others = [c for c in classes if c is not cls]
        self.barter_pairs: List[Tuple[NumStore, NumStore]] = []
        self._walk(requirements_store(cls))

    def _walk(self, available: NumStore, so_far: NumStore = NumStore()):
        for other in self._others:
            rest = available - requirements_store(other)
            if rest.base_dict and rest.min_value >= 0:
                new = so_far + NumStore({other: 1})
                self.barter_pairs.append((rest, new + NumStore({self._cls: -1})))
                self._walk(rest, new)


class ExchangeState:
    def __init__(self, task_dic: NumStore, actor_dic: dict):
        self.rem_acts = actor_dic.copy()
        self.holes: Dict[type, int] = defaultdict(int)
        for tcls, tcount in task_dic:
            have = self.rem_acts.get(tcls, 0)
            if have >= tcount:
                self.rem_acts[tcls] = have - tcount
            else:
                self.holes[tcls] = tcount - have
                self.rem_acts[tcls] = 0

    def valuation(self, trade: NumStore) -> float:
        value = 0.0
        for id_, c in trade:
            if isinstance(id_, int):
                value += c * 1e-5
                continue
            if c > 0:
                if self.holes.get(id_, 0) < c:
                    return 0
                value += c
            elif self.rem_acts.get(id_, 0) < -c:
                value += c + self.rem_acts.get(id_, 0)
        return value
