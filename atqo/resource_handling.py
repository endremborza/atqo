from enum import Enum
from typing import Any, Dict, Optional, Union
from uuid import uuid4

from .utils import subdict, sumdict


class Capability:
    def __init__(
        self,
        resource_needs: Union[Dict[Enum, int], "NumStore"],
        name: Optional[str] = None,
    ):
        """capability that actors can have and tasks might need

        Parameters
        ----------
        resource_needs : Dict[Enum, int]
            need to be integers for simplicity
        name : Optional[str], optional
            identifier, by default None
        """
        self.id_ = name or uuid4().hex
        if isinstance(resource_needs, dict):
            resource_needs = NumStore(resource_needs)
        self.resource_needs = resource_needs

    def __hash__(self):
        return self.id_.__hash__()

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.id_[:3]}, {self.resource_needs})"

    def __add__(self, other: Union["Capability", "NumStore"]):
        if isinstance(other, Capability):
            other = other.resource_needs
        return Capability(self.resource_needs + other)


class ActiveTaskPropertySet:
    pass


class NumStore:
    def __init__(self, use: Optional[Dict[Any, float]] = None):
        if isinstance(use, type(self)):
            use = use._use
        self._use = use or {}
        self.get = self._use.get

    def __eq__(self, other):
        assert isinstance(other, type(self))
        return self._use == other._use

    def __le__(self, other):
        assert isinstance(other, type(self))
        return all(
            [
                v <= other._use.get(k, -float("inf"))
                for k, v in self._use.items()
            ]
        )

    def __ge__(self, other):
        return other <= self

    def __lt__(self, other):
        return not (self == other) & (self <= other)

    def __gt__(self, other):
        return not (self == other) & (self >= other)

    def __mul__(self, other: int):
        return type(self)({k: v * other for k, v in self._use.items()})

    def __add__(self, other):
        assert isinstance(other, type(self))
        return type(self)(sumdict(self._use, other._use))

    def __sub__(self, other):
        assert isinstance(other, type(self))
        return type(self)(subdict(self._use, other._use))

    def __len__(self):
        return len(self._use)

    def __repr__(self):
        return str(self._use)

    def __hash__(self) -> int:
        return frozenset(self._use.items()).__hash__()

    def __iter__(self):
        for it in self._use.items():
            yield it

    @property
    def min_value(self):
        return min(self._use.values())

    @property
    def some_positive(self):
        return max(self._use.values()) > 0


class CapabilitySet(frozenset):
    def __repr__(self) -> str:
        return f"Set({'-'.join([c.id_[:3] for c in self])})"

    @property
    def total_resource_use(self) -> NumStore:
        return sum(self, start=Capability({})).resource_needs
