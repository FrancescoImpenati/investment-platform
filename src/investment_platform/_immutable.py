"""Small immutable containers shared by serializable boundary models."""

from collections.abc import Iterator, Mapping
from types import MappingProxyType
from typing import TypeVar

_Key = TypeVar("_Key")
_Value = TypeVar("_Value")


class FrozenMapping(Mapping[_Key, _Value]):
    """A read-only mapping that is safe to reuse during deep copies."""

    __slots__ = ("_values",)

    def __init__(self, values: Mapping[_Key, _Value]) -> None:
        self._values: Mapping[_Key, _Value] = MappingProxyType(dict(values))

    def __getitem__(self, key: _Key) -> _Value:
        return self._values[key]

    def __iter__(self) -> Iterator[_Key]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return f"FrozenMapping({self._values!r})"

    def __copy__(self) -> "FrozenMapping[_Key, _Value]":
        return self

    def __deepcopy__(self, memo: dict[int, object] | None = None) -> "FrozenMapping[_Key, _Value]":
        if memo is not None:
            memo[id(self)] = self
        return self
