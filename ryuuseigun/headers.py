from re import compile
from typing import Optional
from functools import lru_cache
from collections.abc import Iterator, Mapping, MutableMapping

_HEADER_NAME_PATTERN = compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+\Z")
_INVALID_HEADER_VALUE_PATTERN = compile(r'[\x00-\x08\x0a-\x1f\x7f]')


@lru_cache(maxsize=256)
def _validate_header(name: str, value: str) -> None:
    if _HEADER_NAME_PATTERN.fullmatch(name) is None:
        raise ValueError('Invalid header name')
    if _INVALID_HEADER_VALUE_PATTERN.search(value) is not None:
        raise ValueError('Invalid header value')
    try:
        value.encode('latin-1')
    except UnicodeEncodeError as error:
        raise ValueError('Header values must contain only Latin-1 characters') from error


class Headers(MutableMapping[str, str]):
    __slots__ = ('_items', '_raw_items')

    def __init__(self, values: Optional[Mapping[str, str] | list[tuple[str, str]]] = None) -> None:
        self._items: list[tuple[str, str]] = []
        self._raw_items: Optional[tuple[tuple[bytes, bytes], ...]] = None
        if values is None:
            return
        if isinstance(values, Headers):
            self._raw_items = tuple(values.raw())
            return

        source = values.items() if isinstance(values, Mapping) else values
        for name, value in source:
            self.add(name, value)

    def __getitem__(self, name: str) -> str:
        self._materialize()
        folded = name.casefold()
        for item_name, value in reversed(self._items):
            if item_name.casefold() == folded:
                return value
        raise KeyError(name)

    def __setitem__(self, name: str, value: str) -> None:
        self._materialize()
        _validate_header(name, value)
        if not self._items:
            self._items.append((name, value))
            return
        folded = name.casefold()
        self._items[:] = [
            (item_name, item_value)
            for item_name, item_value in self._items
            if item_name.casefold() != folded
        ]
        self._items.append((name, value))

    def __delitem__(self, name: str) -> None:
        if not self.popall(name):
            raise KeyError(name)

    def __iter__(self) -> Iterator[str]:
        self._materialize()
        seen: set[str] = set()
        for name, _ in self._items:
            folded = name.casefold()
            if folded not in seen:
                seen.add(folded)
                yield name

    def __len__(self) -> int:
        self._materialize()
        return len(set(name.casefold() for name, _ in self._items))

    def add(self, name: str, value: str) -> None:
        self._materialize()
        _validate_header(name, value)
        self._items.append((name, value))

    def getlist(self, name: str) -> list[str]:
        self._materialize()
        folded = name.casefold()
        return [value for item_name, value in self._items if item_name.casefold() == folded]

    def popall(self, name: str) -> list[str]:
        self._materialize()
        if not self._items:
            return []
        folded = name.casefold()
        removed = [value for item_name, value in self._items if item_name.casefold() == folded]
        self._items[:] = [(item_name, value) for item_name, value in self._items if item_name.casefold() != folded]
        return removed

    def raw(self) -> list[tuple[bytes, bytes]]:
        if self._raw_items is not None:
            return list(self._raw_items)
        return [(name.lower().encode('latin-1'), value.encode('latin-1')) for name, value in self._items]

    def _materialize(self) -> None:
        raw_items = self._raw_items
        if raw_items is None:
            return
        self._raw_items = None
        for name, value in raw_items:
            self.add(name.decode('latin-1'), value.decode('latin-1'))

    @classmethod
    def from_raw(cls, values: list[tuple[bytes, bytes]]) -> 'Headers':
        headers = cls()
        headers._raw_items = tuple(values)
        return headers
