from typing import Any, Protocol, TypeAlias
from collections.abc import Mapping, Callable, Awaitable, MutableMapping

type JSONScalar = None | bool | int | float | str
type JSONValue = JSONScalar | list[Any] | dict[str, Any]

HeaderMapping: TypeAlias = Mapping[str, str]
ASGIScope: TypeAlias = MutableMapping[str, Any]
ASGIMessage: TypeAlias = MutableMapping[str, Any]
Receive: TypeAlias = Callable[[], Awaitable[ASGIMessage]]
Send: TypeAlias = Callable[[ASGIMessage], Awaitable[None]]


class ASGIApplication(Protocol):
    async def __call__(self, scope: ASGIScope, receive: Receive, send: Send) -> None: ...
