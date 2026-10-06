from threading import Lock
from functools import wraps
from ryuuseigun.request import Request
from collections.abc import Callable, Awaitable
from ryuuseigun.exceptions import HTTPException
from ryuuseigun.constants import HeaderName, StatusCode
from typing import Any, TypeVar, Optional, Protocol, ParamSpec

type Next[StateT = Any] = Callable[[Request[StateT]], Awaitable[None]]


class Middleware[StateT = Any](Protocol):
    async def __call__(self, req: Request[StateT], next: Next[StateT]) -> None: ...


type MiddlewareCallable[StateT = Any] = Callable[[Request[StateT], Next[StateT]], Awaitable[None]]
Parameters = ParamSpec('Parameters')
Result = TypeVar('Result')


class ConcurrencyLimit:
    def __init__(
        self,
        max_active: int,
        *,
        retry_after: Optional[int] = 1,
        detail: str = 'Server is busy; retry later',
    ) -> None:
        if not isinstance(max_active, int) or isinstance(max_active, bool) or max_active <= 0:
            raise ValueError('max_active must be a positive integer')
        if retry_after is not None and (
            not isinstance(retry_after, int) or isinstance(retry_after, bool) or retry_after < 0
        ):
            raise ValueError('retry_after must be a non-negative integer or None')
        self.max_active = max_active
        self.retry_after = retry_after
        self.detail = detail
        self._active = 0
        self._lock = Lock()

    @property
    def active(self) -> int:
        with self._lock:
            return self._active

    async def __call__(self, req: Request[Any], next: Next[Any]) -> None:
        admitted = False
        with self._lock:
            if self._active < self.max_active:
                self._active += 1
                admitted = True
        if not admitted:
            headers = None if self.retry_after is None else {HeaderName.RETRY_AFTER: str(self.retry_after)}
            raise HTTPException(StatusCode.SERVICE_UNAVAILABLE, self.detail, headers)

        try:
            await next(req)
        finally:
            with self._lock:
                self._active -= 1


def asyncify(function: Callable[Parameters, Result]) -> Callable[Parameters, Awaitable[Result]]:
    from asyncio import to_thread

    @wraps(function)
    async def wrapper(*args: Parameters.args, **kwargs: Parameters.kwargs) -> Result:
        return await to_thread(function, *args, **kwargs)

    return wrapper
