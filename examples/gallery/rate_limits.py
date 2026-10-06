"""Process-local limits for the example. Shared deployments need an async shared backend."""
from math import ceil
from time import monotonic
from functools import wraps
from typing import Concatenate
from ryuuseigun.routing import RouteDecorator
from collections.abc import Callable, Awaitable
from examples.gallery.models import GalleryState
from ryuuseigun import abort, Request, ResponseValue

class Bucket:
    def __init__(self, capacity: int, period: float) -> None:
        self.capacity = capacity
        self.period = period
        self._windows: dict[str, tuple[float, int]] = {}

    def consume(self, *, cost: int = 1) -> RouteDecorator[GalleryState]:
        if cost < 1 or cost > self.capacity:
            raise ValueError('Cost must be positive and no greater than bucket capacity')

        def decorate[**Parameters, Result: ResponseValue](
            handler: Callable[Concatenate[Request[GalleryState], Parameters], Awaitable[Result]],
        ) -> Callable[Concatenate[Request[GalleryState], Parameters], Awaitable[Result]]:
            @wraps(handler)
            async def wrapped(req: Request[GalleryState], /, *args: Parameters.args, **kwargs: Parameters.kwargs) -> Result:
                now = monotonic()
                key = req.state.user or 'anonymous'
                started, used = self._windows.get(key, (now, 0))
                if now - started >= self.period:
                    started, used = now, 0
                if used + cost > self.capacity:
                    abort(429, 'Rate limit exceeded', headers={'Retry-After': str(ceil(self.period - (now - started)))})
                # No await between checking and consuming within this event loop.
                self._windows[key] = started, used + cost
                return await handler(req, *args, **kwargs)
            return wrapped
        return decorate
