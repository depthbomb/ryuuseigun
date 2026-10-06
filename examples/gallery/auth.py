from functools import wraps
from typing import Concatenate
from ryuuseigun.routing import RouteDecorator
from collections.abc import Callable, Awaitable
from examples.gallery.models import GalleryState
from ryuuseigun import abort, Request, ResponseValue

def requires_authentication() -> RouteDecorator[GalleryState]:
    def decorate[**Parameters, Result: ResponseValue](
        handler: Callable[Concatenate[Request[GalleryState], Parameters], Awaitable[Result]],
    ) -> Callable[Concatenate[Request[GalleryState], Parameters], Awaitable[Result]]:
        @wraps(handler)
        async def wrapped(req: Request[GalleryState], /, *args: Parameters.args, **kwargs: Parameters.kwargs) -> Result:
            if req.state.user is None:
                abort(401, 'Sign in first')
            return await handler(req, *args, **kwargs)
        return wrapped
    return decorate
