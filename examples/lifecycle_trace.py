from functools import wraps
from typing import Concatenate
from ryuuseigun.routing import RouteDecorator
from collections.abc import Callable, Awaitable, AsyncIterator
from ryuuseigun import Next, Module, Request, Response, Ryuuseigun, ResponseValue, StreamingResponse

def create_app() -> tuple[Ryuuseigun, list[str]]:
    app = Ryuuseigun(__name__)
    parent = Module('api', url_prefix='/api')
    child = Module('images', url_prefix='/images')
    events: list[str] = []

    def trace() -> RouteDecorator[object]:
        def decorate[**Parameters, Result: ResponseValue](
            handler: Callable[Concatenate[Request[object], Parameters], Awaitable[Result]],
        ) -> Callable[Concatenate[Request[object], Parameters], Awaitable[Result]]:
            @wraps(handler)
            async def wrapped(req: Request[object], /, *args: Parameters.args, **kwargs: Parameters.kwargs) -> Result:
                events.append('decorator enter')
                try:
                    return await handler(req, *args, **kwargs)
                finally:
                    events.append('decorator exit')
            return wrapped
        return decorate

    @app.middleware
    async def resource(req: Request, next: Next) -> None:
        events.append('resource enter')
        try:
            await next(req)
        finally:
            events.append('resource exit')

    @parent.before_request
    async def before(req: Request) -> Response | None:
        events.append('parent before')
        return Response('stopped', status_code=400) if req.query.get('stop') else None

    @child.after_request
    async def after(req: Request, res: Response) -> Response:
        events.append('child after')
        return res

    @child.get('/<int:image_id>')
    @trace()
    async def image(req: Request[object], image_id: int) -> StreamingResponse:
        events.append(f'handler {image_id}')
        if req.query.get('error'):
            raise ValueError('Example failure')

        async def content() -> AsyncIterator[str]:
            try:
                events.append('stream')
                yield 'image'
            finally:
                events.append('stream closed')

        return StreamingResponse(content())

    parent.register_module(child)
    app.register_module(parent)
    return app, events
