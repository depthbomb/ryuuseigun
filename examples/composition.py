"""Reuse a feature with independently supplied services and typed state.

Run `granian --interface asgi examples.composition:app`. The two mounted apps
have separate service instances and lifespans. GET /first/feature or /second/feature;
connect a WebSocket to /first/socket. No service container is involved.

Registration completes before startup. Parent resource contexts open first,
then opted-in child lifespans, then startup callbacks. Shutdown callbacks run
before contexts close in reverse order. On startup failure, entered contexts
unwind before the server receives the failure.

For native requests, application middleware enters before module middleware.
Before hooks run on entry; after hooks run inside-out before response sending.
`await next(req)` finishes after sending and request cleanup. Keep resources used
by a stream in middleware, not a decorator that exits when its handler returns.
Exceptions after headers are sent propagate, since another response is impossible.
Mounted apps are separate hook boundaries. Wrap the whole ASGI application for
cross-cutting policies such as CORS. All state below is local to this process.
"""
from types import SimpleNamespace
from dataclasses import dataclass
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from ryuuseigun import Next, Module, Request, WebSocket, Ryuuseigun

@dataclass
class Services:
    label: str
    ready: bool = False

@dataclass
class RequestState:
    visited: bool = False

@dataclass
class SocketState:
    messages: int = 0

def feature(services: Services) -> Module[RequestState, SocketState]:
    """A feature accepts its dependencies as ordinary Python arguments."""
    module = Module[RequestState, SocketState]('feature')

    @module.middleware
    async def mark(req: Request[RequestState], next: Next[RequestState]) -> None:
        req.state.visited = True
        await next(req)

    @module.get('/feature')
    async def index(req: Request[RequestState]) -> dict[str, str | bool]:
        return {'service': services.label, 'ready': services.ready, 'visited': req.state.visited}

    @module.websocket('/socket')
    async def socket(ws: WebSocket[SocketState]) -> None:
        await ws.accept()
        ws.state.messages += 1
        await ws.send_json({'service': services.label, 'messages': ws.state.messages})

    return module

def create_app(label: str) -> Ryuuseigun[RequestState, Services, SocketState]:
    app = Ryuuseigun[RequestState, Services, SocketState](
        __name__, request_state_factory=RequestState,
        app_state_factory=lambda: Services(label), websocket_state_factory=SocketState,
    )

    @app.lifespan
    @asynccontextmanager
    async def resources(current: Ryuuseigun[RequestState, Services, SocketState]) -> AsyncIterator[None]:
        current.state.ready = True
        try:
            yield
        finally:
            current.state.ready = False

    app.register_module(feature(app.state))
    return app

app: Ryuuseigun[SimpleNamespace] = Ryuuseigun(__name__)
app.mount('/first', create_app('first'), name='first', lifespan=True)
app.mount('/second', create_app('second'), name='second', lifespan=True)
