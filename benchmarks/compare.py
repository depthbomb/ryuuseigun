import asyncio
from typing import Any
from statistics import median
from time import perf_counter
from starlette.applications import Starlette
from ryuuseigun import Next, Request, Ryuuseigun
from quart import Quart, jsonify as quart_jsonify
from starlette.routing import Route as StarletteRoute
from starlette.requests import Request as StarletteRequest
from starlette.responses import JSONResponse as StarletteJSONResponse

type ASGIApp = Any

ITERATIONS = 5_000
ROUNDS = 3


async def starlette_user(incoming: StarletteRequest) -> StarletteJSONResponse:
    return StarletteJSONResponse({'id': int(incoming.path_params['user_id'])})


class PassthroughMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        await self.app(scope, receive, send)


class Receiver:
    def __init__(self) -> None:
        self.first = True

    def reset(self) -> None:
        self.first = True

    async def __call__(self) -> dict[str, Any]:
        if self.first:
            self.first = False
            return {'type': 'http.request', 'body': b'', 'more_body': False}
        return {'type': 'http.disconnect'}


def create_ryuuseigun(*, middleware: bool) -> Ryuuseigun:
    app = Ryuuseigun(__name__)

    if middleware:
        @app.middleware
        async def passthrough(req: Request, next: Next) -> None:
            await next(req)

    @app.get('/users/<int:user_id>')
    async def user(_req, user_id: int) -> dict[str, int]:
        return {'id': user_id}

    return app


def create_starlette(*, middleware: bool) -> ASGIApp:
    app: ASGIApp = Starlette(routes=[StarletteRoute('/users/{user_id:int}', starlette_user)])
    return PassthroughMiddleware(app) if middleware else app


def create_quart(*, middleware: bool) -> Quart:
    app = Quart(__name__)

    if middleware:
        @app.before_request
        async def passthrough() -> None:
            return None

    @app.get('/users/<int:user_id>')
    async def user(user_id: int):
        return quart_jsonify({'id': user_id})

    return app


async def benchmark(name: str, app: ASGIApp) -> tuple[str, float]:
    scope = {
        'type': 'http',
        'asgi': {'version': '3.0', 'spec_version': '2.5'},
        'http_version': '1.1',
        'method': 'GET',
        'scheme': 'http',
        'path': '/users/42',
        'raw_path': b'/users/42',
        'query_string': b'',
        'headers': [(b'host', b'localhost')],
        'client': ('127.0.0.1', 50000),
        'server': ('localhost', 80),
        'root_path': '',
    }

    async def send(message: dict[str, Any]) -> None:
        return None

    receive = Receiver()
    for _ in range(500):
        receive.reset()
        await app(scope, receive, send)

    results: list[float] = []
    for _ in range(ROUNDS):
        started = perf_counter()
        for _ in range(ITERATIONS):
            receive.reset()
            await app(scope, receive, send)
        results.append(perf_counter() - started)

    elapsed = median(results)
    return name, elapsed / ITERATIONS * 1_000_000


async def main() -> None:
    applications = (
        ('ryuuseigun route', create_ryuuseigun(middleware=False)),
        ('starlette route', create_starlette(middleware=False)),
        ('quart route', create_quart(middleware=False)),
        ('ryuuseigun middleware', create_ryuuseigun(middleware=True)),
        ('starlette middleware', create_starlette(middleware=True)),
        ('quart middleware', create_quart(middleware=True)),
    )
    for name, application in applications:
        result_name, microseconds = await benchmark(name, application)
        print(f'{result_name:22} {microseconds:8.2f} us/request  {1_000_000 / microseconds:10,.0f} req/s')


if __name__ == '__main__':
    asyncio.run(main())
