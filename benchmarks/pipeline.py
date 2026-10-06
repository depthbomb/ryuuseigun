from json import dumps
from asyncio import run
from statistics import median
from time import perf_counter_ns
from ryuuseigun.types import ASGIMessage
from ryuuseigun import Next, Module, Request, Response, Ryuuseigun

ITERATIONS = 10_000
ROUNDS = 7

def create_app(depth: int) -> tuple[Ryuuseigun, str]:
    app = Ryuuseigun(__name__)
    root = Module('root', url_prefix='/m0')
    active = root

    async def passthrough(req: Request, next: Next) -> None:
        await next(req)

    async def before(req: Request) -> None:
        req.state.before_count = getattr(req.state, 'before_count', 0) + 1

    async def after(req: Request, response: Response) -> Response:
        return response

    for index in range(depth):
        if index:
            child = Module(f'm{index}', url_prefix=f'/m{index}')
            active.register_module(child)
            active = child
        active.middleware(passthrough)
        active.middleware(passthrough)
        active.before_request(before)
        active.after_request(after)

    async def user(req: Request, user_id: int) -> dict[str, int]:
        return {'id': user_id, 'before': getattr(req.state, 'before_count', 0)}

    if depth:
        active.get('/users/<int:user_id>')(user)
        app.register_module(root)
    else:
        app.get('/users/<int:user_id>')(user)
    path = ''.join(f'/m{index}' for index in range(depth)) + '/users/42'
    return app, path

async def main() -> None:
    async def receive() -> ASGIMessage:
        raise AssertionError('This workload does not read request bodies')

    async def send(message: ASGIMessage) -> None:
        pass

    reports = []
    for depth in (0, 1, 3):
        app, path = create_app(depth)
        client = app.test_client()
        response = await client.get(path)
        assert response.status_code == 200
        assert response.json() == {'id': 42, 'before': depth}
        assert (await client.post(path)).status_code == 405
        assert (await client.request('OPTIONS', path)).status_code == 204
        assert (await client.get('/missing')).status_code == 404
        scope = {
            'type': 'http', 'method': 'GET', 'path': path, 'query_string': b'',
            'headers': [(b'host', b'localhost')], 'http_version': '1.1',
            'asgi': {'version': '3.0', 'spec_version': '2.5'},
        }
        for _ in range(1000):
            await app(scope, receive, send)

        samples = []
        for _ in range(ROUNDS):
            started = perf_counter_ns()
            for _ in range(ITERATIONS):
                await app(scope, receive, send)
            samples.append((perf_counter_ns() - started) / ITERATIONS / 1000)
        reports.append({
            'module_depth': depth, 'middlewares_per_module': 2,
            'before_and_after_hooks_per_module': 1, 'iterations': ITERATIONS,
            'rounds': ROUNDS, 'median_us': median(samples), 'samples_us': samples,
        })
    print(dumps(reports, indent=2))

if __name__ == '__main__':
    run(main())
