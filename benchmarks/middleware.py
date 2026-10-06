from gc import collect
from json import dumps
from functools import wraps
from statistics import median
from asyncio import run, Event
from time import perf_counter_ns
from dataclasses import dataclass
from argparse import ArgumentParser
from ryuuseigun.types import ASGIScope, ASGIMessage
from tracemalloc import start, stop, get_traced_memory
from ryuuseigun import Next, Module, Request, Response, WebSocket, Ryuuseigun, StreamingResponse

@dataclass
class Workload:
    name: str
    app: Ryuuseigun
    path: str
    hooks: int
    streaming: bool = False
    reject: bool = False
    websocket: bool = False

async def passthrough(req: Request, next: Next) -> None:
    await next(req)

async def before(req: Request) -> None:
    req.state.steps = getattr(req.state, 'steps', 0) + 1

async def after(req: Request, response: Response) -> Response:
    return response

def decorate(handler):
    @wraps(handler)
    async def wrapped(req, *args, **kwargs):
        return await handler(req, *args, **kwargs)

    return wrapped

def create_workload(
    name: str, *, middleware: int = 0, decorators: int = 0, hooks: int = 0,
    depth: int = 0, scoped_middleware: int = 0, dynamic: bool = False,
    streaming: bool = False, reject: bool = False,
) -> Workload:
    app = Ryuuseigun(name)
    target = app
    root = None
    for index in range(depth):
        child = Module(f'm{index}', url_prefix=f'/m{index}')
        if root is None:
            root = child
        else:
            target.register_module(child)
        target = child
        for _ in range(scoped_middleware):
            target.middleware(passthrough)
        target.before_request(before)
        target.after_request(after)

    for _ in range(middleware):
        app.middleware(passthrough)
    for _ in range(hooks):
        target.before_request(before)
        target.after_request(after)

    async def handler(req: Request, image_id: int = 42):
        if streaming:
            async def chunks():
                for _ in range(8):
                    yield b'x' * 1024
            return StreamingResponse(chunks())
        return {'id': image_id, 'steps': getattr(req.state, 'steps', 0)}

    for _ in range(decorators):
        handler = decorate(handler)
    route = '/images/<int:image_id>' if dynamic else '/images'
    target.get(route)(handler)
    if reject:
        @target.before_request
        async def deny(req: Request):
            return {'error': 'denied'}, 403
    if root is not None:
        app.register_module(root)
    path = ''.join(f'/m{index}' for index in range(depth)) + ('/images/42' if dynamic else '/images')
    return Workload(name, app, path, depth + hooks, streaming, reject)

def create_workloads() -> list[Workload]:
    return [
        create_workload('plain'),
        create_workload('dynamic', dynamic=True),
        create_workload('middleware_1', middleware=1),
        create_workload('middleware_8', middleware=8),
        create_workload('decorators_2', decorators=2),
        create_workload('decorators_8', decorators=8),
        create_workload('dynamic_decorators_2', decorators=2, dynamic=True),
        create_workload('hooks_8', hooks=8),
        create_workload('nested_hooks_8', depth=8),
        create_workload('nested_middleware_3', depth=3, scoped_middleware=2, dynamic=True),
        create_workload('combined', middleware=1, decorators=2, hooks=2, depth=2, scoped_middleware=1),
        create_workload('rejection', middleware=1, decorators=2, hooks=2, depth=2, reject=True),
        create_workload('stream', middleware=2, decorators=2, hooks=2, streaming=True),
        create_websocket_workload('websocket_plain'),
        create_websocket_workload('websocket_middleware_1', middleware=1),
        create_websocket_workload('websocket_middleware_8', middleware=8),
        create_websocket_workload('websocket_decorators_2', decorators=2),
    ]

def create_websocket_workload(name: str, *, middleware: int = 0, decorators: int = 0) -> Workload:
    app = Ryuuseigun(name)
    async def passthrough(socket, next):
        await next(socket)
    for _ in range(middleware):
        app.websocket_middleware(passthrough)

    async def handler(socket: WebSocket, image_id: int):
        await socket.accept()
        await socket.send_json({'id': image_id})

    for _ in range(decorators):
        handler = decorate(handler)
    app.websocket('/images/<int:image_id>')(handler)
    return Workload(name, app, '/images/42', 0, websocket=True)

async def benchmark(iterations: int, rounds: int, selected: set[str]) -> list[dict]:
    workloads = [workload for workload in create_workloads() if not selected or workload.name in selected]
    scopes: list[ASGIScope] = []
    samples: list[list[float]] = [[] for _ in workloads]
    pending = Event()

    async def receive() -> ASGIMessage:
        await pending.wait()
        raise AssertionError('This workload does not read request bodies')

    async def send(message: ASGIMessage) -> None:
        pass

    async def receive_socket() -> ASGIMessage:
        return {'type': 'websocket.connect'}

    for workload in workloads:
        if workload.websocket:
            async with workload.app.test_client().websocket(workload.path) as socket:
                assert await socket.receive_json() == {'id': 42}
        else:
            response = await workload.app.test_client().get(workload.path)
            assert response.status_code == (403 if workload.reject else 200), workload.name
            if workload.streaming:
                assert response.body == b'x' * 8192
            else:
                expected = {'error': 'denied'} if workload.reject else {'id': 42, 'steps': workload.hooks}
                assert response.json() == expected, (workload.name, response.body)
        scope: ASGIScope = {
            'type': 'websocket' if workload.websocket else 'http',
            'method': 'GET', 'path': workload.path, 'query_string': b'',
            'headers': [(b'host', b'localhost')], 'http_version': '1.1',
            'asgi': {'version': '3.0', 'spec_version': '2.5'},
        }
        scopes.append(scope)
        receive_call = receive_socket if workload.websocket else receive
        for _ in range(500 if not workload.streaming else 50):
            await workload.app(scope, receive_call, send)

    # Reverse alternating rounds so later workloads do not always run on a warmer CPU.
    for round_index in range(rounds):
        order = range(len(workloads)) if round_index % 2 == 0 else reversed(range(len(workloads)))
        for index in order:
            workload = workloads[index]
            scope = scopes[index]
            receive_call = receive_socket if workload.websocket else receive
            count = max(20, iterations // 10) if workload.streaming else iterations
            started = perf_counter_ns()
            for _ in range(count):
                await workload.app(scope, receive_call, send)
            samples[index].append((perf_counter_ns() - started) / count / 1000)

    return [
        {'name': workload.name, 'median_us': median(times), 'samples_us': times}
        for workload, times in zip(workloads, samples, strict=True)
    ]

def benchmark_finalization(route_count: int, rounds: int) -> dict:
    def create_app():
        app = Ryuuseigun('finalization')
        api = Module('api', url_prefix='/api')
        app.middleware(passthrough)
        api.middleware(passthrough)
        api.middleware(passthrough)
        api.before_request(before)
        api.after_request(after)

        async def handler(req: Request, image_id: int):
            return {'id': image_id, 'steps': req.state.steps}

        for index in range(route_count):
            api.get(f'/images/{index}/<int:image_id>', name=f'image_{index}')(handler)
        app.register_module(api)
        return app

    samples = []
    for _ in range(rounds):
        app = create_app()
        collect()
        started = perf_counter_ns()
        app.finalize()
        samples.append((perf_counter_ns() - started) / 1_000_000)

    # Trace a separate run so allocation tracking does not inflate timings.
    app = create_app()
    collect()
    start()
    app.finalize()
    retained, peak = get_traced_memory()
    stop()

    async def validate():
        for index in (0, route_count - 1):
            response = await app.test_client().get(f'/api/images/{index}/42')
            assert response.status_code == 200 and response.json() == {'id': 42, 'steps': 1}
        assert (await app.test_client().post('/api/images/0/42')).status_code == 405
        assert (await app.test_client().request('OPTIONS', '/api/images/0/42')).status_code == 204

    run(validate())
    return {
        'routes': route_count, 'median_ms': median(samples), 'samples_ms': samples,
        'retained_bytes': retained, 'peak_bytes': peak,
    }

if __name__ == '__main__':
    parser = ArgumentParser(description='In-process middleware, decorator, and hook benchmarks with response validation.')
    parser.add_argument('--iterations', type=int, default=6000)
    parser.add_argument('--rounds', type=int, default=9)
    parser.add_argument('--only', nargs='*', default=[])
    parser.add_argument('--finalize-routes', type=int, default=0, help='Measure finalization of this many routes instead.')
    args = parser.parse_args()
    if args.iterations < 1 or args.rounds < 1:
        parser.error('iterations and rounds must be positive')
    if args.finalize_routes < 0:
        parser.error('finalize-routes must be non-negative')
    if args.finalize_routes:
        print(dumps(benchmark_finalization(args.finalize_routes, args.rounds), indent=2))
    else:
        print(dumps(run(benchmark(args.iterations, args.rounds, set(args.only))), indent=2))
