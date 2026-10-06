from json import dumps
from statistics import median
from asyncio import run, Event
from time import perf_counter_ns
from argparse import ArgumentParser
from benchmarks.pipeline import create_app
from ryuuseigun.types import ASGIScope, ASGIMessage
from ryuuseigun import Config, Headers, Request, Response, Ryuuseigun, UploadFile, StreamingResponse

def create_workloads() -> list[tuple[str, Ryuuseigun, list[ASGIScope], bytes, int]]:
    app = Ryuuseigun(__name__)

    @app.get('/health')
    async def health(req: Request) -> dict[str, bool]:
        return {'ok': True}

    @app.get('/users/<int:user_id>')
    async def user(req: Request, user_id: int) -> dict[str, int]:
        return {'id': user_id}

    @app.get('/headers')
    async def headers(req: Request) -> Response:
        response = Response(req.headers['x-token'])
        for index in range(12):
            response.headers.add(f'X-Header-{index}', str(index))
        response.headers.add('Set-Cookie', 'a=1')
        response.headers.add('Set-Cookie', 'b=2')
        return response

    @app.post('/json')
    async def json_body(req: Request):
        return await req.json()

    @app.post('/upload')
    async def upload(req: Request) -> dict[str, int]:
        form = await req.form()
        file = form['file']
        assert isinstance(file, UploadFile)
        data = await file.read()
        await file.seek(0)
        assert await file.tell() == 0
        assert data == b'x' * len(data)
        return {'size': len(data)}

    @app.get('/stream')
    async def stream(req: Request) -> StreamingResponse:
        async def chunks():
            for _ in range(8):
                yield b'x' * 1024

        return StreamingResponse(chunks())

    def scope(path: str, method: str = 'GET', content_type: str = '') -> ASGIScope:
        headers = Headers({'Host': 'localhost', 'X-Token': 'secret'})
        if content_type:
            headers['Content-Type'] = content_type
        return {
            'type': 'http', 'method': method, 'path': path, 'query_string': b'',
            'headers': headers.raw(), 'http_version': '1.1',
            'asgi': {'version': '3.0', 'spec_version': '2.5'},
        }

    def multipart(size: int) -> bytes:
        return (
            b'--boundary\r\nContent-Disposition: form-data; name="file"; filename="data.bin"\r\n'
            b'Content-Type: application/octet-stream\r\n\r\n' + b'x' * size + b'\r\n--boundary--\r\n'
        )
    workloads = [
        ('static_json', app, [scope('/health')], b'', 1),
        ('dynamic_json', app, [scope(f'/users/{index}') for index in range(256)], b'', 1),
        ('headers', app, [scope('/headers')], b'', 1),
        ('post_json', app, [scope('/json', 'POST', 'application/json')], b'{"value":42}', 1),
        ('head', app, [scope('/health', 'HEAD')], b'', 1),
        ('options', app, [scope('/users/42', 'OPTIONS')], b'', 1),
        ('not_found', app, [scope('/missing')], b'', 1),
        ('stream', app, [scope('/stream')], b'', 10),
        ('multipart_memory', app, [scope('/upload', 'POST', 'multipart/form-data; boundary=boundary')], multipart(4096), 20),
        (
            'multipart_memory_large', app, [scope('/upload', 'POST', 'multipart/form-data; boundary=boundary')],
            multipart(512 * 1024), 50,
        ),
    ]
    disk_app = Ryuuseigun('disk', config=Config(upload_spool_threshold=1))
    disk_app.post('/upload')(upload)
    workloads.append((
        'multipart_disk', disk_app,
        [scope('/upload', 'POST', 'multipart/form-data; boundary=boundary')], multipart(4096), 20,
    ))
    workloads.append((
        'multipart_disk_large', disk_app,
        [scope('/upload', 'POST', 'multipart/form-data; boundary=boundary')], multipart(512 * 1024), 50,
    ))
    for depth in (1, 3):
        scoped_app, path = create_app(depth)
        workloads.append((f'module_depth_{depth}', scoped_app, [scope(path)], b'', 1))

    return workloads

async def benchmark(iterations: int, rounds: int, selected: set[str]) -> list[dict]:
    reports = []
    for name, app, scopes, body, divisor in create_workloads():
        if selected and name not in selected:
            continue
        app.finalize()
        pending = Event()
        received = False

        async def receive(payload: bytes = body, disconnected: Event = pending) -> ASGIMessage:
            nonlocal received
            if not received:
                received = True
                return {'type': 'http.request', 'body': payload, 'more_body': False}
            await disconnected.wait()
            raise AssertionError('Unreachable')

        async def send(message: ASGIMessage) -> None:
            pass

        messages = []

        async def capture(message: ASGIMessage, captured: list[ASGIMessage] = messages) -> None:
            captured.append(message)

        await app(scopes[0], receive, capture)
        expected_status = {'not_found': 404, 'options': 204}.get(name, 200)
        assert messages[0]['status'] == expected_status, (name, messages)
        response_body = b''.join(message.get('body', b'') for message in messages)
        expected_body = {
            'static_json': b'{"ok":true}', 'dynamic_json': b'{"id":0}', 'headers': b'secret',
            'post_json': b'{"value":42}', 'head': b'', 'options': b'', 'stream': b'x' * 8192,
            'multipart_memory': b'{"size":4096}', 'multipart_disk': b'{"size":4096}',
            'multipart_memory_large': b'{"size":524288}', 'multipart_disk_large': b'{"size":524288}',
            'module_depth_1': b'{"id":42,"before":1}', 'module_depth_3': b'{"id":42,"before":3}',
        }.get(name)
        if expected_body is not None:
            assert response_body == expected_body, (name, response_body)

        count = max(20, iterations // divisor)
        for index in range(min(count, 500)):
            received = False
            await app(scopes[index % len(scopes)], receive, send)
        samples = []
        for _ in range(rounds):
            started = perf_counter_ns()
            for index in range(count):
                received = False
                await app(scopes[index % len(scopes)], receive, send)
            samples.append((perf_counter_ns() - started) / count / 1000)
        reports.append({'name': name, 'iterations': count, 'median_us': median(samples), 'samples_us': samples})

    return reports

if __name__ == '__main__':
    parser = ArgumentParser(description='In-process ASGI benchmarks with validated response bodies.')
    parser.add_argument('--iterations', type=int, default=5000)
    parser.add_argument('--rounds', type=int, default=7)
    parser.add_argument('--only', nargs='*', default=[])
    args = parser.parse_args()
    print(dumps(run(benchmark(args.iterations, args.rounds, set(args.only))), indent=2))
