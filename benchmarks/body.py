"""Measure buffered body and multipart reads through the ASGI request pipeline."""
from json import dumps
from pathlib import Path
from asyncio import run, Event
from statistics import median
from platform import platform
from time import perf_counter_ns
from argparse import ArgumentParser
from sys import version as python_version
from tracemalloc import start, stop, get_traced_memory
from ryuuseigun import Config, Request, Response, Ryuuseigun

async def measure(kind, size, chunk_size, iterations, rounds):
    payload = (bytes(range(256)) * (size // 256 + 1))[:size]
    wire_body = payload
    headers = []
    if kind == 'multipart':
        wire_body = (
            b'--boundary\r\nContent-Disposition: form-data; name="file"; filename="data.bin"\r\n'
            b'Content-Type: application/octet-stream\r\n\r\n' + payload + b'\r\n--boundary--\r\n'
        )
        headers.append((b'content-type', b'multipart/form-data; boundary=boundary'))

    chunks = [wire_body[index:index + chunk_size] for index in range(0, len(wire_body), chunk_size)] or [b'']
    scope = {'type': 'http', 'method': 'POST', 'path': '/', 'headers': headers}
    app = Ryuuseigun('body-benchmark', config=Config(max_request_body_size=None, propagate_exceptions=True))
    validate = True

    @app.post('/')
    async def read(req: Request) -> Response:
        if kind == 'body':
            data = await req.body()
            if validate:
                assert data == payload
                assert await req.body() is data
            length = len(data)
        else:
            length = 0
            parts = 0
            async for part in req.multipart():
                data = await part.read()
                if validate:
                    assert data == payload
                    assert part.complete and part.size == size
                length += len(data)
                parts += 1
            if validate:
                assert parts == 1
        return Response(str(length))

    app.finalize()
    position = 0
    pending = Event()

    async def receive():
        nonlocal position
        if position == len(chunks):
            await pending.wait()
            raise AssertionError('Unreachable')
        chunk = chunks[position]
        position += 1
        return {'type': 'http.request', 'body': chunk, 'more_body': position < len(chunks)}

    async def send(message):
        pass

    messages = []

    async def capture(message):
        messages.append(message)

    await app(scope, receive, capture)
    assert messages[0]['status'] == 200, messages
    assert b''.join(message.get('body', b'') for message in messages) == str(size).encode()
    validate = False

    count = min(iterations, max(20, 256 * 1024 * 1024 // max(size, 1)))
    for _ in range(min(count, 100)):
        position = 0
        await app(scope, receive, send)

    samples = []
    for _ in range(rounds):
        started = perf_counter_ns()
        for _ in range(count):
            position = 0
            await app(scope, receive, send)
        samples.append((perf_counter_ns() - started) / count / 1000)

    # Measure allocations separately so tracing does not distort timing samples.
    start()
    try:
        position = 0
        await app(scope, receive, send)
        _, peak_bytes = get_traced_memory()
    finally:
        stop()

    return {
        'kind': kind, 'size': size, 'chunk_size': chunk_size, 'iterations': count,
        'median_us': median(samples), 'samples_us': samples, 'peak_traced_bytes': peak_bytes,
    }

async def benchmark(options):
    results = []
    for chunk_size in options.chunk_sizes:
        for size in options.sizes:
            for kind in ('body', 'multipart'):
                results.append(await measure(kind, size, chunk_size, options.iterations, options.rounds))
    return {
        'environment': {'python': python_version, 'platform': platform()},
        'settings': {
            'label': options.label, 'iterations': options.iterations, 'rounds': options.rounds,
            'transport': 'in-process ASGI, prebuilt input chunks, no network I/O',
            'memory': 'peak traced allocations for one request, excluding prebuilt input',
        },
        'results': results,
    }

if __name__ == '__main__':
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--sizes', nargs='+', type=int, default=[0, 1024, 65536, 1048576, 8388608])
    parser.add_argument('--chunk-sizes', nargs='+', type=int, default=[65536])
    parser.add_argument('--iterations', type=int, default=2000)
    parser.add_argument('--rounds', type=int, default=9)
    parser.add_argument('--label', default='')
    parser.add_argument('--output', type=Path, help='Save the report outside the checkout.')
    options = parser.parse_args()
    if min(options.sizes) < 0 or min(*options.chunk_sizes, options.iterations, options.rounds) < 1:
        parser.error('sizes must be non-negative; chunk sizes, iterations and rounds must be positive')
    result = dumps(run(benchmark(options)), indent=2)
    if options.output:
        options.output.write_text(result + '\n', encoding='utf-8')
    print(result)
