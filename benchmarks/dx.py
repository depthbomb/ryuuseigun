"""Costs of opt-in HTTP helpers, including the in-process test transport."""
import sys
from json import dumps
from asyncio import run
from pathlib import Path
from statistics import median
from time import perf_counter_ns
from argparse import ArgumentParser

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ryuuseigun import Response, redirect, Ryuuseigun  # noqa: E402

async def benchmark(iterations, rounds):
    app = Ryuuseigun('dx-benchmark')

    @app.get('/')
    async def home(req):
        return {'ok': True}

    @app.post('/form')
    async def form(req):
        values = await req.form()
        return {'value': values['value']}

    client = app.test_client()
    workloads = {
        'client_plain': lambda: client.get('/'),
        'client_form': lambda: client.post('/form', form={'value': 'test'}),
        'client_multipart': lambda: client.post('/form', form={'value': 'test'}, files={
            'file': ('test.txt', b'content', 'text/plain'),
        }),
    }
    result = {}
    for name, workload in workloads.items():
        response = await workload()
        assert response.status_code == 200
        assert isinstance(response.json(), dict)
        samples = []
        for _ in range(rounds):
            started = perf_counter_ns()
            for _ in range(iterations):
                await workload()
            samples.append((perf_counter_ns() - started) / iterations / 1000)
        result[name] = {'median_us': median(samples), 'samples_us': samples}

    def cookie():
        response = Response()
        response.set_cookie('session', 'token', secure=True, httponly=True)
        return response

    for name, factory in {'response': Response, 'cookie': cookie, 'redirect': lambda: redirect('/account', 303)}.items():
        assert isinstance(factory(), Response)
        samples = []
        for _ in range(rounds):
            started = perf_counter_ns()
            for _ in range(iterations):
                factory()
            samples.append((perf_counter_ns() - started) / iterations / 1000)
        result[name] = {'median_us': median(samples), 'samples_us': samples}
    return result

if __name__ == '__main__':
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--iterations', type=int, default=1000)
    parser.add_argument('--rounds', type=int, default=7)
    options = parser.parse_args()
    if options.iterations < 1 or options.rounds < 1:
        parser.error('iterations and rounds must be positive')
    print(dumps(run(benchmark(options.iterations, options.rounds)), indent=2))
