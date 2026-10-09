"""Compare real HTTP requests through Granian on loopback.

Run from the checkout: python -m benchmarks.server --requests 5000 --rounds 3
Install .[benchmark] first. Use --output with a path outside the checkout to
save the JSON report. All frameworks use one embedded Granian process, asyncio,
HTTP/1.1 keep-alive and one Rust runtime thread. There is no TLS or database.

This is a closed-loop client: every connection waits for its previous response.
The client shares the machine with the server, so these are local comparisons,
not saturation throughput or predictions of production tail latency. RSS is
sampled every 10 ms and includes the server plus its framework dependencies.
The report includes every round, versions, platform, settings and git revision.
"""
from math import ceil
from os import cpu_count
from pathlib import Path
from psutil import Process
from subprocess import run
from json import dumps, loads
from argparse import ArgumentParser
from time import sleep, perf_counter
from importlib.metadata import version
from benchmarks._server import ServerProcess
from threading import Event, Thread, Barrier
from concurrent.futures import ThreadPoolExecutor
from platform import platform, processor, python_version

WORKLOADS = {
    'json': ('GET', '/users/42', None, {'id': 42}),
    'stream_64k': ('GET', '/stream', None, b'x' * 65536),
    'upload_64k': ('POST', '/upload', b'x' * 65536, {'size': 65536}),
}

def request(connection, workload):
    method, path, body, expected = WORKLOADS[workload]
    started = perf_counter()
    connection.request(method, path, body=body)
    response = connection.getresponse()
    content = response.read()
    elapsed = perf_counter() - started
    actual = loads(content) if isinstance(expected, dict) else content
    if response.status != 200 or actual != expected:
        raise RuntimeError(f'{workload} returned an unexpected response: {response.status}')
    return elapsed * 1000

def measure(server, workload, requests, concurrency):
    barrier = Barrier(concurrency + 1, timeout=15)

    def worker(count):
        connection = server.connect(timeout=10)
        try:
            connection.connect()
            barrier.wait()
            return [request(connection, workload) for _ in range(count)]
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker, requests // concurrency + (index < requests % concurrency))
                   for index in range(concurrency)]
        started = perf_counter()
        barrier.wait()
        samples = sorted(sample for future in futures for sample in future.result())
        elapsed = perf_counter() - started
    return {
        'requests': len(samples), 'seconds': elapsed, 'requests_per_second': len(samples) / elapsed,
        'latency_ms': {f'p{percentile}': samples[ceil(len(samples) * percentile / 100) - 1]
                       for percentile in (50, 95, 99)},
    }

def benchmark(options):
    revision = run(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True)
    dirty = run(['git', 'status', '--porcelain'], capture_output=True, text=True)
    report = {
        'environment': {
            'platform': platform(), 'processor': processor(), 'logical_cpus': cpu_count(),
            'python': python_version(),
            'versions': {name: version(name) for name in ('ryuuseigun', 'granian', 'starlette', 'quart', 'orjson', 'psutil')},
            'git_revision': revision.stdout.strip(), 'git_dirty': bool(dirty.stdout.strip()),
        },
        'settings': {
            'requests_per_round': options.requests, 'rounds': options.rounds, 'concurrency': options.concurrency,
            'warmup_per_workload': options.warmup, 'transport': 'HTTP/1.1 keep-alive on loopback',
            'server': 'Granian embedded, one process, one runtime thread, asyncio',
            'client': 'closed-loop threads on the server machine', 'rss_sampling_seconds': 0.01,
        },
        'results': {},
    }
    for framework in options.frameworks:
        with ServerProcess(f'benchmarks.server_apps:create_{framework}', factory=True) as server:
            process = Process(server.process.pid)
            memory = []
            stop = Event()

            def sample_memory(stop, memory, process):
                while not stop.is_set():
                    memory.append(process.memory_info().rss)
                    stop.wait(0.01)

            sampler = Thread(target=sample_memory, args=(stop, memory, process), daemon=True)
            sampler.start()
            results = {}
            try:
                for workload in WORKLOADS:
                    connection = server.connect()
                    try:
                        for _ in range(options.warmup):
                            request(connection, workload)
                    finally:
                        connection.close()
                    before = process.cpu_times()
                    rounds = [measure(server, workload, options.requests, options.concurrency)
                              for _ in range(options.rounds)]
                    after = process.cpu_times()
                    results[workload] = {
                        'rounds': rounds,
                        'server_cpu_seconds': after.user + after.system - before.user - before.system,
                    }
                    sleep(0.1)
            finally:
                stop.set()
                sampler.join()
            report['results'][framework] = {'workloads': results, 'peak_sampled_rss_bytes': max(memory)}
    return report

if __name__ == '__main__':
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--requests', type=int, default=5000)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--concurrency', type=int, default=16)
    parser.add_argument('--warmup', type=int, default=200)
    parser.add_argument('--frameworks', nargs='+', choices=['ryuuseigun', 'starlette', 'quart'],
                        default=['ryuuseigun', 'starlette', 'quart'])
    parser.add_argument('--output', type=Path)
    options = parser.parse_args()
    if min(options.requests, options.rounds, options.concurrency, options.warmup) < 1:
        parser.error('requests, rounds, concurrency and warmup must be positive')
    if options.concurrency > options.requests:
        parser.error('concurrency cannot exceed requests')
    result = dumps(benchmark(options), indent=2)
    if options.output:
        options.output.write_text(result + '\n', encoding='utf-8')
    print(result)
