"""One Granian process with asyncio, HTTP/1.1 and graceful, bounded shutdown.

The embedded server avoids platform-specific process signals and extra worker
processes. It is used only for tests and benchmarks, and binds to loopback.
"""
from socket import socket
from importlib import import_module
from time import sleep, perf_counter
from http.client import HTTPConnection
from multiprocessing import get_context
from asyncio import run, create_task, sleep as asleep

def _serve(target, port, stop, root_path, factory):
    from granian.server.embed import Server
    from granian.constants import HTTPModes, Interfaces

    async def main():
        module, name = target.split(':', 1)
        server = Server(
            getattr(import_module(module), name), address='127.0.0.1', port=port,
            interface=Interfaces.ASGI, http=HTTPModes.http1, runtime_threads=1,
            log_enabled=False, url_path_prefix=root_path or None, factory=factory,
        )

        async def watch():
            while not stop.is_set():
                await asleep(0.05)
            server.stop()

        watcher = create_task(watch())
        try:
            await server.serve()
        finally:
            watcher.cancel()
            try:
                await watcher
            except BaseException:
                pass

    run(main())

class ServerProcess:
    def __init__(self, target, *, root_path='', health_path='/health', factory=False):
        context = get_context('spawn')
        with socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            self.port = reservation.getsockname()[1]
        self.stop = context.Event()
        self.process = context.Process(target=_serve, args=(target, self.port, self.stop, root_path, factory))
        self.health_path = health_path

    def __enter__(self):
        self.process.start()
        deadline = perf_counter() + 15
        try:
            while self.process.is_alive() and perf_counter() < deadline:
                connection = self.connect(timeout=0.25)
                try:
                    connection.request('GET', self.health_path)
                    response = connection.getresponse()
                    response.read()
                    if response.status == 200:
                        return self
                except OSError:
                    pass
                finally:
                    connection.close()
                sleep(0.05)
            raise RuntimeError(f'Server did not become ready: exit code {self.process.exitcode}')
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, exc_type, exc, traceback):
        self.stop.set()
        self.process.join(10)
        forced = self.process.is_alive()
        if forced:
            self.process.terminate()
            self.process.join(5)
        if self.process.is_alive():
            self.process.kill()
            self.process.join(5)
        if exc_type is None and (forced or self.process.exitcode != 0):
            raise RuntimeError(f'Server failed to shut down cleanly: exit code {self.process.exitcode}')

    def connect(self, *, timeout=5):
        return HTTPConnection('127.0.0.1', self.port, timeout=timeout)
