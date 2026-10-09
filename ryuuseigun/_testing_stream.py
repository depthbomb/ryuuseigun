"""A bounded ASGI test transport with explicit consumption and disconnection."""
from math import isfinite
from typing import Any, Self
from ryuuseigun.headers import Headers
from ryuuseigun._tasks import cancel_and_join
from collections.abc import Callable, AsyncIterator, AsyncGenerator
from ryuuseigun.types import ASGIScope, ASGIMessage, ASGIApplication
from asyncio import Task, Event, Queue, timeout, wait, create_task, ensure_future, FIRST_COMPLETED

class HTTPTestStream:
    """Consume HTTP chunks while the application is still running.

    Use through TestClient.stream(). One response event waits for consumption,
    giving deterministic backpressure without simulating TCP buffers. Exiting
    early disconnects and joins the application before its lifespan can close.
    The timeout bounds each receive and graceful cleanup, not the stream's total
    duration. This is an ASGI behavior test, not a network performance test.
    """
    def __init__(
        self, app: ASGIApplication, scope: ASGIScope, body: AsyncGenerator[bytes, None], *,
        timeout_seconds: float, on_headers: Callable[[Headers], None], on_enter: Callable[[], object],
    ) -> None:
        if not isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError('stream timeout must be finite and positive')
        self.app = app
        self.scope = scope
        self.status_code = 0
        self.headers = Headers()
        self.trailers = Headers()
        self.early_hints: list[ASGIMessage] = []
        self.timeout = timeout_seconds
        self._body = body
        self._on_headers = on_headers
        self._on_enter = on_enter
        self._messages: Queue[tuple[ASGIMessage, Event]] = Queue(maxsize=1)
        self._disconnected = Event()
        self._task: Task[None] | None = None
        self._body_complete = False
        self._response_complete = False
        self._has_trailers = False
        self._iterated = False
        self._closed = False

    async def __aenter__(self) -> Self:
        if self._task is not None or self._closed:
            raise RuntimeError('A test stream can only be entered once')
        self._on_enter()
        self._task = create_task(self.app(self.scope, self._receive, self._send))
        try:
            while True:
                message = await self._next_message()
                if message['type'] == 'http.response.early_hint':
                    self.early_hints.append(message)
                    continue
                if message['type'] != 'http.response.start':
                    raise RuntimeError('Expected HTTP response headers')
                self.status_code = message['status']
                self.headers = Headers.from_raw(message.get('headers', []))
                self._has_trailers = bool(message.get('trailers'))
                self._on_headers(self.headers)
                return self
        except BaseException:
            await self._cancel()
            raise

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.aclose()

    def disconnect(self) -> None:
        """Signal a client disconnect; pending sends fail and receives are released."""
        self._disconnected.set()

    async def iter_bytes(self) -> AsyncIterator[bytes]:
        """Iterate once. Application errors propagate, including errors after headers."""
        if self._task is None or self._closed:
            raise RuntimeError('Enter the stream context before reading')
        if self._iterated:
            raise RuntimeError('The response stream has already been consumed')
        self._iterated = True
        while True:
            message = await self._next_message()
            if message['type'] != 'http.response.body':
                raise RuntimeError('Expected an HTTP response body')
            chunk = message.get('body', b'')
            if chunk:
                yield chunk
            if not message.get('more_body', False):
                break
        if self._has_trailers:
            while True:
                message = await self._next_message()
                if message['type'] != 'http.response.trailers':
                    raise RuntimeError('Expected HTTP response trailers')
                for name, value in message.get('headers', []):
                    self.trailers.add(name.decode('latin-1'), value.decode('latin-1'))
                if not message.get('more_trailers', False):
                    break
        async with timeout(self.timeout):
            await self._task
        self._response_complete = True

    async def read(self) -> bytes:
        return b''.join([chunk async for chunk in self.iter_bytes()])

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.disconnect()
        try:
            if self._task is not None:
                async with timeout(self.timeout):
                    try:
                        await self._task
                    except OSError:
                        # ASGI permits a send on a disconnected connection to fail.
                        if self._response_complete:
                            raise
        finally:
            await self._cancel()

    async def _cancel(self) -> None:
        self._closed = True
        self.disconnect()
        try:
            if self._task is not None:
                await cancel_and_join(self._task)
        finally:
            await self._body.aclose()

    async def _receive(self) -> ASGIMessage:
        if self._disconnected.is_set():
            return {'type': 'http.disconnect'}
        if not self._body_complete:
            item = ensure_future(anext(self._body))
            disconnect = create_task(self._disconnected.wait())
            try:
                await wait((item, disconnect), return_when=FIRST_COMPLETED)
                if disconnect.done():
                    return {'type': 'http.disconnect'}
                try:
                    chunk = item.result()
                except StopAsyncIteration:
                    self._body_complete = True
                    return {'type': 'http.request', 'body': b'', 'more_body': False}
                if not isinstance(chunk, bytes):
                    raise TypeError('Streamed request chunks must be bytes')
                return {'type': 'http.request', 'body': chunk, 'more_body': True}
            finally:
                await cancel_and_join(item, disconnect)
        await self._disconnected.wait()
        return {'type': 'http.disconnect'}

    async def _send(self, message: ASGIMessage) -> None:
        if self._disconnected.is_set():
            raise OSError('Test client disconnected')
        acknowledged = Event()
        await self._messages.put((message, acknowledged))
        consumed = create_task(acknowledged.wait())
        disconnect = create_task(self._disconnected.wait())
        try:
            await wait((consumed, disconnect), return_when=FIRST_COMPLETED)
            if disconnect.done():
                raise OSError('Test client disconnected')
        finally:
            await cancel_and_join(consumed, disconnect)

    async def _next_message(self) -> ASGIMessage:
        if self._task is None:
            raise RuntimeError('Stream has not started')
        incoming = create_task(self._messages.get())
        try:
            async with timeout(self.timeout):
                await wait((incoming, self._task), return_when=FIRST_COMPLETED)
                if incoming.done():
                    message, acknowledged = incoming.result()
                    acknowledged.set()
                    return message
                self._task.result()
                raise RuntimeError('ASGI application exited before completing its response')
        finally:
            await cancel_and_join(incoming)
