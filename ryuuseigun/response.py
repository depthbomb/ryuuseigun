from copy import copy
from os import PathLike
from pathlib import Path
from http import HTTPStatus
from time import perf_counter
from mimetypes import guess_type
from datetime import UTC, datetime
from http.cookies import SimpleCookie
from ryuuseigun.headers import Headers
from ryuuseigun._tasks import run_sync, cancel_and_join
from typing import Any, Optional, TypeAlias, TYPE_CHECKING
from orjson import dumps, loads, OPT_INDENT_2, OPT_APPEND_NEWLINE
from email.utils import formatdate, format_datetime, parsedate_to_datetime
from ryuuseigun.types import Send, Receive, ASGIScope, JSONValue, HeaderMapping
from asyncio import wait, Queue, sleep, to_thread, ensure_future, FIRST_COMPLETED
from collections.abc import Mapping, Callable, Awaitable, AsyncIterable, AsyncIterator
from ryuuseigun.constants import (
    MediaType,
    RangeUnit,
    HTTPMethod,
    HeaderName,
    StatusCode,
    ASGIExtension,
    CacheDirective,
    ASGIMessageType,
    ContentEncoding,
)

if TYPE_CHECKING:
    from ryuuseigun.request import Request

AfterSend: TypeAlias = Callable[[], Awaitable[None]]
BodyChunk: TypeAlias = bytes | str
TrailerProvider: TypeAlias = HeaderMapping | Callable[[], Awaitable[HeaderMapping]]


def _extensions(scope: Optional[ASGIScope]) -> Mapping[str, Any]:
    if scope is None:
        return {}
    value = scope.get('extensions', {})
    return value if isinstance(value, Mapping) else {}

def redirect(location: str, status_code: int = StatusCode.FOUND) -> 'Response':
    """Create a redirect; use 303 to follow a POST with a GET."""
    if status_code not in {301, 302, 303, 307, 308}:
        raise ValueError('Redirect status must be 301, 302, 303, 307, or 308')
    return Response(status_code=status_code, headers={HeaderName.LOCATION: location})


def _quote_disposition(value: str) -> str:
    return value.replace('\\', '\\\\').replace('"', '\\"')


class Response:
    __slots__ = (
        '_after_send',
        '_compression',
        '_compression_minimum_size',
        'body',
        'early_hints',
        'headers',
        'status_code',
        'trailers',
    )

    default_media_type = MediaType.TEXT_UTF8

    def __init__(
        self,
        body: bytes | str = b'',
        status_code: int = StatusCode.OK,
        headers: Optional[HeaderMapping] = None,
        media_type: Optional[str] = None,
        *,
        trailers: Optional[TrailerProvider] = None,
        early_hints: tuple[str, ...] = (),
    ) -> None:
        if not StatusCode.MINIMUM <= status_code <= StatusCode.MAXIMUM:
            raise ValueError('status_code must be between 100 and 599')

        self.body = body.encode('utf-8') if isinstance(body, str) else body
        self.status_code = status_code
        self.headers = Headers(headers)
        self.trailers = trailers
        self.early_hints = list(early_hints)
        self._after_send: list[AfterSend] = []
        self._compression: Optional[str] = None
        self._compression_minimum_size = 500
        if media_type is not None:
            self.headers[HeaderName.CONTENT_TYPE] = media_type
        elif self.body and HeaderName.CONTENT_TYPE not in self.headers:
            self.headers[HeaderName.CONTENT_TYPE] = self.default_media_type

    def after_send(self, handler: AfterSend) -> AfterSend:
        self._after_send.append(handler)
        return handler

    def set_cookie(
        self, name: str, value: str = '', *, max_age: int | None = None,
        expires: datetime | None = None, path: str = '/', domain: str | None = None,
        secure: bool = False, httponly: bool = False, samesite: str | None = 'lax',
    ) -> None:
        """Append a cookie without replacing other Set-Cookie headers."""
        if samesite is not None and samesite.lower() not in {'lax', 'strict', 'none'}:
            raise ValueError('samesite must be lax, strict, none, or None')
        if samesite is not None and samesite.lower() == 'none' and not secure:
            raise ValueError('SameSite=None cookies require secure=True')
        for attribute in (path, domain):
            if attribute is not None and any(ord(char) < 32 or ord(char) >= 127 or char == ';' for char in attribute):
                raise ValueError('Invalid cookie path or domain')
        if max_age is not None and (not isinstance(max_age, int) or isinstance(max_age, bool)):
            raise TypeError('max_age must be an integer')
        if name.startswith('__Secure-') and not secure:
            raise ValueError('__Secure- cookies require secure=True')
        if name.startswith('__Host-') and (not secure or path != '/' or domain is not None):
            raise ValueError('__Host- cookies require secure=True, path=/, and no domain')
        cookie = SimpleCookie()
        cookie[name] = value
        morsel = cookie[name]
        morsel['path'] = path
        if domain is not None:
            morsel['domain'] = domain
        if max_age is not None:
            morsel['max-age'] = str(max_age)
        if expires is not None:
            if expires.tzinfo is None or expires.utcoffset() is None:
                raise ValueError('Cookie expires must be a timezone-aware datetime')
            morsel['expires'] = format_datetime(expires.astimezone(UTC), usegmt=True)
        if secure:
            morsel['secure'] = True
        if httponly:
            morsel['httponly'] = True
        if samesite is not None:
            morsel['samesite'] = samesite.capitalize()
        self.headers.add(HeaderName.SET_COOKIE, morsel.OutputString())

    def delete_cookie(
        self, name: str, *, path: str = '/', domain: str | None = None,
        secure: bool = False, httponly: bool = False, samesite: str | None = 'lax',
    ) -> None:
        self.set_cookie(
            name, max_age=0, expires=datetime(1970, 1, 1, tzinfo=UTC), path=path,
            domain=domain, secure=secure, httponly=httponly, samesite=samesite,
        )

    def add_early_hint(self, link: str) -> None:
        if '\r' in link or '\n' in link:
            raise ValueError('Invalid Link header value')
        self.early_hints.append(link)

    def enable_compression(self, encoding: str, *, minimum_size: int = 500) -> None:
        if minimum_size < 0:
            raise ValueError('minimum_size must be non-negative')
        if encoding not in {ContentEncoding.BROTLI, ContentEncoding.GZIP, ContentEncoding.ZSTANDARD}:
            raise ValueError(f'Unsupported content encoding: {encoding}')
        self._compression = encoding
        self._compression_minimum_size = minimum_size

    async def send(
        self,
        send: Send,
        *,
        scope: Optional[ASGIScope] = None,
        receive: Optional[Receive] = None,
        request: Optional['Request[Any]'] = None,
        head: bool = False,
    ) -> None:
        del receive, request
        extensions = _extensions(scope) if self.early_hints or self.trailers is not None else {}
        if self.early_hints:
            await self._send_early_hints(send, extensions)

        suppress_body = self.status_code < StatusCode.OK or self.status_code in {
            StatusCode.NO_CONTENT,
            StatusCode.NOT_MODIFIED,
        }
        body = b'' if suppress_body else self.body
        headers = self.headers
        if self._compression is not None or headers._raw_items is not None:
            headers = Headers(headers)
        if self._compression is not None:
            if len(body) >= 64 * 1024:
                body = await to_thread(self._compressed_body, body, headers)
            else:
                body = self._compressed_body(body, headers)

        # Validate lazy raw headers before constructing a separate wire snapshot.
        # Sending must not add or remove headers on a reusable response.
        if self.status_code != StatusCode.NOT_MODIFIED:
            headers._materialize()
        raw_headers = headers.raw()
        if self.status_code < StatusCode.OK or self.status_code == StatusCode.NO_CONTENT:
            raw_headers = [(name, value) for name, value in raw_headers if name != b'content-length']
        elif self.status_code != StatusCode.NOT_MODIFIED:
            for name, _ in raw_headers:
                if name == b'content-length':
                    break
            else:
                raw_headers.append((b'content-length', str(len(body)).encode('ascii')))

        sends_trailers = self.trailers is not None and ASGIExtension.HTTP_TRAILERS in extensions and not head
        start: dict[str, Any] = {
            'type': ASGIMessageType.HTTP_RESPONSE_START,
            'status': self.status_code,
            'headers': raw_headers,
        }
        if sends_trailers:
            start['trailers'] = True

        try:
            await send(start)
            await send(
                {
                    'type': ASGIMessageType.HTTP_RESPONSE_BODY,
                    'body': b'' if head else body,
                    'more_body': False,
                }
            )
            if sends_trailers:
                await self._send_trailers(send)
        except OSError:
            return
        if self._after_send:
            await self._run_after_send()

    def _compressed_body(self, body: bytes, headers: Headers) -> bytes:
        encoding = self._compression
        if (
            encoding is None
            or len(body) < self._compression_minimum_size
            or not body
            or HeaderName.CONTENT_ENCODING in headers
            or self.status_code < StatusCode.OK
            or self.status_code in {StatusCode.NO_CONTENT, StatusCode.NOT_MODIFIED}
        ):
            return body

        if encoding == ContentEncoding.GZIP:
            from gzip import compress as gzip_compress

            compressed = gzip_compress(body, mtime=0)
        elif encoding == ContentEncoding.ZSTANDARD:
            from compression.zstd import compress as zstd_compress

            compressed = zstd_compress(body)
        else:
            try:
                from brotli import compress as brotli_compress
            except ImportError as error:
                raise RuntimeError("Brotli compression requires the 'brotli' package") from error
            compressed = brotli_compress(body)

        headers[HeaderName.CONTENT_ENCODING] = encoding
        headers.popall(HeaderName.CONTENT_LENGTH)
        headers.popall(HeaderName.ETAG)
        return compressed

    async def _send_early_hints(self, send: Send, extensions: Mapping[str, Any]) -> None:
        if not self.early_hints or ASGIExtension.HTTP_EARLY_HINT not in extensions:
            return
        await send(
            {
                'type': ASGIMessageType.HTTP_RESPONSE_EARLY_HINT,
                'links': [link.encode('latin-1') for link in self.early_hints],
            }
        )

    async def _send_trailers(self, send: Send) -> None:
        provider = self.trailers
        if provider is None:
            return
        values = await provider() if callable(provider) else provider
        await send(
            {
                'type': ASGIMessageType.HTTP_RESPONSE_TRAILERS,
                'headers': Headers(values).raw(),
                'more_trailers': False,
            }
        )

    async def _run_after_send(self) -> None:
        for handler in self._after_send:
            await handler()


class StreamingResponse(Response):
    __slots__ = ('body_iterator',)

    def __init__(
        self,
        body: AsyncIterable[BodyChunk],
        status_code: int = StatusCode.OK,
        headers: Optional[HeaderMapping] = None,
        media_type: Optional[str] = None,
        *,
        trailers: Optional[TrailerProvider] = None,
        early_hints: tuple[str, ...] = (),
    ) -> None:
        super().__init__(b'', status_code, headers, media_type, trailers=trailers, early_hints=early_hints)
        self.body_iterator = body

    async def send(
        self,
        send: Send,
        *,
        scope: Optional[ASGIScope] = None,
        receive: Optional[Receive] = None,
        request: Optional['Request[Any]'] = None,
        head: bool = False,
    ) -> None:
        extensions = _extensions(scope) if self.early_hints or self.trailers is not None else {}
        if self.early_hints:
            await self._send_early_hints(send, extensions)
        self.headers.popall(HeaderName.CONTENT_LENGTH)
        encoder = _StreamingEncoder(self._compression)
        if encoder.active:
            self.headers[HeaderName.CONTENT_ENCODING] = encoder.encoding
            self.headers.popall(HeaderName.ETAG)

        sends_trailers = self.trailers is not None and ASGIExtension.HTTP_TRAILERS in extensions and not head
        start: dict[str, Any] = {
            'type': ASGIMessageType.HTTP_RESPONSE_START,
            'status': self.status_code,
            'headers': self.headers.raw(),
        }
        if sends_trailers:
            start['trailers'] = True

        async def produce() -> None:
            iterator = self.body_iterator.__aiter__()
            try:
                await send(start)
                suppress_body = head or self.status_code < StatusCode.OK or self.status_code in {
                    StatusCode.NO_CONTENT, StatusCode.NOT_MODIFIED,
                }
                if not suppress_body:
                    # Sends and generators may complete without yielding. Bound
                    # each batch so empty or fast streams remain cancellable.
                    chunks_since_yield = 0
                    bytes_since_yield = 0
                    yield_deadline = perf_counter() + 0.001
                    async for chunk in iterator:
                        raw = _encode_chunk(chunk)
                        encoded = await run_sync(encoder.compress, raw) if encoder.active else raw
                        if encoded:
                            await send({'type': ASGIMessageType.HTTP_RESPONSE_BODY, 'body': encoded, 'more_body': True})
                        chunks_since_yield += 1
                        bytes_since_yield += len(raw)
                        if chunks_since_yield >= 16 or bytes_since_yield >= 64 * 1024 or perf_counter() >= yield_deadline:
                            await sleep(0)
                            chunks_since_yield = 0
                            bytes_since_yield = 0
                            yield_deadline = perf_counter() + 0.001

                tail = await run_sync(encoder.finish) if encoder.active and not suppress_body else b''
                await send({
                    'type': ASGIMessageType.HTTP_RESPONSE_BODY,
                    'body': tail,
                    'more_body': False,
                })
                if sends_trailers:
                    await self._send_trailers(send)
            finally:
                close = getattr(iterator, 'aclose', None)
                if close is not None:
                    await close()

        producer = ensure_future(produce())
        tasks = [producer]
        if receive is not None:
            tasks.append(ensure_future(_wait_for_disconnect(receive, request)))
        completed = False
        try:
            done, _ = await wait(tasks, return_when=FIRST_COMPLETED)
            for task in done:
                task.result()
            completed = producer in done
        except OSError:
            pass
        finally:
            await cancel_and_join(*tasks)
        if completed and self._after_send:
            await self._run_after_send()


class JSONResponse(Response):
    default_media_type = MediaType.JSON

    def __init__(
        self,
        value: Any,
        status_code: int = StatusCode.OK,
        headers: Optional[HeaderMapping] = None,
        *,
        pretty: bool = False,
        options: int = 0,
        media_type: str = MediaType.JSON,
    ) -> None:
        if pretty:
            options |= OPT_INDENT_2 | OPT_APPEND_NEWLINE
        super().__init__(dumps(value, option=options), status_code, headers, media_type)


class ServerSentEvent:
    __slots__ = ('comment', 'data', 'event', 'id', 'retry')

    def __init__(
        self,
        data: str,
        *,
        event: Optional[str] = None,
        id: Optional[str] = None,
        retry: Optional[int] = None,
        comment: Optional[str] = None,
    ) -> None:
        if retry is not None and retry < 0:
            raise ValueError('retry must be non-negative')
        self.data = data
        self.event = event
        self.id = id
        self.retry = retry
        self.comment = comment

    def encode(self) -> bytes:
        if self.event is not None and any(character in self.event for character in '\r\n'):
            raise ValueError('event must not contain CR or LF')
        if self.id is not None and any(character in self.id for character in '\r\n\0'):
            raise ValueError('id must not contain CR, LF, or NUL')
        lines: list[str] = []
        if self.comment is not None:
            lines.extend(f': {line}' for line in _event_lines(self.comment))
        if self.event is not None:
            lines.append(f'event: {self.event}')
        if self.id is not None:
            lines.append(f'id: {self.id}')
        if self.retry is not None:
            lines.append(f'retry: {self.retry}')
        lines.extend(f'data: {line}' for line in _event_lines(self.data))
        return ('\n'.join(lines) + '\n\n').encode('utf-8')


class EventStreamResponse(StreamingResponse):
    def __init__(
        self,
        events: AsyncIterable[ServerSentEvent | str],
        status_code: int = StatusCode.OK,
        headers: Optional[HeaderMapping] = None,
        *,
        heartbeat: Optional[float] = 15.0,
    ) -> None:
        if heartbeat is not None and heartbeat <= 0:
            raise ValueError('heartbeat must be positive or None')
        event_headers = Headers(headers)
        event_headers.setdefault(HeaderName.CACHE_CONTROL, CacheDirective.NO_CACHE)
        event_headers.setdefault(HeaderName.X_ACCEL_BUFFERING, 'no')
        super().__init__(
            _event_stream(events, heartbeat),
            status_code,
            event_headers,
            MediaType.EVENT_STREAM_UTF8,
        )

    def enable_compression(self, encoding: str, *, minimum_size: int = 500) -> None:
        del encoding, minimum_size


class FileResponse(Response):
    __slots__ = ('chunk_size', 'path')

    def __init__(
        self,
        path: str | PathLike[str],
        status_code: int = StatusCode.OK,
        headers: Optional[HeaderMapping] = None,
        media_type: Optional[str] = None,
        *,
        download_name: Optional[str] = None,
        as_attachment: bool = False,
        chunk_size: int = 64 * 1024,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError('chunk_size must be positive')
        self.path = Path(path).resolve()
        self.chunk_size = chunk_size
        selected_media_type = media_type or guess_type(download_name or self.path.name)[0] or MediaType.OCTET_STREAM
        super().__init__(b'', status_code, headers, selected_media_type)
        if as_attachment or download_name is not None:
            disposition = 'attachment' if as_attachment else 'inline'
            filename = _quote_disposition(download_name or self.path.name)
            self.headers[HeaderName.CONTENT_DISPOSITION] = f'{disposition}; filename="{filename}"'

    async def send(
        self,
        send: Send,
        *,
        scope: Optional[ASGIScope] = None,
        receive: Optional[Receive] = None,
        request: Optional['Request[Any]'] = None,
        head: bool = False,
    ) -> None:
        del receive, request
        response = copy(self)
        response.headers = Headers(self.headers)
        await response._send_file(send, scope=scope, head=head)

    async def _send_file(self, send: Send, *, scope: Optional[ASGIScope], head: bool) -> None:
        if self.status_code < StatusCode.OK or self.status_code in {StatusCode.NO_CONTENT, StatusCode.NOT_MODIFIED}:
            await super().send(send, scope=scope, head=head)
            return
        stat = await to_thread(self.path.stat)
        etag = f'"{stat.st_mtime_ns:x}-{stat.st_size:x}"'
        self.headers.setdefault(HeaderName.ACCEPT_RANGES, RangeUnit.BYTES)
        self.headers.setdefault(HeaderName.ETAG, etag)
        etag = self.headers[HeaderName.ETAG]
        self.headers.setdefault(HeaderName.LAST_MODIFIED, formatdate(stat.st_mtime, usegmt=True))
        method = str(scope.get('method', HTTPMethod.GET)).upper() if scope is not None else HTTPMethod.GET
        request_headers = Headers.from_raw(scope.get('headers', [])) if scope is not None else Headers()

        eligible = self.status_code == StatusCode.OK
        if eligible and _precondition_failed(request_headers, etag, stat.st_mtime):
            self.status_code = StatusCode.PRECONDITION_FAILED
            self.headers[HeaderName.CONTENT_LENGTH] = '0'
            await super().send(send, scope=scope, head=head)
            return
        if eligible and _not_modified(request_headers, etag, stat.st_mtime, method):
            self.status_code = StatusCode.NOT_MODIFIED
            self.headers.popall(HeaderName.CONTENT_LENGTH)
            await super().send(send, scope=scope, head=head)
            return

        byte_range = None
        if eligible and method == HTTPMethod.GET and _if_range_matches(
            request_headers.get(HeaderName.IF_RANGE), etag, stat.st_mtime,
        ):
            byte_range = _parse_range(request_headers.get(HeaderName.RANGE), stat.st_size)
        if byte_range is False:
            self.status_code = StatusCode.RANGE_NOT_SATISFIABLE
            self.headers[HeaderName.CONTENT_RANGE] = f'{RangeUnit.BYTES} */{stat.st_size}'
            self.headers[HeaderName.CONTENT_LENGTH] = '0'
            await super().send(send, scope=scope, head=head)
            return

        start, end = byte_range if isinstance(byte_range, tuple) else (0, stat.st_size - 1)
        length = max(0, end - start + 1)
        if isinstance(byte_range, tuple):
            self.status_code = StatusCode.PARTIAL_CONTENT
            self.headers[HeaderName.CONTENT_RANGE] = f'{RangeUnit.BYTES} {start}-{end}/{stat.st_size}'
        self.headers[HeaderName.CONTENT_LENGTH] = str(length)

        extensions = _extensions(scope)
        if self.early_hints:
            await self._send_early_hints(send, extensions)
        try:
            await send(
                {
                    'type': ASGIMessageType.HTTP_RESPONSE_START,
                    'status': self.status_code,
                    'headers': self.headers.raw(),
                }
            )
            if head:
                await send({'type': ASGIMessageType.HTTP_RESPONSE_BODY, 'body': b''})
            elif byte_range is None and ASGIExtension.HTTP_PATHSEND in extensions:
                await send({'type': ASGIMessageType.HTTP_RESPONSE_PATHSEND, 'path': str(self.path)})
            else:
                await self._send_file_chunks(send, start, length)
        except OSError:
            return
        if self._after_send:
            await self._run_after_send()

    async def _send_file_chunks(self, send: Send, start: int, length: int) -> None:
        file = await to_thread(self.path.open, 'rb')
        try:
            await to_thread(file.seek, start)
            remaining = length
            while remaining:
                chunk = await to_thread(file.read, min(self.chunk_size, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                await send(
                    {'type': ASGIMessageType.HTTP_RESPONSE_BODY, 'body': chunk, 'more_body': remaining > 0}
                )
            if length == 0:
                await send({'type': ASGIMessageType.HTTP_RESPONSE_BODY, 'body': b''})
        finally:
            await to_thread(file.close)


ResponseValue: TypeAlias = Response | JSONValue | bytes | str | tuple[Any, int] | tuple[Any, int, HeaderMapping]


class _StreamingEncoder:
    __slots__ = ('_encoder', 'active', 'encoding')

    def __init__(self, encoding: Optional[str]) -> None:
        self.encoding = encoding or ''
        self.active = encoding is not None
        self._encoder: Any = None
        if encoding == ContentEncoding.GZIP:
            from zlib import compressobj

            self._encoder = compressobj(wbits=31)
        elif encoding == ContentEncoding.ZSTANDARD:
            from compression.zstd import ZstdCompressor

            self._encoder = ZstdCompressor()
        elif encoding == ContentEncoding.BROTLI:
            try:
                from brotli import Compressor
            except ImportError as error:
                raise RuntimeError("Brotli compression requires the 'brotli' package") from error
            self._encoder = Compressor(quality=4)

    def compress(self, chunk: bytes) -> bytes:
        if not self.active:
            return chunk
        if self.encoding == ContentEncoding.BROTLI:
            return bytes(self._encoder.process(chunk))
        return bytes(self._encoder.compress(chunk))

    def finish(self) -> bytes:
        if not self.active:
            return b''
        if self.encoding == ContentEncoding.GZIP:
            from zlib import Z_FINISH

            return bytes(self._encoder.flush(Z_FINISH))
        if self.encoding == ContentEncoding.ZSTANDARD:
            return bytes(self._encoder.flush(self._encoder.FLUSH_FRAME))
        return bytes(self._encoder.finish())


async def _wait_for_disconnect(receive: Optional[Receive], request: Optional['Request[Any]']) -> None:
    if request is not None:
        await request._wait_for_response_disconnect()
        return
    if receive is None:
        return
    while True:
        message = await receive()
        if message.get('type') == ASGIMessageType.HTTP_DISCONNECT:
            return


async def _event_stream(
    events: AsyncIterable[ServerSentEvent | str],
    heartbeat: Optional[float],
) -> AsyncIterator[bytes]:
    queue: Queue[Optional[ServerSentEvent | str | Exception]] = Queue(maxsize=1)

    async def produce() -> None:
        iterator = events.__aiter__()
        try:
            try:
                async for event in iterator:
                    await queue.put(event)
            finally:
                close = getattr(iterator, 'aclose', None)
                if close is not None:
                    await close()
        except Exception as error:
            await queue.put(error)
        else:
            await queue.put(None)

    producer = ensure_future(produce())
    pending = ensure_future(queue.get())
    try:
        while True:
            done, _ = await wait({pending, producer}, timeout=heartbeat, return_when=FIRST_COMPLETED)
            if not done:
                yield b': heartbeat\n\n'
                continue
            if producer in done:
                producer.result()
            event = await pending
            if event is None:
                return
            if isinstance(event, Exception):
                raise event
            yield event.encode() if isinstance(event, ServerSentEvent) else ServerSentEvent(event).encode()
            pending = ensure_future(queue.get())
    finally:
        await cancel_and_join(producer, pending)


def _event_lines(value: str) -> list[str]:
    return value.replace('\r\n', '\n').replace('\r', '\n').split('\n')


def _encode_chunk(chunk: BodyChunk) -> bytes:
    if isinstance(chunk, str):
        return chunk.encode('utf-8')
    if isinstance(chunk, bytes):
        return chunk
    raise TypeError('Streaming response chunks must be bytes or str')


def _etag_matches(value: str, etag: str, *, weak: bool = True) -> bool:
    expected = etag[2:] if weak and etag.startswith('W/') else etag
    for candidate in value.split(','):
        candidate = candidate.strip()
        if candidate == '*':
            return True
        if not weak and (candidate.startswith('W/') or etag.startswith('W/')):
            continue
        if weak and candidate.startswith('W/'):
            candidate = candidate[2:]
        if candidate == expected:
            return True
    return False


def _date_matches(value: str, modified: float) -> bool:
    try:
        return modified <= parsedate_to_datetime(value).timestamp() + 1
    except (TypeError, ValueError, OverflowError):
        return False


def _precondition_failed(headers: Headers, etag: str, modified: float) -> bool:
    if_match = headers.get(HeaderName.IF_MATCH)
    if if_match is not None:
        return not _etag_matches(if_match, etag, weak=False)
    unmodified = headers.get(HeaderName.IF_UNMODIFIED_SINCE)
    if unmodified is None:
        return False
    try:
        return modified > parsedate_to_datetime(unmodified).timestamp() + 1
    except (TypeError, ValueError, OverflowError):
        return False


def _not_modified(headers: Headers, etag: str, modified: float, method: str) -> bool:
    if method not in {HTTPMethod.GET, HTTPMethod.HEAD}:
        return False
    none_match = headers.get(HeaderName.IF_NONE_MATCH)
    if none_match is not None:
        return _etag_matches(none_match, etag)
    modified_since = headers.get(HeaderName.IF_MODIFIED_SINCE)
    return modified_since is not None and _date_matches(modified_since, modified)


def _if_range_matches(value: Optional[str], etag: str, modified: float) -> bool:
    if value is None:
        return True
    if value.startswith('"') or value.startswith('W/'):
        return _etag_matches(value, etag, weak=False)
    return _date_matches(value, modified)


def _parse_range(value: Optional[str], size: int) -> Optional[tuple[int, int]] | bool:
    if value is None:
        return None
    unit, separator, specification = value.partition('=')
    if unit.strip().lower() != RangeUnit.BYTES:
        return None
    if size == 0 or not separator or ',' in specification:
        return False
    first, separator, last = specification.strip().partition('-')
    if not separator:
        return False
    try:
        if not first:
            suffix = int(last)
            if suffix <= 0:
                return False
            return max(0, size - suffix), size - 1
        start = int(first)
        end = int(last) if last else size - 1
    except ValueError:
        return False
    if start < 0 or start >= size or end < start:
        return False
    return start, min(end, size - 1)


def jsonify(
    value: Any,
    status_code: int = StatusCode.OK,
    headers: Optional[HeaderMapping] = None,
    *,
    options: int = 0,
) -> JSONResponse:
    return JSONResponse(value, status_code, headers, options=options)


def make_response(value: ResponseValue, *, pretty_json: bool = False, json_options: int = 0) -> Response:
    if isinstance(value, Response):
        return value

    status_code = StatusCode.OK
    headers: Optional[HeaderMapping] = None
    body = value
    if isinstance(value, tuple):
        if len(value) == 2:
            body, status_code = value
        elif len(value) == 3:
            body, status_code, headers = value
        else:
            raise TypeError('Response tuples must contain two or three items')

    if isinstance(body, Response):
        body.status_code = status_code
        if headers:
            body.headers.update(headers)
        return body
    if isinstance(body, (Mapping, list)) or body is None or isinstance(body, (bool, int, float)):
        return JSONResponse(body, status_code, headers, pretty=pretty_json, options=json_options)
    if isinstance(body, (str, bytes)):
        return Response(body, status_code, headers)
    raise TypeError(f'Unsupported response value: {type(body).__name__}')


def default_error_response(
    status_code: int,
    detail: Optional[str] = None,
    headers: Optional[HeaderMapping] = None,
    *,
    pretty_json: bool = False,
    json_options: int = 0,
    media_type: str = MediaType.JSON,
) -> Response:
    try:
        title = HTTPStatus(status_code).phrase
    except ValueError:
        title = 'HTTP error'
    if detail is None:
        detail = title
    if media_type == MediaType.PROBLEM_JSON:
        body = {'type': 'about:blank', 'title': title, 'status': status_code, 'detail': detail}
    else:
        body = {'error': {'code': status_code, 'message': detail}}
    return JSONResponse(
        body,
        status_code,
        headers,
        pretty=pretty_json,
        options=json_options,
        media_type=media_type,
    )


json_dumps = dumps
json_loads = loads
