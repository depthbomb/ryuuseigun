from functools import lru_cache
from email.message import Message
from types import SimpleNamespace
from urllib.parse import parse_qsl
from email.parser import BytesParser
from http.cookies import SimpleCookie
from ryuuseigun._tasks import run_sync
from ryuuseigun.headers import Headers
from re import sub, compile, IGNORECASE
from contextvars import Token, ContextVar
from orjson import loads, JSONDecodeError
from tempfile import SpooledTemporaryFile
from asyncio import Lock, sleep, to_thread
from ryuuseigun.config import MultipartLimits
from ryuuseigun._body import RequestBodyBuffer
from email.policy import default as email_policy
from typing import IO, Any, Optional, TYPE_CHECKING
from email.headerregistry import BaseHeader, HeaderRegistry
from ryuuseigun.exceptions import HTTPException, ClientDisconnect
from ryuuseigun.types import Send, Receive, ASGIScope, JSONValue, ASGIMessage
from collections.abc import Mapping, Callable, Sequence, Awaitable, AsyncIterator
from ryuuseigun.constants import MediaType, HeaderName, StatusCode, ASGIMessageType

if TYPE_CHECKING:
    from ryuuseigun.response import Response, ResponseValue

class _MultipartHeaderRegistry(HeaderRegistry):
    def __init__(self) -> None:
        super().__init__()
        # The registry otherwise builds a new class on every header lookup.
        # Cache classes only; parsed header values stay local to each message.
        self._cached_getitem = lru_cache(maxsize=128)(super().__getitem__)

    def __getitem__(self, name: str) -> type[BaseHeader]:
        return self._cached_getitem(name)

_multipart_email_policy = email_policy.clone(header_factory=_MultipartHeaderRegistry())

# Handle only the common ASCII layout. Escapes, encodings, extra parameters,
# folding and additional headers still go through the full MIME parser.
_SIMPLE_PART_HEADERS = compile(
    rb'(?P<disposition_name>Content-Disposition):[ \t]*'
    rb'(?P<disposition>form-data;[ \t]*name="(?P<name>[^"\\\x00-\x1f\x7f-\xff]+)"'
    rb'(?:;[ \t]*filename="(?P<filename>[^"\\\x00-\x1f\x7f-\xff]*)")?[ \t]*)'
    rb'(?:\r\n(?P<content_name>Content-Type):[ \t]*'
    rb'(?P<content_type>[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+[ \t]*))?',
    IGNORECASE,
)

def _simple_part_headers(raw: bytes) -> Optional[tuple[str, Optional[str], Headers, Optional[str]]]:
    # MIME encoded words can appear inside otherwise plain quoted values.
    if b'=?' in raw:
        return None

    match = _SIMPLE_PART_HEADERS.fullmatch(raw)
    if match is None:
        return None

    content_type = match['content_type']
    media_type = content_type.decode('ascii').strip().lower() if content_type is not None else None
    if media_type is not None and media_type.startswith('multipart/'):
        return None

    headers = Headers([(match['disposition_name'].decode('ascii'), match['disposition'].decode('ascii'))])
    if content_type is not None:
        headers.add(match['content_name'].decode('ascii'), content_type.decode('ascii'))

    filename = match['filename']
    return match['name'].decode('ascii'), filename.decode('ascii') if filename is not None else None, headers, media_type

def validate_query_string(
    value: bytes,
    max_size: Optional[int],
    max_parameters: Optional[int],
) -> None:
    if max_size is not None and len(value) > max_size:
        raise HTTPException(StatusCode.URI_TOO_LONG, 'Query string is too large')
    parameter_count = value.count(b'&') + 1 if value else 0
    if max_parameters is not None and parameter_count > max_parameters:
        raise HTTPException(StatusCode.URI_TOO_LONG, 'Too many query parameters')


class QueryParams:
    __slots__ = ('_items', '_value')

    def __init__(self, value: bytes) -> None:
        self._value = value
        self._items: Optional[list[tuple[str, str]]] = None

    def _parse(self) -> list[tuple[str, str]]:
        items = self._items
        if items is None:
            items = parse_qsl(self._value.decode('latin-1'), keep_blank_values=True)
            self._items = items
        return items

    def __contains__(self, name: object) -> bool:
        return any(item_name == name for item_name, _ in self._parse())

    def __getitem__(self, name: str) -> str:
        value = self.get(name)
        if value is None:
            raise KeyError(name)
        return value

    def get(self, name: str, default: Optional[str] = None) -> Optional[str]:
        for item_name, value in self._parse():
            if item_name == name:
                return value
        return default

    def getlist(self, name: str) -> list[str]:
        return [value for item_name, value in self._parse() if item_name == name]

    def items(self) -> list[tuple[str, str]]:
        return list(self._parse())


class UploadFile:
    __slots__ = ('_closed', '_size', 'content_type', 'file', 'filename', 'headers')

    def __init__(
        self,
        filename: str,
        data: bytes | IO[bytes],
        headers: Headers,
        content_type: Optional[str] = None,
        *,
        size: Optional[int] = None,
    ) -> None:
        self.filename = filename
        self.content_type = content_type
        self.headers = headers
        self._closed = False
        if isinstance(data, bytes):
            file = SpooledTemporaryFile(max_size=max(len(data), 1), mode='w+b')
            file.write(data)
            file.seek(0)
            self.file: IO[bytes] = file
            self._size = len(data)
        else:
            self.file = data
            self._size = size if size is not None else self._measure_size()

    @property
    def size(self) -> int:
        return self._size

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def in_memory(self) -> bool:
        return not bool(getattr(self.file, '_rolled', True))

    async def read(self, size: int = -1) -> bytes:
        self._ensure_open()
        await sleep(0)
        if self.in_memory and (0 <= size <= 64 * 1024 or self._size <= 64 * 1024):
            return self.file.read(size)

        return await to_thread(self.file.read, size)

    async def seek(self, offset: int, whence: int = 0) -> int:
        self._ensure_open()
        await sleep(0)
        if self.in_memory:
            return self.file.seek(offset, whence)

        return await to_thread(self.file.seek, offset, whence)

    async def tell(self) -> int:
        self._ensure_open()
        await sleep(0)
        if self.in_memory:
            return self.file.tell()

        return await to_thread(self.file.tell)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self.in_memory:
            self.file.close()
        else:
            await to_thread(self.file.close)

    def _measure_size(self) -> int:
        position = self.file.tell()
        self.file.seek(0, 2)
        size = self.file.tell()
        self.file.seek(position)
        return size

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError('Upload file is closed')


type FormValue = str | UploadFile


class FormData:
    __slots__ = ('_items',)

    def __init__(self, items: Sequence[tuple[str, FormValue]]) -> None:
        self._items = list(items)

    def __contains__(self, name: object) -> bool:
        return any(item_name == name for item_name, _ in self._items)

    def __getitem__(self, name: str) -> FormValue:
        value = self.get(name)
        if value is None:
            raise KeyError(name)
        return value

    def get(self, name: str, default: Optional[FormValue] = None) -> Optional[FormValue]:
        for item_name, value in self._items:
            if item_name == name:
                return value
        return default

    def getlist(self, name: str) -> list[FormValue]:
        return [value for item_name, value in self._items if item_name == name]

    def items(self) -> list[tuple[str, FormValue]]:
        return list(self._items)

    async def close(self) -> None:
        closed: set[int] = set()
        for _, value in self._items:
            if isinstance(value, UploadFile) and id(value) not in closed:
                closed.add(id(value))
                await value.close()


def _content_type_header(value: str) -> Message:
    message = Message()
    message[HeaderName.CONTENT_TYPE] = value
    return message


def _parse_urlencoded_form(body: bytes, content_type: Message) -> FormData:
    charset = content_type.get_content_charset() or 'utf-8'
    try:
        text = body.decode(charset)
        items = parse_qsl(text, keep_blank_values=True, encoding=charset, errors='strict')
    except (LookupError, UnicodeDecodeError, ValueError) as error:
        raise HTTPException(StatusCode.BAD_REQUEST, 'Malformed URL-encoded form body') from error
    return FormData(items)


async def _parse_large_urlencoded_form(body: bytes, content_type: Message) -> FormData:
    charset = content_type.get_content_charset() or 'utf-8'
    items: list[tuple[str, FormValue]] = []
    try:
        text = await to_thread(body.decode, charset)
        start = 0
        while start < len(text):
            end = text.find('&', start + 8192)
            if end < 0:
                end = len(text)
            chunk = text[start:end]
            if len(chunk) > 64 * 1024:
                parsed = await to_thread(parse_qsl, chunk, keep_blank_values=True, encoding=charset, errors='strict')
            else:
                parsed = parse_qsl(chunk, keep_blank_values=True, encoding=charset, errors='strict')
            items.extend(parsed)
            start = end + 1
            await sleep(0)
    except (LookupError, UnicodeDecodeError, ValueError) as error:
        raise HTTPException(StatusCode.BAD_REQUEST, 'Malformed URL-encoded form body') from error
    return FormData(items)


class MultipartPart:
    __slots__ = (
        '_charset',
        '_complete',
        '_owner',
        '_size',
        '_stream_active',
        '_stream_started',
        'content_type',
        'filename',
        'headers',
        'name',
    )

    def __init__(
        self,
        owner: 'MultipartStream',
        name: str,
        filename: Optional[str],
        headers: Headers,
        content_type: Optional[str],
        charset: str,
    ) -> None:
        self.name = name
        self.filename = filename
        self.headers = headers
        self.content_type = content_type
        self._charset = charset
        self._owner = owner
        self._complete = False
        self._size = 0
        self._stream_active = False
        self._stream_started = False

    @property
    def complete(self) -> bool:
        return self._complete

    @property
    def size(self) -> int:
        return self._size

    async def stream(self) -> AsyncIterator[bytes]:
        if self._stream_started:
            raise RuntimeError('The multipart part stream has already been consumed')
        self._stream_started = True
        self._stream_active = True
        try:
            async for chunk in self._owner._read_part(self):
                yield chunk
        finally:
            self._stream_active = False

    async def read(self) -> bytes:
        data = bytearray()
        async for chunk in self.stream():
            data.extend(chunk)
        return bytes(data)

    async def text(self, encoding: Optional[str] = None) -> str:
        try:
            return (await self.read()).decode(encoding or self._charset)
        except (LookupError, UnicodeDecodeError) as error:
            raise HTTPException(StatusCode.BAD_REQUEST, f'Could not decode multipart field {self.name!r}') from error

    async def discard(self) -> None:
        if self._stream_active:
            raise RuntimeError('Cannot discard a multipart part while its stream is active')
        if self._complete:
            return
        self._stream_started = True
        async for _ in self._owner._read_part(self):
            pass


class MultipartStream(AsyncIterator[MultipartPart]):
    _MAX_HEADER_SIZE = 64 * 1024
    _MAX_CHUNK_SIZE = 64 * 1024

    def __init__(
        self,
        source: AsyncIterator[bytes],
        content_type: Message,
        limits: Optional[MultipartLimits] = None,
    ) -> None:
        boundary_value = content_type.get_param('boundary')
        if not isinstance(boundary_value, str):
            raise HTTPException(StatusCode.BAD_REQUEST, 'Multipart form body is missing a boundary')
        try:
            boundary = boundary_value.encode('ascii')
        except UnicodeEncodeError as error:
            raise HTTPException(StatusCode.BAD_REQUEST, 'Invalid multipart boundary') from error
        if not boundary or len(boundary) > 70 or b'\r' in boundary or b'\n' in boundary:
            raise HTTPException(StatusCode.BAD_REQUEST, 'Invalid multipart boundary')

        self._source = source
        self._boundary = b'--' + boundary
        self._separator = b'\r\n' + self._boundary
        self._buffer = bytearray()
        self._buffer_start = 0
        self._active: Optional[MultipartPart] = None
        self._closed = False
        self._eof = False
        self._finished = False
        self._limits = limits or MultipartLimits()
        self._part_count = 0
        self._started = False

    def __aiter__(self) -> 'MultipartStream':
        return self

    async def __anext__(self) -> MultipartPart:
        if self._closed:
            raise StopAsyncIteration
        if self._active is not None and not self._active.complete:
            if self._active._stream_active:
                raise RuntimeError('Consume the active multipart part before requesting the next part')
            await self._active.discard()
        if not self._started:
            self._started = True
            await self._consume_initial_boundary()
        if self._finished:
            raise StopAsyncIteration

        self._part_count += 1
        if self._limits.max_parts is not None and self._part_count > self._limits.max_parts:
            raise HTTPException(StatusCode.PAYLOAD_TOO_LARGE, 'Multipart form contains too many parts')

        raw_headers = await self._read_headers()
        simple = _simple_part_headers(raw_headers)
        if simple is not None:
            simple_name, filename, headers, media_type = simple
            part = MultipartPart(self, simple_name, filename, headers, media_type, 'utf-8')
            self._active = part
            return part

        message = BytesParser(policy=_multipart_email_policy).parsebytes(raw_headers + b'\r\n\r\n', headersonly=True)
        raw_items = tuple(message.raw_items())
        # EmailPolicy reparses strings on each lookup. Retain parsed headers for
        # parameter access while keeping the original octets for part.headers.
        for header_name in (HeaderName.CONTENT_DISPOSITION, HeaderName.CONTENT_TYPE):
            parsed_header = message.get(header_name)
            if parsed_header is not None:
                message.replace_header(header_name, parsed_header)
        if (
            message.defects
            or message.get_content_disposition() != 'form-data'
            or message.get_content_maintype() == 'multipart'
        ):
            raise HTTPException(StatusCode.BAD_REQUEST, 'Malformed multipart form body')
        name = message.get_param('name', header='content-disposition')
        if not isinstance(name, str) or not name:
            raise HTTPException(StatusCode.BAD_REQUEST, 'Multipart form field is missing a name')
        transfer_encoding = message.get('Content-Transfer-Encoding', 'binary').lower()
        if transfer_encoding not in {'binary', '8bit', '7bit'}:
            raise HTTPException(StatusCode.BAD_REQUEST, 'Unsupported multipart transfer encoding')

        filename_value = message.get_param('filename', header='content-disposition')
        try:
            # Keep header octets separate from the Unicode filename parsed by email.
            headers = Headers([
                (header, sub(r'\r?\n[ \t]+', ' ', value).encode('ascii', 'surrogateescape').decode('latin-1'))
                for header, value in raw_items
            ])
        except ValueError as error:
            raise HTTPException(StatusCode.BAD_REQUEST, 'Malformed multipart part headers') from error

        part_content_type = message.get(HeaderName.CONTENT_TYPE)
        part = MultipartPart(
            self,
            name,
            filename_value if isinstance(filename_value, str) else None,
            headers,
            message.get_content_type().lower() if part_content_type is not None else None,
            message.get_content_charset() or 'utf-8',
        )
        self._active = part
        return part

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._active is not None:
            self._active._complete = True
            self._active._stream_active = False
        close = getattr(self._source, 'aclose', None)
        try:
            if close is not None:
                await close()
        finally:
            self._buffer.clear()
            self._buffer_start = 0

    async def _consume_initial_boundary(self) -> None:
        search_from = self._buffer_start
        while True:
            index = self._buffer.find(self._boundary, search_from)
            if index >= 0:
                prefix_valid = (
                    index == self._buffer_start
                    or index >= self._buffer_start + 2 and self._buffer[index - 2:index] == b'\r\n'
                )
                suffix_start = index + len(self._boundary)
                if len(self._buffer) < suffix_start + 2:
                    if self._eof:
                        self._malformed()
                    if index > self._buffer_start:
                        self._consume_buffer(index - self._buffer_start)
                    await self._read_more()
                    search_from = self._buffer_start
                    continue
                suffix = bytes(self._buffer[suffix_start:suffix_start + 2])
                if prefix_valid and suffix in {b'\r\n', b'--'}:
                    self._consume_buffer(suffix_start + 2 - self._buffer_start)
                    self._finished = suffix == b'--'
                    return
                search_from = index + 1
                continue

            if self._eof:
                self._malformed()
            retained = len(self._boundary) + 4
            available = len(self._buffer) - self._buffer_start
            if available > retained:
                self._consume_buffer(available - retained)
            await self._read_more()
            search_from = self._buffer_start

    async def _read_headers(self) -> bytes:
        while True:
            end = self._buffer.find(b'\r\n\r\n', self._buffer_start)
            if end >= 0:
                header_size = end - self._buffer_start
                if header_size > self._MAX_HEADER_SIZE:
                    raise HTTPException(StatusCode.BAD_REQUEST, 'Multipart part headers are too large')
                raw_headers = bytes(self._buffer[self._buffer_start:end])
                self._consume_buffer(header_size + 4)
                return raw_headers
            if len(self._buffer) - self._buffer_start > self._MAX_HEADER_SIZE:
                raise HTTPException(StatusCode.BAD_REQUEST, 'Multipart part headers are too large')
            if self._eof:
                self._malformed()
            await self._read_more()

    async def _read_part(self, part: MultipartPart) -> AsyncIterator[bytes]:
        if self._active is not part or part.complete:
            raise RuntimeError('This multipart part is no longer active')
        search_from = self._buffer_start
        while True:
            index = self._buffer.find(self._separator, search_from)
            if index >= 0:
                suffix_start = index + len(self._separator)
                if len(self._buffer) < suffix_start + 2:
                    available = index - self._buffer_start
                    while available:
                        chunk_size = min(available, self._MAX_CHUNK_SIZE)
                        chunk = bytes(self._buffer[self._buffer_start:self._buffer_start + chunk_size])
                        self._consume_buffer(chunk_size)
                        available -= chunk_size
                        yield self._account_part_chunk(part, chunk)
                    if self._eof:
                        self._malformed()
                    await self._read_more()
                    search_from = self._buffer_start
                    continue

                suffix = bytes(self._buffer[suffix_start:suffix_start + 2])
                if suffix not in {b'\r\n', b'--'}:
                    search_from = index + 1
                    continue
                available = index - self._buffer_start
                while available:
                    chunk_size = min(available, self._MAX_CHUNK_SIZE)
                    chunk = bytes(self._buffer[self._buffer_start:self._buffer_start + chunk_size])
                    self._consume_buffer(chunk_size)
                    available -= chunk_size
                    yield self._account_part_chunk(part, chunk)

                self._consume_buffer(len(self._separator) + 2)
                part._complete = True
                self._active = None
                self._finished = suffix == b'--'
                return

            retained = len(self._separator) + 2
            writable = len(self._buffer) - self._buffer_start - retained
            if writable > 0:
                chunk_size = min(writable, self._MAX_CHUNK_SIZE)
                chunk = bytes(self._buffer[self._buffer_start:self._buffer_start + chunk_size])
                self._consume_buffer(chunk_size)
                search_from = self._buffer_start
                yield self._account_part_chunk(part, chunk)
                continue
            if self._eof:
                self._malformed()
            await self._read_more()
            search_from = self._buffer_start

    def _account_part_chunk(self, part: MultipartPart, chunk: bytes) -> bytes:
        size = part._size + len(chunk)
        limit = self._limits.max_file_size if part.filename is not None else self._limits.max_field_size
        if limit is not None and size > limit:
            kind = 'file' if part.filename is not None else 'field'
            raise HTTPException(StatusCode.PAYLOAD_TOO_LARGE, f'Multipart {kind} is too large')
        part._size = size
        return chunk

    def _consume_buffer(self, size: int) -> None:
        self._buffer_start += size
        if self._buffer_start == len(self._buffer):
            self._buffer.clear()
            self._buffer_start = 0
        elif self._buffer_start >= self._MAX_CHUNK_SIZE and self._buffer_start * 2 >= len(self._buffer):
            del self._buffer[:self._buffer_start]
            self._buffer_start = 0

    async def _read_more(self) -> None:
        try:
            chunk = await anext(self._source)
        except StopAsyncIteration:
            self._eof = True
            return
        if self._buffer_start:
            del self._buffer[:self._buffer_start]
            self._buffer_start = 0
        self._buffer.extend(chunk)

    @staticmethod
    def _malformed() -> None:
        raise HTTPException(StatusCode.BAD_REQUEST, 'Malformed multipart form body')

def _write_upload(file: IO[bytes], chunks: list[bytes], *, rewind: bool = False) -> None:
    for chunk in chunks:
        file.write(chunk)

    if rewind:
        file.seek(0)

async def _collect_multipart_form(parts: MultipartStream, spool_threshold: int) -> FormData:
    items: list[tuple[str, FormValue]] = []
    active_file: Optional[IO[bytes]] = None
    try:
        async for part in parts:
            if part.filename is None:
                items.append((part.name, await part.text()))
                continue

            upload_file = SpooledTemporaryFile(max_size=max(spool_threshold, 1), mode='w+b')
            active_file = upload_file
            if spool_threshold == 0:
                await run_sync(upload_file.rollover)

            pending: list[bytes] = []
            pending_size = 0
            async for chunk in part.stream():
                # Crossing the threshold can create and write a disk file.
                if part.size <= spool_threshold:
                    upload_file.write(chunk)
                    await sleep(0)
                else:
                    pending.append(chunk)
                    pending_size += len(chunk)
                    # Bound buffering while amortizing thread-pool round trips.
                    if pending_size >= 256 * 1024:
                        await run_sync(_write_upload, upload_file, pending)
                        pending.clear()
                        pending_size = 0

            if pending:
                await run_sync(_write_upload, upload_file, pending, rewind=True)
            elif spool_threshold and part.size <= spool_threshold:
                upload_file.seek(0)
            else:
                await run_sync(upload_file.seek, 0)
            items.append(
                (
                    part.name,
                    UploadFile(
                        part.filename,
                        upload_file,
                        part.headers,
                        part.content_type,
                        size=part.size,
                    ),
                )
            )
            active_file = None
        return FormData(items)
    except BaseException:
        if active_file is not None:
            await to_thread(active_file.close)
        await FormData(items).close()
        raise


class Request[StateT = SimpleNamespace]:
    __slots__ = (
        '_body',
        '_closed',
        '_response_handler',
        '_send',
        '_response_started',
        '_response_attempted',
        '_after_handlers',
        '_cookies',
        '_disconnected',
        '_form',
        '_max_body_size',
        '_multipart',
        '_multipart_limits',
        '_pending_body',
        '_handled_errors',
        '_receive',
        '_receive_lock',
        '_received_size',
        '_receive_complete',
        '_route_context',
        '_stream_complete',
        '_stream_started',
        '_upload_spool_threshold',
        '_url_builder',
        'headers',
        'method',
        'path',
        'path_params',
        'query',
        'scope',
        'state',
    )

    def __init__(
        self,
        scope: ASGIScope,
        receive: Receive,
        max_body_size: Optional[int] = None,
        *,
        state: StateT,
        upload_spool_threshold: int = 1024 * 1024,
        multipart_limits: Optional[MultipartLimits] = None,
        url_builder: Optional[Callable[..., str]] = None,
    ) -> None:
        method = scope.get('method')
        path = scope.get('path', '/')
        if not isinstance(method, str) or not method:
            raise RuntimeError('HTTP ASGI scopes must contain a non-empty method')
        if not isinstance(path, str) or not path.startswith('/'):
            raise RuntimeError("HTTP ASGI scope paths must start with '/'")

        self.scope = scope
        self._receive = receive
        self._body: Optional[bytes] = None
        self._closed = False
        self._response_handler: Optional[Callable[[Request[StateT], ResponseValue], Awaitable[None]]] = None
        self._send: Optional[Send] = None
        self._response_started = False
        self._response_attempted = False
        self._after_handlers: tuple[Callable[[Request[Any], Response], Awaitable[ResponseValue]], ...] = ()
        self._cookies: Optional[dict[str, str]] = None
        self._disconnected = False
        self._form: Optional[FormData] = None
        self._max_body_size = max_body_size
        self._multipart: Optional[MultipartStream] = None
        self._multipart_limits = multipart_limits or MultipartLimits()
        self._pending_body: Optional[RequestBodyBuffer] = None
        self._handled_errors: list[Exception] = []
        self._receive_lock: Optional[Lock] = None
        self._received_size = 0
        self._receive_complete = False
        self._route_context: Any = None
        self._stream_complete = False
        self._stream_started = False
        self._upload_spool_threshold = upload_spool_threshold
        self._url_builder = url_builder
        self.method = method.upper()
        self.path = path
        self.headers = Headers.from_raw(scope.get('headers', []))
        self.query = QueryParams(scope.get('query_string', b''))
        self.path_params: dict[str, Any] = {}
        self.state = state

    @property
    def content_type(self) -> Optional[str]:
        value = self.headers.get(HeaderName.CONTENT_TYPE)
        return value.partition(';')[0].strip().lower() if value else None

    @property
    def args(self) -> QueryParams:
        return self.query

    @property
    def cookies(self) -> dict[str, str]:
        if self._cookies is None:
            parsed = SimpleCookie()
            parsed.load('; '.join(self.headers.getlist(HeaderName.COOKIE)))
            self._cookies = {name: morsel.value for name, morsel in parsed.items()}
        return self._cookies

    @property
    def client(self) -> Optional[tuple[str, int]]:
        value = self.scope.get('client')
        return (str(value[0]), int(value[1])) if value else None

    @property
    def http_version(self) -> str:
        return str(self.scope.get('http_version', '1.1'))

    @property
    def disconnected(self) -> bool:
        return self._disconnected

    def supports(self, extension: str) -> bool:
        extensions = self.scope.get('extensions', {})
        return isinstance(extensions, Mapping) and extension in extensions

    def url_for(self, endpoint: str, **values: Any) -> str:
        if self._url_builder is None:
            raise RuntimeError('URL building is unavailable for this request')
        return self._url_builder(endpoint, **values)

    async def stream(self) -> AsyncIterator[bytes]:
        if self._body is not None:
            if self._body:
                yield self._body
            return
        if self._stream_started:
            raise RuntimeError('The request body stream has already been consumed')

        self._validate_content_length()
        self._stream_started = True
        while not self._stream_complete:
            message = await self._next_message()
            message_type = message['type']
            if message_type == ASGIMessageType.HTTP_DISCONNECT:
                self._disconnected = True
                raise ClientDisconnect
            if message_type != ASGIMessageType.HTTP_REQUEST:
                raise HTTPException(StatusCode.BAD_REQUEST, f'Unexpected ASGI message: {message_type}')

            chunk = message.get('body', b'')
            if not message.get('more_body', False):
                self._stream_complete = True
            if chunk:
                yield chunk

    async def wait_for_disconnect(self) -> None:
        if self._pending_body is None:
            self._pending_body = RequestBodyBuffer(self._upload_spool_threshold)

        while not self._disconnected:
            async with self._get_receive_lock():
                message = await self._read_message()
                message_type = message.get('type')
                if message_type == ASGIMessageType.HTTP_DISCONNECT:
                    return
                if message_type != ASGIMessageType.HTTP_REQUEST:
                    raise HTTPException(StatusCode.BAD_REQUEST, f'Unexpected ASGI message: {message_type}')

                # Receive must keep advancing to observe disconnects. Spool unread
                # data so a slow or delayed body consumer does not retain it in RAM.
                await self._pending_body.append(message.get('body', b''), more_body=message.get('more_body', False))
            await sleep(0)

    async def _wait_for_response_disconnect(self) -> None:
        await sleep(0)
        await self.wait_for_disconnect()

    async def body(self) -> bytes:
        if self._body is not None:
            return self._body
        if self._stream_started:
            raise RuntimeError('The request body stream has already been consumed')
        chunks = bytearray()
        async for chunk in self.stream():
            chunks.extend(chunk)

        self._body = bytes(chunks)
        return self._body

    def _validate_content_length(self) -> None:
        content_lengths = self.headers.getlist(HeaderName.CONTENT_LENGTH)
        if len(set(content_lengths)) > 1:
            raise HTTPException(StatusCode.BAD_REQUEST, 'Conflicting Content-Length headers')
        if not content_lengths:
            return
        try:
            declared_size = int(content_lengths[0])
        except ValueError as error:
            raise HTTPException(StatusCode.BAD_REQUEST, 'Invalid Content-Length header') from error
        if declared_size < 0:
            raise HTTPException(StatusCode.BAD_REQUEST, 'Invalid Content-Length header')
        if self._max_body_size is not None and declared_size > self._max_body_size:
            raise HTTPException(StatusCode.PAYLOAD_TOO_LARGE, 'Request body is too large')

    async def _next_message(self) -> ASGIMessage:
        if self._pending_body is not None:
            message = await self._pending_body.read()
            if message is not None:
                return message

        async with self._get_receive_lock():
            if self._pending_body is not None:
                message = await self._pending_body.read()
                if message is not None:
                    return message
            return await self._read_message()

    def _get_receive_lock(self) -> Lock:
        lock = self._receive_lock
        if lock is None:
            lock = Lock()
            self._receive_lock = lock

        return lock

    async def _read_message(self) -> ASGIMessage:
        if self._disconnected:
            return {'type': ASGIMessageType.HTTP_DISCONNECT}

        self._validate_content_length()
        message = await self._receive()
        if message.get('type') == ASGIMessageType.HTTP_DISCONNECT:
            self._disconnected = True

        if message.get('type') == ASGIMessageType.HTTP_REQUEST:
            if self._receive_complete:
                raise HTTPException(StatusCode.BAD_REQUEST, 'Request body is already complete')
            chunk = message.get('body', b'')
            if not isinstance(chunk, bytes):
                raise HTTPException(StatusCode.BAD_REQUEST, 'ASGI request bodies must be bytes')
            self._received_size += len(chunk)
            if self._max_body_size is not None and self._received_size > self._max_body_size:
                raise HTTPException(StatusCode.PAYLOAD_TOO_LARGE, 'Request body is too large')
            self._receive_complete = not message.get('more_body', False)
        return message

    async def json(self) -> JSONValue:
        if self.content_type != MediaType.JSON and not (self.content_type or '').endswith('+json'):
            raise HTTPException(StatusCode.UNSUPPORTED_MEDIA_TYPE, f'Expected an {MediaType.JSON} request')

        try:
            value: JSONValue = loads(await self.body())
        except JSONDecodeError as error:
            raise HTTPException(StatusCode.BAD_REQUEST, 'Malformed JSON') from error
        return value

    def multipart(self) -> MultipartStream:
        if self._form is not None:
            raise RuntimeError('The multipart body has already been collected as form data')
        if self._multipart is not None:
            return self._multipart
        raw_content_type = self.headers.get(HeaderName.CONTENT_TYPE)
        if raw_content_type is None:
            raise HTTPException(StatusCode.UNSUPPORTED_MEDIA_TYPE, 'Expected a multipart request')
        content_type = _content_type_header(raw_content_type)
        if content_type.get_content_type().lower() != MediaType.MULTIPART_FORM_DATA:
            raise HTTPException(StatusCode.UNSUPPORTED_MEDIA_TYPE, 'Expected a multipart request')

        if self._body is None:
            source = self.stream()
        else:
            body = self._body

            async def cached_body() -> AsyncIterator[bytes]:
                if body:
                    yield body

            source = cached_body()
        self._multipart = MultipartStream(source, content_type, self._multipart_limits)
        return self._multipart

    async def form(self) -> FormData:
        if self._form is not None:
            return self._form
        raw_content_type = self.headers.get(HeaderName.CONTENT_TYPE)
        if raw_content_type is None:
            raise HTTPException(StatusCode.UNSUPPORTED_MEDIA_TYPE, 'Expected a form request')
        content_type = _content_type_header(raw_content_type)
        media_type = content_type.get_content_type().lower()
        if media_type not in {MediaType.FORM_URLENCODED, MediaType.MULTIPART_FORM_DATA}:
            raise HTTPException(StatusCode.UNSUPPORTED_MEDIA_TYPE, 'Expected a form request')
        if media_type == MediaType.FORM_URLENCODED:
            body = await self.body()
            if len(body) >= 64 * 1024:
                form = await _parse_large_urlencoded_form(body, content_type)
            else:
                form = _parse_urlencoded_form(body, content_type)
        else:
            form = await _collect_multipart_form(self.multipart(), self._upload_spool_threshold)
        self._form = form
        return form

    async def respond(self, value: ResponseValue) -> None:
        """Send an early middleware response, including request cleanup."""
        if self._response_handler is None:
            raise RuntimeError('This request is not attached to an application')
        await self._response_handler(self, value)

    def _send_message(self, message: ASGIMessage) -> Awaitable[None]:
        if self._send is None:
            raise RuntimeError('This request is not attached to an application')
        if message['type'] == ASGIMessageType.HTTP_RESPONSE_START:
            self._response_started = True
        return self._send(message)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self._multipart is not None:
                await self._multipart.close()
        finally:
            try:
                if self._form is not None:
                    await self._form.close()
            finally:
                if self._pending_body is not None:
                    await self._pending_body.close()

    async def text(self, encoding: str = 'utf-8') -> str:
        try:
            return (await self.body()).decode(encoding)
        except (LookupError, UnicodeDecodeError) as error:
            raise HTTPException(StatusCode.BAD_REQUEST, 'Could not decode request body') from error


_request_context: ContextVar[Request[Any]] = ContextVar('ryuuseigun_request')


def set_request_context(value: Request[Any]) -> Token[Request[Any]]:
    return _request_context.set(value)


def reset_request_context(token: Token[Request[Any]]) -> None:
    _request_context.reset(token)


def current_request() -> Request[Any]:
    try:
        return _request_context.get()
    except LookupError as error:
        raise RuntimeError('No active request context') from error


def url_for(endpoint: str, **values: Any) -> str:
    return current_request().url_for(endpoint, **values)


class RequestProxy:
    @property
    def method(self) -> str:
        return current_request().method

    @property
    def path(self) -> str:
        return current_request().path

    @property
    def args(self) -> QueryParams:
        return current_request().args

    @property
    def headers(self) -> Headers:
        return current_request().headers

    @property
    def query(self) -> QueryParams:
        return current_request().query

    @property
    def cookies(self) -> dict[str, str]:
        return current_request().cookies

    @property
    def client(self) -> Optional[tuple[str, int]]:
        return current_request().client

    @property
    def http_version(self) -> str:
        return current_request().http_version

    @property
    def disconnected(self) -> bool:
        return current_request().disconnected

    @property
    def content_type(self) -> Optional[str]:
        return current_request().content_type

    @property
    def path_params(self) -> dict[str, Any]:
        return current_request().path_params

    @property
    def state(self) -> Any:
        return current_request().state

    async def body(self) -> bytes:
        return await current_request().body()

    def supports(self, extension: str) -> bool:
        return current_request().supports(extension)

    def url_for(self, endpoint: str, **values: Any) -> str:
        return current_request().url_for(endpoint, **values)

    def stream(self) -> AsyncIterator[bytes]:
        return current_request().stream()

    def multipart(self) -> MultipartStream:
        return current_request().multipart()

    async def wait_for_disconnect(self) -> None:
        await current_request().wait_for_disconnect()

    async def json(self) -> JSONValue:
        return await current_request().json()

    async def form(self) -> FormData:
        return await current_request().form()

    async def text(self, encoding: str = 'utf-8') -> str:
        return await current_request().text(encoding)


request = RequestProxy()
