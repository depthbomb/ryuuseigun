from orjson import loads
from math import isfinite
from types import TracebackType
from collections.abc import Mapping
from ryuuseigun.app import Ryuuseigun
from ryuuseigun.headers import Headers
from dataclasses import field, dataclass
from ryuuseigun.types import ASGIMessage
from ryuuseigun._tasks import cancel_and_join
from urllib.request import Request as CookieRequest
from ryuuseigun.websocket import WebSocketDisconnect
from ryuuseigun._testing_lifespan import LifespanSession
from http.cookiejar import CookieJar, DefaultCookiePolicy
from typing import Any, Self, Unpack, Optional, TypedDict
from asyncio import Task, Event, Queue, timeout, create_task
from urllib.parse import quote, unquote, urljoin, urlsplit, urlencode, urlunsplit
from ryuuseigun._testing_http import origin, FileFields, FormFields, encode_form, CookieResponse
from ryuuseigun.constants import (
    MediaType,
    URLScheme,
    HTTPMethod,
    HeaderName,
    StatusCode,
    ASGIExtension,
    ASGIScopeType,
    ASGIMessageType,
    WebSocketCloseCode,
)

_MISSING = object()

class RequestOptions(TypedDict, total=False):
    headers: Mapping[str, str] | None
    body: bytes | str
    json: Any
    query: Mapping[str, str] | None
    form: FormFields | None
    files: FileFields | None
    follow_redirects: bool | None
    http_version: str
    extensions: Mapping[str, Any] | None


def _url_scope(path: str, query: Optional[Mapping[str, str]] = None) -> dict[str, Any]:
    split = urlsplit(path)
    query_string = split.query if query is None else urlencode(query)
    return {
        'path': unquote(split.path or '/', encoding='utf-8', errors='replace'),
        'raw_path': quote(split.path or '/', safe="/%:@!$&'()*+,;=-._~").encode('ascii'),
        'query_string': quote(query_string, safe="%=&+/:;?@!$'()*,-._~").encode('ascii'),
    }


@dataclass(slots=True)
class TestResponse:
    status_code: int
    headers: Headers
    body: bytes
    trailers: Headers = field(default_factory=Headers)
    early_hints: tuple[str, ...] = ()
    history: tuple['TestResponse', ...] = ()
    url: str = ''

    @property
    def text(self) -> str:
        return self.body.decode('utf-8')

    def json(self) -> Any:
        return loads(self.body)


class TestClient[StateT = Any]:
    def __init__(
        self, app: Ryuuseigun[StateT], *, base_url: str = 'http://testserver',
        follow_redirects: bool = False, max_redirects: int = 20, lifespan_timeout: float = 10,
    ) -> None:
        if not isinstance(max_redirects, int) or isinstance(max_redirects, bool) or max_redirects < 0:
            raise ValueError('max_redirects must be non-negative')
        if not isfinite(lifespan_timeout) or lifespan_timeout <= 0:
            raise ValueError('lifespan_timeout must be finite and positive')
        self.app = app
        if urlsplit(base_url).query or urlsplit(base_url).fragment:
            raise ValueError('base_url cannot contain a query or fragment')
        self.base_url = base_url.rstrip('/') + '/'
        self._origin = origin(self.base_url)
        self.follow_redirects = follow_redirects
        self.max_redirects = max_redirects
        self.cookies = CookieJar(DefaultCookiePolicy(strict_ns_domain=DefaultCookiePolicy.DomainStrictNonDomain))
        self._lifespan = LifespanSession(app, lifespan_timeout)
        self._state = 'new'
        self._requests: set[Task[None]] = set()
        self._websockets: list[WebSocketTestSession] = []

    async def __aenter__(self) -> Self:
        if self._state != 'new':
            raise RuntimeError('A test client context can only be entered once')
        self._state = 'starting'
        try:
            await self._lifespan.start()
        except BaseException:
            self._state = 'failed'
            raise
        self._state = 'active'
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None,
    ) -> None:
        self._state = 'closing'
        errors: list[BaseException] = []
        try:
            tasks = tuple(self._requests)
            try:
                await cancel_and_join(*tasks)
            except BaseException as error:
                errors.append(error)
            for task in tasks:
                if not task.cancelled() and (task_error := task.exception()) is not None:
                    errors.append(task_error)
            for session in self._websockets:
                if session._exited or session._task is None:
                    continue
                try:
                    async with timeout(self._lifespan.timeout):
                        await session.__aexit__(None, None, None)
                except BaseException as error:
                    errors.append(error)
                finally:
                    if session._task is not None:
                        await cancel_and_join(session._task)
            try:
                await self._lifespan.stop()
            except BaseException as error:
                errors.append(error)
        finally:
            self._state = 'closed'
        if errors:
            if exc is not None:
                errors.insert(0, exc)
            raise BaseExceptionGroup('Test client cleanup failed', errors)

    def _url(self, path: str) -> str:
        if self._state not in {'new', 'active'}:
            raise RuntimeError(f'Test client is {self._state}')
        url = urljoin(self.base_url, path)
        if origin(url) != self._origin:
            raise ValueError('The in-process test client only supports its configured origin')
        parts = urlsplit(url)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ''))

    async def request(
        self,
        method: str,
        path: str,
        *,
        headers: Optional[Mapping[str, str]] = None,
        body: bytes | str | None = None,
        json: Any = _MISSING,
        form: FormFields | None = None,
        files: FileFields | None = None,
        follow_redirects: bool | None = None,
        query: Optional[Mapping[str, str]] = None,
        http_version: str = '1.1',
        extensions: Optional[Mapping[str, Any]] = None,
    ) -> TestResponse:
        from orjson import dumps

        request_headers = Headers(headers)
        if sum((body is not None, json is not _MISSING, form is not None or files is not None)) > 1:
            raise ValueError('Use only one of body, json, or form/files')
        if json is not _MISSING:
            body = dumps(json)
            request_headers.setdefault(HeaderName.CONTENT_TYPE, MediaType.JSON)
        elif form is not None or files is not None:
            body = encode_form(form, files, request_headers)
        encoded_body = body.encode('utf-8') if isinstance(body, str) else body or b''
        url = self._url(path)
        if query is not None:
            parts = urlsplit(url)
            url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ''))
        method = method.upper()
        history: list[TestResponse] = []
        follow = self.follow_redirects if follow_redirects is None else follow_redirects
        while True:
            res = await self._request_once(method, url, request_headers, encoded_body, http_version, extensions)
            res.history = tuple(history)
            location = res.headers.get(HeaderName.LOCATION)
            if not follow or res.status_code not in {301, 302, 303, 307, 308} or location is None:
                return res
            if len(history) >= self.max_redirects:
                raise RuntimeError(f'Maximum redirects exceeded ({self.max_redirects})')
            history.append(res)
            url = self._url(urljoin(url, location))
            if (res.status_code == 303 and method != HTTPMethod.HEAD) or (
                res.status_code in {301, 302} and method == HTTPMethod.POST
            ):
                method, encoded_body = HTTPMethod.GET, b''
                request_headers.popall(HeaderName.CONTENT_TYPE)
                request_headers.popall(HeaderName.CONTENT_LENGTH)
            request_headers.popall(HeaderName.COOKIE)

    async def _request_once(
        self, method: str, url: str, headers: Headers, encoded_body: bytes,
        http_version: str, extensions: Mapping[str, Any] | None,
    ) -> TestResponse:
        request_headers = Headers(headers)
        cookie_request = None
        if any(self.cookies):
            cookie_request = CookieRequest(url, headers=dict(request_headers.items()))
            self.cookies.add_cookie_header(cookie_request)
            cookie = cookie_request.get_header(HeaderName.COOKIE)
            if cookie is not None:
                request_headers.setdefault(HeaderName.COOKIE, cookie)
        parts = urlsplit(url)
        request_headers.setdefault(HeaderName.HOST, parts.netloc)
        request_headers.setdefault(HeaderName.CONTENT_LENGTH, str(len(encoded_body)))
        messages = [{'type': ASGIMessageType.HTTP_REQUEST, 'body': encoded_body, 'more_body': False}]
        sent: list[ASGIMessage] = []
        disconnected = Event()

        async def receive() -> ASGIMessage:
            if messages:
                return messages.pop(0)
            await disconnected.wait()
            return {'type': ASGIMessageType.HTTP_DISCONNECT}

        async def send(message: ASGIMessage) -> None:
            sent.append(message)

        scope = {
            'type': ASGIScopeType.HTTP,
            'asgi': {'version': '3.0', 'spec_version': '2.5'},
            'http_version': http_version,
            'method': method.upper(),
            'scheme': parts.scheme,
            **_url_scope(url),
            'headers': request_headers.raw(),
            'client': ('127.0.0.1', 50000),
            'server': self._origin[1:],
            'extensions': dict(extensions or {}),
        }
        task = create_task(self.app(scope, receive, send))
        self._requests.add(task)
        try:
            await task
        finally:
            self._requests.discard(task)
            await cancel_and_join(task)

        start = next(message for message in sent if message['type'] == ASGIMessageType.HTTP_RESPONSE_START)
        response_body = b''.join(
            message.get('body', b'')
            for message in sent
            if message['type'] == ASGIMessageType.HTTP_RESPONSE_BODY
        )
        trailer_items = [
            item
            for message in sent
            if message['type'] == ASGIMessageType.HTTP_RESPONSE_TRAILERS
            for item in message['headers']
        ]
        hints = tuple(
            link.decode('latin-1')
            for message in sent
            if message['type'] == ASGIMessageType.HTTP_RESPONSE_EARLY_HINT
            for link in message['links']
        )
        response = TestResponse(
            start['status'],
            Headers.from_raw(start['headers']),
            response_body,
            Headers.from_raw(trailer_items),
            hints,
            url=url,
        )
        if HeaderName.SET_COOKIE in response.headers:
            if cookie_request is None:
                cookie_request = CookieRequest(url, headers=dict(request_headers.items()))
            # cookiejar documents an info() protocol, but typeshed requires HTTPResponse.
            self.cookies.extract_cookies(CookieResponse(response.headers), cookie_request)  # type: ignore[arg-type]
        return response

    async def get(self, path: str, **kwargs: Unpack[RequestOptions]) -> TestResponse:
        return await self.request(HTTPMethod.GET, path, **kwargs)

    async def post(self, path: str, **kwargs: Unpack[RequestOptions]) -> TestResponse:
        return await self.request(HTTPMethod.POST, path, **kwargs)

    async def put(self, path: str, **kwargs: Unpack[RequestOptions]) -> TestResponse:
        return await self.request(HTTPMethod.PUT, path, **kwargs)

    async def patch(self, path: str, **kwargs: Unpack[RequestOptions]) -> TestResponse:
        return await self.request(HTTPMethod.PATCH, path, **kwargs)

    async def delete(self, path: str, **kwargs: Unpack[RequestOptions]) -> TestResponse:
        return await self.request(HTTPMethod.DELETE, path, **kwargs)

    async def query(self, path: str, **kwargs: Unpack[RequestOptions]) -> TestResponse:
        return await self.request(HTTPMethod.QUERY, path, **kwargs)

    def websocket(
        self,
        path: str,
        *,
        headers: Optional[Mapping[str, str]] = None,
        subprotocols: tuple[str, ...] = (),
    ) -> 'WebSocketTestSession':
        url = self._url(path)
        cookie_request = CookieRequest(url, headers=dict(headers or {}))
        self.cookies.add_cookie_header(cookie_request)
        request_headers = dict(cookie_request.header_items())
        request_headers.setdefault(HeaderName.HOST, urlsplit(url).netloc)
        session = WebSocketTestSession(self.app, url, headers=request_headers, subprotocols=subprotocols)
        session.scope['scheme'] = 'wss' if self._origin[0] == 'https' else 'ws'
        session.scope['server'] = self._origin[1:]
        session._owner = self
        self._websockets.append(session)
        return session


class WebSocketUpgradeError(Exception):
    def __init__(self, status_code: int, body: bytes) -> None:
        self.status_code = status_code
        self.body = body
        super().__init__(f'WebSocket upgrade rejected with status {status_code}')


class WebSocketTestSession:
    def __init__(
        self,
        app: Ryuuseigun[Any],
        path: str,
        *,
        headers: Optional[Mapping[str, str]] = None,
        subprotocols: tuple[str, ...] = (),
    ) -> None:
        request_headers = Headers(headers)
        request_headers.setdefault(HeaderName.HOST, 'testserver')
        self.app = app
        self.scope = {
            'type': ASGIScopeType.WEBSOCKET,
            'asgi': {'version': '3.0', 'spec_version': '2.5'},
            'http_version': '1.1',
            'scheme': URLScheme.WEBSOCKET,
            **_url_scope(path),
            'headers': request_headers.raw(),
            'client': ('127.0.0.1', 50000),
            'server': ('testserver', 80),
            'subprotocols': list(subprotocols),
            'extensions': {ASGIExtension.WEBSOCKET_RESPONSE: {}},
        }
        self.accepted_subprotocol: Optional[str] = None
        self.response_headers = Headers()
        self._incoming: Queue[ASGIMessage] = Queue()
        self._outgoing: Queue[ASGIMessage] = Queue()
        self._task: Optional[Task[None]] = None
        self._closed = False
        self._exited = False
        self._owner: TestClient[Any] | None = None

    async def __aenter__(self) -> Self:
        if self._task is not None:
            raise RuntimeError('A WebSocket test session can only be entered once')
        if self._owner is not None:
            self._owner._url(self.scope['path'])
        self._task = create_task(self.app(self.scope, self._incoming.get, self._outgoing.put))
        try:
            await self._incoming.put({'type': ASGIMessageType.WEBSOCKET_CONNECT})
            first = await self._outgoing.get()
            if first['type'] == ASGIMessageType.WEBSOCKET_ACCEPT:
                self.accepted_subprotocol = first.get('subprotocol')
                self.response_headers = Headers.from_raw(first.get('headers', []))
                return self
            if first['type'] == ASGIMessageType.WEBSOCKET_RESPONSE_START:
                body = bytearray()
                while True:
                    message = await self._outgoing.get()
                    body.extend(message.get('body', b''))
                    if not message.get('more_body', False):
                        break
                await self._wait_for_app()
                raise WebSocketUpgradeError(int(first['status']), bytes(body))
            await self._wait_for_app()
            raise WebSocketUpgradeError(StatusCode.FORBIDDEN, b'')
        except BaseException:
            self._exited = True
            await cancel_and_join(self._task)
            raise

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self._exited:
            return
        try:
            if not self._closed:
                await self.close()
            await self._wait_for_app()
        finally:
            self._exited = True
            if self._task is not None:
                await cancel_and_join(self._task)

    async def send_text(self, value: str) -> None:
        await self._incoming.put({'type': ASGIMessageType.WEBSOCKET_RECEIVE, 'text': value})

    async def send_bytes(self, value: bytes) -> None:
        await self._incoming.put({'type': ASGIMessageType.WEBSOCKET_RECEIVE, 'bytes': value})

    async def send_json(self, value: Any) -> None:
        from orjson import dumps

        await self.send_bytes(dumps(value))

    async def receive(self) -> ASGIMessage:
        message = await self._outgoing.get()
        if message['type'] == ASGIMessageType.WEBSOCKET_CLOSE:
            self._closed = True
            raise WebSocketDisconnect(
                int(message.get('code', WebSocketCloseCode.NORMAL)),
                str(message.get('reason', '')),
            )
        return message

    async def receive_text(self) -> str:
        message = await self.receive()
        value = message.get('text')
        if not isinstance(value, str):
            raise TypeError('Expected a text WebSocket message')
        return value

    async def receive_bytes(self) -> bytes:
        message = await self.receive()
        value = message.get('bytes')
        if not isinstance(value, bytes):
            raise TypeError('Expected a binary WebSocket message')
        return value

    async def receive_json(self) -> Any:
        message = await self.receive()
        payload = message.get('text', message.get('bytes'))
        if not isinstance(payload, (str, bytes)):
            raise TypeError('Expected a text or binary WebSocket message')
        return loads(payload)

    async def close(self, code: int = WebSocketCloseCode.NORMAL, reason: str = '') -> None:
        if self._closed:
            return
        self._closed = True
        await self._incoming.put({'type': ASGIMessageType.WEBSOCKET_DISCONNECT, 'code': code, 'reason': reason})

    async def _wait_for_app(self) -> None:
        if self._task is not None:
            await self._task
