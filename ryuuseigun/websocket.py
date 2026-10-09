from dataclasses import dataclass
from types import SimpleNamespace
from ryuuseigun.headers import Headers
from inspect import Parameter, signature
from ryuuseigun.metadata import RouteInfo
from ryuuseigun.request import QueryParams
from orjson import dumps, loads, JSONDecodeError
from ryuuseigun._paths import root_path, route_path
from ryuuseigun.routing import Converter, normalize_path
from collections.abc import Callable, Sequence, Awaitable
from typing import Any, Optional, Protocol, overload, Concatenate
from ryuuseigun.handlers import require_async, validate_middleware
from ryuuseigun.types import Send, Receive, ASGIScope, JSONValue, ASGIMessage, HeaderMapping
from ryuuseigun.constants import HeaderName, StatusCode, ASGIExtension, ASGIMessageType, WebSocketCloseCode

type WebSocketHandler[StateT = Any] = Callable[Concatenate[WebSocket[StateT], ...], Awaitable[None]]
type WebSocketInvoker = Callable[[WebSocket[Any], dict[str, Any]], Awaitable[None]]
type WebSocketNext[StateT = Any] = Callable[[WebSocket[StateT]], Awaitable[None]]
type WebSocketMiddleware[StateT = Any] = Callable[[WebSocket[StateT], WebSocketNext[StateT]], Awaitable[None]]

class WebSocketDecorator[StateT](Protocol):
    def __call__[**Parameters](
        self, handler: Callable[Concatenate[WebSocket[StateT], Parameters], Awaitable[None]], /,
    ) -> Callable[Concatenate[WebSocket[StateT], Parameters], Awaitable[None]]: ...

_MISSING = object()

class WebSocketDisconnect(Exception):
    def __init__(self, code: int = WebSocketCloseCode.NORMAL, reason: str = '') -> None:
        self.code = code
        self.reason = reason
        super().__init__(f'WebSocket disconnected with code {code}: {reason}')

class WebSocket[StateT = SimpleNamespace]:
    __slots__ = (
        '_accepted',
        '_closed',
        '_connected',
        '_default_response_headers',
        '_json_options',
        '_receive',
        '_send',
        '_route_info',
        'headers',
        'path',
        'path_params',
        'query',
        'scope',
        'state',
    )

    @overload
    def __init__(
        self: 'WebSocket[SimpleNamespace]', scope: ASGIScope, receive: Receive, send: Send, *,
        json_options: int = 0, default_response_headers: tuple[tuple[str, str], ...] = (),
    ) -> None: ...

    @overload
    def __init__(
        self, scope: ASGIScope, receive: Receive, send: Send, *, state: StateT,
        json_options: int = 0, default_response_headers: tuple[tuple[str, str], ...] = (),
    ) -> None: ...

    def __init__(
        self,
        scope: ASGIScope,
        receive: Receive,
        send: Send,
        *,
        state: Any = _MISSING,
        json_options: int = 0,
        default_response_headers: tuple[tuple[str, str], ...] = (),
    ) -> None:
        self.scope = scope
        self._receive = receive
        self._send = send
        self._accepted = False
        self._closed = False
        self._connected = False
        self._json_options = json_options
        self._default_response_headers = default_response_headers
        self.path = route_path(scope)
        self._route_info: RouteInfo | None = None
        self.headers = Headers.from_raw(scope.get('headers', []))
        self.query = QueryParams(scope.get('query_string', b''))
        self.path_params: dict[str, Any] = {}
        if state is _MISSING:
            state = SimpleNamespace()
        self.state: StateT = state

    @property
    def accepted(self) -> bool:
        return self._accepted

    @property
    def route(self) -> RouteInfo | None:
        """Selected WebSocket route, available before socket middleware enters."""
        return self._route_info

    @property
    def root_path(self) -> str:
        return root_path(self.scope)

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def subprotocols(self) -> tuple[str, ...]:
        values = self.scope.get('subprotocols', [])
        return tuple(str(value) for value in values)

    def supports(self, extension: str) -> bool:
        extensions = self.scope.get('extensions', {})
        return isinstance(extensions, dict) and extension in extensions

    async def accept(
        self,
        subprotocol: Optional[str] = None,
        headers: Optional[HeaderMapping] = None,
    ) -> None:
        if self._accepted:
            raise RuntimeError('WebSocket has already been accepted')
        if self._closed:
            raise RuntimeError('WebSocket is closed')
        if subprotocol is not None and subprotocol not in self.subprotocols:
            raise ValueError('The selected subprotocol was not offered by the client')
        await self._ensure_connect()
        response_headers = Headers(list(self._default_response_headers))
        if headers is not None:
            response_headers.update(headers)
        await self._send(
            {
                'type': ASGIMessageType.WEBSOCKET_ACCEPT,
                'subprotocol': subprotocol,
                'headers': response_headers.raw(),
            }
        )
        self._accepted = True

    async def receive(self) -> ASGIMessage:
        if not self._accepted:
            raise RuntimeError('Accept the WebSocket before receiving data')
        if self._closed:
            raise RuntimeError('WebSocket is closed')
        message = await self._receive()
        message_type = message.get('type')
        if message_type == ASGIMessageType.WEBSOCKET_DISCONNECT:
            self._closed = True
            raise WebSocketDisconnect(
                int(message.get('code', WebSocketCloseCode.NORMAL)),
                str(message.get('reason', '')),
            )
        if message_type != ASGIMessageType.WEBSOCKET_RECEIVE:
            raise RuntimeError(f'Unexpected ASGI message: {message_type}')
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

    async def receive_json(self) -> JSONValue:
        message = await self.receive()
        payload = message.get('text')
        if payload is None:
            payload = message.get('bytes')
        if not isinstance(payload, (str, bytes)):
            raise TypeError('Expected a text or binary WebSocket message')
        try:
            value: JSONValue = loads(payload)
        except JSONDecodeError as error:
            raise ValueError('Malformed WebSocket JSON') from error
        return value

    async def send_text(self, value: str) -> None:
        self._ensure_writable()
        await self._send({'type': ASGIMessageType.WEBSOCKET_SEND, 'text': value})

    async def send_bytes(self, value: bytes) -> None:
        self._ensure_writable()
        await self._send({'type': ASGIMessageType.WEBSOCKET_SEND, 'bytes': value})

    async def send_json(self, value: Any) -> None:
        await self.send_bytes(dumps(value, option=self._json_options))

    async def close(self, code: int = WebSocketCloseCode.NORMAL, reason: str = '') -> None:
        if self._closed:
            return
        if not WebSocketCloseCode.NORMAL <= code <= WebSocketCloseCode.APPLICATION_MAX:
            raise ValueError('WebSocket close codes must be between 1000 and 4999')
        await self._ensure_connect()
        await self._send({'type': ASGIMessageType.WEBSOCKET_CLOSE, 'code': code, 'reason': reason})
        self._closed = True

    async def reject(
        self,
        status_code: int = StatusCode.FORBIDDEN,
        body: bytes | str = b'',
        headers: Optional[HeaderMapping] = None,
    ) -> None:
        if self._closed:
            raise RuntimeError('A closed WebSocket cannot be rejected')
        if self._accepted:
            raise RuntimeError('An accepted WebSocket cannot be rejected')
        await self._ensure_connect()
        if not self.supports(ASGIExtension.WEBSOCKET_RESPONSE):
            await self.close(WebSocketCloseCode.POLICY_VIOLATION, 'Connection rejected')
            return
        payload = body.encode('utf-8') if isinstance(body, str) else body
        response_headers = Headers(list(self._default_response_headers))
        if headers is not None:
            response_headers.update(headers)
        response_headers.setdefault(HeaderName.CONTENT_LENGTH, str(len(payload)))
        await self._send(
            {
                'type': ASGIMessageType.WEBSOCKET_RESPONSE_START,
                'status': status_code,
                'headers': response_headers.raw(),
            }
        )
        await self._send({'type': ASGIMessageType.WEBSOCKET_RESPONSE_BODY, 'body': payload})
        self._closed = True

    async def _ensure_connect(self) -> None:
        if self._connected:
            return
        message = await self._receive()
        if message.get('type') != ASGIMessageType.WEBSOCKET_CONNECT:
            raise RuntimeError(f"Expected {ASGIMessageType.WEBSOCKET_CONNECT}, received {message.get('type')}")
        self._connected = True

    def _ensure_writable(self) -> None:
        if not self._accepted:
            raise RuntimeError('Accept the WebSocket before sending data')
        if self._closed:
            raise RuntimeError('WebSocket is closed')

@dataclass(slots=True, frozen=True)
class WebSocketRoute:
    path: str
    endpoint: str
    handler: WebSocketHandler
    invoke: WebSocketInvoker
    segments: tuple[str | Converter, ...]
    param_names: tuple[str, ...]
    middlewares: tuple[WebSocketMiddleware, ...]
    module_chain: tuple[Any, ...] = ()

class WebSocketRouter:
    def __init__(self, *, strict_slashes: bool = False) -> None:
        self._strict_slashes = strict_slashes
        self._frozen = False
        self._routes: list[WebSocketRoute] = []
        self._patterns: set[tuple[tuple[bool, str], ...]] = set()

    @property
    def strict_slashes(self) -> bool:
        return self._strict_slashes

    def freeze(self) -> None:
        self._frozen = True

    def add(self, route: WebSocketRoute) -> None:
        if self._frozen:
            raise RuntimeError('WebSocket router has been finalized')
        pattern = _route_pattern(route.segments)
        if pattern in self._patterns:
            raise ValueError(f'Duplicate WebSocket route: {route.path}')
        self._patterns.add(pattern)
        self._routes.append(route)
        self._routes.sort(key=_route_priority, reverse=True)

    def check_many(self, routes: Sequence[WebSocketRoute]) -> None:
        pending: set[tuple[tuple[bool, str], ...]] = set()
        for route in routes:
            pattern = _route_pattern(route.segments)
            if pattern in self._patterns or pattern in pending:
                raise ValueError(f'Duplicate WebSocket route: {route.path}')
            pending.add(pattern)

    def match(self, path: str) -> tuple[Optional[WebSocketRoute], dict[str, Any]]:
        normalized = normalize_path(path, strict_slashes=self._strict_slashes)
        values = [] if normalized == '/' else normalized[1:].split('/')
        for route in self._routes:
            params = _match_segments(route, values)
            if params is not None:
                return route, params
        return None, {}

def build_websocket_invoker(handler: WebSocketHandler, param_names: Sequence[str]) -> WebSocketInvoker:
    require_async(handler, 'WebSocket handler')
    parameters = list(signature(handler).parameters.values())
    if not parameters:
        raise TypeError('WebSocket handlers must accept a socket as their first positional argument')
    if parameters[0].kind not in {Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD}:
        raise TypeError('WebSocket handlers must accept their socket as a positional argument')

    marker = object()
    signature(handler).bind(marker, **dict.fromkeys(param_names, marker))

    def invoke(socket: WebSocket[Any], params: dict[str, Any]) -> Awaitable[None]:
        if params:
            return handler(socket, **params)

        return handler(socket)

    return invoke

def validate_websocket_middleware(middleware: WebSocketMiddleware) -> None:
    validate_middleware(middleware, 'WebSocket middleware')

def _route_pattern(segments: Sequence[str | Converter]) -> tuple[tuple[bool, str], ...]:
    return tuple((isinstance(segment, str), segment if isinstance(segment, str) else segment.name) for segment in segments)

def _route_priority(route: WebSocketRoute) -> tuple[int, ...]:
    priorities = {'int': 4, 'float': 3, 'uuid': 2, 'string': 1, 'path': 0}
    return tuple(5 if isinstance(segment, str) else priorities[segment.name] for segment in route.segments)

def _match_segments(route: WebSocketRoute, values: list[str]) -> Optional[dict[str, Any]]:
    converted: list[Any] = []
    index = 0
    try:
        for segment in route.segments:
            if isinstance(segment, str):
                if index >= len(values) or values[index] != segment:
                    return None
                index += 1
            elif segment.name == 'path':
                converted.append(segment.convert('/'.join(values[index:])))
                index = len(values)
            else:
                if index >= len(values):
                    return None
                converted.append(segment.convert(values[index]))
                index += 1
    except (TypeError, ValueError):
        return None
    if index != len(values):
        return None
    return dict(zip(route.param_names, converted, strict=True))
