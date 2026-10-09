from typing import Any, Optional
from dataclasses import dataclass
from collections.abc import Iterable
from ryuuseigun.constants import HTTPMethod
from ryuuseigun.config import MultipartOverrides
from ryuuseigun._registration import Registration
from ryuuseigun.middleware import MiddlewareCallable
from ryuuseigun.handlers import require_async, validate_middleware
from ryuuseigun.routing import RouteHandler, normalize_methods, normalize_query_media_types
from ryuuseigun.websocket import WebSocketHandler, WebSocketMiddleware, validate_websocket_middleware

@dataclass(slots=True, frozen=True)
class RouteDefinition:
    path: str
    methods: frozenset[str]
    handler: RouteHandler
    endpoint: str
    accept_query: tuple[str, ...] = ()
    multipart: Optional[MultipartOverrides] = None
    middlewares: tuple[MiddlewareCallable[Any], ...] = ()

@dataclass(slots=True, frozen=True)
class MountedModule[StateT = Any, SocketStateT = Any]:
    module: 'Module[StateT, SocketStateT]'
    url_prefix: str

@dataclass(slots=True, frozen=True)
class WebSocketDefinition:
    path: str
    handler: WebSocketHandler
    endpoint: str
    middlewares: tuple[WebSocketMiddleware, ...] = ()

class Module[StateT = Any, SocketStateT = Any](Registration[StateT, SocketStateT]):
    def __init__(self, name: str, *, url_prefix: str = '') -> None:
        super().__init__()
        if not name or '.' in name:
            raise ValueError("Module names must be non-empty and cannot contain '.'")
        self._name = name
        self._url_prefix = url_prefix
        self._routes: list[RouteDefinition] = []
        self._websocket_routes: list[WebSocketDefinition] = []
        self._modules: list[MountedModule[StateT, SocketStateT]] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def url_prefix(self) -> str:
        return self._url_prefix

    @property
    def routes(self) -> tuple[RouteDefinition, ...]:
        return tuple(self._routes)

    @property
    def websocket_routes(self) -> tuple[WebSocketDefinition, ...]:
        return tuple(self._websocket_routes)

    @property
    def modules(self) -> tuple[MountedModule[StateT, SocketStateT], ...]:
        return tuple(self._modules)

    def add_url_rule(
        self, path: str, handler: RouteHandler[StateT], *, methods: Iterable[str] = (HTTPMethod.GET,),
        endpoint: str | None = None, name: str | None = None, query_media_types: tuple[str, ...] = (),
        multipart: MultipartOverrides | None = None, middlewares: Iterable[MiddlewareCallable[StateT]] = (),
    ) -> None:
        self._ensure_mutable()
        require_async(handler, f'Route handler {path} ({getattr(handler, "__name__", type(handler).__name__)})')
        if endpoint is not None and name is not None and endpoint != name:
            raise ValueError('Route endpoint and name must match when both are provided')
        if multipart is not None and not isinstance(multipart, MultipartOverrides):
            raise TypeError('Route multipart settings must be MultipartOverrides')
        active_middlewares = tuple(middlewares)
        for middleware in active_middlewares:
            validate_middleware(middleware)
        route_name = str(name or endpoint or getattr(handler, '__name__', type(handler).__name__))
        self._routes.append(RouteDefinition(
            path, normalize_methods(methods), handler, route_name, normalize_query_media_types(query_media_types),
            multipart, active_middlewares,
        ))

    def add_websocket_rule(
        self, path: str, handler: WebSocketHandler[SocketStateT], *, endpoint: str | None = None,
        middlewares: Iterable[WebSocketMiddleware[SocketStateT]] = (),
    ) -> None:
        self._ensure_mutable()
        require_async(handler, 'WebSocket handler')
        active_middlewares = tuple(middlewares)
        for middleware in active_middlewares:
            validate_websocket_middleware(middleware)
        route_name = str(endpoint or getattr(handler, '__name__', type(handler).__name__))
        self._websocket_routes.append(WebSocketDefinition(path, handler, route_name, active_middlewares))

    def register_module(self, module: 'Module[StateT, SocketStateT]', *, url_prefix: str = '') -> None:
        self._ensure_mutable()
        if module is self:
            raise ValueError('A module cannot register itself')
        self._modules.append(MountedModule(module, url_prefix))

    def _freeze(self) -> None:
        self._frozen = True
        for mounted in self._modules:
            mounted.module._freeze()
