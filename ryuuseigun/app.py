from math import isfinite
from logging import getLogger
from dataclasses import replace
from ipaddress import IPv6Address
from types import SimpleNamespace
from ryuuseigun.module import Module
from ryuuseigun._mounting import Mount
from ryuuseigun.headers import Headers
from inspect import iscoroutinefunction
from ryuuseigun.metadata import RouteInfo
from traceback import format_exception_only
from ryuuseigun.types import ASGIApplication
from ryuuseigun.exceptions import HTTPException
from ryuuseigun._lifespan import LifespanSession
from urllib.parse import quote, quote_from_bytes
from ryuuseigun._registration import Registration
from ryuuseigun.types import Send, Receive, ASGIScope
from typing import Any, Optional, overload, TYPE_CHECKING
from ryuuseigun.middleware import Next, MiddlewareCallable
from contextlib import AsyncExitStack, AbstractAsyncContextManager
from ryuuseigun._paths import route_path, mounted_scope, prefixed_path
from collections.abc import Mapping, Callable, Iterable, Sequence, Awaitable
from ryuuseigun.handlers import AfterHandler, BeforeHandler, validate_middleware
from ryuuseigun.config import Config, MultipartOverrides, resolve_multipart_limits
from ryuuseigun.response import Response, ResponseValue, make_response, default_error_response
from ryuuseigun.request import Request, set_request_context, reset_request_context, validate_query_string
from ryuuseigun.constants import (
    MediaType,
    HTTPMethod,
    HeaderName,
    StatusCode,
    ASGIScopeType,
    ASGIMessageType,
    WebSocketCloseCode,
)
from ryuuseigun.routing import (
    Match,
    Route,
    Router,
    build_path,
    join_paths,
    parse_path,
    RouteHandler,
    normalize_path,
    normalize_methods,
    build_route_invoker,
    normalize_query_media_types,
)
from ryuuseigun.websocket import (
    WebSocket,
    WebSocketNext,
    WebSocketRoute,
    WebSocketRouter,
    WebSocketHandler,
    WebSocketDisconnect,
    WebSocketMiddleware,
    build_websocket_invoker,
    validate_websocket_middleware,
)

if TYPE_CHECKING:
    from ryuuseigun.testing import TestClient

type _HookScope = tuple[tuple[BeforeHandler[Any], ...], tuple[AfterHandler[Any], ...]]
type _PipelineKey = tuple[tuple[int, ...], tuple[int, ...]]

def _normalize_host(value: str) -> str:
    if not value or any(character.isspace() or character in '/\\@,#?%' or ord(character) < 33 for character in value):
        return ''
    value = value.casefold()
    if value.startswith('['):
        closing = value.find(']')
        if closing < 0:
            return ''
        suffix = value[closing + 1:]
        if suffix and (not suffix.startswith(':') or not _valid_port(suffix[1:])):
            return ''
        try:
            return str(IPv6Address(value[1:closing]))
        except ValueError:
            return ''
    if value.count(':') > 1:
        return ''
    host, separator, port = value.rpartition(':')
    if separator:
        if not host or not _valid_port(port):
            return ''
        value = host
    value = value.removesuffix('.')
    if any(not label or not all(character.isascii() and (character.isalnum() or character == '-') for character in label)
           or label.startswith('-') or label.endswith('-') for label in value.split('.')):
        return ''
    return value

def _valid_port(value: str) -> bool:
    return value.isascii() and value.isdecimal() and len(value) <= 5 and int(value) <= 65535

def _redirect_location(path: str, query: bytes) -> str:
    location = quote(path, safe='/')
    return location if not query else f'{location}?{quote_from_bytes(query, safe="!$&\'()*+,-./:;=?@_%~")}'

def _host_is_trusted(value: str, trusted_hosts: tuple[str, ...]) -> bool:
    host = _normalize_host(value)
    if not host:
        return False
    for trusted in trusted_hosts:
        if trusted == '*':
            return True
        if trusted.startswith('*.'):
            suffix = _normalize_host(trusted[2:])
            if suffix and host.endswith(f'.{suffix}'):
                return True
        elif host == _normalize_host(trusted):
            return True
    return False

def _route_endpoint(endpoint: Optional[str], name: Optional[str]) -> Optional[str]:
    if endpoint is not None and name is not None and endpoint != name:
        raise ValueError('Route endpoint and name must match when both are provided')
    return name if name is not None else endpoint

class Ryuuseigun[
    RequestStateT = SimpleNamespace, AppStateT = SimpleNamespace, SocketStateT = SimpleNamespace,
](Registration[RequestStateT, SocketStateT]):
    """An ASGI application with explicitly owned state and resource lifetimes.

    Application state is created once per application instance. HTTP and socket
    factories run once per connection, never share request state, and carry their
    types through registration. Register everything before finalize/startup.
    Resource contexts enter in registration order and unwind in reverse, including
    startup rollback. Request middleware owns sending and cleanup; after_request
    runs before sending, while response after_send callbacks are process-local.
    """
    @overload
    def __init__(
        self: 'Ryuuseigun[SimpleNamespace, SimpleNamespace, SimpleNamespace]', import_name: str, *,
        config: Config | None = None, strict_slashes: bool | None = None, debug: bool | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self: 'Ryuuseigun[SimpleNamespace, SimpleNamespace, SocketStateT]', import_name: str, *,
        websocket_state_factory: Callable[[], SocketStateT],
        config: Config | None = None, strict_slashes: bool | None = None, debug: bool | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self: 'Ryuuseigun[SimpleNamespace, AppStateT, SimpleNamespace]', import_name: str, *,
        app_state_factory: Callable[[], AppStateT],
        config: Config | None = None, strict_slashes: bool | None = None, debug: bool | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self: 'Ryuuseigun[SimpleNamespace, AppStateT, SocketStateT]', import_name: str, *,
        app_state_factory: Callable[[], AppStateT],
        websocket_state_factory: Callable[[], SocketStateT],
        config: Config | None = None, strict_slashes: bool | None = None, debug: bool | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self: 'Ryuuseigun[RequestStateT, SimpleNamespace, SimpleNamespace]', import_name: str, *,
        request_state_factory: Callable[[], RequestStateT],
        config: Config | None = None, strict_slashes: bool | None = None, debug: bool | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self: 'Ryuuseigun[RequestStateT, SimpleNamespace, SocketStateT]', import_name: str, *,
        request_state_factory: Callable[[], RequestStateT],
        websocket_state_factory: Callable[[], SocketStateT],
        config: Config | None = None, strict_slashes: bool | None = None, debug: bool | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self: 'Ryuuseigun[RequestStateT, AppStateT, SimpleNamespace]', import_name: str, *,
        request_state_factory: Callable[[], RequestStateT],
        app_state_factory: Callable[[], AppStateT],
        config: Config | None = None, strict_slashes: bool | None = None, debug: bool | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self: 'Ryuuseigun[RequestStateT, AppStateT, SocketStateT]', import_name: str, *,
        request_state_factory: Callable[[], RequestStateT],
        app_state_factory: Callable[[], AppStateT],
        websocket_state_factory: Callable[[], SocketStateT],
        config: Config | None = None, strict_slashes: bool | None = None, debug: bool | None = None,
    ) -> None: ...

    def __init__(
        self,
        import_name: str,
        *,
        config: Optional[Config] = None,
        strict_slashes: Optional[bool] = None,
        debug: Optional[bool] = None,
        request_state_factory: Optional[Callable[[], RequestStateT]] = None,
        app_state_factory: Optional[Callable[[], AppStateT]] = None,
        websocket_state_factory: Optional[Callable[[], SocketStateT]] = None,
    ) -> None:
        super().__init__()
        settings = config or Config()
        if strict_slashes is not None or debug is not None:
            settings = replace(
                settings,
                strict_slashes=settings.strict_slashes if strict_slashes is None else strict_slashes,
                debug=settings.debug if debug is None else debug,
            )

        self.import_name = import_name
        self.logger = getLogger(f'ryuuseigun.{import_name}')
        self._config = settings
        self._multipart_limits = settings.multipart_limits
        self._router = Router(
            strict_slashes=settings.strict_slashes,
            automatic_head=settings.automatic_head,
            automatic_options=settings.automatic_options,
        )
        self._websocket_router = WebSocketRouter(strict_slashes=settings.strict_slashes)
        self._endpoints: dict[str, RouteHandler] = {}
        self._url_routes: dict[str, Route] = {}
        self._websocket_endpoints: dict[str, WebSocketHandler] = {}
        self._routes: list[Route] = []
        self._websocket_routes: list[WebSocketRoute] = []
        self._pipelines: dict[int, Next[RequestStateT]] = {}
        self._method_pipelines: dict[int, Next[RequestStateT]] = {}
        self._websocket_pipelines: dict[int, WebSocketNext[SocketStateT]] = {}
        self._fallback_pipeline: Next[RequestStateT] = self._terminal
        self._request_state_factory: Callable[[], Any] = (
            SimpleNamespace if request_state_factory is None else request_state_factory
        )
        self._startup_handlers: list[Callable[[], Awaitable[None]]] = []
        self._shutdown_handlers: list[Callable[[], Awaitable[None]]] = []
        self._lifespan_factories: list[
            Callable[['Ryuuseigun[RequestStateT, AppStateT, SocketStateT]'], AbstractAsyncContextManager[None]]
        ] = []
        application_factory: Callable[[], Any] = app_state_factory or SimpleNamespace
        self.state: AppStateT = application_factory()
        self._websocket_state_factory: Callable[[], Any] = websocket_state_factory or SimpleNamespace
        self._finalizers: list[Callable[[], None]] = []
        self._mounts: list[Mount] = []
        self._mount_lifespans: dict[int, LifespanSession] = {}
        self._route_info: dict[int, RouteInfo] = {}

    @property
    def mounts(self) -> tuple[Mount, ...]:
        return tuple(self._mounts)

    def mount(
        self, path: str, app: ASGIApplication, *, name: str,
        lifespan: bool = False, lifespan_timeout: float = 10,
    ) -> None:
        """Delegate a prefix to an independent app, using the longest matching prefix.

        Mounts own their prefixes before native route matching. Native hooks belong
        to each app; shared policies go in outer ASGI middleware. Child lifespans
        are opt-in, start after parent resource contexts, and unwind in reverse.
        url_for('name:route', ...) reverses a child's named route; use
        url_for('name', path='/file') for an arbitrary ASGI application.
        """
        self._ensure_mutable()
        if not path.startswith('/') or any(char in path for char in '?#<>'):
            raise ValueError('Mount paths must be literal absolute paths')
        prefix = path.rstrip('/')
        if not name or ':' in name or any(item.name == name or item.path == prefix for item in self._mounts):
            raise ValueError('Mount names and paths must be unique; names cannot contain a colon')
        if not callable(app):
            raise TypeError('Mounted applications must be ASGI callables')
        if not isfinite(lifespan_timeout) or lifespan_timeout <= 0:
            raise ValueError('lifespan_timeout must be finite and positive')
        if app is self or isinstance(app, Ryuuseigun) and app._contains_app(self):
            raise ValueError('Cyclic application mounting')
        self._mounts.append(Mount(prefix, app, name, lifespan, lifespan_timeout))

    def _contains_app(self, app: ASGIApplication) -> bool:
        return any(
            mount.app is app or isinstance(mount.app, Ryuuseigun) and mount.app._contains_app(app)
            for mount in self._mounts
        )

    def inspect_routes(self) -> tuple[RouteInfo, ...]:
        """Finalize registration and return immutable HTTP/WebSocket pipeline descriptions."""
        self.finalize()
        return tuple(self._route_info.values())

    async def _dispatch_mount(self, scope: ASGIScope, receive: Receive, send: Send) -> bool:
        path = route_path(scope)
        for mount in sorted(self._mounts, key=lambda item: len(item.path), reverse=True):
            if mount.path and path != mount.path and not path.startswith(mount.path + '/'):
                continue
            if self._config.trusted_hosts:
                try:
                    self._validate_host(Headers.from_raw(scope.get('headers', [])))
                except HTTPException as error:
                    if scope['type'] == ASGIScopeType.WEBSOCKET:
                        await self._create_websocket(scope, receive, send).reject(error.status_code)
                    else:
                        response = self._make_error_response(error.status_code, error.detail, error.headers)
                        self._apply_default_response_headers(response)
                        await response.send(send, scope=scope, receive=receive, head=scope.get('method') == 'HEAD')
                    return True
            child = mounted_scope(scope, mount.path)
            session = self._mount_lifespans.get(id(mount.app))
            if session is not None:
                child['state'] = dict(session.state)
            await mount.app(child, receive, send)
            return True

        return False

    @property
    def routes(self) -> tuple[Route, ...]:
        return tuple(self._routes)

    def on_finalize(self, handler: Callable[[], None]) -> Callable[[], None]:
        """Register setup work that runs before dispatch is frozen."""
        self._ensure_mutable()
        if iscoroutinefunction(handler) or iscoroutinefunction(type(handler).__call__):
            raise TypeError('Finalization callbacks must be synchronous')
        self._finalizers.append(handler)
        return handler

    @property
    def router(self) -> Router:
        return self._router

    @property
    def websocket_router(self) -> WebSocketRouter:
        return self._websocket_router

    @property
    def config(self) -> Config:
        return self._config

    @property
    def debug(self) -> bool:
        return self._config.debug

    async def __call__(self, scope: ASGIScope, receive: Receive, send: Send) -> None:
        if not self._frozen:
            self.finalize()
        scope_type = scope.get('type')
        if self._mounts and scope_type in {ASGIScopeType.HTTP, ASGIScopeType.WEBSOCKET}:
            if await self._dispatch_mount(scope, receive, send):
                return
        if scope_type == ASGIScopeType.LIFESPAN:
            await self._lifespan(receive, send)
            return
        if scope_type == ASGIScopeType.WEBSOCKET:
            await self._dispatch_websocket(scope, receive, send)
            return
        if scope_type != ASGIScopeType.HTTP:
            raise RuntimeError(f'Unsupported ASGI scope type: {scope_type}')

        query_string = self._scope_query_string(scope)
        try:
            if query_string:
                validate_query_string(
                    query_string,
                    self._config.max_query_string_size,
                    self._config.max_query_parameters,
                )
        except HTTPException as error:
            response = self._make_error_response(error.status_code, error.detail, error.headers)
            if self._config.default_response_headers:
                self._apply_default_response_headers(response)
            await response.send(
                send,
                scope=scope,
                receive=receive,
                head=str(scope.get('method', '')).upper() == HTTPMethod.HEAD,
            )
            return

        request: Request[RequestStateT] = Request(
            scope,
            receive,
            self._config.max_request_body_size,
            state=self._request_state_factory(),
            upload_spool_threshold=self._config.upload_spool_threshold,
            multipart_limits=self._multipart_limits,
            url_builder=self.url_for,
        )
        request._response_handler = self._respond
        request._send = send
        token = set_request_context(request)
        try:
            try:
                if self._config.trusted_hosts:
                    self._validate_host(request.headers)
            except Exception as error:
                await request.respond(await self._handle_error(request, error, ()))
                return
            try:
                match = self._router.match(request.path, request.method)
            except ValueError:
                match = Match(None)
            scope_route = match.route or match.scope_route
            module_chain = scope_route.module_chain if scope_route is not None else ()
            request._route_context = (match, module_chain)
            if scope_route is not None:
                request._route_info = self._route_info[id(scope_route)]
                scope['route'] = request.route
                scope['endpoint'] = scope_route.handler
            request.path_params = match.params
            if scope_route is not None:
                request._multipart_limits = scope_route.multipart_limits
                pipelines = self._pipelines if match.route is not None else self._method_pipelines
                pipeline = pipelines[id(scope_route)]
            else:
                pipeline = self._fallback_pipeline
            await pipeline(request)
        finally:
            try:
                if not request._closed:
                    await request.close()
            finally:
                reset_request_context(token)

    def add_websocket_rule(
        self,
        path: str,
        handler: WebSocketHandler[SocketStateT],
        *,
        endpoint: Optional[str] = None,
        module_chain: tuple[Module[Any], ...] = (),
        middlewares: Iterable[WebSocketMiddleware[SocketStateT]] = (),
    ) -> None:
        self._ensure_mutable()
        route = self._create_websocket_route(
            path, handler, endpoint=endpoint, module_chain=module_chain, middlewares=tuple(middlewares),
        )
        existing_handler = self._websocket_endpoints.get(route.endpoint)
        if existing_handler is not None and existing_handler is not handler:
            raise ValueError(f'Duplicate WebSocket endpoint: {route.endpoint}')
        self._websocket_router.add(route)
        self._websocket_routes.append(route)
        self._websocket_endpoints[route.endpoint] = handler

    def add_url_rule(
        self,
        path: str,
        handler: RouteHandler[RequestStateT],
        *,
        methods: Iterable[str] = (HTTPMethod.GET,),
        endpoint: Optional[str] = None,
        name: Optional[str] = None,
        module_chain: tuple[Module[Any], ...] = (),
        query_media_types: tuple[str, ...] = (),
        multipart: Optional[MultipartOverrides] = None,
        middlewares: Iterable[MiddlewareCallable[RequestStateT]] = (),
    ) -> None:
        self._ensure_mutable()
        route = self._create_route(
            path,
            handler,
            methods=methods,
            endpoint=_route_endpoint(endpoint, name),
            module_chain=module_chain,
            query_media_types=query_media_types,
            multipart=multipart,
            middlewares=tuple(middlewares),
        )
        existing_handler = self._endpoints.get(route.endpoint)
        if existing_handler is not None and existing_handler is not handler:
            raise ValueError(f'Duplicate endpoint: {route.endpoint}')
        existing_route = self._url_routes.get(route.endpoint)
        if existing_route is not None and existing_route.path != route.path:
            raise ValueError(f'Duplicate endpoint: {route.endpoint}')

        self._router.add(route)
        self._routes.append(route)
        self._endpoints[route.endpoint] = handler
        self._url_routes[route.endpoint] = route

    def url_for(self, endpoint: str, **values: Any) -> str:
        mount_name, separator, child_name = endpoint.partition(':')
        for mount in self._mounts:
            if mount.name != mount_name:
                continue
            if separator:
                builder = getattr(mount.app, 'url_for', None)
                if not callable(builder):
                    raise ValueError('This mounted application does not expose url_for')
                child_path = builder(child_name, **values)
            else:
                child_path = values.pop('path', '/')
                if values:
                    raise ValueError('Mount URL building only accepts path')
            if not isinstance(child_path, str) or not child_path.startswith('/') or child_path.startswith('//'):
                raise ValueError('Mounted URLs must be application-relative absolute paths')
            return quote(mount.path, safe='/') + child_path
        route = self._url_routes.get(endpoint)
        if route is None:
            raise KeyError(f'Unknown endpoint: {endpoint}')
        return build_path(route, values)

    def register_module(self, module: Module[RequestStateT, SocketStateT], *, url_prefix: str = '') -> None:
        self._ensure_mutable()
        routes = self._collect_module_routes(
            module,
            url_prefix=url_prefix,
            parent_prefix='',
            parent_name='',
            chain=(),
            seen=set(),
        )
        endpoints: set[str] = set()
        for route in routes:
            if route.endpoint in self._endpoints or route.endpoint in endpoints:
                raise ValueError(f'Duplicate endpoint: {route.endpoint}')
            endpoints.add(route.endpoint)
        self._router.check_many(routes)
        websocket_routes = self._collect_module_websocket_routes(
            module,
            url_prefix=url_prefix,
            parent_prefix='',
            parent_name='',
            chain=(),
            seen=set(),
        )
        websocket_endpoints: set[str] = set()
        for websocket_route in websocket_routes:
            if websocket_route.endpoint in self._websocket_endpoints or websocket_route.endpoint in websocket_endpoints:
                raise ValueError(f'Duplicate WebSocket endpoint: {websocket_route.endpoint}')
            websocket_endpoints.add(websocket_route.endpoint)
        self._websocket_router.check_many(websocket_routes)

        for route in routes:
            self._router.add(route)
            self._routes.append(route)
            self._endpoints[route.endpoint] = route.handler
            self._url_routes[route.endpoint] = route
        for websocket_route in websocket_routes:
            self._websocket_router.add(websocket_route)
            self._websocket_routes.append(websocket_route)
            self._websocket_endpoints[websocket_route.endpoint] = websocket_route.handler
        module._freeze()

    def test_client(
        self, *, base_url: str = 'http://testserver', follow_redirects: bool = False,
        max_redirects: int = 20, lifespan_timeout: float = 10, root_path: str = '',
    ) -> 'TestClient[RequestStateT]':
        from ryuuseigun.testing import TestClient

        return TestClient(
            self, base_url=base_url, follow_redirects=follow_redirects,
            max_redirects=max_redirects, lifespan_timeout=lifespan_timeout, root_path=root_path,
        )

    def startup(self, handler: Callable[[], Awaitable[None]]) -> Callable[[], Awaitable[None]]:
        self._ensure_mutable()
        self._validate_lifecycle_handler(handler)
        self._startup_handlers.append(handler)
        return handler

    def shutdown(self, handler: Callable[[], Awaitable[None]]) -> Callable[[], Awaitable[None]]:
        self._ensure_mutable()
        self._validate_lifecycle_handler(handler)
        self._shutdown_handlers.append(handler)
        return handler

    def lifespan(
        self,
        factory: Callable[['Ryuuseigun[RequestStateT, AppStateT, SocketStateT]'], AbstractAsyncContextManager[None]],
    ) -> Callable[['Ryuuseigun[RequestStateT, AppStateT, SocketStateT]'], AbstractAsyncContextManager[None]]:
        self._ensure_mutable()
        self._lifespan_factories.append(factory)
        return factory

    def finalize(self) -> None:
        """Freeze registration and compile the request pipelines once."""
        if self._frozen:
            return
        for finalizer in self._finalizers:
            finalizer()
        described_routes: tuple[Route | WebSocketRoute, ...] = (*self._routes, *self._websocket_routes)
        for described_route in described_routes:
            protocol = 'http' if isinstance(described_route, Route) else 'websocket'
            ordered_middleware: list[Callable[..., Awaitable[Any]]] = list(
                self.middlewares if protocol == 'http' else self.websocket_middlewares
            )
            for module in described_route.module_chain:
                ordered_middleware.extend(module.middlewares if protocol == 'http' else module.websocket_middlewares)
            ordered_middleware.extend(described_route.middlewares)
            self._route_info[id(described_route)] = RouteInfo(
                described_route.path, described_route.endpoint,
                described_route.methods if isinstance(described_route, Route) else frozenset(),
                tuple(module.name for module in described_route.module_chain),
                tuple(getattr(item, '__qualname__', type(item).__qualname__) for item in ordered_middleware),
                protocol,
            )
        self._fallback_pipeline = self._compile_pipeline(None)
        pipelines: dict[_PipelineKey, Next[RequestStateT]] = {((), ()): self._fallback_pipeline}
        for route in self._routes:
            # Pipelines read the matched route from the request. Routes with the
            # same scopes and middleware can share their immutable compiled chain.
            scopes = tuple(id(module) for module in route.module_chain)
            middleware = tuple(id(handler) for handler in route.middlewares)
            for selected, target in ((middleware, self._pipelines), ((), self._method_pipelines)):
                key = (scopes, selected)
                pipeline = pipelines.get(key)
                if pipeline is None:
                    pipeline = self._compile_pipeline(route, include_route_middlewares=bool(selected))
                    pipelines[key] = pipeline
                target[id(route)] = pipeline

        for socket_route in self._websocket_routes:
            self._websocket_pipelines[id(socket_route)] = self._compile_websocket_pipeline(socket_route)
        self._router.freeze()
        self._websocket_router.freeze()
        self._frozen = True
        if self.debug:
            self.logger.debug(
                'Finalized %s: %d HTTP routes, %d WebSocket routes',
                self.import_name, len(self._routes), len(self._websocket_routes),
            )

    def _wrap_middleware(
        self, middleware: Callable[[Request[RequestStateT], Next[RequestStateT]], Awaitable[object]],
        next_call: Next[RequestStateT],
    ) -> Next[RequestStateT]:
        async def wrapped(request: Request[RequestStateT]) -> None:
            try:
                result = await middleware(request, next_call)
                if result is not None or not request._response_attempted:
                    raise TypeError('Middleware must await next(request) or request.respond(...), and return None')
            except BaseException as error:
                await self._respond_to_error(request, error)

        return wrapped

    def _compile_pipeline(self, route: Route | None, *, include_route_middlewares: bool = True) -> Next[RequestStateT]:
        call_next: Next[RequestStateT] = self._terminal
        if route is not None and include_route_middlewares:
            for middleware in reversed(route.middlewares):
                call_next = self._wrap_middleware(middleware, call_next)
        scopes: tuple[Registration[Any], ...] = (self, *(route.module_chain if route else ()))
        after_handlers: tuple[AfterHandler[Any], ...] = ()
        scope_afters = []
        for scope in scopes:
            after_handlers = (*reversed(scope.after_handlers), *after_handlers)
            scope_afters.append(after_handlers)

        pending_hooks: list[_HookScope] = []
        for scope, afters in reversed(tuple(zip(scopes, scope_afters, strict=True))):
            before = scope.before_handlers
            if before or scope.after_handlers:
                pending_hooks.append((before, afters))

            # Middleware is a resource and error boundary. Adjacent hook scopes
            # can share a runner while retaining each before hook's active after hooks.
            if scope.middlewares and pending_hooks:
                call_next = self._wrap_hooks(call_next, tuple(reversed(pending_hooks)))
                pending_hooks.clear()

            for middleware in reversed(scope.middlewares):
                call_next = self._wrap_middleware(middleware, call_next)

        if pending_hooks:
            call_next = self._wrap_hooks(call_next, tuple(reversed(pending_hooks)))

        return call_next

    def _wrap_hooks(
        self, next_call: Next[RequestStateT], scopes: tuple[_HookScope, ...],
    ) -> Next[RequestStateT]:
        async def wrapped(request: Request[RequestStateT]) -> None:
            try:
                for before, after in scopes:
                    request._after_handlers = after
                    for handler in before:
                        value = await handler(request)
                        if value is not None:
                            await self._respond(request, value)
                            return

                await next_call(request)
            except BaseException as error:
                await self._respond_to_error(request, error)

        return wrapped

    async def _terminal(self, request: Request[RequestStateT]) -> None:
        match, _ = request._route_context
        try:
            if match.route is not None:
                if request.method == HTTPMethod.QUERY:
                    self._validate_query_request(request, match.route)
                value = await match.route.invoke(request, request.path_params)
            elif self._config.automatic_options and request.method == HTTPMethod.OPTIONS and match.allowed_methods:
                value = Response(
                    status_code=StatusCode.NO_CONTENT,
                    headers={HeaderName.ALLOW: ', '.join(sorted(match.allowed_methods))},
                )
            elif match.allowed_methods:
                raise HTTPException(
                    StatusCode.METHOD_NOT_ALLOWED, headers={HeaderName.ALLOW: ', '.join(sorted(match.allowed_methods))},
                )
            else:
                value = self._slash_redirect(request, match)
                if value is None:
                    raise HTTPException(StatusCode.NOT_FOUND)
            await self._respond(request, value)
        except BaseException as error:
            await self._respond_to_error(request, error)

    async def _respond_to_error(self, request: Request[RequestStateT], error: BaseException) -> None:
        # Successful responses close in _respond. Earlier failures, including
        # cancellation, must also close before resource middleware unwinds.
        try:
            if request._response_attempted or not isinstance(error, Exception):
                raise error
            _, chain = request._route_context
            await request.respond(await self._handle_error(request, error, chain))
        finally:
            if not request._closed:
                await request.close()

    async def _respond(self, request: Request[RequestStateT], value: ResponseValue) -> None:
        if request._response_attempted:
            raise RuntimeError('A response has already been sent for this request')
        request._response_attempted = True
        match, chain = request._route_context if request._route_context is not None else (Match(None), ())
        try:
            try:
                response = self._make_response(value)
                for handler in request._after_handlers:
                    try:
                        value = await handler(request, response)
                        response = value if isinstance(value, Response) else self._make_response(value)
                    except Exception as error:
                        response = await self._handle_error(request, error, chain)
                if match.query_route is not None:
                    self._add_accept_query(response, match.query_route)
                if self._config.default_response_headers:
                    self._apply_default_response_headers(response)
                await response.send(
                    request._send_message, scope=request.scope, receive=request._receive,
                    request=request, head=request.method == HTTPMethod.HEAD,
                )
            except Exception as error:
                if request._response_started:
                    raise
                response = await self._handle_error(request, error, chain)
                if match.query_route is not None:
                    self._add_accept_query(response, match.query_route)
                if self._config.default_response_headers:
                    self._apply_default_response_headers(response)
                await response.send(
                    request._send_message, scope=request.scope, receive=request._receive,
                    request=request, head=request.method == HTTPMethod.HEAD,
                )
        finally:
            if not request._closed:
                await request.close()

    async def _handle_error(
        self,
        request: Request[RequestStateT],
        error: Exception,
        module_chain: Sequence[Module[Any]],
    ) -> Response:
        if any(error is handled for handled in request._handled_errors):
            raise error
        status_code = error.status_code if isinstance(error, HTTPException) else StatusCode.INTERNAL_SERVER_ERROR
        handler = self._find_error_handler(error, status_code, module_chain)
        if handler is not None:
            try:
                return self._make_response(await handler(request, error))
            except Exception as handler_error:
                error = handler_error
                status_code = (
                    handler_error.status_code
                    if isinstance(handler_error, HTTPException)
                    else StatusCode.INTERNAL_SERVER_ERROR
                )

        if self._config.propagate_exceptions and not isinstance(error, HTTPException):
            request._handled_errors.append(error)
            raise error
        if isinstance(error, HTTPException):
            return self._make_error_response(status_code, error.detail, error.headers)
        match, _ = request._route_context if request._route_context is not None else (Match(None), ())
        route = match.route or match.scope_route
        self.logger.error(
            'Unhandled HTTP exception: %s %s', request.method,
            route.endpoint if route is not None else '<unmatched>',
            exc_info=error,
            extra={'phase': 'http', 'route': route.path if self.debug and route is not None else None},
        )
        detail = f'{type(error).__name__}: {error}' if self._config.expose_error_details else None
        return self._make_error_response(StatusCode.INTERNAL_SERVER_ERROR, detail)

    def _find_error_handler(
        self,
        error: Exception,
        status_code: int,
        module_chain: Sequence[Module[Any]],
    ) -> Optional[Callable[..., Awaitable[ResponseValue]]]:
        registries: list[Mapping[int | type[Exception], Callable[..., Awaitable[ResponseValue]]]] = [
            module.error_handlers for module in reversed(module_chain)
        ]
        registries.append(self.error_handlers)
        for registry in registries:
            if isinstance(error, HTTPException):
                handler = registry.get(status_code)
                if handler is not None:
                    return handler
            for error_type in type(error).__mro__:
                handler = registry.get(error_type)
                if handler is not None:
                    return handler
            if not isinstance(error, HTTPException):
                handler = registry.get(status_code)
                if handler is not None:
                    return handler
        return None

    def _create_route(
        self,
        path: str,
        handler: RouteHandler[RequestStateT],
        *,
        methods: Iterable[str],
        endpoint: Optional[str],
        module_chain: tuple[Module[Any], ...],
        query_media_types: tuple[str, ...] = (),
        multipart: Optional[MultipartOverrides] = None,
        middlewares: tuple[MiddlewareCallable[Any], ...] = (),
    ) -> Route:
        normalized_path = normalize_path(path, strict_slashes=self._router.strict_slashes)
        _, param_names = parse_path(normalized_path)
        for middleware in middlewares:
            validate_middleware(middleware)
        selected_methods = normalize_methods(methods)
        route_name = endpoint or str(getattr(handler, '__name__', type(handler).__name__))
        try:
            invoke = build_route_invoker(handler, param_names)
        except (TypeError, ValueError) as error:
            raise type(error)(
                f'{", ".join(sorted(selected_methods))} {normalized_path} ({route_name}): {error}. '
                'Expected async def handler(req, ...path_parameters)'
            ) from error
        return Route(
            normalized_path,
            selected_methods,
            route_name,
            handler,
            invoke,
            param_names,
            middlewares,
            module_chain,
            normalize_query_media_types(query_media_types),
            resolve_multipart_limits(multipart, self._multipart_limits),
        )

    def _collect_module_routes(
        self,
        module: Module[RequestStateT, SocketStateT],
        *,
        url_prefix: str,
        parent_prefix: str,
        parent_name: str,
        chain: tuple[Module[Any], ...],
        seen: set[int],
    ) -> list[Route]:
        if id(module) in seen:
            raise ValueError('Cyclic module registration')
        branch_seen = {*seen, id(module)}
        prefix = join_paths(join_paths(parent_prefix, url_prefix), module.url_prefix)
        qualified_name = f'{parent_name}.{module.name}' if parent_name else module.name
        module_chain = (*chain, module)
        routes = [
            self._create_route(
                join_paths(prefix, definition.path),
                definition.handler,
                methods=definition.methods,
                endpoint=f'{qualified_name}.{definition.endpoint}',
                module_chain=module_chain,
                query_media_types=definition.accept_query,
                multipart=definition.multipart,
                middlewares=definition.middlewares,
            )
            for definition in module.routes
        ]
        for mounted in module.modules:
            routes.extend(
                self._collect_module_routes(
                    mounted.module,
                    url_prefix=mounted.url_prefix,
                    parent_prefix=prefix,
                    parent_name=qualified_name,
                    chain=module_chain,
                    seen=branch_seen,
                )
            )
        return routes

    def _make_response(self, value: ResponseValue) -> Response:
        if isinstance(value, Response):
            return value

        return make_response(
            value,
            pretty_json=self._config.pretty_json,
            json_options=self._config.json_options,
        )

    def _make_error_response(
        self,
        status_code: int,
        detail: Optional[str] = None,
        headers: Optional[Headers] = None,
    ) -> Response:
        return default_error_response(
            status_code,
            detail,
            headers,
            pretty_json=self._config.pretty_json,
            json_options=self._config.json_options,
            media_type=self._config.error_media_type,
        )

    def _apply_default_response_headers(self, response: Response) -> None:
        for name, value in self._config.default_response_headers:
            response.headers.setdefault(name, value)

    def _validate_host(self, headers: Headers) -> None:
        if not self._config.trusted_hosts:
            return
        hosts = headers.getlist(HeaderName.HOST)
        if len(hosts) != 1 or not _host_is_trusted(hosts[0], self._config.trusted_hosts):
            raise HTTPException(StatusCode.BAD_REQUEST, 'Invalid host header')

    def _slash_redirect(self, request: Request[Any], match: Match) -> Optional[Response]:
        if not self._config.redirect_slashes or request.path == '/' or match.route is not None or match.allowed_methods:
            return None
        redirect_path = request.path[:-1] if request.path.endswith('/') else f'{request.path}/'
        try:
            alternate = self._router.match(redirect_path, request.method)
        except ValueError:
            return None
        if alternate.route is None and not alternate.allowed_methods:
            return None
        query_string = self._scope_query_string(request.scope)
        location = prefixed_path(request.scope, _redirect_location(redirect_path, query_string))
        return Response(status_code=StatusCode.PERMANENT_REDIRECT, headers={HeaderName.LOCATION: location})

    @staticmethod
    def _scope_query_string(scope: ASGIScope) -> bytes:
        value = scope.get('query_string', b'')
        if not isinstance(value, bytes):
            raise RuntimeError('ASGI scope query_string must be bytes')
        return value

    @staticmethod
    def _validate_query_request(request: Request[Any], route: Route) -> None:
        content_type = request.content_type
        if content_type is None:
            raise HTTPException(StatusCode.BAD_REQUEST, 'QUERY requests require a Content-Type header')
        try:
            normalized_content_type = normalize_query_media_types((content_type,))[0]
        except (IndexError, ValueError) as error:
            raise HTTPException(StatusCode.BAD_REQUEST, 'QUERY requests require a valid Content-Type header') from error
        if '*' in normalized_content_type:
            raise HTTPException(StatusCode.BAD_REQUEST, 'QUERY requests require a concrete Content-Type header')
        if route.accept_query and not any(
            accepted == '*/*'
            or accepted == normalized_content_type
            or accepted.endswith('/*') and normalized_content_type.startswith(accepted[:-1])
            for accepted in route.accept_query
        ):
            raise HTTPException(StatusCode.UNSUPPORTED_MEDIA_TYPE, f'Unsupported QUERY media type: {normalized_content_type}')

    @staticmethod
    def _add_accept_query(response: Response, route: Optional[Route]) -> None:
        if route is not None and route.accept_query:
            response.headers.setdefault(HeaderName.ACCEPT_QUERY, ', '.join(f'"{value}"' for value in route.accept_query))

    async def _dispatch_websocket(self, scope: ASGIScope, receive: Receive, send: Send) -> None:
        try:
            query_string = self._scope_query_string(scope)
            if query_string:
                validate_query_string(
                    query_string,
                    self._config.max_query_string_size,
                    self._config.max_query_parameters,
                )
        except HTTPException as error:
            rejection_scope = {**scope, 'query_string': b''}
            socket = self._create_websocket(rejection_scope, receive, send)
            await socket.reject(
                error.status_code,
                error.detail,
                {HeaderName.CONTENT_TYPE: MediaType.TEXT_UTF8},
            )
            return

        socket = self._create_websocket(scope, receive, send)
        try:
            if self._config.trusted_hosts:
                self._validate_host(socket.headers)
        except HTTPException as error:
            await socket.reject(
                error.status_code,
                error.detail,
                {HeaderName.CONTENT_TYPE: MediaType.TEXT_UTF8},
            )
            return
        try:
            route, params = self._websocket_router.match(socket.path)
        except ValueError:
            route, params = None, {}
        if route is None:
            redirect = self._websocket_slash_redirect(socket)
            if redirect is not None:
                await socket.reject(
                    StatusCode.PERMANENT_REDIRECT,
                    headers={HeaderName.LOCATION: redirect},
                )
                return
            await socket.reject(
                StatusCode.NOT_FOUND,
                'WebSocket route not found',
                {HeaderName.CONTENT_TYPE: MediaType.TEXT_UTF8},
            )
            return

        socket.path_params = params
        socket._route_info = self._route_info[id(route)]
        scope['route'] = socket.route
        scope['endpoint'] = route.handler

        try:
            await self._websocket_pipelines[id(route)](socket)
            if self._config.websocket_auto_close and not socket.closed:
                await socket.close()
        except WebSocketDisconnect:
            if not socket.closed:
                await socket.close()
            return
        except Exception as error:
            if not socket.closed:
                if socket.accepted:
                    await socket.close(WebSocketCloseCode.INTERNAL_ERROR, 'Internal server error')
                else:
                    await socket.reject(
                        StatusCode.INTERNAL_SERVER_ERROR,
                        'Internal server error',
                        {HeaderName.CONTENT_TYPE: MediaType.TEXT_UTF8},
                    )
            if self._config.propagate_exceptions:
                raise
            self.logger.error(
                'Unhandled WebSocket exception: %s', route.endpoint, exc_info=error,
                extra={'phase': 'websocket', 'route': route.path if self.debug else None},
            )

    def _compile_websocket_pipeline(self, route: WebSocketRoute) -> WebSocketNext[SocketStateT]:
        async def endpoint(current: WebSocket[SocketStateT]) -> None:
            await route.invoke(current, current.path_params)
            if self._config.websocket_auto_close and not current.closed:
                await current.close()

        call_next: WebSocketNext[SocketStateT] = endpoint
        middlewares: list[WebSocketMiddleware[SocketStateT]] = [*self.websocket_middlewares]
        for module in route.module_chain:
            middlewares.extend(module.websocket_middlewares)
        middlewares.extend(route.middlewares)
        for middleware in reversed(middlewares):
            previous = call_next

            def wrapped(
                current: WebSocket[SocketStateT],
                active: WebSocketMiddleware[SocketStateT] = middleware,
                next_call: WebSocketNext[SocketStateT] = previous,
            ) -> Awaitable[None]:
                return active(current, next_call)

            call_next = wrapped

        return call_next

    def _create_websocket(self, scope: ASGIScope, receive: Receive, send: Send) -> WebSocket[SocketStateT]:
        return WebSocket(
            scope,
            receive,
            send,
            state=self._websocket_state_factory(),
            json_options=self._config.json_options,
            default_response_headers=self._config.default_response_headers,
        )

    def _websocket_slash_redirect(self, socket: WebSocket[Any]) -> Optional[str]:
        if not self._config.redirect_slashes or socket.path == '/':
            return None
        redirect_path = socket.path[:-1] if socket.path.endswith('/') else f'{socket.path}/'
        try:
            route, _ = self._websocket_router.match(redirect_path)
        except ValueError:
            return None
        if route is None:
            return None
        query_string = self._scope_query_string(socket.scope)
        return prefixed_path(socket.scope, _redirect_location(redirect_path, query_string))

    def _create_websocket_route(
        self,
        path: str,
        handler: WebSocketHandler[SocketStateT],
        *,
        endpoint: Optional[str],
        module_chain: tuple[Module[Any], ...],
        middlewares: tuple[WebSocketMiddleware[SocketStateT], ...] = (),
    ) -> WebSocketRoute:
        normalized_path = normalize_path(path, strict_slashes=self._websocket_router.strict_slashes)
        segments, param_names = parse_path(normalized_path)
        for middleware in middlewares:
            validate_websocket_middleware(middleware)
        route_name = endpoint or str(getattr(handler, '__name__', type(handler).__name__))
        try:
            invoke = build_websocket_invoker(handler, param_names)
        except (TypeError, ValueError) as error:
            raise type(error)(
                f'WebSocket {normalized_path} ({route_name}): {error}. '
                'Expected async def handler(socket, ...path_parameters)'
            ) from error
        return WebSocketRoute(
            normalized_path,
            route_name,
            handler,
            invoke,
            tuple(segments),
            param_names,
            middlewares,
            module_chain,
        )

    def _collect_module_websocket_routes(
        self,
        module: Module[RequestStateT, SocketStateT],
        *,
        url_prefix: str,
        parent_prefix: str,
        parent_name: str,
        chain: tuple[Module[Any], ...],
        seen: set[int],
    ) -> list[WebSocketRoute]:
        if id(module) in seen:
            raise ValueError('Cyclic module registration')
        branch_seen = {*seen, id(module)}
        prefix = join_paths(join_paths(parent_prefix, url_prefix), module.url_prefix)
        qualified_name = f'{parent_name}.{module.name}' if parent_name else module.name
        module_chain = (*chain, module)
        routes = [
            self._create_websocket_route(
                join_paths(prefix, definition.path),
                definition.handler,
                endpoint=f'{qualified_name}.{definition.endpoint}',
                module_chain=module_chain,
                middlewares=definition.middlewares,
            )
            for definition in module.websocket_routes
        ]
        for mounted in module.modules:
            routes.extend(
                self._collect_module_websocket_routes(
                    mounted.module,
                    url_prefix=mounted.url_prefix,
                    parent_prefix=prefix,
                    parent_name=qualified_name,
                    chain=module_chain,
                    seen=branch_seen,
                )
            )
        return routes

    @staticmethod
    def _validate_lifecycle_handler(handler: Callable[[], Awaitable[None]]) -> None:
        if not iscoroutinefunction(handler):
            raise TypeError('Lifecycle handlers must be async')

    async def _lifespan(self, receive: Receive, send: Send) -> None:
        async with AsyncExitStack() as stack:
            while True:
                message = await receive()
                if message['type'] == ASGIMessageType.LIFESPAN_STARTUP:
                    try:
                        for factory in self._lifespan_factories:
                            await stack.enter_async_context(factory(self))
                        for mount in self._mounts:
                            if mount.lifespan and id(mount.app) not in self._mount_lifespans:
                                session = LifespanSession(mount.app, mount.lifespan_timeout)
                                await session.start()
                                self._mount_lifespans[id(mount.app)] = session
                                stack.callback(self._mount_lifespans.pop, id(mount.app), None)
                                stack.push_async_callback(session.stop)
                        for handler in self._startup_handlers:
                            await handler()
                    except Exception as error:
                        self.logger.error('Application startup failed', exc_info=error, extra={'phase': 'startup'})
                        failure = str(error)
                        try:
                            await stack.aclose()
                        except Exception as cleanup_error:
                            self.logger.error(
                                'Application startup rollback failed', exc_info=cleanup_error, extra={'phase': 'rollback'},
                            )
                            failures = ExceptionGroup('Startup and rollback failed', [error, cleanup_error])
                            failure = ''.join(format_exception_only(failures, show_group=True))
                        await send({'type': ASGIMessageType.LIFESPAN_STARTUP_FAILED, 'message': failure})
                        return
                    await send({'type': ASGIMessageType.LIFESPAN_STARTUP_COMPLETE})
                elif message['type'] == ASGIMessageType.LIFESPAN_SHUTDOWN:
                    try:
                        try:
                            for handler in reversed(self._shutdown_handlers):
                                await handler()
                        finally:
                            await stack.aclose()
                    except Exception as error:
                        self.logger.error('Application shutdown failed', exc_info=error, extra={'phase': 'shutdown'})
                        await send({'type': ASGIMessageType.LIFESPAN_SHUTDOWN_FAILED, 'message': str(error)})
                        return
                    await send({'type': ASGIMessageType.LIFESPAN_SHUTDOWN_COMPLETE})
                    return
