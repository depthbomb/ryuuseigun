from types import MappingProxyType
from ryuuseigun.request import Request
from ryuuseigun.constants import HTTPMethod
from typing import Any, overload, Concatenate
from ryuuseigun.config import MultipartOverrides
from ryuuseigun.middleware import MiddlewareCallable
from ryuuseigun.response import Response, ResponseValue
from collections.abc import Mapping, Callable, Iterable, Awaitable
from ryuuseigun.routing import RouteHandler, RouteDecorator, normalize_query_media_types
from ryuuseigun.websocket import (
    WebSocket,
    WebSocketHandler,
    WebSocketDecorator,
    WebSocketMiddleware,
    validate_websocket_middleware,
)
from ryuuseigun.handlers import (
    AfterHandler,
    ErrorHandler,
    BeforeHandler,
    ErrorDecorator,
    ErrorHandlerFor,
    validate_handler,
    validate_middleware,
    validate_error_handler_key,
)

class Registration[StateT, SocketStateT = Any]:
    def __init__(self) -> None:
        self._frozen = False
        self._middlewares: list[MiddlewareCallable[StateT]] = []
        self._websocket_middlewares: list[WebSocketMiddleware[SocketStateT]] = []
        self._before_handlers: list[BeforeHandler[StateT]] = []
        self._after_handlers: list[AfterHandler[StateT]] = []
        self._error_handlers: dict[int | type[Exception], Callable[..., Awaitable[ResponseValue]]] = {}

    @property
    def middlewares(self) -> tuple[MiddlewareCallable[StateT], ...]:
        return tuple(self._middlewares)

    @property
    def websocket_middlewares(self) -> tuple[WebSocketMiddleware[SocketStateT], ...]:
        return tuple(self._websocket_middlewares)

    @property
    def before_handlers(self) -> tuple[BeforeHandler[StateT], ...]:
        return tuple(self._before_handlers)

    @property
    def after_handlers(self) -> tuple[AfterHandler[StateT], ...]:
        return tuple(self._after_handlers)

    @property
    def error_handlers(self) -> Mapping[int | type[Exception], Callable[..., Awaitable[ResponseValue]]]:
        return MappingProxyType(self._error_handlers)

    def route(
        self, path: str, *, methods: Iterable[str] = (HTTPMethod.GET,),
        endpoint: str | None = None, name: str | None = None,
        multipart: MultipartOverrides | None = None,
        middlewares: Iterable[MiddlewareCallable[StateT]] = (),
    ) -> RouteDecorator[StateT]:
        selected_methods = tuple(methods)
        selected_middlewares = tuple(middlewares)

        def decorator[**Parameters, Result: ResponseValue](
            handler: Callable[Concatenate[Request[StateT], Parameters], Awaitable[Result]],
        ) -> Callable[Concatenate[Request[StateT], Parameters], Awaitable[Result]]:
            self.add_url_rule(
                path, handler, methods=selected_methods, endpoint=endpoint, name=name,
                multipart=multipart, middlewares=selected_middlewares,
            )
            return handler

        return decorator

    def get(
        self, path: str, *, endpoint: str | None = None, name: str | None = None,
        multipart: MultipartOverrides | None = None, middlewares: Iterable[MiddlewareCallable[StateT]] = (),
    ) -> RouteDecorator[StateT]:
        return self.route(
            path, methods=(HTTPMethod.GET,), endpoint=endpoint, name=name,
            multipart=multipart, middlewares=middlewares,
        )

    def post(
        self, path: str, *, endpoint: str | None = None, name: str | None = None,
        multipart: MultipartOverrides | None = None, middlewares: Iterable[MiddlewareCallable[StateT]] = (),
    ) -> RouteDecorator[StateT]:
        return self.route(
            path, methods=(HTTPMethod.POST,), endpoint=endpoint, name=name,
            multipart=multipart, middlewares=middlewares,
        )

    def put(
        self, path: str, *, endpoint: str | None = None, name: str | None = None,
        multipart: MultipartOverrides | None = None, middlewares: Iterable[MiddlewareCallable[StateT]] = (),
    ) -> RouteDecorator[StateT]:
        return self.route(
            path, methods=(HTTPMethod.PUT,), endpoint=endpoint, name=name,
            multipart=multipart, middlewares=middlewares,
        )

    def patch(
        self, path: str, *, endpoint: str | None = None, name: str | None = None,
        multipart: MultipartOverrides | None = None, middlewares: Iterable[MiddlewareCallable[StateT]] = (),
    ) -> RouteDecorator[StateT]:
        return self.route(
            path, methods=(HTTPMethod.PATCH,), endpoint=endpoint, name=name,
            multipart=multipart, middlewares=middlewares,
        )

    def delete(
        self, path: str, *, endpoint: str | None = None, name: str | None = None,
        multipart: MultipartOverrides | None = None, middlewares: Iterable[MiddlewareCallable[StateT]] = (),
    ) -> RouteDecorator[StateT]:
        return self.route(
            path, methods=(HTTPMethod.DELETE,), endpoint=endpoint, name=name,
            multipart=multipart, middlewares=middlewares,
        )

    def query(
        self, path: str, *, accepts: Iterable[str] = (), endpoint: str | None = None, name: str | None = None,
        multipart: MultipartOverrides | None = None, middlewares: Iterable[MiddlewareCallable[StateT]] = (),
    ) -> RouteDecorator[StateT]:
        accepted_types = normalize_query_media_types(accepts)
        selected_middlewares = tuple(middlewares)

        def decorator[**Parameters, Result: ResponseValue](
            handler: Callable[Concatenate[Request[StateT], Parameters], Awaitable[Result]],
        ) -> Callable[Concatenate[Request[StateT], Parameters], Awaitable[Result]]:
            self.add_url_rule(
                path, handler, methods=(HTTPMethod.QUERY,), endpoint=endpoint, name=name,
                query_media_types=accepted_types, multipart=multipart, middlewares=selected_middlewares,
            )
            return handler

        return decorator

    def websocket(
        self, path: str, *, endpoint: str | None = None, middlewares: Iterable[WebSocketMiddleware[SocketStateT]] = (),
    ) -> WebSocketDecorator[SocketStateT]:
        selected_middlewares = tuple(middlewares)

        def decorator[**Parameters](
            handler: Callable[Concatenate[WebSocket[SocketStateT], Parameters], Awaitable[None]],
        ) -> Callable[Concatenate[WebSocket[SocketStateT], Parameters], Awaitable[None]]:
            self.add_websocket_rule(path, handler, endpoint=endpoint, middlewares=selected_middlewares)
            return handler

        return decorator

    def add_url_rule(
        self, path: str, handler: RouteHandler[StateT], *, methods: Iterable[str] = (HTTPMethod.GET,),
        endpoint: str | None = None, name: str | None = None, query_media_types: tuple[str, ...] = (),
        multipart: MultipartOverrides | None = None, middlewares: Iterable[MiddlewareCallable[StateT]] = (),
    ) -> None:
        raise NotImplementedError

    def add_websocket_rule(
        self, path: str, handler: WebSocketHandler[SocketStateT], *, endpoint: str | None = None,
        middlewares: Iterable[WebSocketMiddleware[SocketStateT]] = (),
    ) -> None:
        raise NotImplementedError

    def middleware(self, handler: MiddlewareCallable[StateT]) -> MiddlewareCallable[StateT]:
        self._ensure_mutable()
        validate_middleware(handler)
        self._middlewares.append(handler)
        return handler

    def websocket_middleware(self, handler: WebSocketMiddleware[SocketStateT]) -> WebSocketMiddleware[SocketStateT]:
        self._ensure_mutable()
        validate_websocket_middleware(handler)
        self._websocket_middlewares.append(handler)
        return handler

    def before_request[Result: ResponseValue](
        self, handler: Callable[[Request[StateT]], Awaitable[Result]],
    ) -> Callable[[Request[StateT]], Awaitable[Result]]:
        self._ensure_mutable()
        validate_handler(handler, 'Before-request handler', 1)
        self._before_handlers.append(handler)
        return handler

    def after_request[Result: ResponseValue](
        self, handler: Callable[[Request[StateT], Response], Awaitable[Result]],
    ) -> Callable[[Request[StateT], Response], Awaitable[Result]]:
        self._ensure_mutable()
        validate_handler(handler, 'After-request handler', 2)
        self._after_handlers.append(handler)
        return handler

    @overload
    def errorhandler(self, key: int) -> ErrorDecorator[Exception, StateT]: ...

    @overload
    def errorhandler[ErrorT: Exception](self, key: type[ErrorT]) -> ErrorDecorator[ErrorT, StateT]: ...

    def errorhandler(self, key: int | type[Exception]) -> Any:
        def decorator(handler: Callable[..., Awaitable[ResponseValue]]) -> Callable[..., Awaitable[ResponseValue]]:
            self.register_error_handler(key, handler)
            return handler

        return decorator

    @overload
    def register_error_handler(self, key: int, handler: ErrorHandler[StateT]) -> None: ...

    @overload
    def register_error_handler[ErrorT: Exception](
        self, key: type[ErrorT], handler: ErrorHandlerFor[ErrorT, StateT],
    ) -> None: ...

    def register_error_handler(
        self, key: int | type[Exception], handler: Callable[..., Awaitable[ResponseValue]],
    ) -> None:
        self._ensure_mutable()
        validate_error_handler_key(key)
        validate_handler(handler, 'Error handler', 2)
        self._error_handlers[key] = handler

    def _ensure_mutable(self) -> None:
        if self._frozen:
            raise RuntimeError('Configuration has been finalized and cannot be changed')
