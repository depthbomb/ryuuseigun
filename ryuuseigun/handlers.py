from typing import Any, Protocol
from ryuuseigun.request import Request
from collections.abc import Callable, Awaitable
from ryuuseigun.response import Response, ResponseValue
from inspect import Parameter, signature, iscoroutinefunction

type BeforeHandler[StateT = Any] = Callable[[Request[StateT]], Awaitable[ResponseValue]]
type AfterHandler[StateT = Any] = Callable[[Request[StateT], Response], Awaitable[ResponseValue]]
type ErrorHandlerFor[ErrorT: Exception, StateT = Any] = Callable[[Request[StateT], ErrorT], Awaitable[ResponseValue]]
type ErrorHandler[StateT = Any] = ErrorHandlerFor[Exception, StateT]

class ErrorDecorator[ErrorT: Exception, StateT](Protocol):
    def __call__[Result: ResponseValue](
        self, handler: Callable[[Request[StateT], ErrorT], Awaitable[Result]], /,
    ) -> Callable[[Request[StateT], ErrorT], Awaitable[Result]]: ...

def validate_error_handler_key(key: int | type[Exception]) -> None:
    if isinstance(key, bool):
        raise TypeError('Error handler keys must be status codes or exception types')
    if isinstance(key, int):
        if not 100 <= key <= 599:
            raise ValueError('Error handler status codes must be between 100 and 599')
        return
    if not isinstance(key, type) or not issubclass(key, Exception):
        raise TypeError('Error handler keys must be status codes or exception types')

def require_async(handler: Callable[..., Any], label: str) -> None:
    if not iscoroutinefunction(handler) and not iscoroutinefunction(type(handler).__call__):
        name = getattr(handler, '__name__', type(handler).__name__)
        raise TypeError(f'{label} {name!r} must be async')

def validate_handler(handler: Callable[..., Any], label: str, arity: int) -> None:
    require_async(handler, label)
    parameters = tuple(signature(handler).parameters.values())
    positional = {Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD}
    if len(parameters) < arity or any(parameter.kind not in positional for parameter in parameters[:arity]):
        name = getattr(handler, '__qualname__', type(handler).__name__)
        raise TypeError(f'{label} {name!r} must accept {arity} explicit positional arguments')
    try:
        signature(handler).bind(*([object()] * arity))
    except TypeError as error:
        name = getattr(handler, '__qualname__', type(handler).__name__)
        raise TypeError(f'{label} {name!r} must accept {arity} positional arguments') from error

def validate_middleware(handler: Callable[..., Any], label: str = 'Middleware') -> None:
    validate_handler(handler, label, 2)
