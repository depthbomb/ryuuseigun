from uuid import UUID
from re import fullmatch
from urllib.parse import quote
from ryuuseigun.request import Request
from dataclasses import field, dataclass
from inspect import Parameter, signature
from ryuuseigun.constants import HTTPMethod
from ryuuseigun.config import MultipartLimits
from ryuuseigun.handlers import require_async
from ryuuseigun.response import ResponseValue
from ryuuseigun.middleware import MiddlewareCallable
from collections.abc import Callable, Iterable, Sequence, Awaitable
from typing import Any, Optional, Protocol, TypeAlias, Concatenate, TYPE_CHECKING

if TYPE_CHECKING:
    from ryuuseigun.module import Module

type RouteHandler[StateT = Any] = Callable[Concatenate[Request[StateT], ...], Awaitable[ResponseValue]]
RouteInvoker: TypeAlias = Callable[[Request[Any], dict[str, Any]], Awaitable[ResponseValue]]

class RouteDecorator[StateT](Protocol):
    def __call__[**Parameters, Result: ResponseValue](
        self, handler: Callable[Concatenate[Request[StateT], Parameters], Awaitable[Result]], /,
    ) -> Callable[Concatenate[Request[StateT], Parameters], Awaitable[Result]]: ...

RouteKey: TypeAlias = tuple[tuple[tuple[bool, str], ...], str]


@dataclass(slots=True, frozen=True)
class Converter:
    name: str
    convert: Callable[[str], Any]


@dataclass(slots=True, frozen=True)
class Route:
    path: str
    methods: frozenset[str]
    endpoint: str
    handler: RouteHandler
    invoke: RouteInvoker
    param_names: tuple[str, ...]
    middlewares: tuple[MiddlewareCallable[Any], ...]
    module_chain: tuple['Module[Any]', ...] = ()
    accept_query: tuple[str, ...] = ()
    multipart_limits: MultipartLimits = MultipartLimits()


@dataclass(slots=True)
class Match:
    route: Optional[Route]
    params: dict[str, Any] = field(default_factory=dict)
    allowed_methods: frozenset[str] = frozenset()
    scope_route: Optional[Route] = None
    query_route: Optional[Route] = None


class _Node:
    __slots__ = ('dynamic', 'dynamic_edges', 'path', 'routes', 'static')

    def __init__(self) -> None:
        self.static: dict[str, _Node] = {}
        self.dynamic: dict[str, tuple[Converter, _Node]] = {}
        self.dynamic_edges: tuple[tuple[Converter, _Node], ...] = ()
        self.path: Optional[tuple[Converter, _Node]] = None
        self.routes: dict[str, Route] = {}


def _to_string(value: str) -> str:
    if not value:
        raise ValueError
    return value


def _to_int(value: str) -> int:
    if not value.isdecimal():
        raise ValueError
    return int(value)


def _to_float(value: str) -> float:
    if fullmatch(r'(?:\d+(?:\.\d*)?|\.\d+)', value) is None:
        raise ValueError
    return float(value)


def _to_path(value: str) -> str:
    if not value:
        raise ValueError
    return value


_CONVERTERS = {
    'string': Converter('string', _to_string),
    'int': Converter('int', _to_int),
    'float': Converter('float', _to_float),
    'uuid': Converter('uuid', UUID),
    'path': Converter('path', _to_path),
}
_CONVERTER_ORDER = ('int', 'float', 'uuid', 'string')


def normalize_path(path: str, *, strict_slashes: bool) -> str:
    if not path.startswith('/'):
        raise ValueError("Route paths must start with '/'")
    if '//' in path:
        raise ValueError('Route paths cannot contain empty segments')
    if not strict_slashes and path != '/':
        return path.rstrip('/')
    return path


def join_paths(prefix: str, path: str) -> str:
    if not prefix:
        return path or '/'
    if not prefix.startswith('/'):
        raise ValueError("URL prefixes must start with '/'")
    if not path:
        return prefix.rstrip('/') or '/'
    return f'{prefix.rstrip("/")}/{path.lstrip("/")}'


def parse_path(path: str) -> tuple[list[str | Converter], tuple[str, ...]]:
    if path == '/':
        return [], ()

    path_segments = path[1:].split('/')
    segments: list[str | Converter] = []
    names: list[str] = []
    for index, segment in enumerate(path_segments):
        if not (segment.startswith('<') and segment.endswith('>')):
            if '<' in segment or '>' in segment:
                raise ValueError(f'Invalid route segment: {segment}')
            segments.append(segment)
            continue

        declaration = segment[1:-1]
        converter_name, separator, name = declaration.partition(':')
        if not separator:
            name = converter_name
            converter_name = 'string'
        if not name.isidentifier() or name in names:
            raise ValueError(f'Invalid or duplicate route parameter: {name}')
        if converter_name not in _CONVERTERS:
            raise ValueError(f'Unknown route converter: {converter_name}')
        if converter_name == 'path' and index != len(path_segments) - 1:
            raise ValueError('The path converter must be the final route segment')

        segments.append(_CONVERTERS[converter_name])
        names.append(name)
    return segments, tuple(names)


def build_path(route: Route, values: dict[str, Any]) -> str:
    missing = [name for name in route.param_names if name not in values]
    unexpected = [name for name in values if name not in route.param_names]
    if missing:
        raise ValueError(f'Missing route parameters for {route.endpoint!r}: {", ".join(missing)}')
    if unexpected:
        raise ValueError(f'Unexpected route parameters for {route.endpoint!r}: {", ".join(unexpected)}')

    segments, _ = parse_path(route.path)
    built: list[str] = []
    parameter_index = 0
    safe = "!$&'()*+,-.:;=@_~"
    for segment in segments:
        if isinstance(segment, str):
            built.append(quote(segment, safe=safe))
            continue

        name = route.param_names[parameter_index]
        parameter_index += 1
        value = str(values[name])
        try:
            segment.convert(value)
        except (TypeError, ValueError) as error:
            raise ValueError(f'Invalid value for route parameter {name!r}: {values[name]!r}') from error
        if segment.name != 'path' and '/' in value:
            raise ValueError(f'Route parameter {name!r} cannot contain a slash')
        if segment.name == 'path' and (value.startswith('/') or value.endswith('/') or '//' in value):
            raise ValueError(f'Path route parameter {name!r} must contain non-empty path segments')
        built.append(quote(value, safe=f'{safe}/' if segment.name == 'path' else safe))
    return '/' + '/'.join(built)


def build_route_invoker(handler: RouteHandler, param_names: Sequence[str]) -> RouteInvoker:
    require_async(handler, 'Route handler')
    handler_signature = signature(handler)
    parameters = tuple(handler_signature.parameters.values())
    if not parameters or parameters[0].kind not in {Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD}:
        raise TypeError('Route handlers must accept a request as their first positional argument')
    marker = object()
    handler_signature.bind(marker, **dict.fromkeys(param_names, marker))

    def invoke(request: Request[Any], params: dict[str, Any]) -> Awaitable[ResponseValue]:
        if params:
            return handler(request, **params)

        return handler(request)

    return invoke


class Router:
    __slots__ = (
        '_root', '_route_keys', '_static_paths', '_frozen', '_automatic_head', '_automatic_options', '_strict_slashes',
    )

    def __init__(
        self,
        *,
        strict_slashes: bool = False,
        automatic_head: bool = True,
        automatic_options: bool = True,
    ) -> None:
        self._frozen = False
        self._root = _Node()
        self._route_keys: set[RouteKey] = set()
        self._static_paths: dict[str, _Node] = {}
        self._strict_slashes = strict_slashes
        self._automatic_head = automatic_head
        self._automatic_options = automatic_options

    @property
    def strict_slashes(self) -> bool:
        return self._strict_slashes

    @property
    def automatic_head(self) -> bool:
        return self._automatic_head

    @property
    def automatic_options(self) -> bool:
        return self._automatic_options

    def freeze(self) -> None:
        self._frozen = True

    def add(self, route: Route) -> None:
        if self._frozen:
            raise RuntimeError('Router has been finalized')
        segments, _ = parse_path(route.path)
        keys = self._keys(route, segments)
        conflict = next((key for key in keys if key in self._route_keys), None)
        if conflict is not None:
            raise ValueError(f'Duplicate route for {conflict[1]} {route.path}')

        node = self._root
        for segment in segments:
            if isinstance(segment, str):
                node = node.static.setdefault(segment, _Node())
            elif segment.name == 'path':
                if node.path is None:
                    node.path = (segment, _Node())
                node = node.path[1]
            else:
                if segment.name not in node.dynamic:
                    node.dynamic[segment.name] = (segment, _Node())
                    node.dynamic_edges = tuple(node.dynamic[name] for name in _CONVERTER_ORDER if name in node.dynamic)
                node = node.dynamic[segment.name][1]

        for method in route.methods:
            node.routes[method] = route
        if all(isinstance(segment, str) for segment in segments):
            self._static_paths[route.path] = node
        self._route_keys.update(keys)

    def check_many(self, routes: Iterable[Route]) -> None:
        pending: set[RouteKey] = set()
        for route in routes:
            segments, _ = parse_path(route.path)
            for key in self._keys(route, segments):
                if key in self._route_keys or key in pending:
                    raise ValueError(f'Duplicate route for {key[1]} {route.path}')
                pending.add(key)

    def match(self, path: str, method: str) -> Match:
        path = normalize_path(path, strict_slashes=self._strict_slashes)
        static_node = self._static_paths.get(path)
        if static_node is not None:
            route = static_node.routes.get(method)
            if route is None and self._automatic_head and method == HTTPMethod.HEAD:
                route = static_node.routes.get(HTTPMethod.GET)
            if route is not None:
                return Match(route, query_route=static_node.routes.get(HTTPMethod.QUERY))

        segments = [] if path == '/' else path[1:].split('/')
        node = self._root
        values: list[Any] = []
        index = 0
        while index < len(segments):
            segment = segments[index]
            static_child = node.static.get(segment)
            if static_child is not None:
                node = static_child
                index += 1
                continue

            matched_dynamic = False
            for converter, child in node.dynamic_edges:
                try:
                    values.append(converter.convert(segment))
                    node = child
                    index += 1
                    matched_dynamic = True
                    break
                except (ValueError, TypeError):
                    continue
            if matched_dynamic:
                continue

            if node.path is not None:
                converter, child = node.path
                try:
                    values.append(converter.convert('/'.join(segments[index:])))
                    node = child
                    index = len(segments)
                    continue
                except ValueError:
                    pass
            break

        if index == len(segments):
            route = node.routes.get(method)
            if route is None and self._automatic_head and method == HTTPMethod.HEAD:
                route = node.routes.get(HTTPMethod.GET)
            if route is not None:
                return Match(
                    route,
                    dict(zip(route.param_names, values, strict=True)),
                    query_route=node.routes.get(HTTPMethod.QUERY),
                )

        candidates: list[tuple[_Node, int, list[Any]]] = [(self._root, 0, [])]
        completed: list[tuple[_Node, list[Any]]] = []

        while candidates:
            node, index, values = candidates.pop()
            if index == len(segments):
                completed.append((node, values))
                continue

            segment = segments[index]
            if node.path is not None:
                converter, child = node.path
                try:
                    converted = converter.convert('/'.join(segments[index:]))
                    candidates.append((child, len(segments), [*values, converted]))
                except ValueError:
                    pass

            for converter, child in reversed(node.dynamic_edges):
                try:
                    converted = converter.convert(segment)
                    candidates.append((child, index + 1, [*values, converted]))
                except (ValueError, TypeError):
                    pass

            static_child = node.static.get(segment)
            if static_child is not None:
                candidates.append((static_child, index + 1, values))

        allowed: set[str] = set()
        scope_route: Optional[Route] = None
        scope_params: dict[str, Any] = {}
        for node, values in completed:
            allowed.update(node.routes)
            if scope_route is None and node.routes:
                scope_route = next(iter(node.routes.values()))
                scope_params = dict(zip(scope_route.param_names, values, strict=True))
            route = node.routes.get(method)
            if route is None and self._automatic_head and method == HTTPMethod.HEAD:
                route = node.routes.get(HTTPMethod.GET)
            if route is not None:
                return Match(
                    route,
                    dict(zip(route.param_names, values, strict=True)),
                    query_route=node.routes.get(HTTPMethod.QUERY),
                )

        if self._automatic_head and HTTPMethod.GET in allowed:
            allowed.add(HTTPMethod.HEAD)
        if self._automatic_options and allowed:
            allowed.add(HTTPMethod.OPTIONS)
        query_route = next(
            (node.routes[HTTPMethod.QUERY] for node, _ in completed if HTTPMethod.QUERY in node.routes),
            None,
        )
        return Match(
            None,
            params=scope_params,
            allowed_methods=frozenset(allowed),
            scope_route=scope_route,
            query_route=query_route,
        )

    @staticmethod
    def _keys(route: Route, segments: Sequence[str | Converter]) -> set[RouteKey]:
        pattern = tuple((isinstance(segment, str), segment if isinstance(segment, str) else segment.name) for segment in segments)
        return {(pattern, method) for method in route.methods}


def normalize_methods(methods: Iterable[str]) -> frozenset[str]:
    normalized = frozenset(method.upper() for method in methods)
    if not normalized:
        raise ValueError('At least one HTTP method is required')
    return normalized


def normalize_query_media_types(media_types: Iterable[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    token = r"[!#$%&'*+.^_`|~0-9A-Za-z-]+"
    for media_type in media_types:
        value = media_type.strip().lower()
        invalid_wildcard = value.startswith('*/') and value != '*/*'
        if value != '*/*' and (invalid_wildcard or fullmatch(rf'{token}/(?:{token}|\*)', value) is None):
            raise ValueError(f'Invalid QUERY media type: {media_type}')
        if value not in normalized:
            normalized.append(value)
    return tuple(normalized)
