"""Read-only routing information for application middleware and inspection tools."""
from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class RouteInfo:
    """The selected route, without exposing the dispatch pipeline's internals.

    path is the application's route template, before an external mount prefix.
    modules lists outer-to-inner module names. Automatic HTTP method responses
    expose the route whose method rules were consulted; unmatched paths use None.
    middleware lists callable names in entry order for debugging, not invocation.
    """
    path: str
    endpoint: str
    methods: frozenset[str]
    modules: tuple[str, ...] = ()
    middleware: tuple[str, ...] = ()
    protocol: str = 'http'
