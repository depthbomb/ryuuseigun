"""ASGI paths stay absolute in scope; routers match paths relative to root_path."""
from urllib.parse import quote
from ryuuseigun.types import ASGIScope

def root_path(scope: ASGIScope) -> str:
    value = scope.get('root_path', '')
    if not isinstance(value, str) or (value and not value.startswith('/')):
        raise ValueError('ASGI root_path must be empty or start with a slash')

    return value.rstrip('/')

def route_path(scope: ASGIScope) -> str:
    path = scope.get('path', '/')
    if not isinstance(path, str) or not path.startswith('/'):
        raise ValueError('ASGI path must start with a slash')
    root = root_path(scope)
    if root and (path == root or path.startswith(root + '/')):
        return path[len(root):] or '/'

    return path

def prefixed_path(scope: ASGIScope, path: str) -> str:
    return quote(root_path(scope), safe='/') + path

def mounted_scope(scope: ASGIScope, prefix: str) -> ASGIScope:
    child = dict(scope)
    root = root_path(scope)
    relative = route_path(scope)
    child['root_path'] = root + prefix
    # Also support servers that supply an already stripped path.
    absolute = root + relative
    if absolute != scope.get('path'):
        child['path'] = absolute
        raw = scope.get('raw_path')
        if isinstance(raw, bytes):
            child['raw_path'] = quote(root, safe='/').encode('ascii') + raw

    return child
