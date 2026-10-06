"""Explicit, optional OpenAPI 3.1 schemas backed by Pydantic validation."""
from html import escape
from copy import deepcopy
from functools import wraps
from re import sub, findall
from pydantic import BaseModel
from dataclasses import dataclass
from ryuuseigun.app import Ryuuseigun
from orjson import dumps, OPT_INDENT_2
from ryuuseigun.request import Request
from typing import Any, Literal, Concatenate
from ryuuseigun.routing import RouteDecorator
from pydantic.json_schema import models_json_schema
from collections.abc import Mapping, Callable, Awaitable
from ryuuseigun.response import Response, ResponseValue, make_response
from ryuuseigun.validation import parse_json, parse_query, install_validation

@dataclass(frozen=True, slots=True)
class _Operation:
    body: type[BaseModel] | None
    query: type[BaseModel] | None
    responses: Mapping[int, type[BaseModel] | None]
    summary: str | None
    operation_id: str | None
    security: tuple[str, ...]

class OpenAPI[StateT]:
    def __init__(
        self, app: Ryuuseigun[StateT], *, title: str, version: str,
        schema_url: str = '/openapi.json', docs_url: str | None = '/docs',
        security_schemes: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        self.app = app
        self.title = title
        self.version = version
        self.security_schemes = deepcopy(dict(security_schemes or {}))
        self._document: dict[str, Any] | None = None
        self._body = b''
        install_validation(app)

        @app.get(schema_url, name='ryuuseigun_openapi')
        async def schema(req: Request[StateT]) -> Response:
            return Response(self._body, media_type='application/json')

        if docs_url is not None:
            page = (
                '<!doctype html><html><meta charset="utf-8"><title>' + escape(title) + '</title>'
                '<h1>' + escape(title) + '</h1><p>API version ' + escape(version) + '</p>'
                '<p><a href="' + escape(schema_url, quote=True) + '">Download schema</a></p>'
                '<pre id="schema"></pre><script>fetch('
                + dumps(schema_url).decode().replace('<', '\\u003c')
                + ').then(r=>r.json()).then(s=>{document.getElementById("schema").textContent='
                'JSON.stringify(s,null,2)});</script></html>'
            )

            @app.get(docs_url, name='ryuuseigun_openapi_docs')
            async def docs(req: Request[StateT]) -> Response:
                return Response(page, media_type='text/html; charset=utf-8')

        app.on_finalize(self._build)

    def schema(
        self, *, responses: Mapping[int, type[BaseModel] | None],
        body: type[BaseModel] | None = None, query: type[BaseModel] | None = None,
        summary: str | None = None, operation_id: str | None = None, security: tuple[str, ...] = (),
    ) -> RouteDecorator[StateT]:
        if not responses or any(code < 100 or code > 599 for code in responses):
            raise ValueError('Declare at least one valid response status')
        if any(name not in self.security_schemes for name in security):
            raise ValueError('Operation references an undeclared security scheme')
        operation = _Operation(body, query, dict(responses), summary, operation_id, security)

        def decorate[**Parameters, Result: ResponseValue](
            handler: Callable[Concatenate[Request[StateT], Parameters], Awaitable[Result]],
        ) -> Callable[Concatenate[Request[StateT], Parameters], Awaitable[Result]]:
            @wraps(handler)
            async def wrapped(req: Request[StateT], /, *args: Parameters.args, **kwargs: Parameters.kwargs) -> Result:
                if body is not None:
                    await parse_json(req, body)
                if query is not None:
                    parse_query(req, query)
                result = await handler(req, *args, **kwargs)
                response = make_response(result)
                if response.status_code not in operation.responses:
                    raise ValueError(f'Undocumented response status: {response.status_code}')
                model = operation.responses[response.status_code]
                if model is not None:
                    model.model_validate_json(response.body, strict=True)
                return result

            wrapped.__dict__['__ryuuseigun_openapi__'] = operation
            return wrapped

        return decorate

    def document(self) -> dict[str, Any]:
        self.app.finalize()
        if self._document is None:
            raise RuntimeError('OpenAPI schema has not been finalized')
        return deepcopy(self._document)

    def _build(self) -> None:
        declared = []
        models: set[tuple[type[BaseModel], Literal['validation', 'serialization']]] = set()
        for route in self.app.routes:
            operation = getattr(route.handler, '__ryuuseigun_openapi__', None)
            if not isinstance(operation, _Operation):
                continue
            declared.append((route, operation))
            for model in (operation.body, operation.query):
                if model is not None:
                    models.add((model, 'validation'))
            models.update((model, 'serialization') for model in operation.responses.values() if model is not None)
        schemas, definitions = models_json_schema(
            sorted(models, key=lambda pair: (pair[0].__module__, pair[0].__qualname__, pair[1])),
            ref_template='#/components/schemas/{model}',
        )
        components = definitions.get('$defs', {})
        paths: dict[str, Any] = {}
        templates: dict[str, str] = {}
        identifiers: set[str] = set()
        for route, operation in declared:
            path = sub(r'<(?:[^:>]+:)?([^>]+)>', r'{\1}', route.path)
            template = sub(r'\{[^}]+\}', '{}', path)
            if template in templates and templates[template] != path:
                raise ValueError(f'Conflicting OpenAPI path templates: {templates[template]} and {path}')
            templates[template] = path
            parameters = []
            for converter, name in findall(r'<(?:(\w+):)?(\w+)>', route.path):
                schema: dict[str, Any] = {'type': {'int': 'integer', 'float': 'number'}.get(converter, 'string')}
                if converter == 'uuid':
                    schema['format'] = 'uuid'
                parameters.append({'in': 'path', 'name': name, 'required': True, 'schema': schema})
            if operation.query is not None:
                ref = schemas[(operation.query, 'validation')]['$ref'].rsplit('/', 1)[-1]
                query_schema = components[ref]
                if query_schema.get('type') != 'object':
                    raise ValueError('OpenAPI query models must be objects with scalar fields')
                for name, field in query_schema.get('properties', {}).items():
                    if field.get('type') not in {'string', 'integer', 'number', 'boolean'}:
                        raise ValueError('OpenAPI query models currently support required/defaulted scalar fields only')
                    parameters.append({
                        'in': 'query', 'name': name, 'required': name in query_schema.get('required', ()), 'schema': field,
                    })
            responses: dict[str, Any] = {}
            for code, model in operation.responses.items():
                response: dict[str, Any] = {'description': f'HTTP {code}'}
                if model is not None:
                    response['content'] = {'application/json': {'schema': schemas[(model, 'serialization')]}}
                responses[str(code)] = response
            if operation.body is not None:
                responses.setdefault('400', {'description': 'Malformed JSON'})
                responses.setdefault('415', {'description': 'Expected application/json'})
            if operation.body is not None or operation.query is not None:
                responses.setdefault('422', {'description': 'Request validation failed'})
            for method in sorted(route.methods):
                if method.lower() not in {'get', 'post', 'put', 'patch', 'delete', 'head', 'options', 'trace'}:
                    raise ValueError(f'OpenAPI 3.1 cannot describe {method}; leave this route undocumented')
                identifier = operation.operation_id or f'{route.endpoint}_{method.lower()}'
                if identifier in identifiers:
                    raise ValueError(f'Duplicate OpenAPI operation identifier: {identifier}')
                identifiers.add(identifier)
                entry: dict[str, Any] = {'operationId': identifier, 'responses': deepcopy(responses)}
                if parameters:
                    entry['parameters'] = deepcopy(parameters)
                if operation.summary:
                    entry['summary'] = operation.summary
                if operation.security:
                    entry['security'] = [{name: [] for name in operation.security}]
                if operation.body is not None:
                    entry['requestBody'] = {
                        'required': True,
                        'content': {'application/json': {'schema': schemas[(operation.body, 'validation')]}},
                    }
                path_item = paths.setdefault(path, {})
                if method.lower() in path_item:
                    raise ValueError(f'Conflicting OpenAPI path: {method} {path}')
                path_item[method.lower()] = entry
        self._document = {
            'openapi': '3.1.1', 'info': {'title': self.title, 'version': self.version}, 'paths': paths,
            'components': {'schemas': components, 'securitySchemes': self.security_schemes},
        }
        self._body = dumps(self._document, option=OPT_INDENT_2)
