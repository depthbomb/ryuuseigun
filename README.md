# ☄️ Ryūseigun

Ryuuseigun is a small async Python web framework built around ASGI. It covers the pieces most web apps need without bringing along a large stack: typed routes, JSON and form parsing, middleware, modules, streaming responses, WebSockets, and a test client.

The project is still young, so the API may move around before the first stable release. It is already a useful fit for small and medium sites where you want direct control over request handling and do not need a full application platform.

## Getting started

Ryuuseigun requires Python 3.14 or newer. From a checkout, install it with the Granian server extra:

```console
python -m pip install -e ".[server]"
```

Create an application:

```python
from ryuuseigun import Request, Ryuuseigun

app = Ryuuseigun(__name__)


@app.get('/')
async def home(request: Request) -> dict[str, str]:
    return {'message': 'Hello from Ryuuseigun'}


@app.post('/echo')
async def echo(request: Request) -> dict[str, object]:
    return {'received': await request.json()}
```

Save that as `app.py`, then run it:

```console
granian --interface asgi app:app
```

Ryuuseigun turns dictionaries and other JSON-compatible values into JSON responses. You can also return a `Response`, a `(body, status)` tuple, or one of the streaming and file response types.

## Routing

Every HTTP handler receives the request as its first positional argument, regardless of its name or annotation. Path parameters are passed by keyword after it. WebSocket handlers follow the same rule, with a socket first.

Routes can include typed parameters. Ryuuseigun includes converters for strings, integers, floats, UUIDs, and paths:

```python
from uuid import UUID


@app.get('/users/<int:user_id>')
async def user(request: Request, user_id: int) -> dict[str, int]:
    return {'id': user_id}


@app.get('/objects/<uuid:object_id>')
async def object_details(request: Request, object_id: UUID) -> dict[str, str]:
    return {'id': str(object_id)}
```

Give a route a name when you want to build links without repeating its path:

```python
from ryuuseigun import url_for


@app.get('/people/<int:person_id>', name='person')
async def person(request: Request, person_id: int) -> dict[str, int]:
    return {'id': person_id}


@app.get('/people-link')
async def person_link(request: Request) -> dict[str, str]:
    return {'href': url_for('person', person_id=42)}
```

`app.url_for(...)`, `request.url_for(...)`, and the context-aware `url_for(...)` helper all use the same route names.

## Requests and forms

The request object gives you headers, cookies, query parameters, path parameters, client information, and the request body in a few useful forms:

```python
@app.get('/search')
async def search(request: Request) -> dict[str, object]:
    return {
        'query': request.args.get('q', ''),
        'tags': request.args.getlist('tag'),
    }


@app.post('/profile')
async def profile(request: Request) -> dict[str, object]:
    form = await request.form()
    return {'display_name': form.get('display_name')}
```

`request.body()`, `request.text()`, `request.json()`, and `request.form()` collect and cache their result. For large bodies, `request.stream()` lets you handle incoming chunks directly.

## Streaming file uploads

Multipart uploads can be streamed straight to an object store or another destination. The file never needs to be collected into one large in-memory value:

```python
@app.post('/assets')
async def upload_assets(request: Request) -> dict[str, int]:
    uploaded = 0

    async for part in request.multipart():
        if part.filename:
            await object_store.upload(part.filename, part.stream())
            uploaded += 1

    return {'uploaded': uploaded}
```

Calling `request.form()` instead gives you `UploadFile` objects backed by spooled temporary files. Small files stay in memory and larger files roll over to disk. This is convenient when code needs to seek or reread an upload, while direct multipart streaming is usually the better choice for large files.

Disk writes run in worker threads and are batched with up to 320 KiB of additional buffering per active file. Cancellation waits for an outstanding write before closing the file.

Multipart limits are configurable without affecting applications that never parse multipart bodies:

```python
from ryuuseigun import Config

app = Ryuuseigun(
    __name__,
    config=Config(
        max_request_body_size=64 * 1024**2,
        max_multipart_parts=100,
        max_multipart_field_size=1024**2,
        max_multipart_file_size=32 * 1024**2,
    ),
)
```

A route can inherit those defaults or override just the limits it needs:

```python
from ryuuseigun import MultipartOverrides

@app.post(
    '/videos',
    multipart=MultipartOverrides(max_parts=5, max_file_size=2 * 1024**3),
)
async def upload_video(request: Request) -> dict[str, bool]:
    async for part in request.multipart():
        if part.filename:
            await object_store.upload(part.filename, part.stream())
    return {'ok': True}
```

Omitted `MultipartOverrides` fields inherit application limits. An explicit `None` disables only that limit. Route options accept `MultipartOverrides`; `MultipartLimits` describes fully resolved limits and is not a route override. The whole-request body limit still applies unless `max_request_body_size` is also `None`.

## Middleware and hooks

Middleware wraps handler execution, response sending, and request cleanup. Use `try/finally` or an async context manager to keep resources alive through a streamed response:

```python
from ryuuseigun import Next, Response

@app.middleware
async def database(request: Request, next: Next) -> None:
    async with pool.connection() as connection:
        request.state.connection = connection
        await next(request)
```

`next(request)` returns `None` after the response has been sent and request uploads have been closed. To stop a request early, await `request.respond(...)` and return:

```python
async def require_api_key(request: Request, next: Next) -> None:
    if request.headers.get('X-API-Key') != 'example-key':
        await request.respond(Response('Invalid API key', 401))
        return
    await next(request)

@app.get('/private', middlewares=(require_api_key,))
async def private(request: Request) -> dict[str, bool]:
    return {'authenticated': True}
```

The `middlewares` tuple runs in declaration order and works on application, module, and WebSocket route decorators. Use `after_request` to change headers or replace a response before sending starts:

```python
@app.after_request
async def identify(request: Request, response: Response) -> Response:
    response.headers['X-Service'] = 'example'
    return response
```

Hooks have fixed positional signatures: `before_request(request)`, `after_request(request, response)`, and `errorhandler(request, error)`. Before hooks return `None` to continue or a response value to stop. After and error hooks return a response value. Typed applications and modules check the request state type on routes, middleware, and hooks.

Application middleware enters first, followed by application before hooks, then each module's middleware and before hooks, then route middleware and the handler. After hooks run from the innermost entered scope outward, in reverse registration order. Sending and cleanup finish before middleware unwinds. A scope's middleware can reject a request before that scope's hooks run. Application errors after response headers have been sent propagate without attempting another response.

For public services, the optional concurrency limiter can reject excess work cleanly instead of letting an application queue grow without bound:

```python
from ryuuseigun import ConcurrencyLimit

app.middleware(ConcurrencyLimit(500, retry_after=1))
```

Rejected requests receive `503 Service Unavailable` and a `Retry-After` header. Capacity stays reserved through response sending and request cleanup, including streaming and file downloads. The limiter can also be attached to selected routes with `@app.get('/download', middlewares=(ConcurrencyLimit(10),))`.

## Modules

Modules group related routes and can carry their own middleware, hooks, and error handlers:

```python
from ryuuseigun import Module

api = Module('api', url_prefix='/api')


@api.get('/status', name='status')
async def status(request: Request) -> dict[str, bool]:
    return {'ready': True}


app.register_module(api, url_prefix='/v1')
```

That route is available at `/v1/api/status` and can be reversed with `app.url_for('api.status')`.

Modules can be nested, which is handy when a larger app has separate areas with their own behavior. Registering a module freezes it and its children.

Applications finalize on lifespan startup or the first HTTP or WebSocket request. You can also call `app.finalize()` explicitly. Finalization compiles route pipelines once and rejects later route, middleware, hook, module, and lifecycle registration. Finish registration before starting the server or making test requests. Configuration and public registration collections are read-only; shared runtime resources belong in `app.state`.

## Streaming, files, and WebSockets

Ryuuseigun includes:

- `StreamingResponse` for async response bodies
- `FileResponse` with range and conditional request support
- `EventStreamResponse` for server-sent events
- `WebSocket` routes with JSON, text, and binary helpers
- optional gzip, Brotli, and Zstandard response compression

Streaming responses keep listening for client disconnects. Any unread request body is buffered for later consumption, using `upload_spool_threshold` to spill to a temporary file. The request body size limit still applies, and temporary files are closed during request cleanup.

Compression is an after-request hook:

```python
from ryuuseigun import Compression

app.after_request(Compression())
```

Streaming compression runs in worker threads. Brotli streams use quality 4 to keep response latency low; buffered Brotli responses retain the codec's default quality.

Brotli support uses the optional compression extra:

```console
python -m pip install -e ".[server,compression]"
```

See `examples/modern_http.py` for streaming responses, server-sent events, file responses, lifespan resources, and WebSockets in one application.

## Testing

The built-in client calls the ASGI application directly, so ordinary request tests do not need a live server:

```python
from unittest import IsolatedAsyncioTestCase


class AppTests(IsolatedAsyncioTestCase):
    async def test_home(self) -> None:
        response = await app.test_client().get('/')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'message': 'Hello from Ryuuseigun'})
```

Run the project checks with:

```console
python -m unittest discover -s tests
python -m ruff check ryuuseigun tests examples benchmarks
python -m mypy ryuuseigun examples tests/typing_contracts.py
```

Run the in-process ASGI benchmarks with `python -m benchmarks.hotpaths`. They check response status and bodies before timing static and dynamic routes, middleware, headers, JSON bodies, streaming, and multipart uploads. Use `--rounds 9 --iterations 6000` for longer runs or `--only static_json dynamic_json` to select workloads. These timings exclude network and server overhead.

Use `python -m benchmarks.middleware` to measure ordinary decorators, middleware, nested hooks, and WebSocket dispatch separately. Add `--finalize-routes 1000` to measure pipeline finalization time and allocations for an application with 1,000 routes.

The `examples` directory has smaller focused applications for routing, modules, middleware, typed request state, and newer HTTP features.

## Migrating earlier checkouts

See the developer experience sections below for the current test client and optional integrations.

- Reinstall the checkout with `python -m pip install -e .`, use `ryuuseigun` for package imports, and create applications with `Ryuuseigun(...)`.
- Add a first request argument to every HTTP route and hook; add a first socket argument to every WebSocket route. Names and annotations no longer select argument injection.
- Change middleware to await `next(request)` and return `None`. Use `request.respond(...)` for early responses and `after_request` for response changes. Register `Compression()` with `app.after_request(...)`.
- Replace `@use(...)` and `@use_websocket(...)` with `middlewares=(...)` on the route decorator.
- Replace route multipart dictionaries and `MultipartLimits(...)` with `MultipartOverrides(...)`. Omitted fields inherit; explicit `None` means unlimited.
- Complete registration before startup or the first request. To change a finalized application's routes or hooks, construct a new application.

## Diagnosing failures

`app.logger` is a standard Python logger named `ryuuseigun.<import_name>`. Ryuuseigun logs tracebacks for unexpected HTTP and WebSocket errors it handles, and for lifespan failures. Configure logging in your application with `logging.basicConfig()` or your normal logging configuration. Expected HTTP errors, disconnects, and handled application exceptions do not produce automatic error logs.

`Config(debug=True)` adds a debug-level registration summary and route-pattern context to HTTP error records. Client error details still require `expose_error_details=True`. `propagate_exceptions=True` passes unexpected exceptions to your server or test runner, which owns reporting them. Errors after response headers have been sent also propagate; Ryuuseigun cannot replace an already-started response. Request bodies, cookies, authorization headers, and query strings are not added to automatic log metadata. Application exception messages can contain whatever the application puts in them.

## Testing application resources and HTTP flows

Use an async client context when startup creates resources. It drives the real ASGI lifespan on the test's event loop and runs shutdown even if an assertion fails:

```python
async def test_application() -> None:
    async with app.test_client(base_url='https://testserver') as client:
        login = await client.post('/login', json={'username': 'demo', 'password': 'test-password'}, follow_redirects=True)
        assert login.status_code == 200
        assert login.history[0].status_code == 303
```

The one-shot `await app.test_client().get('/')` form deliberately skips lifespan. A client context can be entered once; overlapping lifespan contexts for the same app are rejected. Concurrent requests inside one context are supported. Prefer fresh application factories for independent tests. `lifespan_timeout=10` controls the startup/shutdown handshake timeout. Exceptions are controlled by the application's `Config(propagate_exceptions=True)`, without mutating config for individual clients.

The client maintains cookies with domain, path, expiry, and secure transport rules. Use an HTTPS `base_url` when testing secure cookies. Redirects default to disabled, can be enabled per request or client, and are capped by `max_redirects=20`. External-origin requests and redirects raise `ValueError`; this transport never makes network requests. A redirect response's predecessors appear in `response.history`, and `response.url` identifies the final URL.

Use `form={'name': 'value'}` for URL-encoded data and `files={'document': ('notes.txt', b'hello', 'text/plain')}` for uploads. Combine `form` and `files` for multipart requests. Lists of `(name, value)` pairs preserve repeated fields and files. Choose one of `body`, `json`, or `form/files`; conflicting encodings fail before dispatch. Multipart boundaries are generated by the client. Responses remain buffered, so use direct ASGI tests for incremental streaming or disconnect behavior.

## Cookies and redirects

```python
from ryuuseigun import Response, redirect

def signed_in(session_token: str) -> Response:
    res = redirect('/account', status_code=303)
    res.set_cookie('session', session_token, max_age=3600, secure=True, httponly=True)
    return res

def signed_out() -> Response:
    res = redirect('/login', status_code=303)
    res.delete_cookie('session', path='/')
    return res
```

Cookies default to `path='/'`, `samesite='lax'`, `secure=False`, and `httponly=False`. Each call appends a separate `Set-Cookie` header. `expires` takes a timezone-aware datetime. Deletion must match the original name, path, and domain; pass `secure=True` when deleting prefixed secure cookies. `SameSite=None` requires `secure=True`, and `__Host-`/`__Secure-` requirements are validated. Applications own session storage, revocation, CSRF protection, and redirect destination policy.

`redirect()` defaults to 302 and supports 301, 302, 303, 307, and 308. Use 303 for navigation after a successful POST; 307 and 308 preserve the request method and body.

## Choosing a decorator or lifecycle callback

| Mechanism | Use it for | When it finishes |
| --- | --- | --- |
| Ordinary async decorator | Authentication, permissions, route-specific rate limits | When the handler returns. |
| `before_request` | Shared checks and request state | Before the handler; a response stops further processing. |
| `after_request` | Headers and response changes | Before sending the response. |
| Middleware | Resource scopes, concurrency, end-to-end timing | After downstream sending and cleanup. |
| `lifespan` | Pools and shared clients | Application shutdown. |

Keep the routing decorator outermost, preserve `functools.wraps`, and await the wrapped handler. Authentication above a rate-limit decorator runs first, allowing the limit to use authenticated identity. Reversing those decorators charges rejected authentication attempts too.

`examples/gallery/auth.py` and `examples/gallery/rate_limits.py` show typed decorator factories that preserve path arguments and return types. `examples/lifecycle_trace.py` provides an executable trace through nested modules, early responses, errors, and streaming. A decorator's `finally` runs before a returned stream is consumed. Put a resource needed by the stream in middleware or in the generator's own context.

## Optional validation and API schemas

Install optional integrations in the project virtual environment:

```powershell
& .\.venv\Scripts\python.exe -m pip install -e '.[validation]'
```

`ryuuseigun.validation` provides `parse_json(req, Model)`, `parse_query(req, Model)`, and `install_validation(app)`. Install the error handler when using those helpers on their own. Model errors become structured 422 responses with raw inputs and exception context omitted. Malformed JSON remains 400; `parse_json` requires `application/json` and otherwise returns 415. Pydantic model configuration controls coercion and extra-field behavior. Custom validation messages remain application-controlled.

`ryuuseigun.openapi.OpenAPI` installs validation error handling and adds `/openapi.json` and a small dependency-free schema viewer at `/docs`. Set `docs_url=None` to omit the viewer. This adapter uses explicit models:

```python
from pydantic import BaseModel
from ryuuseigun.openapi import OpenAPI
from ryuuseigun.validation import parse_json
from ryuuseigun import Request, Ryuuseigun, JSONResponse

class CreateItem(BaseModel):
    title: str

class Item(BaseModel):
    id: int
    title: str

app = Ryuuseigun(__name__)
api = OpenAPI(app, title='Items', version='1')

@app.post('/items')
@api.schema(body=CreateItem, responses={201: Item})
async def create_item(req: Request) -> JSONResponse:
    item = await parse_json(req, CreateItem)
    return JSONResponse({'id': 1, 'title': item.title}, status_code=201)
```

The schema decorator validates declared JSON/query input before calling the handler and caches validated models on that request. It validates modeled response bodies strictly and preserves the handler's return value. Use `None` for a status whose payload is not modeled. Undeclared returned statuses and invalid modeled responses are application errors. Hooks and outer decorators can still return other responses; declare their expected errors explicitly.

OpenAPI 3.1.1 documents are generated at finalization and cached, with no per-request schema inspection. Module paths, path converters, declared responses, scalar query models, and explicit security schemes are supported. `api.document()` returns a defensive copy. Query models currently support required/defaulted string, integer, number, and boolean fields. Authentication declarations describe your own authentication decorators; they do not implement authentication. Routes without schema metadata are omitted. Keep uploads, streaming payloads, custom media types, WebSockets, and QUERY routes outside this initial adapter. `functools.wraps` preserves schema metadata through ordinary wrappers.

Applications using only the core do not import Pydantic or SQLAlchemy. For integrations needing finalization, `app.routes` exposes a read-only route tuple and `app.on_finalize(callback)` registers synchronous setup before pipelines are frozen. Register routes first, make setup callbacks idempotent if they can fail, and complete registration before serving requests.

## Gallery and middleware integration examples

`examples/gallery` combines an application factory, typed state, module hooks, cookie authentication, CSRF checks, rate-limit decorators, SQLite through SQLAlchemy asyncio, bounded multipart uploads, downloads, request IDs/logging, and OpenAPI. Transactions commit before successful responses; the engine closes at shutdown. The example uses one configured demo identity. Sessions and image records persist in SQLite; rate limits are process-local. Replace the demo identity check and the process-local limiter when adapting it to a multi-user or multi-worker service. Uploads are treated as opaque bytes and downloaded as attachments.

For a local HTTP demonstration:

```powershell
& .\.venv\Scripts\python.exe -m pip install -e '.[server,examples,integration-tests]'
$env:GALLERY_PASSWORD = 'replace-for-local-testing'
$env:GALLERY_ORIGIN = 'http://127.0.0.1:8000'
$env:GALLERY_INSECURE_LOCAL_COOKIES = '1'
& .\.venv\Scripts\granian.exe --interface asgi --host 127.0.0.1 --port 8000 examples.gallery_server:app
```

The default database is `gallery.db`; override it with `GALLERY_DATABASE_URL`. POST JSON credentials to `/login`, follow the redirect to `/account`, and use its `csrf_token` in `X-CSRF-Token` for later writes. POST `{"title":"Example"}` to `/api/images`, upload the `image` file field to `/api/images/<id>/content`, and GET that same URL to download. `/api/images?limit=20` lists records. `/logout` revokes the session. The one-MiB request limit includes multipart overhead.

`examples/cors.py` shows Starlette's optional `CORSMiddleware` around the complete ASGI application, including generated error responses and preflight requests. Serve `examples.cors:application`. Its explicit origin allowlist is separate from authentication and CSRF checks. Framework ASGI annotations accept mutable mappings, matching common ASGI middleware without conversion wrappers.

Run the optional integration tests after installing the extras:

```powershell
& .\.venv\Scripts\python.exe -m unittest discover -s tests\integrations -q
& .\.venv\Scripts\mypy.exe ryuuseigun examples tests\typing_contracts.py tests\integrations\optional_typing_contracts.py
```

Core tests remain under `tests`. The integration suite is separate so core-only installations need no database, validation, schema-validator, or third-party middleware packages. Python 3.14+ remains required, and this early version's public APIs are still subject to change.

`python -m benchmarks.dx --optional` measures response helpers, form/upload encoding, the richer test client, and optional model validation/schema serving. Client timings include its transport and cookie handling; compare server dispatch with `benchmarks.middleware` separately.
