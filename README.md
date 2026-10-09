# ☄️ Ryūseigun

Ryuuseigun is a small async Python web framework for people who like building things their own way. It handles routing, requests, responses, middleware, streaming and WebSockets. You choose how the rest of your app works, including storage, authentication and rendering.

Start with a single file, then split features into modules or mount separate ASGI apps as the project grows. The core only depends on `orjson`.

## Getting started

You'll need Python 3.14 or newer. With a virtual environment active:

```console
python -m pip install 'ryuuseigun[server]'
```

Save this as `app.py`:

```python
from ryuuseigun import Request, Ryuuseigun

app = Ryuuseigun(__name__)

@app.get('/')
async def home(req: Request) -> dict[str, str]:
    return {'message': 'Hello!'}

@app.get('/people/<int:person_id>')
async def person(req: Request, person_id: int) -> dict[str, int]:
    return {'id': person_id}
```

Then run it:

```console
python -m granian --interface asgi app:app
```

The server extra installs Granian. Any compatible ASGI server can serve the app.

## Working with requests

Handlers receive the request first, followed by path arguments. Routes support `str`, `int`, `float`, `uuid` and `path` converters. Dictionaries become JSON; you can also return text, bytes, `(body, status)` or a response object.

```python
@app.post('/echo')
async def echo(req: Request):
    return {'received': await req.json()}

@app.post('/profile')
async def profile(req: Request):
    form = await req.form()
    return {'name': form.get('name')}
```

Headers, cookies and query parameters are available on `req.headers`, `req.cookies` and `req.query`. Body helpers cache their results. Use `req.stream()` for incoming chunks or `req.multipart()` to stream uploaded parts directly. `req.form()` gives you `UploadFile` objects backed by spooled temporary files.

Use `Config` to set body, query and multipart limits, trusted hosts, slash behavior and default response headers. Route-level `MultipartOverrides` can adjust individual upload limits.

Responses include `JSONResponse`, `FileResponse`, `StreamingResponse` and `EventStreamResponse`. `Response.set_cookie()`, `delete_cookie()` and `redirect()` cover common HTTP chores. See [the HTTP examples](examples/modern_http.py) for conditional requests, compression, ranges and server-sent events.

## Middleware and resources

Middleware stays active through response sending and request cleanup, including streamed responses:

```python
from time import perf_counter
from ryuuseigun import Next

@app.middleware
async def timing(req: Request, next: Next) -> None:
    started = perf_counter()
    try:
        await next(req)
    finally:
        app.logger.info('%s took %.3fs', req.path, perf_counter() - started)
```

Call `await next(req)` to continue or `await req.respond(...)` to respond directly. Middleware returns `None`. Ordinary handler decorators can implement things like authentication; put the routing decorator outermost and preserve `functools.wraps`.

| Hook | Useful for | Lifetime |
| --- | --- | --- |
| Handler decorator | Authentication, per-route policy | Until the handler returns |
| `before_request` | Request setup or an early response | Before the handler |
| `after_request` | Changing headers or the response | Before sending |
| Middleware | Resource scopes and timing | Through sending and cleanup |
| `lifespan` | Database connections and shared clients | Startup through shutdown |

Application middleware enters before module middleware. Before hooks run from outer scopes inward; after hooks run in reverse. An async context manager registered with `@app.lifespan` opens at startup and closes at shutdown. Contexts unwind in reverse order, including when startup fails. Keep resources used by a stream in middleware or the generator's own context.

State can be typed at all three levels: `Ryuuseigun[RequestState, AppState, SocketState]`. Supply `request_state_factory`, `app_state_factory` and `websocket_state_factory` for the classes you use. Application state is created once per app; request state is fresh for each HTTP request, and socket state for each WebSocket connection. The defaults are independent `SimpleNamespace` instances.

[The composition example](examples/composition.py) shows typed state, reusable modules and services passed in as ordinary Python arguments. [The lifecycle trace](examples/lifecycle_trace.py) shows the order of hooks, errors and cleanup. Response `after_send` callbacks run in the serving process after a successful send; use your own job system for work that needs durable delivery.

## Modules and ASGI apps

Modules group routes, middleware and hooks inside an application:

```python
from ryuuseigun import Module

api = Module('api', url_prefix='/api')

@api.get('/ping')
async def ping(req: Request):
    return {'pong': True}

app.register_module(api)
```

Mount an independent ASGI app with `app.mount('/admin', admin_app, name='admin')`. Add `lifespan=True` when the parent should start and stop that child. The longest matching mount owns its prefix, before native routes are considered. Each mounted app owns its hooks and configuration. For a policy that covers the whole site, wrap the outer ASGI app, as in [the CORS example](examples/cors.py).

`app.url_for('api.ping')` builds a module route. `app.url_for('admin:dashboard')` calls a mounted app's URL builder; `app.url_for('admin', path='/assets/icon.svg')` works with any ASGI app. Mount names reserve their URL namespace. Inside a handler, `req.url_for(...)` and the context-aware `url_for(...)` helper include the external `root_path`, including nested mounts. `req.path` is relative to the current app.

## Inspecting and extending an app

Finish registration before serving requests. `app.finalize()` freezes it explicitly; startup and the first request also finalize it. Synchronous `app.on_finalize(callback)` hooks let integrations finish setup before that happens. Callbacks that fail may be retried, so keep them idempotent.

`req.route` and `socket.route` expose immutable `RouteInfo` with the route template, endpoint, methods, module names and middleware names. Unmatched requests have no route. `app.inspect_routes()` finalizes and lists native HTTP and WebSocket routes; `app.mounts` lists separate applications.

```console
python -m ryuuseigun inspect examples.basic:app
python -m ryuuseigun inspect examples.composition:app --json
```

The command imports the application and shows its routes and mounts without starting lifespan resources. Inspect a child application separately for its own routes. Extensions can use the public registration methods, middleware types, `RouteInfo` and the `ASGIApplication` callable type without depending on dispatch internals.

## Testing

The built-in client supports JSON, forms, multipart files, cookies, redirects and WebSockets:

```python
async def test_home():
    async with app.test_client() as client:
        response = await client.get('/')
        assert response.json() == {'message': 'Hello!'}
```

The context manager starts and stops lifespan. For an app wrapped in third-party middleware, use `TestClient(application)` from `ryuuseigun.testing`. Pass `root_path='/prefix'` to test a deployment prefix.

Use `async with client.stream('GET', '/events') as response` and iterate `response.iter_bytes()` to test a stream as it arrives. The transport holds one outgoing ASGI message at a time and waits for the consumer. Leaving the context disconnects and waits for cleanup; `response.disconnect()` can trigger that explicitly. Async iterables also work as upload bodies. Streaming requests have a configurable timeout and do not follow redirects.

[This runnable example](examples/testing_streams.py) tests an open-ended stream through CORS middleware and checks disconnect cleanup.

## More examples

The [examples directory](examples) has small apps for [routing](examples/basic.py), [modules](examples/modules.py), [middleware](examples/middleware.py), [typed request state](examples/typed_request_state.py) and [HTTP features](examples/modern_http.py).

The [gallery](examples/gallery/app.py) puts several pieces together: a factory, typed state, cookie sessions, CSRF checks, rate limits, uploads and persistent SQLite storage through [sqrrl](https://pypi.org/project/sqrrl/). Input checks are ordinary Python in the handlers.

From a checkout, with its virtual environment active, run the local HTTP demo in PowerShell:

```powershell
python -m pip install -e '.[server,examples]'
$env:GALLERY_PASSWORD = 'replace-for-local-testing'
$env:GALLERY_ORIGIN = 'http://127.0.0.1:8000'
$env:GALLERY_INSECURE_LOCAL_COOKIES = '1'
python -m granian --interface asgi examples.gallery_server:app
```

The demo applies its checked-in migrations at startup and closes the database at shutdown. It uses `gallery.db`, or the path in `GALLERY_DATABASE_PATH`. Start with a fresh demo database when moving from the old gallery. For several workers, run migrations before starting them, and replace the process-local rate limiter with a shared one.

POST `{"username":"demo","password":"..."}` to `/login`, follow the redirect to `/account`, then send its `csrf_token` as `X-CSRF-Token` for writes. POST `{"title":"Example"}` to `/api/images`. PUT a multipart `image` file to `/api/images/<id>/content`, then GET that path to download it. This example has one configured demo identity.

After changing the gallery schema, regenerate the sqrrl models and create a migration. CI checks both the generated code and migration history.

## Compatibility and development

Python 3.14 is the supported baseline, including standard-library Zstandard support. CI checks Linux, Windows and macOS, runs real Granian requests, and tests the built wheel outside the checkout. Examples and features on `master` can be ahead of the latest PyPI release.

Public APIs are the documented classes, functions, methods and typing contracts. Names starting with `_` are internal. Patch releases aim to preserve public behavior; before 1.0, minor releases may make documented breaking changes. Check the [release notes](https://github.com/depthbomb/ryuuseigun/releases) when upgrading.

Changes since 0.9.0: mounted ASGI apps and prefix-aware URLs, public route inspection, typed app and socket state, and streaming tests. The OpenAPI and Pydantic adapters and the `validation` extra have been removed. The gallery now uses sqrrl and `GALLERY_DATABASE_PATH`.

```console
python -m pip install -e '.[dev,server,compression,examples]'
python -m ruff check ryuuseigun tests examples benchmarks
python -m mypy ryuuseigun examples tests/typing_contracts.py tests/integrations/optional_typing_contracts.py
python -m sqrrl generate --check --config examples/gallery/sqrrl.json
python -m sqrrl migrate check --config examples/gallery/sqrrl.json
python -m pytest tests --cov=ryuuseigun --cov-branch
python -m build
python -m twine check --strict dist/*
```

For local HTTP comparisons with Starlette and Quart:

```console
python -m pip install -e '.[benchmark]'
python -m benchmarks.server --requests 5000 --rounds 3 --concurrency 16
```

The runner checks JSON, streaming and upload responses using the same Granian settings. It reports throughput, p50/p95/p99 latency, CPU time, sampled RSS and environment details. Use `--output /path/outside/the/checkout/results.json` to keep a report. These are closed-loop tests sharing one machine with the client, so treat them as local comparisons. Smaller in-process benchmarks remain in [benchmarks](benchmarks).
