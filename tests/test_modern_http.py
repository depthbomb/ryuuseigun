import gzip
from pathlib import Path
from orjson import OPT_SORT_KEYS
from tempfile import NamedTemporaryFile
from asyncio import Event, Queue, wait_for
from contextlib import asynccontextmanager
from unittest import IsolatedAsyncioTestCase
from ryuuseigun.types import ASGIScope, ASGIMessage
from ryuuseigun.testing import WebSocketUpgradeError
from ryuuseigun import (
    Config,
    Module,
    Ryuuseigun,
    Request,
    Response,
    MediaType,
    URLScheme,
    WebSocket,
    HTTPMethod,
    HeaderName,
    StatusCode,
    Compression,
    FileResponse,
    ASGIExtension,
    ASGIScopeType,
    WebSocketNext,
    ASGIMessageType,
    ContentEncoding,
    ServerSentEvent,
    StreamingResponse,
    EventStreamResponse,
)


class PublicConstantTests(IsolatedAsyncioTestCase):
    async def test_constants_are_available_from_the_package_root(self) -> None:
        self.assertEqual(HTTPMethod.QUERY, 'QUERY')
        self.assertEqual(StatusCode.URI_TOO_LONG, 414)
        self.assertEqual(StatusCode.UNSUPPORTED_MEDIA_TYPE, 415)
        self.assertEqual(HeaderName.ACCEPT_QUERY, 'Accept-Query')
        self.assertEqual(MediaType.FORM_URLENCODED, 'application/x-www-form-urlencoded')
        self.assertEqual(MediaType.JSON, 'application/json')
        self.assertEqual(MediaType.MULTIPART_FORM_DATA, 'multipart/form-data')
        self.assertEqual(MediaType.PROBLEM_JSON, 'application/problem+json')
        self.assertEqual(ContentEncoding.ZSTANDARD, 'zstd')
        self.assertEqual(ASGIScopeType.HTTP, 'http')
        self.assertEqual(ASGIMessageType.HTTP_RESPONSE_START, 'http.response.start')
        self.assertEqual(ASGIExtension.HTTP_PATHSEND, 'http.response.pathsend')
        self.assertEqual(URLScheme.WEBSOCKET_SECURE, 'wss')


def scope(
    path: str = '/',
    *,
    method: str = 'GET',
    headers: tuple[tuple[bytes, bytes], ...] = (),
    extensions: tuple[str, ...] = (),
) -> ASGIScope:
    return {
        'type': 'http',
        'asgi': {'version': '3.0', 'spec_version': '2.5'},
        'http_version': '2',
        'method': method,
        'scheme': 'https',
        'path': path,
        'raw_path': path.encode(),
        'query_string': b'',
        'headers': list(headers),
        'client': ('127.0.0.1', 50000),
        'server': ('testserver', 443),
        'extensions': {name: {} for name in extensions},
    }


class StreamingTests(IsolatedAsyncioTestCase):
    async def test_request_and_response_stream_without_buffering(self) -> None:
        app = Ryuuseigun(__name__)

        @app.post('/echo')
        async def echo(req: Request) -> StreamingResponse:
            async def chunks():
                async for chunk in req.stream():
                    yield chunk.upper()

            return StreamingResponse(chunks(), media_type='application/octet-stream')

        incoming: Queue[ASGIMessage] = Queue()
        sent: list[ASGIMessage] = []
        await incoming.put({'type': 'http.request', 'body': b'ab', 'more_body': True})
        await incoming.put({'type': 'http.request', 'body': b'cd', 'more_body': False})

        async def send(message: ASGIMessage) -> None:
            sent.append(message)

        await app(scope('/echo', method='POST'), incoming.get, send)

        body = b''.join(message.get('body', b'') for message in sent if message['type'] == 'http.response.body')
        self.assertEqual(body, b'ABCD')
        self.assertEqual([message.get('more_body', False) for message in sent[1:]], [True, True, False])

    async def test_stream_is_cancelled_when_the_client_disconnects(self) -> None:
        app = Ryuuseigun(__name__)
        incoming: Queue[ASGIMessage] = Queue()
        generated_first = Event()
        cleaned_up = Event()
        sent: list[ASGIMessage] = []

        @app.get('/stream')
        async def stream(_req) -> StreamingResponse:
            async def chunks():
                try:
                    yield b'first'
                    generated_first.set()
                    await Event().wait()
                finally:
                    cleaned_up.set()

            return StreamingResponse(chunks())

        await incoming.put({'type': 'http.request', 'body': b'', 'more_body': False})

        async def send(message: ASGIMessage) -> None:
            sent.append(message)
            if message['type'] == 'http.response.body' and message.get('body') == b'first':
                await incoming.put({'type': 'http.disconnect'})

        await app(scope('/stream'), incoming.get, send)

        self.assertTrue(generated_first.is_set())
        self.assertTrue(cleaned_up.is_set())
        self.assertEqual(sent[-1]['body'], b'first')

    async def test_disconnect_is_observed_after_partially_reading_request_body(self) -> None:
        app = Ryuuseigun(__name__)
        incoming: Queue[ASGIMessage] = Queue()
        cleaned_up = Event()

        @app.post('/stream')
        async def stream(req: Request) -> StreamingResponse:
            body = req.stream()
            self.assertEqual(await anext(body), b'partial')

            async def chunks():
                try:
                    yield b'first'
                    await Event().wait()
                finally:
                    cleaned_up.set()

            return StreamingResponse(chunks())

        await incoming.put({'type': 'http.request', 'body': b'partial', 'more_body': True})
        await incoming.put({'type': 'http.disconnect'})

        async def send(message: ASGIMessage) -> None:
            return None

        await wait_for(app(scope('/stream', method='POST'), incoming.get, send), timeout=0.5)

        self.assertTrue(cleaned_up.is_set())

    async def test_sse_formats_events(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/events')
        async def events(_req) -> EventStreamResponse:
            async def source():
                yield ServerSentEvent('hello\nworld', event='message', id='7', retry=1000)
                yield 'done'

            return EventStreamResponse(source(), heartbeat=None)

        response = await app.test_client().get('/events')

        self.assertEqual(response.headers['Content-Type'], 'text/event-stream; charset=utf-8')
        self.assertEqual(
            response.body,
            b'event: message\nid: 7\nretry: 1000\ndata: hello\ndata: world\n\ndata: done\n\n',
        )


class ExtensionTests(IsolatedAsyncioTestCase):
    async def test_early_hints_trailers_and_after_send(self) -> None:
        app = Ryuuseigun(__name__)
        completed: list[str] = []

        @app.get('/extended')
        async def extended(_req) -> Response:
            res = Response(
                'body',
                trailers={'Digest': 'sha-256=result'},
                early_hints=('</style.css>; rel=preload; as=style',),
            )

            @res.after_send
            async def complete() -> None:
                completed.append('done')

            return res

        response = await app.test_client().get(
            '/extended',
            extensions={'http.response.early_hint': {}, 'http.response.trailers': {}},
        )

        self.assertEqual(response.early_hints, ('</style.css>; rel=preload; as=style',))
        self.assertEqual(response.trailers['Digest'], 'sha-256=result')
        self.assertEqual(completed, ['done'])

    async def test_unsupported_extensions_are_safely_skipped(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/')
        async def index(_req) -> Response:
            return Response('ok', trailers={'Digest': 'value'}, early_hints=('</asset>',))

        response = await app.test_client().get('/')

        self.assertEqual(response.body, b'ok')
        self.assertEqual(response.early_hints, ())
        self.assertEqual(len(response.trailers), 0)


class QueryMethodTests(IsolatedAsyncioTestCase):
    async def test_query_decorator_body_and_discovery(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/records')
        async def records(_req) -> str:
            return 'records'

        @app.query('/records', accepts=('application/json', 'application/sql'))
        async def query_records(req: Request) -> dict[str, object]:
            return {'method': req.method, 'query': await req.json()}

        client = app.test_client()
        response = await client.query('/records', json={'path': '$.items[*]'})
        options = await client.request('OPTIONS', '/records')
        head = await client.request('HEAD', '/records')

        self.assertEqual(response.json(), {'method': 'QUERY', 'query': {'path': '$.items[*]'}})
        self.assertEqual(response.headers['Accept-Query'], '"application/json", "application/sql"')
        self.assertIn('QUERY', options.headers['Allow'])
        self.assertEqual(options.headers['Accept-Query'], response.headers['Accept-Query'])
        self.assertEqual(head.headers['Accept-Query'], response.headers['Accept-Query'])

    async def test_query_requires_and_validates_content_type(self) -> None:
        app = Ryuuseigun(__name__)

        @app.query('/search', accepts=('application/json',))
        async def search(req: Request) -> dict[str, object]:
            return await req.json()

        client = app.test_client()
        missing = await client.query('/search', body='{}')
        unsupported = await client.query('/search', body='query', headers={'Content-Type': 'application/sql'})

        self.assertEqual(missing.status_code, 400)
        self.assertEqual(unsupported.status_code, 415)
        self.assertEqual(unsupported.headers['Accept-Query'], '"application/json"')

    async def test_module_query_and_generic_query_route(self) -> None:
        app = Ryuuseigun(__name__)
        feature = Module('search', url_prefix='/search')

        @feature.query('/module', accepts=('text/plain',))
        async def module_query(req: Request) -> str:
            return await req.text()

        @app.route('/generic', methods=('QUERY',))
        async def generic_query(req: Request) -> str:
            return await req.text()

        app.register_module(feature)
        client = app.test_client()
        module_response = await client.query('/search/module', body='needle', headers={'Content-Type': 'text/plain'})
        generic_response = await client.query('/generic', body='needle', headers={'Content-Type': 'text/plain'})

        self.assertEqual(module_response.text, 'needle')
        self.assertEqual(module_response.headers['Accept-Query'], '"text/plain"')
        self.assertEqual(generic_response.text, 'needle')

    async def test_invalid_query_media_type_is_rejected_at_registration(self) -> None:
        app = Ryuuseigun(__name__)

        with self.assertRaisesRegex(ValueError, 'Invalid QUERY media type'):
            app.query('/search', accepts=('application/json; charset=utf-8',))


class FileAndCompressionTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        temporary = NamedTemporaryFile(delete=False)
        temporary.write(b'0123456789')
        temporary.close()
        self.path = Path(temporary.name)

    async def asyncTearDown(self) -> None:
        self.path.unlink(missing_ok=True)

    async def test_file_ranges_conditionals_and_pathsend(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/file')
        async def file(_req) -> FileResponse:
            return FileResponse(self.path)

        client = app.test_client()
        full = await client.get('/file')
        partial = await client.get('/file', headers={'Range': 'bytes=2-5'})
        cached = await client.get('/file', headers={'If-None-Match': full.headers['ETag']})

        self.assertEqual(full.body, b'0123456789')
        self.assertEqual(partial.status_code, 206)
        self.assertEqual(partial.body, b'2345')
        self.assertEqual(partial.headers['Content-Range'], 'bytes 2-5/10')
        self.assertEqual(cached.status_code, 304)

        incoming = Queue()
        messages: list[ASGIMessage] = []
        await incoming.put({'type': 'http.request', 'body': b'', 'more_body': False})

        async def send(message: ASGIMessage) -> None:
            messages.append(message)

        await app(scope('/file', extensions=('http.response.pathsend',)), incoming.get, send)
        pathsend = next(message for message in messages if message['type'] == 'http.response.pathsend')
        self.assertEqual(Path(pathsend['path']), self.path.resolve())

    async def test_range_on_empty_file_is_unsatisfiable(self) -> None:
        self.path.write_bytes(b'')
        app = Ryuuseigun(__name__)

        @app.get('/file')
        async def file(_req) -> FileResponse:
            return FileResponse(self.path)

        res = await app.test_client().get('/file', headers={'Range': 'bytes=-1'})

        self.assertEqual(res.status_code, StatusCode.RANGE_NOT_SATISFIABLE)
        self.assertEqual(res.headers['Content-Range'], 'bytes */0')

    async def test_buffered_and_streaming_compression(self) -> None:
        app = Ryuuseigun(__name__)
        app.after_request(Compression(minimum_size=0, encodings=('gzip',)))

        @app.get('/plain')
        async def plain(_req) -> str:
            return 'compress me'

        @app.get('/stream')
        async def stream(_req) -> StreamingResponse:
            async def chunks():
                yield 'one'
                yield 'two'

            return StreamingResponse(chunks(), media_type='text/plain')

        plain_response = await app.test_client().get('/plain', headers={'Accept-Encoding': 'gzip'})
        stream_response = await app.test_client().get('/stream', headers={'Accept-Encoding': 'gzip'})

        self.assertEqual(gzip.decompress(plain_response.body), b'compress me')
        self.assertEqual(gzip.decompress(stream_response.body), b'onetwo')
        self.assertEqual(plain_response.headers['Vary'], 'Accept-Encoding')

    async def test_reused_buffered_response_remains_compressed(self) -> None:
        app = Ryuuseigun(__name__)
        app.after_request(Compression(minimum_size=0, encodings=('gzip',)))
        shared = Response('reusable')

        @app.get('/')
        async def index(_req) -> Response:
            return shared

        client = app.test_client()
        first = await client.get('/', headers={'Accept-Encoding': 'gzip'})
        second = await client.get('/', headers={'Accept-Encoding': 'gzip'})

        self.assertEqual(gzip.decompress(first.body), b'reusable')
        self.assertEqual(gzip.decompress(second.body), b'reusable')


class LifecycleAndWebSocketTests(IsolatedAsyncioTestCase):
    async def test_lifecycle_hooks_and_contexts(self) -> None:
        app = Ryuuseigun(__name__)
        calls: list[str] = []

        @app.lifespan
        @asynccontextmanager
        async def resources(current: Ryuuseigun):
            current.state.pool = 'ready'
            calls.append('enter')
            yield
            calls.append('exit')

        @app.startup
        async def startup() -> None:
            calls.append(app.state.pool)

        @app.shutdown
        async def shutdown() -> None:
            calls.append('shutdown')

        pending = [{'type': 'lifespan.startup'}, {'type': 'lifespan.shutdown'}]
        sent: list[ASGIMessage] = []

        async def receive() -> ASGIMessage:
            return pending.pop(0)

        async def send(message: ASGIMessage) -> None:
            sent.append(message)

        await app({'type': 'lifespan'}, receive, send)

        self.assertEqual(calls, ['enter', 'ready', 'shutdown', 'exit'])
        self.assertEqual(sent[-1]['type'], 'lifespan.shutdown.complete')

    async def test_lifespan_context_closes_when_shutdown_handler_fails(self) -> None:
        app = Ryuuseigun(__name__)
        closed = Event()

        @app.lifespan
        @asynccontextmanager
        async def resources(current: Ryuuseigun):
            yield
            closed.set()

        @app.shutdown
        async def shutdown() -> None:
            raise RuntimeError('shutdown failed')

        pending = [{'type': 'lifespan.startup'}, {'type': 'lifespan.shutdown'}]
        sent: list[ASGIMessage] = []

        async def receive() -> ASGIMessage:
            return pending.pop(0)

        async def send(message: ASGIMessage) -> None:
            sent.append(message)

        await app({'type': 'lifespan'}, receive, send)

        self.assertTrue(closed.is_set())
        self.assertEqual(sent[-1]['type'], ASGIMessageType.LIFESPAN_SHUTDOWN_FAILED)

    async def test_websocket_routes_modules_middleware_and_json(self) -> None:
        app = Ryuuseigun(__name__)
        feature = Module('chat', url_prefix='/chat')
        calls: list[str] = []

        async def route_middleware(socket: WebSocket, next: WebSocketNext) -> None:
            calls.append('route-before')
            await next(socket)
            calls.append('route-after')

        @feature.websocket_middleware
        async def feature_middleware(socket: WebSocket, next: WebSocketNext) -> None:
            calls.append('feature-before')
            await next(socket)
            calls.append('feature-after')

        @feature.websocket('/<int:room>', middlewares=(route_middleware,))
        async def chat(socket: WebSocket, room: int) -> None:
            await socket.accept('json')
            value = await socket.receive_json()
            await socket.send_json({'room': room, 'value': value})

        app.register_module(feature)

        async with app.test_client().websocket('/chat/4', subprotocols=('json',)) as socket:
            self.assertEqual(socket.accepted_subprotocol, 'json')
            await socket.send_json({'message': 'hello'})
            self.assertEqual(await socket.receive_json(), {'room': 4, 'value': {'message': 'hello'}})

        self.assertEqual(calls, ['feature-before', 'route-before', 'route-after', 'feature-after'])

    async def test_websocket_denial_response(self) -> None:
        app = Ryuuseigun(__name__)

        with self.assertRaises(WebSocketUpgradeError) as caught:
            async with app.test_client().websocket('/missing'):
                pass

        self.assertEqual(caught.exception.status_code, 404)
        self.assertEqual(caught.exception.body, b'WebSocket route not found')

    async def test_websocket_json_options_and_automatic_close_are_configurable(self) -> None:
        async def run(websocket_auto_close: bool) -> list[ASGIMessage]:
            app = Ryuuseigun(
                __name__,
                config=Config(
                    json_options=OPT_SORT_KEYS,
                    websocket_auto_close=websocket_auto_close,
                    default_response_headers=(('X-Framework', 'ryuuseigun'),),
                ),
            )

            @app.websocket('/')
            async def socket_handler(socket: WebSocket) -> None:
                await socket.accept()
                await socket.send_json({'z': 1, 'a': 2})

            pending = [{'type': ASGIMessageType.WEBSOCKET_CONNECT}]
            sent: list[ASGIMessage] = []

            async def receive() -> ASGIMessage:
                return pending.pop(0)

            async def send(message: ASGIMessage) -> None:
                sent.append(message)

            websocket_scope: ASGIScope = {
                'type': ASGIScopeType.WEBSOCKET,
                'path': '/',
                'query_string': b'',
                'headers': [(b'host', b'testserver')],
                'subprotocols': [],
                'extensions': {},
            }
            await app(websocket_scope, receive, send)
            return sent

        automatic = await run(True)
        manual = await run(False)

        self.assertIn((b'x-framework', b'ryuuseigun'), automatic[0]['headers'])
        self.assertEqual(automatic[1]['bytes'], b'{"a":2,"z":1}')
        self.assertEqual(automatic[-1]['type'], ASGIMessageType.WEBSOCKET_CLOSE)
        self.assertEqual(manual[1]['bytes'], b'{"a":2,"z":1}')
        self.assertNotIn(ASGIMessageType.WEBSOCKET_CLOSE, [message['type'] for message in manual])
