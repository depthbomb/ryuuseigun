from __future__ import annotations
from pathlib import Path
from gzip import decompress
from typing import Optional
from unittest.mock import patch
from types import SimpleNamespace
from contextvars import ContextVar
from contextlib import asynccontextmanager
from ryuuseigun.exceptions import HTTPException
from unittest import IsolatedAsyncioTestCase
from ryuuseigun.testing import WebSocketTestSession
from ryuuseigun.types import ASGIScope, ASGIMessage
from tempfile import TemporaryDirectory, SpooledTemporaryFile
from ryuuseigun.request import _content_type_header, _parse_urlencoded_form
from asyncio import Event, Queue, sleep, gather, wait_for, all_tasks, create_task, CancelledError
from ryuuseigun import (
    Next,
    Config,
    Module,
    Ryuuseigun,
    Headers,
    Request,
    Response,
    WebSocket,
    UploadFile,
    Compression,
    FileResponse,
    ServerSentEvent,
    StreamingResponse,
    EventStreamResponse,
)


def scope(method: str = 'GET', headers: Optional[dict[str, str]] = None) -> ASGIScope:
    return {
        'type': 'http', 'path': '/', 'method': method, 'query_string': b'',
        'headers': Headers(headers).raw(),
    }


async def collect(response: Response, request_scope: Optional[ASGIScope] = None, *, head: bool = False):
    messages: list[ASGIMessage] = []

    async def send(message: ASGIMessage) -> None:
        messages.append(message)

    await response.send(send, scope=request_scope, head=head)
    return messages


class StreamOwnershipTests(IsolatedAsyncioTestCase):
    async def test_synchronous_yields_remain_cancellable(self) -> None:
        entered, closed = Event(), Event()

        async def source():
            try:
                while True:
                    entered.set()
                    yield b'chunk'
            finally:
                closed.set()

        task = create_task(collect(StreamingResponse(source())))
        await entered.wait()
        task.cancel()
        with self.assertRaises(CancelledError):
            await wait_for(task, 1)
        self.assertTrue(closed.is_set())

    async def test_sse_source_cancellation_propagates_without_hanging(self) -> None:
        before = all_tasks()

        async def source():
            yield 'hello'
            raise CancelledError

        with self.assertRaises(CancelledError):
            await wait_for(collect(EventStreamResponse(source(), heartbeat=None)), 1)
        self.assertEqual(all_tasks(), before)

    async def test_cancel_joins_producer_even_during_second_cancellation(self) -> None:
        before = all_tasks()
        entered, closing, release, closed = Event(), Event(), Event(), Event()

        async def source():
            try:
                entered.set()
                await Event().wait()
                yield b'unreachable'
            finally:
                closing.set()
                await release.wait()
                closed.set()

        task = create_task(collect(StreamingResponse(source())))
        await entered.wait()
        task.cancel('original cancellation')
        await closing.wait()
        task.cancel('second cancellation')
        await sleep(0)
        self.assertFalse(task.done())
        release.set()
        with self.assertRaisesRegex(CancelledError, 'original cancellation'):
            await wait_for(task, 1)
        self.assertTrue(closed.is_set())
        self.assertEqual(all_tasks(), before)

    async def test_generator_context_persists_and_closes_after_send_failure(self) -> None:
        for sse in (False, True):
            for heartbeat in (None, 0.01):
                with self.subTest(sse=sse, heartbeat=heartbeat):
                    state = ContextVar('state', default='outer')
                    seen: list[str] = []
                    closed = Event()

                    async def source(state=state, seen=seen, closed=closed):
                        token = state.set('inner')
                        try:
                            yield 'first'
                            seen.append(state.get())
                            yield 'second'
                            await Event().wait()
                        finally:
                            state.reset(token)
                            closed.set()

                    response = EventStreamResponse(source(), heartbeat=heartbeat) if sse else StreamingResponse(source())
                    count = 0

                    async def send(message: ASGIMessage) -> None:
                        nonlocal count
                        if message['type'] == 'http.response.body':
                            count += 1
                            if count == 2:
                                raise OSError('closed')

                    before = all_tasks()
                    await wait_for(response.send(send), 1)
                    self.assertEqual(seen, ['inner'])
                    self.assertEqual(state.get(), 'outer')
                    self.assertTrue(closed.is_set())
                    self.assertEqual(all_tasks(), before)

    async def test_sse_closes_custom_iterator(self) -> None:
        class Source:
            closed = False

            def __aiter__(self):
                return self

            async def __anext__(self):
                return 'hello'

            async def aclose(self):
                self.closed = True

        for heartbeat in (None, 0.01):
            source = Source()

            async def send(message: ASGIMessage) -> None:
                if message['type'] == 'http.response.body':
                    raise OSError('closed')

            await EventStreamResponse(source, heartbeat=heartbeat).send(send)
            self.assertTrue(source.closed)

    async def test_sse_heartbeat_and_source_failure_leave_no_tasks(self) -> None:
        release = Event()
        before = all_tasks()

        async def source():
            await release.wait()
            yield 'last'
            raise ValueError('source failed')

        async def send(message: ASGIMessage) -> None:
            if message.get('body') == b': heartbeat\n\n':
                release.set()

        with self.assertRaisesRegex(ValueError, 'source failed'):
            await wait_for(EventStreamResponse(source(), heartbeat=0.001).send(send), 1)
        self.assertTrue(release.is_set())
        self.assertEqual(all_tasks(), before)

    async def test_watcher_buffers_unread_body_and_counts_each_body_once(self) -> None:
        incoming: Queue[ASGIMessage] = Queue()
        drained = Event()
        for body in (b'ab', b'cd', b'ef'):
            incoming.put_nowait({'type': 'http.request', 'body': body, 'more_body': body != b'ef'})

        async def receive():
            if incoming.empty():
                drained.set()
            return await incoming.get()

        request = Request(scope('POST'), receive, 6, state=SimpleNamespace())
        watcher = create_task(request.wait_for_disconnect())
        await wait_for(drained.wait(), 1)
        self.assertEqual(request._received_size, 6)
        self.assertEqual(incoming.qsize(), 0)
        self.assertEqual(await request.body(), b'abcdef')
        self.assertEqual(request._received_size, 6)
        incoming.put_nowait({'type': 'http.disconnect'})
        await wait_for(watcher, 1)
        await request.close()

    async def test_watcher_limit_failure_terminates_committed_response(self) -> None:
        entered, closed = Event(), Event()
        messages: list[ASGIMessage] = []
        before = all_tasks()

        async def receive() -> ASGIMessage:
            await entered.wait()
            return {'type': 'http.request', 'body': b'12345', 'more_body': True}

        async def source():
            try:
                entered.set()
                await Event().wait()
                yield b'never'
            finally:
                closed.set()

        async def send(message: ASGIMessage) -> None:
            messages.append(message)

        request = Request(scope('POST'), receive, 4, state=SimpleNamespace())
        with self.assertRaises(HTTPException) as caught:
            await wait_for(StreamingResponse(source()).send(send, receive=receive, request=request), 1)
        self.assertEqual(caught.exception.status_code, 413)
        self.assertEqual([message['type'] for message in messages], ['http.response.start'])
        self.assertTrue(closed.is_set())
        self.assertEqual(all_tasks(), before)
        await request.close()


class ResponseRegressionTests(IsolatedAsyncioTestCase):
    async def test_trailers_follow_a_complete_body(self) -> None:
        async def source():
            yield b'hello'

        for response in (Response('hello', trailers={'X-End': 'yes'}),
                         StreamingResponse(source(), trailers={'X-End': 'yes'})):
            started = ended = trailers = False

            async def send(message: ASGIMessage) -> None:
                nonlocal started, ended, trailers
                match message['type']:
                    case 'http.response.start':
                        self.assertFalse(started)
                        started = True
                        self.assertTrue(message['trailers'])
                    case 'http.response.body':
                        self.assertTrue(started)
                        self.assertFalse(ended)
                        ended = not message.get('more_body', False)
                    case 'http.response.trailers':
                        self.assertTrue(ended)
                        trailers = True

            await response.send(send, scope={'extensions': {'http.response.trailers': {}}})
            self.assertTrue(trailers)

    async def test_reused_response_negotiates_per_request_concurrently(self) -> None:
        app = Ryuuseigun(__name__)
        app.after_request(Compression(minimum_size=0, encodings=('gzip',)))
        template = Response(b'hello' * 20000, headers={'ETag': '"original"'})
        template.headers.add('Set-Cookie', 'a=1')
        template.headers.add('Set-Cookie', 'b=2')

        @app.get('/')
        async def index(_req):
            return template

        client = app.test_client()
        encodings = ['gzip', 'identity', 'gzip;q=0', 'gzip']
        responses = await gather(*(client.get('/', headers={'Accept-Encoding': value}) for value in encodings))
        for encoding, response in zip(encodings, responses, strict=True):
            self.assertIn('Accept-Encoding', response.headers['Vary'])
            self.assertEqual(response.headers.getlist('Set-Cookie'), ['a=1', 'b=2'])
            if encoding == 'gzip':
                self.assertEqual(response.headers['Content-Encoding'], 'gzip')
                self.assertEqual(decompress(response.body), template.body)
                self.assertNotIn('ETag', response.headers)
            else:
                self.assertNotIn('Content-Encoding', response.headers)
                self.assertEqual(response.body, template.body)
                self.assertEqual(response.headers['ETag'], '"original"')
            self.assertEqual(int(response.headers['Content-Length']), len(response.body))
        self.assertEqual(template.body, b'hello' * 20000)
        self.assertNotIn('Content-Encoding', template.headers)

    async def test_sse_roundtrips_text_and_validates_mutable_metadata(self) -> None:
        for value in ('', 'hello\n', '\n\n', 'hello\u2028world', 'a\rb\r\nc'):
            encoded = ServerSentEvent(value).encode().decode()
            data = '\n'.join(line.removeprefix('data: ') for line in encoded.split('\n') if line.startswith('data: '))
            self.assertEqual(data, value.replace('\r\n', '\n').replace('\r', '\n'))
        for field, invalid in (('event', 'x\ny'), ('event', 'x\ry'), ('id', 'x\ny'), ('id', 'x\ry'), ('id', 'x\0y')):
            event = ServerSentEvent('data')
            setattr(event, field, invalid)
            with self.assertRaises(ValueError):
                event.encode()

    async def test_cookie_fields_are_joined(self) -> None:
        request_scope = scope()
        request_scope['headers'] = [(b'cookie', b'session=abc'), (b'cookie', b'pref=dark')]
        request = Request(request_scope, Queue().get, state=SimpleNamespace())
        self.assertEqual(request.cookies, {'session': 'abc', 'pref': 'dark'})

    async def test_file_range_and_validator_matrix(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'file.txt'
            path.write_bytes(b'0123456789')
            cases = [
                ('HEAD', {'Range': 'bytes=0-1'}, 200, b''),
                ('POST', {'Range': 'bytes=0-1'}, 200, b'0123456789'),
                ('GET', {'Range': 'items=0-1'}, 200, b'0123456789'),
                ('GET', {'Range': 'bytes=999-', 'If-Range': '"stale"'}, 200, b'0123456789'),
                ('GET', {'Range': 'bytes=999-', 'If-Range': '"custom"'}, 416, b''),
                ('GET', {'Range': 'bytes=0-1', 'If-Range': '"custom"'}, 206, b'01'),
                ('GET', {'Range': 'bytes=0-1', 'If-Range': 'W/"custom"'}, 200, b'0123456789'),
                ('GET', {'If-Match': '"custom"', 'If-Unmodified-Since': 'Thu, 01 Jan 1970 00:00:00 GMT'}, 200, b'0123456789'),
                ('GET', {'If-Unmodified-Since': 'invalid'}, 200, b'0123456789'),
                ('GET', {'If-Match': '"stale"'}, 412, b''),
                ('GET', {'If-None-Match': '"custom"'}, 304, b''),
                ('GET', {'If-None-Match': 'W/"custom"'}, 304, b''),
            ]
            template = FileResponse(path, headers={'ETag': '"custom"'})
            for method, headers, status, body in cases:
                with self.subTest(method=method, headers=headers):
                    messages = await collect(template, scope(method, headers), head=method == 'HEAD')
                    self.assertEqual(messages[0]['status'], status)
                    self.assertEqual(b''.join(message.get('body', b'') for message in messages), body)
                    if method == 'HEAD':
                        self.assertEqual(Headers.from_raw(messages[0]['headers'])['Content-Length'], '10')
            self.assertEqual(template.status_code, 200)

    async def test_file_bodyless_and_error_statuses_ignore_ranges_and_pathsend(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'file.txt'
            path.write_bytes(b'0123456789')
            for status in (103, 204, 304, 404):
                request_scope = scope(headers={'Range': 'bytes=0-1'})
                request_scope['extensions'] = {'http.response.pathsend': {}}
                messages = await collect(FileResponse(path, status_code=status), request_scope)
                self.assertEqual(messages[0]['status'], status)
                if status != 404:
                    self.assertEqual([message['type'] for message in messages], ['http.response.start', 'http.response.body'])
                    self.assertEqual(messages[1]['body'], b'')
                    if status != 304:
                        self.assertNotIn('Content-Length', Headers.from_raw(messages[0]['headers']))


class BindingAndRoutingTests(IsolatedAsyncioTestCase):
    async def test_two_argument_hooks_keep_optional_request_and_response_injection(self) -> None:
        app = Ryuuseigun(__name__)

        @app.before_request
        async def before(connection: Optional[Request] = None):
            self.assertIsInstance(connection, Request)

        @app.after_request
        async def after(connection: Request, response: Optional[Response] = None):
            self.assertIsInstance(connection, Request)
            self.assertIsInstance(response, Response)
            return response

        @app.errorhandler(ValueError)
        async def error_handler(connection: Request, error: Optional[ValueError] = None):
            self.assertIsInstance(connection, Request)
            self.assertIsInstance(error, ValueError)
            return Response('handled')

        @app.get('/')
        async def index(_req):
            raise ValueError('test')

        self.assertEqual((await app.test_client().get('/')).text, 'handled')

    async def test_user_middleware_can_catch_propagated_error_handler_failure(self) -> None:
        app = Ryuuseigun(__name__, config=Config(propagate_exceptions=True))

        @app.middleware
        async def catch(req: Request, next: Next):
            try:
                await next(req)
            except RuntimeError:
                await req.respond(Response('caught'))

        @app.errorhandler(ValueError)
        async def fail(_req, error: ValueError):
            raise RuntimeError('handler')

        @app.get('/')
        async def index(_req):
            raise ValueError('original')

        self.assertEqual((await app.test_client().get('/')).text, 'caught')

    async def test_host_validation_rejects_reported_malformed_authorities(self) -> None:
        app = Ryuuseigun(__name__, config=Config(trusted_hosts=('*.example.org', '[::1]')))

        @app.get('/')
        async def index(_req):
            return 'ok'

        for host in ('evil.com\\x.example.org', 'evil.com/x.example.org', 'evil.com@x.example.org',
                     'evil.com, x.example.org'):
            response = await app.test_client().get('/', headers={'Host': host})
            self.assertEqual(response.status_code, 400)
        for host in ('a.example.org', 'a.example.org:8080', '[::1]:8080'):
            response = await app.test_client().get('/', headers={'Host': host})
            self.assertEqual(response.status_code, 200)

    async def test_routes_bind_request_positionally_and_validate_path_arguments(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/')
        async def index(connection: Request[SimpleNamespace]):
            return connection.path

        @app.websocket('/socket')
        async def socket_route(connection: WebSocket):
            await connection.accept()
            await connection.send_text(connection.path)

        async def colliding(req):
            return 'bad'

        async def unexpected(socket):
            pass

        async def positional(socket, value, /):
            pass

        for path, handler in (('/<value>', unexpected), ('/<value>', positional)):
            with self.assertRaises(TypeError):
                app.websocket(path)(handler)
        with self.assertRaises(TypeError):
            app.get('/<req>')(colliding)

        async def valid(req, /, **kwargs):
            return kwargs['req']

        app.get('/<req>')(valid)
        self.assertEqual((await app.test_client().get('/')).text, '/')
        async with app.test_client().websocket('/socket') as session:
            self.assertEqual(await session.receive_text(), '/socket')

        self.assertEqual((await app.test_client().get('/works')).text, 'works')

    async def test_typed_optional_hook_arguments_keep_defaults(self) -> None:
        app = Ryuuseigun(__name__)
        seen = []

        @app.before_request
        async def before(_req, label: str = 'before'):
            seen.append(label)

        @app.before_request
        async def with_request(req: Optional[Request] = None):
            self.assertIsInstance(req, Request)

        @app.after_request
        async def after(_req, response: Response, label: str = 'after'):
            self.assertIsInstance(response, Response)
            seen.append(label)
            return response

        @app.errorhandler(ValueError)
        async def error_handler(_req, error: ValueError, label: str = 'error'):
            self.assertIsInstance(error, ValueError)
            seen.append(label)
            return Response('handled')

        @app.get('/')
        async def index(_req):
            raise ValueError('original')

        self.assertEqual((await app.test_client().get('/')).text, 'handled')
        self.assertEqual(seen, ['before', 'error', 'after'])

    async def test_hook_arguments_are_positional_regardless_of_names(self) -> None:
        app = Ryuuseigun(__name__)
        seen = []

        async def hook(first, second='default'):
            seen.append((isinstance(first, Request), isinstance(second, Response)))
            return second

        app.after_request(hook)
        self.assertEqual((await app.test_client().get('/missing')).status_code, 404)
        self.assertEqual(seen, [(True, True)])

    async def test_propagated_error_is_not_dispatched_again_by_outer_wrappers(self) -> None:
        for count in (0, 1, 2):
            app = Ryuuseigun(__name__, config=Config(propagate_exceptions=True))
            calls = []

            async def middleware(req: Request, next: Next):
                return await next(req)

            for _ in range(count):
                app.middleware(middleware)

            @app.before_request
            async def before(_req):
                pass

            @app.errorhandler(ValueError)
            async def error_handler(_req, error: ValueError, calls=calls):
                calls.append('value')
                raise RuntimeError('handler failed')

            @app.errorhandler(RuntimeError)
            async def other_handler(_req, error: RuntimeError, calls=calls):
                calls.append('runtime')
                return 'unexpected'

            @app.get('/')
            async def index(_req):
                raise ValueError('original')

            with self.assertRaisesRegex(RuntimeError, 'handler failed'):
                await app.test_client().get('/')
            self.assertEqual(calls, ['value'])

    async def test_module_preserves_strict_slashes_for_http_and_websocket(self) -> None:
        app = Ryuuseigun(__name__, strict_slashes=True)
        parent = Module('parent', url_prefix='/parent/')
        child = Module('child', url_prefix='/child/')

        @child.get('/item/')
        async def item(_req):
            return 'ok'

        @child.websocket('/socket/')
        async def socket_route(socket: WebSocket):
            await socket.accept()

        parent.register_module(child)
        app.register_module(parent)
        self.assertEqual((await app.test_client().get('/parent/child/item/')).status_code, 200)
        self.assertEqual((await app.test_client().get('/parent/child/item')).status_code, 404)
        async with app.test_client().websocket('/parent/child/socket/'):
            pass

    async def test_redirects_encode_decoded_paths_and_preserve_query(self) -> None:
        app = Ryuuseigun(__name__, config=Config(strict_slashes=True, redirect_slashes=True))
        for index, path in enumerate(('/hello?world/', '/snow☃/', '/\\evil.example/', '/a%b#c/')):
            async def endpoint(_req):
                return 'ok'
            app.add_url_rule(path, endpoint, endpoint=str(index))
        for path, location in (('/hello%3Fworld', '/hello%3Fworld/'), ('/snow☃', '/snow%E2%98%83/'),
                               ('/%5Cevil.example', '/%5Cevil.example/'), ('/a%25b%23c', '/a%25b%23c/')):
            response = await app.test_client().get(f'{path}?a=b%20c')
            self.assertEqual(response.status_code, 308)
            self.assertEqual(response.headers['Location'], f'{location}?a=b%20c')

    async def test_test_client_matches_asgi_url_decoding(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/<path:value>')
        async def index(req: Request, value: str):
            return {'value': value, 'raw': req.scope['raw_path'].decode('ascii')}

        @app.websocket('/<path:value>')
        async def socket_route(socket: WebSocket, value: str):
            await socket.accept()
            await socket.send_text(value)

        for path, value, raw in (('/caf%C3%A9', 'café', '/caf%C3%A9'), ('/café', 'café', '/caf%C3%A9'),
                                 ('/a%2Fb', 'a/b', '/a%2Fb'), ('/%252F', '%2F', '/%252F')):
            self.assertEqual((await app.test_client().get(path)).json(), {'value': value, 'raw': raw})
            async with app.test_client().websocket(path) as session:
                self.assertEqual(await session.receive_text(), value)


class LifecycleRegressionTests(IsolatedAsyncioTestCase):
    async def test_lifespan_cancellation_and_receive_failure_close_resources(self) -> None:
        for phase in ('startup', 'idle', 'receive', 'send'):
            app = Ryuuseigun(__name__)
            entered, closed = Event(), Event()
            incoming: Queue[ASGIMessage] = Queue()
            incoming.put_nowait({'type': 'lifespan.startup'})

            @app.lifespan
            @asynccontextmanager
            async def resources(app, closed=closed):
                try:
                    yield
                finally:
                    closed.set()

            @app.startup
            async def startup(phase=phase, entered=entered):
                if phase == 'startup':
                    entered.set()
                    await Event().wait()

            async def receive(phase=phase, incoming=incoming):
                if phase == 'receive' and incoming.empty():
                    raise ValueError('receive failed')
                return await incoming.get()

            async def send(message, entered=entered, phase=phase):
                entered.set()
                if phase == 'send':
                    raise ValueError('send failed')

            task = create_task(app({'type': 'lifespan'}, receive, send))
            await entered.wait()
            if phase in ('receive', 'send'):
                with self.assertRaises(ValueError):
                    await task
            else:
                task.cancel()
                with self.assertRaises(CancelledError):
                    await task
            self.assertTrue(closed.is_set(), phase)

    async def test_startup_and_rollback_failures_are_both_reported(self) -> None:
        app = Ryuuseigun(__name__)
        calls = []

        @app.lifespan
        @asynccontextmanager
        async def resources(app):
            try:
                yield
            finally:
                calls.append('closed')
                raise RuntimeError('rollback failed')

        @app.startup
        async def startup():
            raise ValueError('startup failed')

        async def receive():
            return {'type': 'lifespan.startup'}

        messages = []

        async def send(message):
            messages.append(message)

        await app({'type': 'lifespan'}, receive, send)
        self.assertEqual(calls, ['closed'])
        self.assertEqual(messages[0]['type'], 'lifespan.startup.failed')
        self.assertIn('startup failed', messages[0]['message'])
        self.assertIn('rollback failed', messages[0]['message'])

    async def test_cancelled_websocket_entry_joins_application(self) -> None:
        app = Ryuuseigun(__name__)
        entered, closed = Event(), Event()

        @app.websocket('/')
        async def socket_route(socket: WebSocket):
            try:
                entered.set()
                await Event().wait()
            finally:
                closed.set()

        before = all_tasks()
        session = WebSocketTestSession(app, '/')
        task = create_task(session.__aenter__())
        await entered.wait()
        task.cancel()
        with self.assertRaises(CancelledError):
            await task
        self.assertTrue(closed.is_set())
        self.assertTrue(session._task.done())
        self.assertEqual(all_tasks(), before)

    async def test_reject_after_close_does_not_send_twice(self) -> None:
        messages = []

        async def receive():
            return {'type': 'websocket.connect'}

        async def send(message):
            messages.append(message)

        socket = WebSocket({'type': 'websocket', 'path': '/', 'extensions': {'websocket.http.response': {}}}, receive, send)
        await socket.reject()
        with self.assertRaises(RuntimeError):
            await socket.reject()
        self.assertEqual(len(messages), 2)


class FormRegressionTests(IsolatedAsyncioTestCase):
    async def test_cancelled_form_collection_closes_active_spooled_file(self) -> None:
        boundary = 'cancel-boundary'
        first_chunk = (
            f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="large.bin"\r\n\r\n'
            'partial'
        ).encode()
        waiting = Event()
        blocker = Event()
        calls = 0

        async def receive() -> ASGIMessage:
            nonlocal calls
            calls += 1
            if calls == 1:
                return {'type': 'http.request', 'body': first_chunk, 'more_body': True}
            waiting.set()
            await blocker.wait()
            return {'type': 'http.disconnect'}

        files: list[object] = []

        def make_file(*args, **kwargs):
            file = SpooledTemporaryFile(*args, **kwargs)
            files.append(file)
            return file

        req = Request(
            scope('POST', {'Content-Type': f'multipart/form-data; boundary={boundary}'}),
            receive,
            state=SimpleNamespace(),
        )
        with patch('ryuuseigun.request.SpooledTemporaryFile', make_file):
            task = create_task(req.form())
            await waiting.wait()
            task.cancel()
            with self.assertRaises(CancelledError):
                await task

        self.assertEqual(len(files), 1)
        self.assertTrue(files[0].closed)
        await req.close()

    async def test_streaming_multipart_handles_boundaries_split_between_messages(self) -> None:
        boundary = 'bytewise-boundary'
        payload = b'begin\r\n--bytewise-boundaryX\r\nend'
        body = (
            b'preamble\r\n'
            + f'--{boundary}\r\nContent-Disposition: form-data; name="ignored"\r\n\r\nskip me\r\n'.encode()
            + f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="split.bin"\r\n\r\n'.encode()
            + payload
            + f'\r\n--{boundary}--\r\nepilogue'.encode()
        )
        incoming = Queue()
        for index, value in enumerate(body):
            incoming.put_nowait(
                {
                    'type': 'http.request',
                    'body': bytes((value,)),
                    'more_body': index < len(body) - 1,
                }
            )
        req = Request(
            scope('POST', {'Content-Type': f'multipart/form-data; boundary={boundary}'}),
            incoming.get,
            state=SimpleNamespace(),
        )

        parts = req.multipart()
        ignored = await anext(parts)
        upload = await anext(parts)
        self.assertEqual(ignored.name, 'ignored')
        self.assertTrue(ignored.complete)
        self.assertEqual(ignored.size, 7)
        self.assertEqual(upload.filename, 'split.bin')
        self.assertEqual(await upload.read(), payload)
        with self.assertRaises(StopAsyncIteration):
            await anext(parts)
        await req.close()

    async def test_concurrent_multipart_uploads_spool_to_disk_across_small_chunks(self) -> None:
        boundary = 'streaming-boundary'
        first_payload = b'a' * 80_000 + f'\r\n--{boundary}X'.encode() + b'b' * 80_000
        second_payload = bytes(range(256)) * 700
        body = (
            f'--{boundary}\r\nContent-Disposition: form-data; name="title"\r\n\r\nConcurrent\r\n'.encode()
            + f'--{boundary}\r\nContent-Disposition: form-data; name="files"; filename="first.bin"\r\n'
              'Content-Type: application/octet-stream\r\n\r\n'.encode()
            + first_payload
            + f'\r\n--{boundary}\r\nContent-Disposition: form-data; name="files"; filename="second.bin"\r\n'
              'Content-Type: application/octet-stream\r\n\r\n'.encode()
            + second_payload
            + f'\r\n--{boundary}--\r\n'.encode()
        )

        async def parse_upload() -> tuple[bytes, bytes]:
            incoming = Queue()
            chunks = [body[index:index + 4093] for index in range(0, len(body), 4093)]
            for index, chunk in enumerate(chunks):
                incoming.put_nowait(
                    {
                        'type': 'http.request',
                        'body': chunk,
                        'more_body': index < len(chunks) - 1,
                    }
                )
            content_type = f'multipart/form-data; boundary="{boundary}"'
            req = Request(
                scope('POST', {'Content-Type': content_type}),
                incoming.get,
                state=SimpleNamespace(),
                upload_spool_threshold=1024,
            )
            form = await req.form()
            self.assertEqual(form['title'], 'Concurrent')
            uploads = form.getlist('files')
            self.assertEqual(len(uploads), 2)
            self.assertTrue(all(isinstance(upload, UploadFile) for upload in uploads))
            first, second = uploads
            assert isinstance(first, UploadFile)
            assert isinstance(second, UploadFile)
            self.assertFalse(first.in_memory)
            self.assertFalse(second.in_memory)
            self.assertEqual(first.size, len(first_payload))
            self.assertEqual(second.size, len(second_payload))
            first_data = await first.read()
            await second.seek(100)
            second_data = await second.read(200)
            await req.close()
            self.assertTrue(first.closed)
            self.assertTrue(second.closed)
            return first_data, second_data

        results = await gather(*(parse_upload() for _ in range(6)))
        self.assertTrue(all(first == first_payload and second == second_payload[100:300] for first, second in results))

    async def test_large_form_parser_preserves_order_charset_and_cached_result(self) -> None:
        for charset in ('utf-8', 'iso-8859-1'):
            body = ('a=one&a=two&empty=&value=%E9&' if charset == 'iso-8859-1' else
                    'a=one&a=two&empty=&value=%C3%A9&').encode() * 4000
            content_type = f'application/x-www-form-urlencoded; charset={charset}'
            incoming = Queue()
            incoming.put_nowait({'type': 'http.request', 'body': body})
            req = Request(scope('POST', {'Content-Type': content_type}), incoming.get, state=SimpleNamespace())
            form = await req.form()
            expected = _parse_urlencoded_form(body, _content_type_header(content_type))
            self.assertEqual(form.items(), expected.items())
            self.assertIs(await req.form(), form)

    async def test_large_form_retains_malformed_encoding_error(self) -> None:
        incoming = Queue()
        incoming.put_nowait({'type': 'http.request', 'body': b'a=x&' * 20000 + b'bad=%FF'})
        req = Request(scope('POST', {'Content-Type': 'application/x-www-form-urlencoded'}),
                      incoming.get, state=SimpleNamespace())
        with self.assertRaises(HTTPException) as caught:
            await req.form()
        self.assertEqual(caught.exception.status_code, 400)
