from asyncio import Event, sleep, timeout
from contextlib import asynccontextmanager
from unittest import IsolatedAsyncioTestCase
from ryuuseigun.testing import TestClient as Client
from ryuuseigun import Config, Response, Ryuuseigun, StreamingResponse

class StreamingClientTests(IsolatedAsyncioTestCase):
    async def test_chunks_are_bounded_and_resources_survive_until_disconnect(self):
        app = Ryuuseigun(__name__)
        events, produced = [], []

        @app.middleware
        async def resource(req, next):
            events.append('open')
            try:
                await next(req)
            finally:
                events.append('close')

        @app.get('/')
        async def stream(req):
            async def chunks():
                try:
                    for value in range(100):
                        produced.append(value)
                        yield str(value).encode()
                finally:
                    events.append('generator closed')
            return StreamingResponse(chunks())

        async with app.test_client().stream('GET', '/') as response:
            self.assertEqual(response.status_code, 200)
            iterator = response.iter_bytes()
            self.assertEqual(await anext(iterator), b'0')
            await sleep(0)
            self.assertLessEqual(len(produced), 2)
            self.assertEqual(events, ['open'])
            response.disconnect()
        self.assertEqual(events, ['open', 'generator closed', 'close'])

    async def test_client_closes_streams_before_application_resources(self):
        app = Ryuuseigun(__name__)
        events = []

        @app.lifespan
        @asynccontextmanager
        async def resources(app):
            events.append('startup')
            yield
            events.append('shutdown')

        @app.get('/')
        async def infinite(req):
            async def chunks():
                try:
                    yield b'first'
                    await Event().wait()
                finally:
                    events.append('stream closed')
            return StreamingResponse(chunks())

        async with app.test_client() as client:
            response = await client.stream('GET', '/').__aenter__()
            self.assertEqual(await anext(response.iter_bytes()), b'first')
        self.assertEqual(events, ['startup', 'stream closed', 'shutdown'])

    async def test_request_chunks_cookies_trailers_and_root_path(self):
        app = Ryuuseigun(__name__)

        @app.post('/echo')
        async def echo(req):
            received = await req.body()
            async def chunks():
                yield received[:2]
                yield received[2:]
            response = StreamingResponse(chunks(), trailers={'X-End': 'yes'})
            response.set_cookie('seen', '1')
            return response

        @app.get('/cookie')
        async def cookie(req):
            return req.cookies.get('seen', '')

        async def body():
            yield b'ab'
            yield b'cd'

        client = app.test_client(root_path='/prefix')
        async with client.stream('POST', '/prefix/echo', body=body(), extensions={'http.response.trailers': {}}) as response:
            self.assertEqual(await response.read(), b'abcd')
            self.assertEqual(response.trailers['X-End'], 'yes')
        self.assertEqual((await client.get('/prefix/cookie')).text, '1')

    async def test_failure_before_headers_after_headers_and_timeout(self):
        async def failing(scope, receive, send):
            raise RuntimeError('before headers')

        with self.assertRaisesRegex(RuntimeError, 'before headers'):
            async with Client(failing).stream('GET', '/'):
                pass

        app = Ryuuseigun(__name__, config=Config(propagate_exceptions=True))
        @app.get('/')
        async def failure(req):
            async def chunks():
                yield b'first'
                raise RuntimeError('after headers')
            return StreamingResponse(chunks())

        with self.assertRaisesRegex(RuntimeError, 'after headers'):
            async with app.test_client().stream('GET', '/') as response:
                await response.read()

        closed = Event()
        async def stalled(scope, receive, send):
            try:
                await Event().wait()
            finally:
                closed.set()

        with self.assertRaises(TimeoutError):
            async with Client(stalled).stream('GET', '/', timeout_seconds=0.01):
                pass
        self.assertTrue(closed.is_set())

    async def test_arbitrary_wrappers_and_lifespan_state(self):
        async def app(scope, receive, send):
            if scope['type'] == 'lifespan':
                scope['state']['value'] = 'started'
                self.assertEqual((await receive())['type'], 'lifespan.startup')
                await send({'type': 'lifespan.startup.complete'})
                self.assertEqual((await receive())['type'], 'lifespan.shutdown')
                await send({'type': 'lifespan.shutdown.complete'})
                return
            await Response(scope['state']['value']).send(send)

        async def wrapper(scope, receive, send):
            await app(scope, receive, send)

        async with Client(wrapper) as client:
            self.assertEqual((await client.get('/')).text, 'started')
            async with client.stream('GET', '/') as response:
                self.assertEqual(await response.read(), b'started')

    async def test_disconnect_unblocks_an_unfinished_request_upload(self):
        closed = Event()
        app = Ryuuseigun(__name__)

        @app.post('/')
        async def accept(req):
            async def chunks():
                yield b'accepted'
                await Event().wait()
            return StreamingResponse(chunks())

        async def upload():
            try:
                yield b'first'
                await Event().wait()
            finally:
                closed.set()

        async with timeout(2):
            async with app.test_client().stream('POST', '/', body=upload()) as response:
                self.assertEqual(await anext(response.iter_bytes()), b'accepted')
                await sleep(0)
        self.assertTrue(closed.is_set())
