from asyncio import Event
from dataclasses import FrozenInstanceError
from contextlib import asynccontextmanager
from unittest import IsolatedAsyncioTestCase
from ryuuseigun import Config, Module, Request, Response, Ryuuseigun

class CompositionTests(IsolatedAsyncioTestCase):
    async def invoke(self, app, path, root_path=''):
        sent = []

        async def receive():
            await Event().wait()

        async def send(message):
            sent.append(message)

        await app({
            'type': 'http', 'method': 'GET', 'path': path, 'root_path': root_path,
            'query_string': b'', 'headers': [(b'host', b'testserver')],
        }, receive, send)
        return sent

    async def test_prefixes_urls_redirects_and_metadata(self):
        app = Ryuuseigun(__name__, config=Config(strict_slashes=True, redirect_slashes=True))
        module = Module('users', url_prefix='/users')
        seen = []

        @app.middleware
        async def inspect(req, next):
            seen.append(req.route)
            await next(req)

        @module.get('/<int:user>/')
        async def user(req: Request, user: int):
            return req.url_for('users.user', user=user)

        app.register_module(module)
        for path in ('/prefix/users/3/', '/users/3/'):
            result = await self.invoke(app, path, '/prefix')
            self.assertEqual(result[0]['status'], 200)
            self.assertEqual(result[1]['body'], b'/prefix/users/3/')
            self.assertEqual(seen[-1].path, '/users/<int:user>/')
            self.assertEqual(seen[-1].modules, ('users',))
            with self.assertRaises(FrozenInstanceError):
                seen[-1].path = '/changed'

        redirected = await self.invoke(app, '/prefix/users/3', '/prefix')
        self.assertEqual(dict(redirected[0]['headers'])[b'location'], b'/prefix/users/3/')
        self.assertEqual(app.inspect_routes()[0].endpoint, 'users.user')

    async def test_mount_boundaries_nested_urls_and_child_lifespans(self):
        parent = Ryuuseigun('parent')
        child = Ryuuseigun('child')
        events = []

        @child.lifespan
        @asynccontextmanager
        async def resources(app):
            events.append('start')
            yield
            events.append('stop')

        @child.get('/hello')
        async def hello(req):
            return req.url_for('hello')

        parent.mount('/child', child, name='child', lifespan=True)
        self.assertEqual(parent.url_for('child:hello'), '/child/hello')
        async with parent.test_client() as client:
            self.assertEqual((await client.get('/child/hello')).text, '/child/hello')
            self.assertEqual((await client.get('/children/hello')).status_code, 404)
            self.assertEqual(events, ['start'])
        self.assertEqual(events, ['start', 'stop'])
        with self.assertRaises(RuntimeError):
            parent.mount('/late', child, name='late')

    async def test_mount_host_policy_and_startup_rollback(self):
        parent = Ryuuseigun('parent', config=Config(trusted_hosts=('allowed.example',)))
        child = Ryuuseigun('child')

        @child.get('/')
        async def index(req):
            return 'child'

        parent.mount('/child', child, name='child')
        self.assertEqual((await parent.test_client().get('/child/')).status_code, 400)
        events = []
        first, broken, root = Ryuuseigun('first'), Ryuuseigun('broken'), Ryuuseigun('root')

        @first.lifespan
        @asynccontextmanager
        async def resources(app):
            events.append('open')
            try:
                yield
            finally:
                events.append('closed')

        @broken.startup
        async def fail():
            raise RuntimeError('failed child')

        root.mount('/first', first, name='first', lifespan=True)
        root.mount('/broken', broken, name='broken', lifespan=True)
        with self.assertRaisesRegex(RuntimeError, 'failed child'):
            async with root.test_client():
                pass
        self.assertEqual(events, ['open', 'closed'])

    async def test_websocket_mount_and_cycle_rejection(self):
        parent, child = Ryuuseigun('parent'), Ryuuseigun('child')

        @child.websocket('/socket')
        async def socket(ws):
            await ws.accept()
            await ws.send_json({'path': ws.path, 'root': ws.root_path, 'endpoint': ws.route.endpoint})

        parent.mount('/child', child, name='child')
        with self.assertRaisesRegex(ValueError, 'Cyclic'):
            child.mount('/parent', parent, name='parent')
        async with parent.test_client().websocket('/child/socket') as session:
            self.assertEqual(await session.receive_json(), {'path': '/socket', 'root': '/child', 'endpoint': 'socket'})

    async def test_external_mount_scope_state_and_longest_prefix(self):
        root = Ryuuseigun('root')

        async def external(scope, receive, send):
            if scope['type'] == 'lifespan':
                scope['state']['value'] = 'external'
                while True:
                    message = await receive()
                    phase = message['type'].split('.')[-1]
                    await send({'type': f'lifespan.{phase}.complete'})
                    if phase == 'shutdown':
                        return
            else:
                await Response(scope['state']['value']).send(send)

        root.mount('/api', Ryuuseigun('empty'), name='api')
        root.mount('/api/external', external, name='external', lifespan=True)
        async with root.test_client() as client:
            self.assertEqual((await client.get('/api/external/')).text, 'external')

