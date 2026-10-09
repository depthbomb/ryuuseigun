from asyncio import Event
from contextlib import asynccontextmanager
from dataclasses import FrozenInstanceError
from unittest import IsolatedAsyncioTestCase
from ryuuseigun import Config, Module, Request, Response, Ryuuseigun

class CompositionTests(IsolatedAsyncioTestCase):
    async def test_stripped_unicode_prefix_preserves_external_scope(self):
        app = Ryuuseigun('root')
        received = []

        async def external(scope, receive, send):
            received.append(scope)
            await Response('external').send(send)

        app.mount('/caf\u00e9', external, name='static')
        self.assertEqual(app.url_for('static', path='/asset'), '/caf%C3%A9/asset')
        scope = {
            'type': 'http', 'method': 'GET', 'path': '/caf\u00e9/asset x', 'root_path': '/outer space',
            'raw_path': b'/caf%C3%A9/asset%20x', 'headers': [], 'query_string': b'a=1',
        }
        async def receive():
            return {'type': 'http.disconnect'}
        async def send(message):
            pass

        await app(scope, receive, send)
        self.assertEqual(scope['path'], '/caf\u00e9/asset x')
        self.assertEqual(received[0]['path'], '/outer space/caf\u00e9/asset x')
        self.assertEqual(received[0]['raw_path'], b'/outer%20space/caf%C3%A9/asset%20x')
        self.assertEqual(received[0]['root_path'], '/outer space/caf\u00e9')
        self.assertEqual(received[0]['query_string'], b'a=1')

    async def test_mount_names_do_not_shadow_route_urls(self):
        async def endpoint(req):
            return 'ok'

        for name in ('child', 'child:route'):
            app = Ryuuseigun('root')
            app.add_url_rule('/route', endpoint, endpoint=name)
            with self.assertRaisesRegex(ValueError, 'conflicts'):
                app.mount('/child', Ryuuseigun('child'), name='child')

            app = Ryuuseigun('root')
            app.mount('/child', Ryuuseigun('child'), name='child')
            with self.assertRaisesRegex(ValueError, 'conflicts'):
                app.add_url_rule('/route', endpoint, endpoint=name)

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

