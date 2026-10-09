from ryuuseigun import Ryuuseigun
from unittest import IsolatedAsyncioTestCase

class ExternalMountTests(IsolatedAsyncioTestCase):
    async def test_external_starlette_mount(self):
        # This is the exact embedding pattern that previously returned 404.
        from starlette.routing import Mount
        from starlette.applications import Starlette
        child = Ryuuseigun('child')

        @child.get('/hello')
        async def hello(req):
            return req.url_for('hello')

        result = []

        async def receive():
            return {'type': 'http.request', 'body': b''}

        async def send(message):
            result.append(message)

        await Starlette(routes=[Mount('/prefix', app=child)])({
            'type': 'http', 'method': 'GET', 'path': '/prefix/hello',
            'headers': [], 'query_string': b'', 'root_path': '',
        }, receive, send)
        self.assertEqual(result[0]['status'], 200)
        self.assertEqual(result[1]['body'], b'/prefix/hello')
