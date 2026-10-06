import asyncio
from examples.basic import app as basic_app
from unittest import IsolatedAsyncioTestCase
from examples.modules import app as modules_app
from examples.modern_http import app as modern_app
from examples.middleware import app as middleware_app
from examples.typed_request_state import app as typed_state_app


class ExampleTests(IsolatedAsyncioTestCase):
    async def test_basic_example(self) -> None:
        client = basic_app.test_client()

        user = await client.get('/users/42')
        echo = await client.post('/echo', json={'hello': 'world'})
        profile = await client.post(
            '/profile',
            body='display_name=Ryuuseigun',
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
        )
        search = await client.get('/search?q=fast&tag=async&tag=typed')
        teapot = await client.get('/teapot')

        self.assertEqual(user.json(), {'id': 42, 'active': True})
        self.assertEqual(echo.status_code, 201)
        self.assertEqual(echo.json(), {'hello': 'world'})
        self.assertEqual(profile.json(), {'display_name': 'Ryuuseigun'})
        self.assertEqual(search.json(), {'query': 'fast', 'tags': ['async', 'typed']})
        self.assertEqual(teapot.status_code, 418)

    async def test_modules_example(self) -> None:
        client = modules_app.test_client()

        unauthorized = await client.get('/v1/api/users')
        user = await client.get('/v1/api/users/1', headers={'X-Client': 'example'})
        missing = await client.get('/v1/api/users/100', headers={'X-Client': 'example'})

        self.assertEqual(unauthorized.status_code, 401)
        self.assertEqual(unauthorized.headers['X-API-Version'], '1')
        self.assertEqual(user.json(), {'id': 1, 'name': 'Ada'})
        self.assertEqual(user.headers['X-Module'], 'api')
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(missing.json()['path'], '/v1/api/users/100')

    async def test_middleware_example(self) -> None:
        client = middleware_app.test_client()

        unauthorized = await client.get('/private')
        authorized = await client.get('/private', headers={'X-API-Key': 'example-key'})
        digest = await client.get('/digest/ryuuseigun')
        domain_error = await client.get('/domain-error')

        self.assertEqual(unauthorized.status_code, 401)
        self.assertEqual(authorized.status_code, 200)
        self.assertTrue(authorized.json()['request_id'])
        self.assertIn('Server-Timing', authorized.headers)
        self.assertEqual(len(digest.json()['sha256']), 64)
        self.assertEqual(domain_error.status_code, 422)

    async def test_modern_http_example(self) -> None:
        client = modern_app.test_client()

        upload = await client.post('/upload', body='stream me')
        query = await client.query('/records', json={'where': {'active': True}})
        stream = await client.get('/stream')
        source = await client.get('/source', headers={'Range': 'bytes=0-9'})

        self.assertEqual(upload.json(), {'bytes_received': 9})
        self.assertEqual(query.json()['query'], {'where': {'active': True}})
        self.assertEqual(stream.text, 'chunk 0\nchunk 1\nchunk 2\n')
        self.assertEqual(source.status_code, 206)

    async def test_typed_request_state_example(self) -> None:
        client = typed_state_app.test_client()

        first, second = await asyncio.gather(
            client.get('/session', headers={'X-User-ID': '7'}),
            client.get('/session'),
        )

        self.assertEqual(first.json()['user_id'], 7)
        self.assertIsNone(second.json()['user_id'])
        self.assertNotEqual(first.json()['request_id'], second.json()['request_id'])
        self.assertEqual(first.headers['X-Request-ID'], first.json()['request_id'])
