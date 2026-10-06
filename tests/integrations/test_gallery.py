from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from examples.gallery import create_app
from openapi_spec_validator import validate

class GalleryTests(IsolatedAsyncioTestCase):
    async def test_complete_flow_and_persistence(self):
        with TemporaryDirectory() as directory:
            database = 'sqlite+aiosqlite:///' + (Path(directory) / 'gallery.db').as_posix()
            app = create_app(password='test-password', database_url=database)
            async with app.test_client(base_url='https://testserver') as client:
                self.assertEqual((await client.get('/api/images')).status_code, 401)
                login = await client.post('/login', json={'username': 'demo', 'password': 'test-password'}, follow_redirects=True)
                self.assertEqual(login.status_code, 200)
                csrf = {'X-CSRF-Token': login.json()['csrf_token']}
                self.assertEqual((await client.post('/api/images', json={'title': 'Hello'})).status_code, 403)
                self.assertEqual((await client.post('/api/images', headers=csrf, json={'title': ''})).status_code, 422)
                created = await client.post('/api/images', headers=csrf, json={'title': 'Hello'})
                self.assertEqual(created.status_code, 201)
                self.assertEqual(created.headers['Cache-Control'], 'no-store')
                identifier = created.json()['id']
                limited = await client.post('/api/images', headers=csrf, json={'title': 'Again'})
                self.assertEqual(limited.status_code, 429)
                self.assertIn('Retry-After', limited.headers)
                content_path = f'/api/images/{identifier}/content'
                uploaded = await client.put(content_path, headers=csrf, files={'image': ('hello.bin', b'content', 'application/octet-stream')})
                self.assertEqual(uploaded.status_code, 204)
                self.assertEqual((await client.get(content_path)).body, b'content')
                oversized = await client.put(content_path, headers=csrf, files={
                    'image': ('large.bin', b'x' * (1024 * 1024 + 1), 'application/octet-stream'),
                })
                self.assertEqual(oversized.status_code, 413)
                validate((await client.get('/openapi.json')).json())
                self.assertEqual((await client.post('/logout', headers=csrf)).status_code, 204)
                self.assertEqual((await client.get('/account')).status_code, 401)
            self.assertFalse(app.state.ready)

            reopened = create_app(password='test-password', database_url=database)
            async with reopened.test_client(base_url='https://testserver') as client:
                await client.post('/login', json={'username': 'demo', 'password': 'test-password'})
                self.assertEqual((await client.get('/api/images')).json()['images'], [{'id': identifier, 'title': 'Hello'}])
