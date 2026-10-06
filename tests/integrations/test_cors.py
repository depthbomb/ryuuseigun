from asyncio import Event
from unittest import IsolatedAsyncioTestCase
from examples.cors import application

class CORSTests(IsolatedAsyncioTestCase):
    async def test_success_error_and_preflight(self):
        for path, method, status in (('/data', 'GET', 200), ('/missing', 'GET', 404), ('/data', 'OPTIONS', 200)):
            sent = []
            headers = [(b'origin', b'https://frontend.example')]
            if method == 'OPTIONS':
                headers.append((b'access-control-request-method', b'POST'))

            async def receive():
                await Event().wait()

            async def send(message, sent=sent):
                sent.append(message)

            await application({
                'type': 'http', 'method': method, 'path': path, 'headers': headers,
                'query_string': b'', 'asgi': {'version': '3.0', 'spec_version': '2.5'},
            }, receive, send)
            self.assertEqual(sent[0]['status'], status)
            self.assertEqual(dict(sent[0]['headers'])[b'access-control-allow-origin'], b'https://frontend.example')
