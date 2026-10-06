import asyncio
from unittest import IsolatedAsyncioTestCase
from examples.lifecycle_trace import create_app

class LifecycleExampleTests(IsolatedAsyncioTestCase):
    async def test_decorator_exits_before_stream_and_resource_exits_after(self):
        app, events = create_app()
        self.assertEqual((await app.test_client().get('/api/images/3')).text, 'image')
        self.assertEqual(events, [
            'resource enter', 'parent before', 'decorator enter', 'handler 3', 'decorator exit',
            'child after', 'stream', 'stream closed', 'resource exit',
        ])
        events.clear()
        self.assertEqual((await app.test_client().get('/api/images/3?stop=1')).status_code, 400)
        self.assertEqual(events, ['resource enter', 'parent before', 'resource exit'])
        events.clear()
        with self.assertLogs(app.logger, 'ERROR'):
            self.assertEqual((await app.test_client().get('/api/images/3?error=1')).status_code, 500)
        self.assertEqual(events, [
            'resource enter', 'parent before', 'decorator enter', 'handler 3', 'decorator exit',
            'child after', 'resource exit',
        ])

    async def test_stream_cancellation_releases_generator_and_resource(self):
        app, events = create_app()
        first = True

        async def receive():
            nonlocal first
            if first:
                first = False
                return {'type': 'http.request', 'body': b'', 'more_body': False}
            await asyncio.Event().wait()

        async def send(message):
            if message['type'] == 'http.response.body' and message.get('body'):
                raise asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await app({
                'type': 'http', 'method': 'GET', 'path': '/api/images/1', 'headers': [],
                'query_string': b'', 'asgi': {'version': '3.0', 'spec_version': '2.5'},
            }, receive, send)
        self.assertEqual(events[-2:], ['stream closed', 'resource exit'])
