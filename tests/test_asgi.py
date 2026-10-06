import asyncio
from typing import Optional
from ryuuseigun.testing import TestResponse
from unittest import IsolatedAsyncioTestCase
from ryuuseigun.types import ASGIScope, ASGIMessage
from ryuuseigun import Config, Ryuuseigun, Headers, Request, request, Response


def http_scope(
    path: str = '/',
    *,
    method: str = 'GET',
    headers: Optional[list[tuple[bytes, bytes]]] = None,
) -> ASGIScope:
    return {
        'type': 'http',
        'asgi': {'version': '3.0', 'spec_version': '2.5'},
        'http_version': '1.1',
        'method': method,
        'scheme': 'http',
        'path': path,
        'raw_path': path.encode(),
        'query_string': b'',
        'headers': headers or [(b'host', b'testserver')],
        'client': ('127.0.0.1', 50000),
        'server': ('testserver', 80),
    }


async def invoke(
    app: Ryuuseigun,
    scope: ASGIScope,
    messages: Optional[list[ASGIMessage]] = None,
) -> tuple[TestResponse, int, list[ASGIMessage]]:
    pending = list(messages or [{'type': 'http.request', 'body': b'', 'more_body': False}])
    sent: list[ASGIMessage] = []
    receives = 0

    async def receive() -> ASGIMessage:
        nonlocal receives
        receives += 1
        if pending:
            return pending.pop(0)
        return {'type': 'http.disconnect'}

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    await app(scope, receive, send)
    start = next(message for message in sent if message['type'] == 'http.response.start')
    body = b''.join(message.get('body', b'') for message in sent if message['type'] == 'http.response.body')
    return TestResponse(start['status'], Headers.from_raw(start['headers']), body), receives, sent


class RequestBodyTests(IsolatedAsyncioTestCase):
    async def test_body_limit_rejects_declared_size_without_reading(self) -> None:
        app = Ryuuseigun(__name__, config=Config(max_request_body_size=4))

        @app.post('/body')
        async def body(req: Request) -> dict[str, int]:
            return {'size': len(await req.body())}

        response, receives, _ = await invoke(
            app,
            http_scope('/body', method='POST', headers=[(b'content-length', b'5')]),
            [{'type': 'http.request', 'body': b'12345', 'more_body': False}],
        )

        self.assertEqual(response.status_code, 413)
        self.assertEqual(receives, 0)

    async def test_body_limit_rejects_chunked_payload(self) -> None:
        app = Ryuuseigun(__name__, config=Config(max_request_body_size=4))

        @app.post('/body')
        async def body(req: Request) -> dict[str, int]:
            return {'size': len(await req.body())}

        response, receives, _ = await invoke(
            app,
            http_scope('/body', method='POST'),
            [
                {'type': 'http.request', 'body': b'12', 'more_body': True},
                {'type': 'http.request', 'body': b'345', 'more_body': False},
            ],
        )

        self.assertEqual(response.status_code, 413)
        self.assertEqual(receives, 2)

    async def test_exact_limit_empty_body_and_unlimited_body(self) -> None:
        limited = Ryuuseigun(__name__, config=Config(max_request_body_size=4))
        unlimited = Ryuuseigun(__name__, config=Config(max_request_body_size=None))

        async def body(req: Request) -> dict[str, object]:
            first = await req.body()
            second = await req.body()
            return {'body': first.decode(), 'cached': first is second}

        limited.add_url_rule('/body', body, methods=('POST',))
        unlimited.add_url_rule('/body', body, methods=('POST',))

        exact = await limited.test_client().post('/body', body='1234')
        empty = await limited.test_client().post('/body', body=b'')
        large = await unlimited.test_client().post('/body', body='123456789')

        self.assertEqual(exact.json(), {'body': '1234', 'cached': True})
        self.assertEqual(empty.json(), {'body': '', 'cached': True})
        self.assertEqual(large.status_code, 200)

    async def test_invalid_and_conflicting_content_lengths_are_rejected(self) -> None:
        app = Ryuuseigun(__name__)

        @app.post('/body')
        async def body(req: Request) -> bytes:
            return await req.body()

        invalid, _, _ = await invoke(app, http_scope('/body', method='POST', headers=[(b'content-length', b'x')]))
        conflicting, _, _ = await invoke(
            app,
            http_scope('/body', method='POST', headers=[(b'content-length', b'1'), (b'content-length', b'2')]),
        )

        self.assertEqual(invalid.status_code, 400)
        self.assertEqual(conflicting.status_code, 400)

    async def test_disconnect_and_unexpected_messages_are_client_errors(self) -> None:
        app = Ryuuseigun(__name__)

        @app.post('/body')
        async def body(req: Request) -> bytes:
            return await req.body()

        disconnected, _, _ = await invoke(app, http_scope('/body', method='POST'), [{'type': 'http.disconnect'}])
        unexpected, _, _ = await invoke(app, http_scope('/body', method='POST'), [{'type': 'lifespan.startup'}])

        self.assertEqual(disconnected.status_code, 400)
        self.assertEqual(unexpected.status_code, 400)

    async def test_text_rejects_unknown_encodings_and_invalid_bytes(self) -> None:
        app = Ryuuseigun(__name__)

        @app.post('/text')
        async def text(req: Request) -> str:
            return await req.text('utf-8')

        invalid = await app.test_client().post('/text', body=b'\xff')

        self.assertEqual(invalid.status_code, 400)


class ProtocolTests(IsolatedAsyncioTestCase):
    async def test_outbound_headers_are_validated_before_sending(self) -> None:
        with self.assertRaisesRegex(ValueError, 'header name'):
            Headers({'Bad Name': 'value'})
        with self.assertRaisesRegex(ValueError, 'header value'):
            Headers({'X-Test': 'value\x00'})
        with self.assertRaisesRegex(ValueError, 'Latin-1'):
            Headers({'X-Test': '\u20ac'})

    async def test_request_context_is_isolated_across_concurrent_tasks(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/context/<value>')
        async def context(_req, value: str) -> dict[str, str]:
            await asyncio.sleep(0.01 if value == 'slow' else 0)
            return {'value': value, 'path': request.path}

        client = app.test_client()
        slow, fast = await asyncio.gather(client.get('/context/slow'), client.get('/context/fast'))

        self.assertEqual(slow.json(), {'value': 'slow', 'path': '/context/slow'})
        self.assertEqual(fast.json(), {'value': 'fast', 'path': '/context/fast'})

    async def test_repeated_request_and_response_headers_are_preserved(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/headers')
        async def headers(req: Request) -> Response:
            res = Response(','.join(req.headers.getlist('x-tag')))
            res.headers.add('Set-Cookie', 'one=1')
            res.headers.add('Set-Cookie', 'two=2')
            return res

        response, _, sent = await invoke(
            app,
            http_scope('/headers', headers=[(b'x-tag', b'one'), (b'x-tag', b'two')]),
        )
        start = next(message for message in sent if message['type'] == 'http.response.start')
        cookies = [value for name, value in start['headers'] if name == b'set-cookie']

        self.assertEqual(response.text, 'one,two')
        self.assertEqual(cookies, [b'one=1', b'two=2'])

    async def test_bodyless_statuses_suppress_body_and_length(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/no-content')
        async def no_content(_req) -> Response:
            return Response('discarded', 204, headers={'Content-Length': '9'})

        @app.get('/not-modified')
        async def not_modified(_req) -> Response:
            return Response('discarded', 304)

        no_content_response = await app.test_client().get('/no-content')
        not_modified_response = await app.test_client().get('/not-modified')

        self.assertEqual(no_content_response.body, b'')
        self.assertNotIn('Content-Length', no_content_response.headers)
        self.assertEqual(not_modified_response.body, b'')
        self.assertNotIn('Content-Length', not_modified_response.headers)

    async def test_malformed_http_scopes_fail_clearly(self) -> None:
        app = Ryuuseigun(__name__)

        async def receive() -> ASGIMessage:
            return {'type': 'http.request', 'body': b'', 'more_body': False}

        async def send(message: ASGIMessage) -> None:
            return None

        with self.assertRaisesRegex(RuntimeError, 'non-empty method'):
            await app({'type': 'http', 'path': '/'}, receive, send)
        with self.assertRaisesRegex(RuntimeError, 'paths must start'):
            await app({'type': 'http', 'method': 'GET', 'path': 'relative'}, receive, send)

    async def test_lifespan_startup_and_shutdown_are_acknowledged(self) -> None:
        app = Ryuuseigun(__name__)
        pending = [{'type': 'lifespan.startup'}, {'type': 'lifespan.shutdown'}]
        sent: list[ASGIMessage] = []

        async def receive() -> ASGIMessage:
            return pending.pop(0)

        async def send(message: ASGIMessage) -> None:
            sent.append(message)

        await app({'type': 'lifespan'}, receive, send)

        self.assertEqual(
            sent,
            [{'type': 'lifespan.startup.complete'}, {'type': 'lifespan.shutdown.complete'}],
        )
