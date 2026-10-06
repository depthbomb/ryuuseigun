from random import Random
from unittest.mock import patch
from types import SimpleNamespace
from ryuuseigun.types import ASGIMessage
from tempfile import SpooledTemporaryFile
from threading import Event as ThreadEvent
from unittest import TestCase, IsolatedAsyncioTestCase
from ryuuseigun import Config, Ryuuseigun, Headers, Request, Response, UploadFile
from asyncio import Event, sleep, gather, wait_for, create_task, CancelledError, get_running_loop

def multipart_request(payload: bytes, threshold: int, *, extra_headers: bytes = b'') -> Request:
    body = (
        b'--boundary\r\nContent-Disposition: form-data; name="file"; filename="data.bin"\r\n'
        b'Content-Type: application/octet-stream\r\n' + extra_headers + b'\r\n'
        + payload + b'\r\n--boundary--\r\n'
    )
    position = 0

    async def receive() -> ASGIMessage:
        nonlocal position
        chunk = body[position:position + 1024]
        position += len(chunk)
        return {'type': 'http.request', 'body': chunk, 'more_body': position < len(body)}

    return Request(
        {
            'type': 'http', 'method': 'POST', 'path': '/',
            'headers': [(b'content-type', b'multipart/form-data; boundary=boundary')],
        },
        receive, state=SimpleNamespace(), upload_spool_threshold=threshold,
    )

class ResponseSnapshotTests(IsolatedAsyncioTestCase):
    async def test_send_preserves_reusable_headers_and_separates_wire_snapshots(self):
        for lazy in (False, True):
            for status in (103, 200, 204, 304):
                for head in (False, True):
                    with self.subTest(lazy=lazy, status=status, head=head):
                        headers = Headers({'Content-Length': '7'})
                        headers.add('Set-Cookie', 'a=1')
                        headers.add('Set-Cookie', 'b=2')
                        response = Response(b'payload', status)
                        response.headers = Headers.from_raw(headers.raw()) if lazy else headers
                        original = response.headers.raw()

                        async def collect(current=response, is_head=head):
                            messages = []

                            async def send(message):
                                messages.append(message)
                                await sleep(0)

                            await current.send(send, head=is_head)
                            return messages

                        first, second = await gather(collect(), collect())
                        self.assertEqual(first, second)
                        sent_headers = Headers.from_raw(first[0]['headers'])
                        self.assertEqual(sent_headers.getlist('set-cookie'), ['a=1', 'b=2'])
                        self.assertEqual(sent_headers.get('content-length'), None if status in (103, 204) else '7')
                        self.assertEqual(first[1]['body'], b'payload' if status == 200 and not head else b'')
                        first[0]['headers'].append((b'x-local', b'one'))
                        self.assertNotIn((b'x-local', b'one'), second[0]['headers'])
                        self.assertEqual(response.headers.raw(), original)

    async def test_lazy_headers_are_validated_before_sending(self):
        for raw in ([(b'bad name', b'value')], [(b'x-test', b'invalid\r\nvalue')]):
            response = Response('body')
            response.headers = Headers.from_raw(raw)
            messages = []

            async def send(message, captured=messages):
                captured.append(message)

            with self.assertRaises(ValueError):
                await response.send(send)
            self.assertEqual(messages, [])

    async def test_generated_length_tracks_body_without_mutating_response_headers(self):
        response = Response('one')
        for body in (b'one', b'a longer body', b''):
            response.body = body
            messages = []

            async def send(message, captured=messages):
                captured.append(message)

            await response.send(send)
            self.assertEqual(Headers.from_raw(messages[0]['headers'])['Content-Length'], str(len(body)))
            self.assertNotIn('Content-Length', response.headers)

class RoutePrecedenceTests(TestCase):
    def test_exact_routes_preserve_method_fallback_and_converter_order(self):
        async def handler(req, **params):
            return params

        for reverse in (False, True):
            for automatic_head in (False, True):
                app = Ryuuseigun(__name__, config=Config(automatic_head=automatic_head))
                routes = [
                    ('/items/<path:tail>', 'path', ('PATCH',)),
                    ('/items/<value>', 'string', ('DELETE', 'GET')),
                    ('/items/<float:value>', 'float', ('PUT', 'GET')),
                    ('/items/<int:value>', 'int', ('POST', 'GET', 'HEAD')),
                    ('/items/42', 'exact', ('GET', 'QUERY')),
                ]
                for path, name, methods in reversed(routes) if reverse else routes:
                    app.add_url_rule(path, handler, endpoint=name, methods=methods)
                app.finalize()
                expected = {
                    'GET': ('exact', {}), 'POST': ('int', {'value': 42}),
                    'PUT': ('float', {'value': 42.0}), 'DELETE': ('string', {'value': '42'}),
                    'PATCH': ('path', {'tail': '42'}), 'QUERY': ('exact', {}),
                    'HEAD': ('exact', {}) if automatic_head else ('int', {'value': 42}),
                }
                for method, (endpoint, params) in expected.items():
                    with self.subTest(reverse=reverse, head=automatic_head, method=method):
                        match = app.router.match('/items/42/', method)
                        self.assertEqual(match.route.endpoint, endpoint)
                        self.assertEqual(match.params, params)
                        match.params['unrelated'] = True
                        self.assertEqual(app.router.match('/items/42', method).params, params)
                options = app.router.match('/items/42', 'OPTIONS')
                self.assertEqual(options.scope_route.endpoint, 'exact')
                self.assertEqual(options.query_route.endpoint, 'exact')
                self.assertEqual(options.allowed_methods, {'GET', 'HEAD', 'QUERY', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'})

class UploadOptimizationTests(IsolatedAsyncioTestCase):
    async def test_spool_boundaries_preserve_data_positions_and_cleanup(self):
        for size, threshold in (
            (0, 0), (0, 1), (4096, 4096), (4096, 4095), (4096, 0), (131072, 262144), (786469, 1024),
        ):
            with self.subTest(size=size, threshold=threshold):
                payload = Random(0).randbytes(size)
                req = multipart_request(payload, threshold)
                try:
                    form = await req.form()
                    file = form['file']
                    self.assertIsInstance(file, UploadFile)
                    self.assertEqual(file.in_memory, bool(threshold and size <= threshold))
                    self.assertEqual(file.size, size)
                    self.assertEqual(await file.read(), payload)
                    self.assertEqual(await file.tell(), size)
                    self.assertEqual(await file.seek(0), 0)
                    self.assertEqual(await file.read(7), payload[:7])
                finally:
                    await req.close()
                self.assertTrue(file.closed)
                self.assertTrue(file.file.closed)

    async def test_memory_file_operations_remain_cancellable(self):
        file = UploadFile('data.bin', b'data', Headers())

        async def consume():
            for _ in range(1000):
                await file.seek(0)
                await file.read(1)

        task = create_task(consume())
        await sleep(0)
        await sleep(0)
        task.cancel()
        try:
            with self.assertRaises(CancelledError):
                await task
        finally:
            await file.close()

    async def test_cancelled_rollover_write_finishes_before_file_cleanup(self):
        entered = Event()
        release = ThreadEvent()
        loop = get_running_loop()
        files = []

        def make_file(*args, **kwargs):
            file = SpooledTemporaryFile(*args, **kwargs)
            files.append(file)
            original_write = file.write

            def write(data):
                loop.call_soon_threadsafe(entered.set)
                if not release.wait(5):
                    raise TimeoutError('Writer was not released')
                return original_write(data)

            file.write = write
            return file

        req = multipart_request(b'x' * 4096, 1)
        with patch('ryuuseigun.request.SpooledTemporaryFile', make_file):
            task = create_task(req.form())
            try:
                await wait_for(entered.wait(), 1)
                task.cancel()
                await sleep(0)
                self.assertFalse(task.done())
                self.assertFalse(files[0].closed)
                release.set()
                with self.assertRaises(CancelledError):
                    await wait_for(task, 1)
                self.assertTrue(files[0].closed)
            finally:
                release.set()
                task.cancel()
                await gather(task, return_exceptions=True)
                await req.close()

    async def test_cached_part_headers_preserve_raw_folding_and_duplicates(self):
        req = multipart_request(b'data', 4096, extra_headers=b'X-Note: first\r\n second\r\nX-Note: last\r\n')
        try:
            form = await req.form()
            file = form['file']
            self.assertEqual(file.filename, 'data.bin')
            self.assertEqual(file.content_type, 'application/octet-stream')
            self.assertEqual(file.headers.getlist('X-Note'), ['first second', 'last'])
            self.assertEqual(file.headers['Content-Disposition'], 'form-data; name="file"; filename="data.bin"')
        finally:
            await req.close()
