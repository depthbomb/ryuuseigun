from pathlib import Path
from gzip import decompress
from threading import get_ident
from unittest.mock import patch
from types import SimpleNamespace
from importlib.util import find_spec
from threading import Event as ThreadEvent
from unittest import IsolatedAsyncioTestCase
from ryuuseigun.response import _StreamingEncoder
from tempfile import TemporaryDirectory, SpooledTemporaryFile
from asyncio import Event, Queue, sleep, gather, wait_for, create_task, CancelledError, get_running_loop
from ryuuseigun import (
    Config,
    Ryuuseigun,
    Headers,
    Request,
    Response,
    Compression,
    FileResponse,
    ConcurrencyLimit,
    StreamingResponse,
)


def scope(method='POST', headers=None):
    return {'type': 'http', 'method': method, 'path': '/', 'headers': Headers(headers).raw(), 'query_string': b''}


class StreamingFixTests(IsolatedAsyncioTestCase):
    async def test_unread_upload_does_not_hide_disconnect(self):
        app = Ryuuseigun(__name__)
        incoming = Queue()
        closed = Event()

        @app.post('/')
        async def stream(_req):
            async def chunks():
                try:
                    yield b'first'
                    await Event().wait()
                finally:
                    closed.set()
            return StreamingResponse(chunks())

        async def send(message):
            pass

        incoming.put_nowait({'type': 'http.request', 'body': b'partial', 'more_body': True})
        incoming.put_nowait({'type': 'http.disconnect'})
        await wait_for(app(scope(), incoming.get, send), 1)
        self.assertTrue(closed.is_set())
        self.assertTrue(incoming.empty())

    async def test_limiter_holds_capacity_during_streaming(self):
        app = Ryuuseigun(__name__)
        limiter = ConcurrencyLimit(1)
        app.middleware(limiter)
        entered, release = Event(), Event()

        @app.get('/')
        async def stream(_req):
            async def chunks():
                entered.set()
                await release.wait()
                yield b'finished'
            return StreamingResponse(chunks())

        first = create_task(app.test_client().get('/'))
        try:
            await wait_for(entered.wait(), 1)
            self.assertEqual(limiter.active, 1)
            rejected = await wait_for(app.test_client().get('/'), 1)
            self.assertEqual(rejected.status_code, 503)
            release.set()
            self.assertEqual((await wait_for(first, 1)).body, b'finished')
            self.assertEqual(limiter.active, 0)
        finally:
            first.cancel()
            await gather(first, return_exceptions=True)

    async def test_unicode_multipart_filenames_and_raw_headers(self):
        for collected in (False, True):
            with self.subTest(collected=collected):
                app = Ryuuseigun(__name__)
                filename = '\u6587\u4ef6-\U0001f600.txt'
                disposition = f'form-data; name="file"; filename="{filename}"'

                @app.post('/')
                async def upload(req: Request, collected=collected):
                    if collected:
                        form = await req.form()
                        part = form['file']
                    else:
                        part = await anext(req.multipart())
                    return {
                        'filename': part.filename,
                        'body': (await part.read()).decode(),
                        'disposition': part.headers['Content-Disposition'].encode('latin-1').decode('utf-8'),
                    }

                payload = (
                    f'--probe\r\nContent-Disposition: {disposition}\r\n'
                    'Content-Type: text/plain\r\n\r\nhello\r\n--probe--\r\n'
                ).encode()
                response = await app.test_client().post(
                    '/', body=payload, headers={'Content-Type': 'multipart/form-data; boundary=probe'},
                )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), {'filename': filename, 'body': 'hello', 'disposition': disposition})

    async def test_folded_multipart_headers_and_invalid_header_values(self):
        app = Ryuuseigun(__name__)

        @app.post('/')
        async def upload(req: Request):
            form = await req.form()
            return {'filename': form['file'].filename}

        prefix = '--probe\r\nContent-Disposition: form-data; name="file";\r\n\tfilename="\u6587\u4ef6.txt"\r\n'
        headers = {'Content-Type': 'multipart/form-data; boundary=probe'}
        response = await app.test_client().post('/', body=(prefix + '\r\nhello\r\n--probe--\r\n').encode(), headers=headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'filename': '\u6587\u4ef6.txt'})
        invalid = prefix + 'X-Bad: invalid\x00value\r\n\r\nhello\r\n--probe--\r\n'
        response = await app.test_client().post('/', body=invalid.encode(), headers=headers)
        self.assertEqual(response.status_code, 400)

    async def test_streaming_compression_and_finalization_run_off_loop(self):
        app = Ryuuseigun(__name__)
        app.after_request(Compression(encodings=('gzip',)))
        payload = b'hello' * 20000

        @app.get('/')
        async def stream(_req):
            async def chunks():
                yield payload[:65536]
                yield payload[65536:]
            return StreamingResponse(chunks())

        compression_threads, finalization_threads = [], []
        original_compress = _StreamingEncoder.compress
        original_finish = _StreamingEncoder.finish

        def compress(encoder, chunk):
            compression_threads.append(get_ident())
            return original_compress(encoder, chunk)

        def finish(encoder):
            finalization_threads.append(get_ident())
            return original_finish(encoder)

        with patch.object(_StreamingEncoder, 'compress', compress), patch.object(_StreamingEncoder, 'finish', finish):
            response = await app.test_client().get('/', headers={'Accept-Encoding': 'gzip'})

        self.assertEqual(decompress(response.body), payload)
        self.assertTrue(compression_threads)
        self.assertTrue(finalization_threads)
        self.assertNotIn(get_ident(), compression_threads + finalization_threads)

    async def test_streaming_codecs_roundtrip_multiple_chunks(self):
        from compression.zstd import decompress as unzstd
        decoders = {'gzip': decompress, 'zstd': unzstd}
        if find_spec('brotli') is not None:
            from brotli import decompress as unbrotli
            decoders['br'] = unbrotli

        payload = bytes(range(256)) * 1000
        for encoding, decode in decoders.items():
            with self.subTest(encoding=encoding):
                app = Ryuuseigun(__name__)
                app.after_request(Compression(encodings=(encoding,)))

                @app.get('/')
                async def stream(_req):
                    async def chunks():
                        for index in range(0, len(payload), 4093):
                            yield payload[index:index + 4093]
                    return StreamingResponse(chunks())

                response = await app.test_client().get('/', headers={'Accept-Encoding': encoding})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.headers['Content-Encoding'], encoding)
                self.assertEqual(decode(response.body), payload)

    async def test_delayed_body_reader_replays_disk_buffer_and_closes_file(self):
        incoming = Queue()
        drained = Event()
        chunks = [bytes((index,)) * 8193 for index in range(32)]
        for index, chunk in enumerate(chunks):
            incoming.put_nowait({'type': 'http.request', 'body': chunk, 'more_body': index < len(chunks) - 1})

        async def receive():
            if incoming.empty():
                drained.set()
            return await incoming.get()

        files = []

        def make_file(*args, **kwargs):
            file = SpooledTemporaryFile(*args, **kwargs)
            files.append(file)
            return file

        req = Request(scope(), receive, sum(map(len, chunks)), state=SimpleNamespace(), upload_spool_threshold=1024)
        with patch('ryuuseigun._body.SpooledTemporaryFile', make_file):
            watcher = create_task(req.wait_for_disconnect())
            try:
                await wait_for(drained.wait(), 2)
                self.assertEqual(len(files), 1)
                self.assertTrue(files[0]._rolled)
                self.assertEqual(await req.body(), b''.join(chunks))
                self.assertEqual(req._received_size, sum(map(len, chunks)))
                incoming.put_nowait({'type': 'http.disconnect'})
                await wait_for(watcher, 1)
            finally:
                watcher.cancel()
                await gather(watcher, return_exceptions=True)
                await req.close()
        self.assertTrue(files[0].closed)

    async def test_concurrent_body_reader_and_watcher_preserve_byte_order(self):
        incoming = Queue()
        req = Request(scope(), incoming.get, state=SimpleNamespace(), upload_spool_threshold=32)
        chunks = [bytes((index,)) * 1031 for index in range(64)]
        watcher = create_task(req.wait_for_disconnect())

        async def read():
            result = bytearray()
            async for chunk in req.stream():
                result.extend(chunk)
                await sleep(0)
            return result

        reader = create_task(read())
        try:
            for index, chunk in enumerate(chunks):
                incoming.put_nowait({'type': 'http.request', 'body': chunk, 'more_body': index < len(chunks) - 1})
                await sleep(0)
            self.assertEqual(await wait_for(reader, 2), b''.join(chunks))
            incoming.put_nowait({'type': 'http.disconnect'})
            await wait_for(watcher, 1)
        finally:
            reader.cancel()
            watcher.cancel()
            await gather(reader, watcher, return_exceptions=True)
            await req.close()

    async def test_cancelled_watcher_finishes_buffer_write_before_cleanup(self):
        incoming = Queue()
        payload = b'x' * 100000
        incoming.put_nowait({'type': 'http.request', 'body': payload, 'more_body': False})
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
                    raise TimeoutError('test writer was not released')
                return original_write(data)

            file.write = write
            return file

        req = Request(scope(), incoming.get, state=SimpleNamespace(), upload_spool_threshold=1)
        with patch('ryuuseigun._body.SpooledTemporaryFile', make_file):
            watcher = create_task(req.wait_for_disconnect())
            try:
                await wait_for(entered.wait(), 1)
                watcher.cancel()
                await sleep(0)
                self.assertFalse(watcher.done())
                self.assertFalse(files[0].closed)
                release.set()
                with self.assertRaises(CancelledError):
                    await wait_for(watcher, 1)
                self.assertEqual(await wait_for(req.body(), 1), payload)
            finally:
                release.set()
                watcher.cancel()
                await gather(watcher, return_exceptions=True)
                await req.close()
        self.assertTrue(files[0].closed)

    async def test_limiter_covers_file_send_and_releases_after_send_error(self):
        for outcome in ('complete', 'disconnect', 'error', 'cancel'):
            with self.subTest(outcome=outcome), TemporaryDirectory() as directory:
                path = Path(directory) / 'file.txt'
                path.write_bytes(b'contents')
                app = Ryuuseigun(__name__)
                limiter = ConcurrencyLimit(1)
                entered, release = Event(), Event()

                @app.get('/', middlewares=(limiter,))
                async def download(_req, path=path):
                    return FileResponse(path)

                async def send(message, outcome=outcome, entered=entered, release=release):
                    if message['type'] == 'http.response.body':
                        entered.set()
                        await release.wait()
                        if outcome == 'disconnect':
                            raise OSError('client gone')
                        if outcome == 'error':
                            raise RuntimeError('send failed')

                task = create_task(app(scope('GET'), Queue().get, send))
                try:
                    await wait_for(entered.wait(), 1)
                    self.assertEqual(limiter.active, 1)
                    self.assertEqual((await app.test_client().get('/')).status_code, 503)
                    if outcome == 'cancel':
                        task.cancel()
                    else:
                        release.set()
                    result = (await gather(task, return_exceptions=True))[0]
                    if outcome == 'error':
                        self.assertIsInstance(result, RuntimeError)
                    elif outcome == 'cancel':
                        self.assertIsInstance(result, CancelledError)
                    else:
                        self.assertIsNone(result)
                    self.assertEqual(limiter.active, 0)
                finally:
                    release.set()
                    task.cancel()
                    await gather(task, return_exceptions=True)

    async def test_limiter_survives_response_replacement_and_early_request_close(self):
        app = Ryuuseigun(__name__)
        limiter = ConcurrencyLimit(1)
        entered, release = Event(), Event()

        @app.after_request
        async def replace(req, response):
            await req.close()
            entered.set()
            await release.wait()
            return Response('replacement')

        app.middleware(limiter)

        @app.get('/')
        async def route(_req):
            return 'original'

        task = create_task(app.test_client().get('/'))
        try:
            await wait_for(entered.wait(), 1)
            self.assertEqual(limiter.active, 1)
            release.set()
            self.assertEqual((await wait_for(task, 1)).text, 'replacement')
            self.assertEqual(limiter.active, 0)
        finally:
            task.cancel()
            await gather(task, return_exceptions=True)

    async def test_cancellation_joins_compression_before_releasing_capacity(self):
        app = Ryuuseigun(__name__)
        limiter = ConcurrencyLimit(1)
        app.middleware(limiter)
        app.after_request(Compression(encodings=('gzip',)))
        entered, closed = Event(), Event()
        release = ThreadEvent()
        loop = get_running_loop()
        original = _StreamingEncoder.compress

        @app.get('/')
        async def stream(_req):
            async def chunks():
                try:
                    yield b'x' * 65536
                finally:
                    closed.set()
            return StreamingResponse(chunks())

        def compress(encoder, chunk):
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(5):
                raise TimeoutError('test worker was not released')
            return original(encoder, chunk)

        with patch.object(_StreamingEncoder, 'compress', compress):
            task = create_task(app.test_client().get('/', headers={'Accept-Encoding': 'gzip'}))
            try:
                await wait_for(entered.wait(), 1)
                task.cancel()
                await sleep(0)
                task.cancel()
                await sleep(0)
                self.assertFalse(task.done())
                self.assertFalse(closed.is_set())
                self.assertEqual(limiter.active, 1)
                release.set()
                with self.assertRaises(CancelledError):
                    await wait_for(task, 1)
                self.assertTrue(closed.is_set())
                self.assertEqual(limiter.active, 0)
            finally:
                release.set()
                task.cancel()
                await gather(task, return_exceptions=True)

    async def test_upload_limit_still_applies_while_watcher_drains_body(self):
        app = Ryuuseigun(__name__, config=Config(max_request_body_size=4))
        incoming = Queue()
        closed = Event()

        @app.post('/')
        async def stream(_req):
            async def chunks():
                try:
                    yield b'first'
                    await Event().wait()
                finally:
                    closed.set()
            return StreamingResponse(chunks())

        async def send(message):
            pass

        from ryuuseigun.exceptions import HTTPException
        incoming.put_nowait({'type': 'http.request', 'body': b'abc', 'more_body': True})
        incoming.put_nowait({'type': 'http.request', 'body': b'def', 'more_body': True})
        with self.assertRaises(HTTPException) as caught:
            await wait_for(app(scope(), incoming.get, send), 1)
        self.assertEqual(caught.exception.status_code, 413)
        self.assertTrue(closed.is_set())
