import asyncio
from datetime import UTC, datetime
from contextlib import asynccontextmanager
from unittest import TestCase, IsolatedAsyncioTestCase
from ryuuseigun import Config, Ryuuseigun, UploadFile, redirect, StreamingResponse, WebSocketDisconnect

class ResponseHelperTests(TestCase):
    def test_multiple_cookies_and_validation(self):
        response = redirect('/account', 303)
        response.set_cookie('session', 'a;b', secure=True, httponly=True)
        response.set_cookie('theme', 'dark', expires=datetime(2030, 1, 1, tzinfo=UTC))
        self.assertEqual(len(response.headers.getlist('Set-Cookie')), 2)
        self.assertEqual(response.headers['Location'], '/account')
        for options in ({'samesite': 'invalid'}, {'samesite': 'none'}, {'path': '/; Secure'}):
            with self.assertRaises(ValueError):
                response.set_cookie('test', **options)
        with self.assertRaises(ValueError):
            response.set_cookie('__Host-session', secure=False)
        with self.assertRaises(ValueError):
            redirect('/ok\r\nX-Bad: yes')
        with self.assertRaises(ValueError):
            redirect('/', 200)

class DiagnosticsTests(IsolatedAsyncioTestCase):
    async def test_websocket_failure_and_expected_disconnect(self):
        app = Ryuuseigun('socket-reporting')

        @app.websocket('/fail')
        async def fail(ws):
            await ws.accept()
            raise RuntimeError('socket failed')

        @app.websocket('/disconnect')
        async def disconnect(ws):
            await ws.accept()
            await ws.receive_text()

        @app.websocket('/explicit-disconnect')
        async def explicit_disconnect(ws):
            await ws.accept()
            raise WebSocketDisconnect()

        with self.assertLogs(app.logger, 'ERROR') as captured:
            async with app.test_client().websocket('/fail'):
                pass
        self.assertEqual(len(captured.records), 1)
        with self.assertNoLogs(app.logger, 'ERROR'):
            async with app.test_client().websocket('/disconnect'):
                pass
            async with app.test_client().websocket('/explicit-disconnect') as socket:
                with self.assertRaises(WebSocketDisconnect):
                    await asyncio.wait_for(socket.receive(), timeout=1)

    async def test_unhandled_logged_once_without_request_secrets(self):
        app = Ryuuseigun('diagnostic-test', debug=True)

        @app.middleware
        async def outer(req, next):
            await next(req)

        @app.get('/failure/<int:item>')
        async def failure(req, item):
            raise RuntimeError('failure')

        with self.assertLogs(app.logger, 'DEBUG') as captured:
            res = await app.test_client().get('/failure/3?secret=hidden', headers={'Authorization': 'secret-token'})
        errors = [record for record in captured.records if record.levelname == 'ERROR']
        self.assertEqual(len(errors), 1)
        self.assertIsNotNone(errors[0].exc_info)
        self.assertEqual(errors[0].route, '/failure/<int:item>')
        self.assertNotIn('secret', '\n'.join(captured.output))
        self.assertEqual(res.status_code, 500)
        self.assertNotIn('RuntimeError', res.text)

    async def test_handled_propagated_and_stream_errors_are_not_double_reported(self):
        app = Ryuuseigun('handled-test')

        @app.errorhandler(ValueError)
        async def handle(req, error):
            return 'handled', 400

        @app.get('/handled')
        async def handled(req):
            raise ValueError('handled')

        @app.get('/stream')
        async def stream(req):
            async def chunks():
                yield b'first'
                raise RuntimeError('stream broke')
            return StreamingResponse(chunks())

        with self.assertNoLogs(app.logger, 'ERROR'):
            self.assertEqual((await app.test_client().get('/handled')).status_code, 400)
            with self.assertRaisesRegex(RuntimeError, 'stream broke'):
                await app.test_client().get('/stream')

        propagated = Ryuuseigun('propagated', config=Config(propagate_exceptions=True))
        propagated.get('/')(handled)
        with self.assertNoLogs(propagated.logger, 'ERROR'):
            with self.assertRaises(ValueError):
                await propagated.test_client().get('/')

    async def test_failed_error_handler_keeps_exception_chain(self):
        app = Ryuuseigun('failed-handler')

        @app.errorhandler(ValueError)
        async def handler(req, error):
            raise RuntimeError('handler failed')

        @app.get('/')
        async def route(req):
            raise ValueError('original')

        with self.assertLogs(app.logger, 'ERROR') as captured:
            self.assertEqual((await app.test_client().get('/')).status_code, 500)
        self.assertEqual(len(captured.records), 1)
        self.assertIsInstance(captured.records[0].exc_info[1].__context__, ValueError)

    def test_registration_error_has_route_and_signature(self):
        app = Ryuuseigun('registration')
        with self.assertRaisesRegex(TypeError, r'GET /broken .*Expected async def handler'):
            app.get('/broken')(lambda req: 'sync')

class ClientLifecycleTests(IsolatedAsyncioTestCase):
    async def test_http_cleanup_failure_and_test_failure_both_survive(self):
        app = Ryuuseigun('request-cleanup', config=Config(propagate_exceptions=True))
        started = asyncio.Event()

        @app.get('/')
        async def route(req):
            try:
                started.set()
                await asyncio.Event().wait()
            finally:
                raise OSError('cleanup failed')

        with self.assertRaises(BaseExceptionGroup) as captured:
            async with app.test_client() as client:
                task = asyncio.create_task(client.get('/'))
                await started.wait()
                raise AssertionError('test failed')
        result = await asyncio.gather(task, return_exceptions=True)
        self.assertIsInstance(result[0], OSError)
        self.assertIsInstance(captured.exception.exceptions[0], AssertionError)
        self.assertIsInstance(captured.exception.exceptions[1], OSError)

    async def test_caught_websocket_exception_is_not_raised_again_by_client(self):
        app = Ryuuseigun('caught-socket', config=Config(propagate_exceptions=True))

        @app.websocket('/')
        async def fail(ws):
            await ws.accept()
            raise ValueError('failed')

        async with app.test_client() as client:
            with self.assertRaisesRegex(ValueError, 'failed'):
                async with client.websocket('/'):
                    pass

    async def test_lifespan_concurrency_overlap_and_cleanup(self):
        app = Ryuuseigun('lifecycle')
        events = []

        @app.lifespan
        @asynccontextmanager
        async def lifespan(app):
            events.append('startup')
            app.state.ready = True
            try:
                yield
            finally:
                app.state.ready = False
                events.append('shutdown')

        @app.get('/')
        async def route(req):
            return {'ready': app.state.ready}

        async with app.test_client() as client:
            with self.assertRaisesRegex(RuntimeError, 'already has'):
                async with app.test_client():
                    self.fail('Overlapping context entered')
            responses = await asyncio.gather(client.get('/'), client.get('/'))
            self.assertTrue(all(res.json()['ready'] for res in responses))
        self.assertEqual(events, ['startup', 'shutdown'])
        with self.assertRaisesRegex(RuntimeError, 'closed'):
            await client.get('/')

    async def test_startup_failure_timeout_and_cancel_release_owner(self):
        for mode in ('failure', 'timeout', 'cancel'):
            with self.subTest(mode=mode):
                app = Ryuuseigun(f'startup-{mode}')
                entered, cleaned = asyncio.Event(), asyncio.Event()

                @app.lifespan
                @asynccontextmanager
                async def lifespan(app, entered=entered, cleaned=cleaned, mode=mode):
                    try:
                        entered.set()
                        if mode == 'failure':
                            raise RuntimeError('bad startup')
                        await asyncio.Event().wait()
                        yield
                    finally:
                        cleaned.set()

                client = app.test_client(lifespan_timeout=.02 if mode == 'timeout' else 1)
                if mode == 'failure':
                    with self.assertLogs(app.logger, 'ERROR'):
                        with self.assertRaisesRegex(RuntimeError, 'startup failed'):
                            await client.__aenter__()
                elif mode == 'timeout':
                    with self.assertRaisesRegex(TimeoutError, 'startup timed out'):
                        await client.__aenter__()
                else:
                    task = asyncio.create_task(client.__aenter__())
                    await entered.wait()
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                self.assertTrue(cleaned.is_set())
                self.assertTrue(client._lifespan.task.done())
                with self.assertRaisesRegex(RuntimeError, 'failed'):
                    await client.get('/')

    async def test_shutdown_failure_preserves_test_failure(self):
        app = Ryuuseigun('shutdown-failure')

        @app.shutdown
        async def shutdown():
            raise RuntimeError('shutdown failed')

        with self.assertLogs(app.logger, 'ERROR'):
            with self.assertRaises(BaseExceptionGroup) as captured:
                async with app.test_client():
                    raise AssertionError('test failed')
        self.assertIsInstance(captured.exception.exceptions[0], AssertionError)
        self.assertIn('shutdown failed', str(captured.exception.exceptions[1]))

    async def test_owned_requests_and_websockets_close_before_resources(self):
        app = Ryuuseigun('owned-tasks')
        events = []
        started = asyncio.Event()

        @app.shutdown
        async def shutdown():
            events.append('shutdown')

        @app.get('/wait')
        async def waiting(req):
            try:
                started.set()
                await asyncio.Event().wait()
            finally:
                events.append('http closed')

        @app.websocket('/socket')
        async def socket(ws):
            try:
                await ws.accept()
                await ws.receive_text()
            finally:
                events.append('socket closed')

        async with app.test_client() as client:
            task = asyncio.create_task(client.get('/wait'))
            await started.wait()
            await client.websocket('/socket').__aenter__()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(events[-1], 'shutdown')
        self.assertCountEqual(events[:-1], ['http closed', 'socket closed'])

class ClientHTTPTests(IsolatedAsyncioTestCase):
    async def test_cookie_domains_paths_and_expiry(self):
        app = Ryuuseigun('cookie-policy')

        @app.get('/set')
        async def set_cookies(req):
            res = redirect('/private/echo', 303)
            res.set_cookie('good', 'yes', domain='example.test')
            res.set_cookie('bad', 'no', domain='elsewhere.test')
            res.set_cookie('scoped', 'yes', path='/private')
            res.set_cookie('expired', 'no', expires=datetime(2000, 1, 1, tzinfo=UTC))
            return res

        @app.get('/private/echo')
        @app.get('/echo', name='public_echo')
        async def echo(req):
            return req.cookies

        client = app.test_client(base_url='https://example.test')
        self.assertEqual((await client.get('/set', follow_redirects=True)).json(), {'good': 'yes', 'scoped': 'yes'})
        self.assertEqual((await client.get('/echo')).json(), {'good': 'yes'})

    async def test_cookie_redirect_form_flow(self):
        app = Ryuuseigun('cookies')

        @app.post('/login')
        async def login(req):
            form = await req.form()
            res = redirect('/account', 303)
            res.set_cookie('session', str(form['username']), secure=True)
            res.set_cookie('private', 'hidden', path='/private', secure=True)
            return res

        @app.get('/account')
        async def account(req):
            return {'cookies': req.cookies, 'scheme': req.scope['scheme'], 'method': req.method}

        @app.post('/logout')
        async def logout(req):
            res = redirect('/account', 303)
            res.delete_cookie('session')
            return res

        async with app.test_client(base_url='https://testserver:8443') as client:
            res = await client.post('/login', form={'username': 'demo'}, follow_redirects=True)
            self.assertEqual(res.json(), {'cookies': {'session': 'demo'}, 'scheme': 'https', 'method': 'GET'})
            self.assertEqual(len(res.history), 1)
            self.assertEqual(res.url, 'https://testserver:8443/account')
            logged_out = await client.post('/logout', follow_redirects=True)
            self.assertEqual(logged_out.json()['cookies'], {})

        insecure = app.test_client()
        res = await insecure.post('/login', form={'username': 'demo'}, follow_redirects=True)
        self.assertEqual(res.json()['cookies'], {})

    async def test_redirect_methods_origin_and_loop(self):
        app = Ryuuseigun('redirects')

        @app.post('/redirect/<int:code>')
        async def move(req, code):
            return redirect('/target', code)

        @app.route('/target', methods=('GET', 'POST'))
        async def target(req):
            return {'method': req.method, 'body': await req.text()}

        @app.get('/loop')
        async def loop(req):
            return redirect('/loop')

        @app.get('/external')
        async def external(req):
            return redirect('https://example.org/')

        client = app.test_client(max_redirects=2)
        for code in (301, 302, 303, 307, 308):
            res = await client.post(f'/redirect/{code}', body='body', follow_redirects=True)
            expected = {'method': 'POST', 'body': 'body'} if code in (307, 308) else {'method': 'GET', 'body': ''}
            self.assertEqual(res.json(), expected)
        with self.assertRaisesRegex(ValueError, 'origin'):
            await client.get('/external', follow_redirects=True)
        with self.assertRaisesRegex(RuntimeError, 'Maximum redirects'):
            await client.get('/loop', follow_redirects=True)

    async def test_multipart_repeated_fields_and_conflicts(self):
        app = Ryuuseigun('multipart-client')

        @app.post('/')
        async def upload(req):
            form = await req.form()
            uploads = form.getlist('file')
            self.assertTrue(all(isinstance(item, UploadFile) for item in uploads))
            return {'tags': form.getlist('tag'), 'files': [(await item.read()).decode() for item in uploads]}

        client = app.test_client()
        res = await client.post('/', form=[('tag', 'one'), ('tag', 'two')], files=[
            ('file', ('a.txt', b'A', 'text/plain')), ('file', ('b.txt', b'B', 'text/plain')),
        ])
        self.assertEqual(res.json(), {'tags': ['one', 'two'], 'files': ['A', 'B']})
        for options in ({'body': b'', 'json': {}}, {'json': {}, 'form': {}}, {'body': 'x', 'files': {}}):
            with self.assertRaisesRegex(ValueError, 'only one'):
                await client.post('/', **options)
