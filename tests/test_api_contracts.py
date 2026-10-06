from unittest.mock import patch
from dataclasses import FrozenInstanceError
from unittest import IsolatedAsyncioTestCase
from asyncio import Event, gather, create_task, CancelledError
from ryuuseigun import (
    Config,
    Module,
    Ryuuseigun,
    Request,
    Response,
    WebSocket,
    UploadFile,
    MultipartLimits,
    StreamingResponse,
    MultipartOverrides,
)

class LifecycleContractTests(IsolatedAsyncioTestCase):
    async def test_resources_cover_streaming_upload_cleanup_and_after_hooks(self):
        app = Ryuuseigun(__name__)
        events = []
        uploads = []
        active = False

        @app.middleware
        async def resource(req, next, events=events):
            nonlocal active
            active = True
            events.append('open')
            try:
                await next(req)
            finally:
                self.assertTrue(uploads[0].closed)
                events.append('close')
                active = False

        @app.after_request
        async def after(req, response):
            self.assertTrue(active)
            events.append('after')
            return response

        @app.post('/')
        async def route(req):
            form = await req.form()
            uploads.append(form['file'])

            async def chunks():
                self.assertTrue(active)
                events.append('read')
                yield await uploads[0].read()

            return StreamingResponse(chunks())

        original_close = UploadFile.close

        async def close(upload):
            self.assertTrue(active)
            events.append('cleanup')
            await original_close(upload)

        body = b'--b\r\nContent-Disposition: form-data; name="file"; filename="a"\r\n\r\nhello\r\n--b--\r\n'
        with patch.object(UploadFile, 'close', close):
            response = await app.test_client().post('/', body=body, headers={'Content-Type': 'multipart/form-data; boundary=b'})
        self.assertEqual(response.text, 'hello')
        self.assertEqual(events, ['open', 'after', 'read', 'cleanup', 'close'])

    async def test_send_failure_unwinds_once_without_sending_another_response(self):
        for failure_type in (RuntimeError, CancelledError):
            with self.subTest(failure=failure_type):
                app = Ryuuseigun(__name__)
                events = []
                sent = []

                @app.middleware
                async def resource(req, next, events=events):
                    events.append('open')
                    try:
                        await next(req)
                    finally:
                        events.append('close')

                @app.errorhandler(Exception)
                async def error(req, exception, events=events):
                    events.append('error')
                    return 'unexpected'

                @app.get('/')
                async def route(req):
                    return 'ok'

                async def receive():
                    await Event().wait()

                async def send(message, events=events, sent=sent, failure_type=failure_type):
                    self.assertEqual(events, ['open'])
                    sent.append(message)
                    if message['type'] == 'http.response.body':
                        raise failure_type()

                with self.assertRaises(failure_type):
                    await app({'type': 'http', 'method': 'GET', 'path': '/'}, receive, send)
                self.assertEqual(events, ['open', 'close'])
                self.assertEqual([message['type'] for message in sent], ['http.response.start', 'http.response.body'])

    async def test_stream_cancellation_closes_generator_before_middleware(self):
        app = Ryuuseigun(__name__)
        entered = Event()
        events = []

        @app.middleware
        async def resource(req, next, events=events):
            events.append('open')
            try:
                await next(req)
            finally:
                events.append('close')

        @app.get('/')
        async def route(req):
            async def chunks():
                try:
                    entered.set()
                    await Event().wait()
                    yield b'unreachable'
                finally:
                    events.append('generator-close')
            return StreamingResponse(chunks())

        task = create_task(app.test_client().get('/'))
        try:
            await entered.wait()
            task.cancel()
            with self.assertRaises(CancelledError):
                await task
        finally:
            task.cancel()
            await gather(task, return_exceptions=True)
        self.assertEqual(events, ['open', 'generator-close', 'close'])

    async def test_declared_route_middleware_short_circuits_with_scoped_hooks(self):
        for use_module in (False, True):
            with self.subTest(module=use_module):
                app = Ryuuseigun(__name__)
                target = Module('api') if use_module else app
                events = []

                @target.after_request
                async def after(req, response):
                    response.headers['X-Hook'] = 'yes'
                    return response

                async def auth(req, next, events=events):
                    events.append('auth')
                    await req.respond(('denied', 403))
                    events.append('sent')

                @target.get('/', middlewares=(auth,))
                async def route(req):
                    self.fail('Unauthorized handler ran')

                if use_module:
                    app.register_module(target)
                response = await app.test_client().get('/')
                self.assertEqual(response.status_code, 403)
                self.assertEqual(response.headers['X-Hook'], 'yes')
                self.assertEqual(events, ['auth', 'sent'])

    async def test_middleware_cannot_return_a_response_or_send_twice(self):
        for mode in ('return', 'twice'):
            app = Ryuuseigun(__name__, config=Config(propagate_exceptions=True))

            @app.middleware
            async def invalid(req, next, mode=mode):
                if mode == 'return':
                    return Response('legacy')
                await req.respond('first')
                await req.respond('second')

            with self.assertRaises(TypeError if mode == 'return' else RuntimeError):
                await app.test_client().get('/')

class RegistrationContractTests(IsolatedAsyncioTestCase):
    async def test_all_registration_is_frozen_by_finalize_and_first_use(self):
        for trigger in ('explicit', 'http', 'websocket', 'lifespan'):
            with self.subTest(trigger=trigger):
                app = Ryuuseigun(__name__)

                async def route(req):
                    return 'ok'

                async def middleware(req, next):
                    await next(req)

                async def before(req):
                    pass

                async def after(req, response):
                    return response

                async def error(req, exception):
                    return 'error'

                async def socket(connection):
                    await connection.accept()

                async def lifecycle():
                    pass

                app.get('/')(route)
                app.websocket('/socket')(socket)
                if trigger == 'explicit':
                    app.finalize()
                elif trigger == 'http':
                    await app.test_client().get('/')
                elif trigger == 'websocket':
                    async with app.test_client().websocket('/socket'):
                        pass
                else:
                    messages = iter(({'type': 'lifespan.startup'}, {'type': 'lifespan.shutdown'}))
                    async def receive(messages=messages):
                        return next(messages)
                    async def send(message):
                        self.assertTrue(message['type'].endswith('.complete'))
                    await app({'type': 'lifespan'}, receive, send)
                app.finalize()
                attempts = (
                    lambda app=app: app.get('/late')(route),
                    lambda app=app: app.websocket('/late')(socket),
                    lambda app=app: app.register_module(Module('late')),
                    lambda app=app: app.middleware(middleware),
                    lambda app=app: app.websocket_middleware(middleware),
                    lambda app=app: app.before_request(before),
                    lambda app=app: app.after_request(after),
                    lambda app=app: app.register_error_handler(Exception, error),
                    lambda app=app: app.startup(lifecycle),
                    lambda app=app: app.shutdown(lifecycle),
                    lambda app=app: app.lifespan(lambda current: None),
                    lambda app=app: app.router.add(app.router.match('/', 'GET').route),
                    lambda app=app: app.websocket_router.add(app.websocket_router.match('/socket')[0]),
                )
                for attempt in attempts:
                    with self.assertRaisesRegex(RuntimeError, 'finalized'):
                        attempt()
                self.assertEqual((await app.test_client().get('/')).text, 'ok')

    async def test_public_registries_and_routes_are_read_only(self):
        app = Ryuuseigun(__name__)
        module = Module('api')

        @module.get('/')
        async def route(req):
            return 'ok'

        app.register_module(module)
        for registry in (app, module):
            with self.assertRaises(AttributeError):
                registry.middlewares.append(None)
            with self.assertRaises(TypeError):
                registry.error_handlers[500] = None
        with self.assertRaises(AttributeError):
            app.config = Config()
        with self.assertRaises(AttributeError):
            app.router.strict_slashes = True
        with self.assertRaises(FrozenInstanceError):
            app.router.match('/', 'GET').route.path = '/changed'
        with self.assertRaises(FrozenInstanceError):
            module.routes[0].path = '/changed'

    async def test_request_and_socket_are_first_regardless_of_names(self):
        app = Ryuuseigun(__name__)

        @app.get('/<request>')
        async def route(incoming, request, label='default'):
            self.assertIsInstance(incoming, Request)
            return {'path': request, 'label': label}

        @app.websocket('/<socket>')
        async def socket_route(incoming, socket):
            self.assertIsInstance(incoming, WebSocket)
            await incoming.accept()
            await incoming.send_text(socket)

        response = await app.test_client().get('/value')
        self.assertEqual(response.json(), {'path': 'value', 'label': 'default'})
        async with app.test_client().websocket('/value') as session:
            self.assertEqual(await session.receive_text(), 'value')

    async def test_missing_request_or_socket_fails_at_registration(self):
        app = Ryuuseigun(__name__)

        async def no_arguments():
            pass

        for register in (app.get('/'), app.websocket('/'), app.before_request, app.after_request, app.errorhandler(Exception)):
            with self.assertRaises(TypeError):
                register(no_arguments)

class MultipartOverrideTests(IsolatedAsyncioTestCase):
    async def test_omitted_limits_inherit_and_none_only_disables_selected_limit(self):
        body = b'--b\r\nContent-Disposition: form-data; name="file"; filename="a"\r\n\r\n12345\r\n--b--\r\n'
        shared = MultipartOverrides(max_parts=2)
        for limit, expected in ((4, 413), (5, 200)):
            for use_module in (False, True):
                with self.subTest(limit=limit, module=use_module):
                    app = Ryuuseigun(__name__, config=Config(max_multipart_file_size=limit, max_multipart_field_size=7))
                    target = Module('api') if use_module else app

                    async def parse(req):
                        await req.form()
                        return 'ok'

                    target.post('/inherited', multipart=shared)(parse)
                    target.post('/unlimited', endpoint='unlimited', multipart=MultipartOverrides(max_file_size=None))(parse)
                    if use_module:
                        app.register_module(target)
                    for path, status in (('/inherited', expected), ('/unlimited', 200)):
                        response = await app.test_client().post(path, body=body, headers={'Content-Type': 'multipart/form-data; boundary=b'})
                        self.assertEqual(response.status_code, status)
                    limits = app.router.match('/unlimited', 'POST').route.multipart_limits
                    self.assertEqual(limits.max_field_size, 7)
                    self.assertEqual(limits.max_parts, 1000)
                    self.assertIsNone(limits.max_file_size)

    async def test_resolved_limits_and_dicts_are_rejected(self):
        async def route(req):
            return 'ok'

        for registry in (Ryuuseigun(__name__), Module('api')):
            for options in ({'max_parts': 2}, MultipartLimits(max_parts=2)):
                with self.assertRaisesRegex(TypeError, 'MultipartOverrides'):
                    registry.post('/', multipart=options)(route)
        for value in (-1, True, '2'):
            with self.assertRaises(ValueError):
                MultipartOverrides(max_parts=value)
