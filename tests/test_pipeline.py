from functools import wraps
from dataclasses import dataclass, field
from asyncio import sleep, gather, CancelledError
from unittest import IsolatedAsyncioTestCase
from ryuuseigun import Config, Module, Ryuuseigun, Request, Response, WebSocket, UploadFile, StreamingResponse

class PipelineTests(IsolatedAsyncioTestCase):
    async def test_decorators_and_nested_hooks_preserve_rejections_and_streaming(self):
        app = Ryuuseigun(__name__)
        api = Module('api', url_prefix='/api')
        features = Module('features', url_prefix='/features')
        events = []

        @app.middleware
        async def resource(req, next):
            events.append('open')
            req.state.active = True
            try:
                await next(req)
            finally:
                req.state.active = False
                events.append('close')

        @api.before_request
        async def user_agent(req):
            events.append('user-agent')
            if not req.headers.get('User-Agent'):
                return 'missing user agent', 400

        @api.before_request
        async def daily_limit(req):
            events.append('daily')
            if req.headers.get('X-Daily') == 'exhausted':
                return 'daily limit', 429

        @api.after_request
        async def cors(req, response):
            events.append('cors')
            return response, response.status_code, {'Access-Control-Allow-Origin': '*'}

        @features.before_request
        async def feature(req):
            events.append('feature')

        @features.after_request
        async def feature_response(req, response):
            events.append('feature-after')
            response.headers['X-Feature'] = 'yes'
            return response

        def consume(*, cost):
            def decorate(handler):
                @wraps(handler)
                async def wrapped(req, *args, **kwargs):
                    events.append(f'rate:{cost}')
                    if req.headers.get('X-Bucket') == 'exhausted':
                        return 'bucket limit', 429
                    return await handler(req, *args, **kwargs)
                return wrapped
            return decorate

        def requires_authentication():
            def decorate(handler):
                @wraps(handler)
                async def wrapped(req, *args, **kwargs):
                    events.append('auth')
                    if req.headers.get('Authorization') != 'Bearer example':
                        return 'unauthorized', 401
                    return await handler(req, *args, **kwargs)
                return wrapped
            return decorate

        @features.get('/<int:item_id>')
        @consume(cost=2)
        @requires_authentication()
        async def item(req: Request, item_id: int):
            events.append('handler')
            async def chunks():
                self.assertTrue(req.state.active)
                events.append('stream')
                try:
                    yield str(item_id)
                finally:
                    self.assertTrue(req.state.active)
                    events.append('stream-close')
            return StreamingResponse(chunks())

        api.register_module(features)
        app.register_module(api)
        cases = (
            ({}, 400, 'missing user agent', ['open', 'user-agent', 'cors', 'close']),
            ({'User-Agent': 'test', 'X-Daily': 'exhausted'}, 429, 'daily limit',
             ['open', 'user-agent', 'daily', 'cors', 'close']),
            ({'User-Agent': 'test', 'X-Bucket': 'exhausted'}, 429, 'bucket limit',
             ['open', 'user-agent', 'daily', 'feature', 'rate:2', 'feature-after', 'cors', 'close']),
            ({'User-Agent': 'test'}, 401, 'unauthorized',
             ['open', 'user-agent', 'daily', 'feature', 'rate:2', 'auth', 'feature-after', 'cors', 'close']),
            ({'User-Agent': 'test', 'Authorization': 'Bearer example'}, 200, '42',
             ['open', 'user-agent', 'daily', 'feature', 'rate:2', 'auth', 'handler',
              'feature-after', 'cors', 'stream', 'stream-close', 'close']),
        )
        for headers, status, body, expected_events in cases:
            with self.subTest(status=status, body=body):
                events.clear()
                response = await app.test_client().get('/api/features/42', headers=headers)
                self.assertEqual(response.status_code, status)
                self.assertEqual(response.text, body)
                self.assertEqual(response.headers['Access-Control-Allow-Origin'], '*')
                self.assertEqual('X-Feature' in response.headers, 'feature' in expected_events)
                self.assertEqual(events, expected_events)

    async def test_scope_entry_stays_local_during_concurrent_failures(self):
        for boundary in (False, True):
            with self.subTest(middleware_boundary=boundary):
                app = Ryuuseigun(__name__)
                outer = Module('outer', url_prefix='/outer')
                inner = Module('inner', url_prefix='/inner')
                last = Module('last', url_prefix='/last')

                @outer.after_request
                async def outer_header(req, response):
                    response.headers['X-Outer'] = 'yes'
                    return response

                if boundary:
                    @inner.middleware
                    async def admit(req, next):
                        await sleep(0)
                        if req.headers.get('X-Deny') == 'middleware':
                            await req.respond(('denied', 401))
                            return
                        await next(req)

                @inner.before_request
                async def inner_before(req):
                    await sleep(0)
                    if req.headers.get('X-Deny') == 'hook':
                        return 'denied', 403
                    if req.headers.get('X-Deny') == 'error':
                        raise LookupError('denied')

                @inner.after_request
                async def inner_header(req, response):
                    response.headers['X-Inner'] = 'yes'
                    return response

                @last.after_request
                async def last_header(req, response):
                    response.headers['X-Last'] = 'yes'
                    return response

                @app.errorhandler(LookupError)
                async def lookup_error(req, error):
                    await sleep(0)
                    return 'denied', 422

                @last.get('/')
                async def index(req):
                    await sleep(0)
                    return 'ok'

                inner.register_module(last)
                outer.register_module(inner)
                app.register_module(outer)
                denials = ['none', 'hook', 'error'] * 8
                if boundary:
                    denials += ['middleware'] * 8
                responses = await gather(*(
                    app.test_client().get('/outer/inner/last/', headers={'X-Deny': denial})
                    for denial in denials
                ))
                for denial, response in zip(denials, responses, strict=True):
                    self.assertEqual(response.status_code, {'none': 200, 'hook': 403, 'error': 422, 'middleware': 401}[denial])
                    self.assertEqual(response.headers['X-Outer'], 'yes')
                    self.assertEqual('X-Inner' in response.headers, denial != 'middleware')
                    self.assertEqual('X-Last' in response.headers, denial == 'none')

    async def test_decorated_handlers_receive_middleware_updated_path_parameters(self):
        app = Ryuuseigun(__name__)

        @app.middleware
        async def inject(req, next):
            req.path_params['label'] = 'injected'
            await next(req)

        def decorate(handler):
            @wraps(handler)
            async def wrapped(req, *args, **kwargs):
                value = await handler(req, *args, **kwargs)
                return value, 201
            return wrapped

        @app.get('/')
        @decorate
        async def index(req: Request, label: str = 'default'):
            return label

        response = await app.test_client().get('/')
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.text, 'injected')

    async def test_after_hooks_can_replace_response_with_json_or_response(self):
        app = Ryuuseigun(__name__)
        module = Module('api')

        @module.after_request
        async def replace(req, response):
            return {'original': response.status_code}, 202

        @app.after_request
        async def finalize(req, response):
            self.assertEqual(response.status_code, 202)
            self.assertEqual(response.body, b'{"original":200}')
            return Response('replaced', 203)

        @module.get('/')
        async def index(req):
            return 'original'

        app.register_module(module)
        response = await app.test_client().get('/')
        self.assertEqual(response.status_code, 203)
        self.assertEqual(response.text, 'replaced')

    async def test_reused_policies_preserve_route_and_module_identity(self):
        @dataclass
        class Policy:
            calls: list[str] = field(default_factory=list, compare=False)

            async def __call__(self, req, next):
                self.calls.append(req.path)
                if req.headers.get('Authorization') != 'example':
                    await req.respond(('denied', 403))
                    return
                await next(req)

        first, second = Policy(), Policy()
        self.assertEqual(first, second)
        app = Ryuuseigun(__name__)

        @app.get('/a', middlewares=(first,))
        async def a(req):
            return 'a'

        @app.get('/b', middlewares=(first,))
        async def b(req):
            return 'b'

        @app.get('/c', middlewares=(second,))
        async def c(req):
            return 'c'

        for name in ('left', 'right'):
            module = Module(name, url_prefix=f'/{name}')
            module.middleware(first)

            @module.before_request
            async def identify(req, name=name):
                req.state.origin = name

            @module.after_request
            async def header(req, response):
                response.headers['X-Origin'] = req.state.origin
                return response

            @module.get('/')
            async def index(req):
                return req.state.origin

            app.register_module(module)

        paths = ('/a', '/b', '/c', '/left/', '/right/')
        responses = await gather(*(
            app.test_client().get(path, headers={'Authorization': 'example'}) for path in paths
        ))
        for path, response in zip(paths, responses, strict=True):
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.text, path.strip('/'))
        self.assertEqual(responses[3].headers['X-Origin'], 'left')
        self.assertEqual(responses[4].headers['X-Origin'], 'right')
        self.assertEqual((await app.test_client().get('/c')).status_code, 403)
        self.assertEqual((await app.test_client().post('/a')).status_code, 405)
        self.assertEqual((await app.test_client().request('OPTIONS', '/a')).status_code, 204)
        self.assertCountEqual(first.calls, ['/a', '/b', '/left/', '/right/'])
        self.assertEqual(second.calls, ['/c', '/c'])

    async def test_failures_close_uploads_before_resource_middleware_unwinds(self):
        class Fatal(BaseException):
            pass

        for stage in ('middleware', 'before', 'handler'):
            for failure_type in (RuntimeError, CancelledError, Fatal):
                with self.subTest(stage=stage, failure=failure_type):
                    app = Ryuuseigun(__name__, config=Config(propagate_exceptions=True))
                    uploads = []
                    events = []
                    failure = failure_type('stop')

                    @app.middleware
                    async def resource(req, next, uploads=uploads, events=events):
                        try:
                            await next(req)
                        finally:
                            self.assertEqual(len(uploads), 1)
                            self.assertTrue(uploads[0].closed)
                            events.append('closed')

                    async def fail(req, uploads=uploads, failure=failure):
                        form = await req.form()
                        upload = form['file']
                        self.assertIsInstance(upload, UploadFile)
                        uploads.append(upload)
                        raise failure

                    if stage == 'middleware':
                        @app.middleware
                        async def middleware(req, next, fail=fail):
                            await fail(req)
                    elif stage == 'before':
                        app.before_request(fail)

                    @app.post('/')
                    async def index(req, stage=stage, fail=fail):
                        if stage == 'handler':
                            await fail(req)
                        self.fail('Failed request reached the handler')

                    body = b'--b\r\nContent-Disposition: form-data; name="file"; filename="data"\r\n\r\nhello\r\n--b--\r\n'
                    with self.assertRaises(failure_type) as caught:
                        await app.test_client().post('/', body=body, headers={'Content-Type': 'multipart/form-data; boundary=b'})
                    self.assertIs(caught.exception, failure)
                    self.assertEqual(events, ['closed'])

    async def test_websocket_decorators_and_middleware_keep_deferred_entry_and_close_order(self):
        app = Ryuuseigun(__name__)
        module = Module('chat', url_prefix='/chat')
        events = []

        def resource(label):
            async def middleware(socket, next):
                events.append(f'{label}-enter')
                pending = next(socket)
                self.assertEqual(events[-1], f'{label}-enter')
                socket.path_params['room'] += 1
                try:
                    await pending
                finally:
                    self.assertTrue(socket.closed)
                    events.append(f'{label}-exit')
            return middleware

        app.websocket_middleware(resource('app'))
        module.websocket_middleware(resource('module'))

        def decorate(handler):
            @wraps(handler)
            async def wrapped(*args, **kwargs):
                events.append('decorator')
                await handler(*args, **kwargs)
            return wrapped

        @module.websocket('/<int:room>', middlewares=(resource('route'),))
        @decorate
        async def chat(socket: WebSocket, room: int):
            events.append('handler')
            await socket.accept()
            await socket.send_json({'room': room})

        app.register_module(module)
        async with app.test_client().websocket('/chat/42') as socket:
            self.assertEqual(await socket.receive_json(), {'room': 45})
        self.assertEqual(events, [
            'app-enter', 'module-enter', 'route-enter', 'decorator', 'handler',
            'route-exit', 'module-exit', 'app-exit',
        ])
