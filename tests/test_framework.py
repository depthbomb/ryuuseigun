import asyncio
from uuid import UUID
from typing import Optional
from ryuuseigun import MultipartOverrides
from collections.abc import AsyncIterator
from unittest import IsolatedAsyncioTestCase
from ryuuseigun import (
    Next,
    abort,
    Config,
    Module,
    Ryuuseigun,
    Request,
    request,
    url_for,
    Response,
    asyncify,
    JSONValue,
    UploadFile,
    MultipartLimits,
    ConcurrencyLimit,
)


class RoutingTests(IsolatedAsyncioTestCase):
    async def test_named_routes_can_build_encoded_urls(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/people/<int:person_id>/files/<path:file_path>', name='person_file')
        async def person_file(_req, person_id: int, file_path: str) -> str:
            return f'{person_id}:{file_path}'

        @app.get('/person-link')
        async def person_link(_req) -> str:
            return url_for('person_file', person_id=2, file_path='notes/today.txt')

        self.assertEqual(
            app.url_for('person_file', person_id=42, file_path='résumés/final copy.pdf'),
            '/people/42/files/r%C3%A9sum%C3%A9s/final%20copy.pdf',
        )
        self.assertEqual((await app.test_client().get(app.url_for('person_file', person_id=42, file_path='a/b'))).text, '42:a/b')
        self.assertEqual((await app.test_client().get('/person-link')).text, '/people/2/files/notes/today.txt')

    async def test_module_route_names_are_qualified_for_url_building(self) -> None:
        app = Ryuuseigun(__name__)
        api = Module('api', url_prefix='/v1')

        @api.get('/items/<int:item_id>', name='item')
        async def item(_req, item_id: int) -> str:
            return str(item_id)

        app.register_module(api, url_prefix='/service')

        self.assertEqual(app.url_for('api.item', item_id=7), '/service/v1/items/7')

    async def test_url_building_rejects_unknown_missing_and_invalid_values(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/items/<int:item_id>', name='item')
        async def item(_req, item_id: int) -> str:
            return str(item_id)

        with self.assertRaisesRegex(KeyError, 'Unknown endpoint'):
            app.url_for('missing')
        with self.assertRaisesRegex(ValueError, 'Missing route parameters'):
            app.url_for('item')
        with self.assertRaisesRegex(ValueError, 'Unexpected route parameters'):
            app.url_for('item', item_id=1, extra=True)
        with self.assertRaisesRegex(ValueError, 'Invalid value'):
            app.url_for('item', item_id='not-an-int')

    async def test_static_routes_take_priority_and_parameters_are_converted(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/items/<item>')
        async def dynamic(_req, item: str) -> dict[str, str]:
            return {'route': 'dynamic', 'item': item}

        @app.get('/items/latest')
        async def latest(_req) -> dict[str, str]:
            return {'route': 'static'}

        @app.get('/numbers/<int:value>/<float:ratio>/<uuid:key>')
        async def converted(_req, value: int, ratio: float, key: UUID) -> dict[str, object]:
            return {'value': value, 'ratio': ratio, 'key': str(key)}

        client = app.test_client()
        static_response = await client.get('/items/latest')
        converted_response = await client.get('/numbers/42/1.5/12345678-1234-5678-1234-567812345678')

        self.assertEqual(static_response.json(), {'route': 'static'})
        self.assertEqual(
            converted_response.json(),
            {'value': 42, 'ratio': 1.5, 'key': '12345678-1234-5678-1234-567812345678'},
        )

    async def test_path_converter_and_non_strict_slashes(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/files/<path:name>')
        async def files(_req, name: str) -> dict[str, str]:
            return {'name': name}

        response = await app.test_client().get('/files/a/b/c/')

        self.assertEqual(response.json(), {'name': 'a/b/c'})

    async def test_head_and_options_are_automatic(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/health')
        async def health(_req) -> str:
            return 'healthy'

        client = app.test_client()
        head = await client.request('HEAD', '/health')
        options = await client.request('OPTIONS', '/health')

        self.assertEqual(head.status_code, 200)
        self.assertEqual(head.body, b'')
        self.assertEqual(head.headers['Content-Length'], '7')
        self.assertEqual(options.status_code, 204)
        self.assertEqual(options.headers['Allow'], 'GET, HEAD, OPTIONS')

    async def test_unknown_method_has_json_405_and_allow_header(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/only-get')
        async def only_get(_req) -> str:
            return 'ok'

        response = await app.test_client().post('/only-get')

        self.assertEqual(response.status_code, 405)
        self.assertEqual(response.headers['Allow'], 'GET, HEAD, OPTIONS')
        self.assertEqual(response.json()['error']['code'], 405)

    async def test_strict_slashes_distinguish_routes(self) -> None:
        app = Ryuuseigun(__name__, strict_slashes=True)

        @app.get('/with-slash/')
        async def with_slash(_req) -> str:
            return 'yes'

        client = app.test_client()

        self.assertEqual((await client.get('/with-slash/')).status_code, 200)
        self.assertEqual((await client.get('/with-slash')).status_code, 404)

    async def test_request_can_be_explicit_or_context_local(self) -> None:
        app = Ryuuseigun(__name__)

        @app.post('/explicit/<int:item_id>')
        async def explicit(req: Request, item_id: int) -> dict[str, object]:
            return {'method': req.method, 'id': item_id, 'body': await req.json()}

        @app.get('/local')
        async def local(_req) -> dict[str, str]:
            await asyncio.sleep(0)
            return {'path': request.path}

        client = app.test_client()
        explicit_response = await client.post('/explicit/7', json={'valid': True})
        local_one, local_two = await asyncio.gather(client.get('/local?call=1'), client.get('/local?call=2'))

        self.assertEqual(explicit_response.json(), {'method': 'POST', 'id': 7, 'body': {'valid': True}})
        self.assertEqual(local_one.json(), {'path': '/local'})
        self.assertEqual(local_two.json(), {'path': '/local'})


class ModuleAndMiddlewareTests(IsolatedAsyncioTestCase):
    async def test_nested_modules_compose_prefixes(self) -> None:
        app = Ryuuseigun(__name__)
        api = Module('api', url_prefix='/api')
        features = Module('features', url_prefix='/features')

        @features.get('/enabled')
        async def enabled(_req) -> dict[str, bool]:
            return {'enabled': True}

        api.register_module(features)
        app.register_module(api, url_prefix='/v1')

        response = await app.test_client().get('/v1/api/features/enabled')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'enabled': True})

    async def test_app_module_and_route_middleware_order(self) -> None:
        app = Ryuuseigun(__name__)
        module = Module('api', url_prefix='/api')
        calls: list[str] = []

        @app.middleware
        async def app_middleware(req: Request, next: Next) -> None:
            calls.append('app-before')
            await next(req)
            calls.append('app-after')

        @app.after_request
        async def add_header(req: Request, res: Response) -> Response:
            res.headers['X-App'] = 'yes'
            return res

        @module.middleware
        async def module_middleware(req: Request, next: Next) -> None:
            calls.append('module-before')
            await next(req)
            calls.append('module-after')

        async def route_middleware(req: Request, next: Next) -> None:
            calls.append('route-before')
            await next(req)
            calls.append('route-after')

        @module.get('/value', middlewares=(route_middleware,))
        async def value(_req) -> dict[str, bool]:
            calls.append('handler')
            return {'ok': True}

        app.register_module(module)
        response = await app.test_client().get('/api/value')

        self.assertEqual(response.headers['X-App'], 'yes')
        self.assertEqual(
            calls,
            ['app-before', 'module-before', 'route-before', 'handler', 'route-after', 'module-after', 'app-after'],
        )

    async def test_before_can_short_circuit_and_after_still_runs(self) -> None:
        app = Ryuuseigun(__name__)
        module = Module('private', url_prefix='/private')
        called = False

        @module.before_request
        async def require_auth(req: Request) -> Optional[tuple[dict[str, bool], int]]:
            if req.headers.get('Authorization') is None:
                return {'blocked': True}, 401
            return None

        @module.after_request
        async def mark(_req, res: Response) -> Response:
            res.headers['X-Module'] = 'private'
            return res

        @module.get('/data')
        async def data(_req) -> dict[str, bool]:
            nonlocal called
            called = True
            return {'secret': True}

        app.register_module(module)
        response = await app.test_client().get('/private/data')

        self.assertFalse(called)
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.headers['X-Module'], 'private')

    async def test_app_middleware_and_hooks_apply_to_not_found_responses(self) -> None:
        app = Ryuuseigun(__name__)
        calls: list[str] = []

        @app.middleware
        async def around(req: Request, next: Next) -> None:
            calls.append('middleware-before')
            await next(req)
            calls.append('middleware-after')

        @app.before_request
        async def before(_req) -> None:
            calls.append('before')

        @app.after_request
        async def after(_req, res: Response) -> Response:
            calls.append('after')
            res.headers['X-After'] = 'yes'
            return res

        response = await app.test_client().get('/missing')

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.headers['X-After'], 'yes')
        self.assertEqual(calls, ['middleware-before', 'before', 'after', 'middleware-after'])


class ErrorAndJSONTests(IsolatedAsyncioTestCase):
    async def test_abort_handles_any_status_without_registration_loop(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/gone')
        async def gone(_req) -> None:
            abort(410, 'No longer here')

        response = await app.test_client().get('/gone')

        self.assertEqual(response.status_code, 410)
        self.assertEqual(response.json(), {'error': {'code': 410, 'message': 'No longer here'}})

    async def test_module_error_handler_overrides_app_handler(self) -> None:
        app = Ryuuseigun(__name__)
        module = Module('mod')

        @app.errorhandler(ValueError)
        async def app_value_error(_req, error: Exception) -> tuple[dict[str, str], int]:
            return {'scope': 'app'}, 500

        @module.errorhandler(ValueError)
        async def module_value_error(req: Request, error: Exception) -> tuple[dict[str, str], int]:
            return {'scope': 'module', 'path': req.path}, 422

        @module.get('/bad')
        async def bad(_req) -> str:
            raise ValueError('bad')

        app.register_module(module)
        response = await app.test_client().get('/bad')

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json(), {'scope': 'module', 'path': '/bad'})

    async def test_orjson_request_and_response(self) -> None:
        app = Ryuuseigun(__name__)

        @app.post('/echo')
        async def echo(req: Request) -> tuple[JSONValue, int, dict[str, str]]:
            return await req.json(), 201, {'X-JSON': 'orjson'}

        response = await app.test_client().post('/echo', json={'unicode': 'é', 'items': [1, 2]})

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.headers['Content-Type'], 'application/json')
        self.assertEqual(response.headers['X-JSON'], 'orjson')
        self.assertEqual(response.json(), {'unicode': 'é', 'items': [1, 2]})

    async def test_invalid_json_and_wrong_content_type(self) -> None:
        app = Ryuuseigun(__name__)

        @app.post('/json')
        async def parse(req: Request) -> JSONValue:
            return await req.json()

        client = app.test_client()
        malformed = await client.post('/json', body='{', headers={'Content-Type': 'application/json'})
        wrong_type = await client.post('/json', body='{}', headers={'Content-Type': 'text/plain'})

        self.assertEqual(malformed.status_code, 400)
        self.assertEqual(wrong_type.status_code, 415)

    async def test_json_null_is_a_valid_test_client_payload(self) -> None:
        app = Ryuuseigun(__name__)

        @app.post('/json')
        async def parse(req: Request) -> JSONValue:
            return await req.json()

        response = await app.test_client().post('/json', json=None)

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json())

    async def test_urlencoded_forms_are_typed_cached_multidicts(self) -> None:
        app = Ryuuseigun(__name__)

        @app.post('/form')
        async def parse(req: Request) -> dict[str, object]:
            form = await req.form()
            cached = await req.form()
            name = form['name']
            tags = form.getlist('tag')
            return {
                'name': name,
                'tags': tags,
                'empty': form['empty'],
                'cached': form is cached,
            }

        res = await app.test_client().post(
            '/form',
            body='name=Andr%E9&tag=async&tag=typed&empty=',
            headers={'Content-Type': 'application/x-www-form-urlencoded; charset=iso-8859-1'},
        )

        self.assertEqual(
            res.json(),
            {'name': 'André', 'tags': ['async', 'typed'], 'empty': '', 'cached': True},
        )

    async def test_multipart_forms_include_typed_uploads(self) -> None:
        app = Ryuuseigun(__name__)

        @app.post('/form')
        async def parse(req: Request) -> dict[str, object]:
            form = await req.form()
            upload = form['avatar']
            if not isinstance(upload, UploadFile):
                raise TypeError('avatar was not parsed as an upload')
            return {
                'title': form['title'],
                'filename': upload.filename,
                'content_type': upload.content_type,
                'size': upload.size,
                'content': (await upload.read()).decode(),
            }

        boundary = 'ryuuseigun-boundary'
        body = (
            f'--{boundary}\r\n'
            'Content-Disposition: form-data; name="title"\r\n\r\n'
            'Profile\r\n'
            f'--{boundary}\r\n'
            'Content-Disposition: form-data; name="avatar"; filename="hello.txt"\r\n'
            'Content-Type: text/plain\r\n\r\n'
            'hello upload\r\n'
            f'--{boundary}--\r\n'
        )
        res = await app.test_client().post(
            '/form',
            body=body,
            headers={'Content-Type': f'multipart/form-data; boundary={boundary}'},
        )

        self.assertEqual(
            res.json(),
            {
                'title': 'Profile',
                'filename': 'hello.txt',
                'content_type': 'text/plain',
                'size': 12,
                'content': 'hello upload',
            },
        )

    async def test_multipart_parts_stream_directly_to_a_destination(self) -> None:
        app = Ryuuseigun(__name__)
        uploads: dict[str, bytes] = {}
        chunk_sizes: list[int] = []

        class ObjectStore:
            async def upload(self, filename: str, stream: AsyncIterator[bytes]) -> None:
                data = bytearray()
                async for chunk in stream:
                    chunk_sizes.append(len(chunk))
                    data.extend(chunk)
                uploads[filename] = bytes(data)

        object_store = ObjectStore()

        @app.post('/stream-form')
        async def stream_form(_req) -> dict[str, object]:
            seen = []
            async for part in request.multipart():
                if part.filename:
                    await object_store.upload(part.filename, part.stream())
                seen.append(part)
            return {
                'parts': [
                    {
                        'name': part.name,
                        'filename': part.filename,
                        'content_type': part.content_type,
                        'complete': part.complete,
                        'size': part.size,
                    }
                    for part in seen
                ]
            }

        boundary = 'stream-boundary'
        payload = b'x' * 70_000 + f'\r\n--{boundary}X'.encode() + b'y' * 70_000
        body = (
            f'--{boundary}\r\nContent-Disposition: form-data; name="title"\r\n\r\nignored\r\n'.encode()
            + f'--{boundary}\r\nContent-Disposition: form-data; name="asset"; filename="large.bin"\r\n'
              'Content-Type: application/octet-stream\r\n\r\n'.encode()
            + payload
            + f'\r\n--{boundary}--\r\n'.encode()
        )
        response = await app.test_client().post(
            '/stream-form',
            body=body,
            headers={'Content-Type': f'multipart/form-data; boundary={boundary}'},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(uploads, {'large.bin': payload})
        self.assertLessEqual(max(chunk_sizes), 64 * 1024)
        self.assertEqual(
            response.json(),
            {
                'parts': [
                    {
                        'name': 'title',
                        'filename': None,
                        'content_type': None,
                        'complete': True,
                        'size': 7,
                    },
                    {
                        'name': 'asset',
                        'filename': 'large.bin',
                        'content_type': 'application/octet-stream',
                        'complete': True,
                        'size': len(payload),
                    },
                ]
            },
        )

    async def test_multipart_limits_apply_to_streams_and_support_route_overrides(self) -> None:
        app = Ryuuseigun(
            __name__,
            config=Config(
                max_request_body_size=None,
                max_multipart_parts=2,
                max_multipart_field_size=4,
                max_multipart_file_size=5,
            ),
        )

        async def parse(req: Request) -> dict[str, int]:
            parts = 0
            async for part in req.multipart():
                await part.read()
                parts += 1
            return {'parts': parts}

        app.add_url_rule('/limited', parse, methods=('POST',), endpoint='limited')
        app.add_url_rule(
            '/override',
            parse,
            methods=('POST',),
            endpoint='override',
            multipart=MultipartOverrides(max_parts=3, max_field_size=8, max_file_size=8),
        )

        @app.post('/ignored')
        async def ignored(_req) -> str:
            return 'ok'

        boundary = 'limits'

        def body(*parts: str) -> str:
            return ''.join(f'--{boundary}\r\n{part}\r\n' for part in parts) + f'--{boundary}--\r\n'

        headers = {'Content-Type': f'multipart/form-data; boundary={boundary}'}
        large_field = body('Content-Disposition: form-data; name="field"\r\n\r\n12345')
        large_file = body('Content-Disposition: form-data; name="file"; filename="a.bin"\r\n\r\n123456')
        too_many = body(
            'Content-Disposition: form-data; name="one"\r\n\r\n1',
            'Content-Disposition: form-data; name="two"\r\n\r\n2',
            'Content-Disposition: form-data; name="three"\r\n\r\n3',
        )

        client = app.test_client()
        self.assertEqual((await client.post('/limited', body=large_field, headers=headers)).status_code, 413)
        self.assertEqual((await client.post('/limited', body=large_file, headers=headers)).status_code, 413)
        self.assertEqual((await client.post('/limited', body=too_many, headers=headers)).status_code, 413)
        self.assertEqual((await client.post('/override', body=large_field, headers=headers)).status_code, 200)
        self.assertEqual((await client.post('/override', body=large_file, headers=headers)).status_code, 200)
        self.assertEqual((await client.post('/override', body=too_many, headers=headers)).json(), {'parts': 3})
        self.assertEqual((await client.post('/ignored', body=large_field, headers=headers)).status_code, 200)

    async def test_module_multipart_limits_and_limit_validation(self) -> None:
        app = Ryuuseigun(__name__, config=Config(max_multipart_field_size=1))
        module = Module('forms')

        @module.post('/parse', multipart=MultipartOverrides(max_field_size=3))
        async def parse(req: Request) -> str:
            form = await req.form()
            return str(form['value'])

        app.register_module(module)
        boundary = 'module-limit'
        body = (
            f'--{boundary}\r\nContent-Disposition: form-data; name="value"\r\n\r\nabc\r\n'
            f'--{boundary}--\r\n'
        )
        response = await app.test_client().post(
            '/parse',
            body=body,
            headers={'Content-Type': f'multipart/form-data; boundary={boundary}'},
        )

        self.assertEqual(response.text, 'abc')

        async def invalid() -> str:
            return 'invalid'

        with self.assertRaisesRegex(TypeError, 'unknown'):
            MultipartOverrides(unknown=1)
        for value in (-1, True):
            with self.assertRaises(ValueError):
                MultipartLimits(max_file_size=value)  # type: ignore[arg-type]

    async def test_form_rejects_unsupported_and_malformed_content(self) -> None:
        app = Ryuuseigun(__name__)

        @app.post('/form')
        async def parse(req: Request) -> dict[str, object]:
            form = await req.form()
            return {'items': form.items()}

        client = app.test_client()
        unsupported = await client.post('/form', body='plain', headers={'Content-Type': 'text/plain'})
        malformed = await client.post(
            '/form',
            body='not multipart',
            headers={'Content-Type': 'multipart/form-data'},
        )

        self.assertEqual(unsupported.status_code, 415)
        self.assertEqual(malformed.status_code, 400)

    async def test_sync_helpers_run_work_off_loop(self) -> None:
        @asyncify
        def add(left: int, right: int) -> int:
            return left + right

        self.assertEqual(await add(2, 3), 5)

    async def test_concurrency_limit_rejects_excess_and_releases_capacity(self) -> None:
        app = Ryuuseigun(__name__)
        limiter = ConcurrencyLimit(1, retry_after=2)
        app.middleware(limiter)
        entered = asyncio.Event()
        release = asyncio.Event()

        @app.get('/')
        async def index(_req) -> str:
            entered.set()
            await release.wait()
            return 'ok'

        client = app.test_client()
        active = asyncio.create_task(client.get('/'))
        await entered.wait()
        rejected = await client.get('/')

        self.assertEqual(rejected.status_code, 503)
        self.assertEqual(rejected.headers['Retry-After'], '2')
        self.assertEqual(limiter.active, 1)

        release.set()
        self.assertEqual((await active).status_code, 200)
        self.assertEqual(limiter.active, 0)
        self.assertEqual((await client.get('/')).status_code, 200)

    async def test_concurrency_limit_releases_capacity_when_cancelled(self) -> None:
        app = Ryuuseigun(__name__)
        limiter = ConcurrencyLimit(1)
        app.middleware(limiter)
        entered = asyncio.Event()

        @app.get('/')
        async def index(_req) -> str:
            entered.set()
            await asyncio.Event().wait()
            return 'unreachable'

        task = asyncio.create_task(app.test_client().get('/'))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(limiter.active, 0)


class ValidationTests(IsolatedAsyncioTestCase):
    async def test_sync_routes_are_rejected(self) -> None:
        app = Ryuuseigun(__name__)

        with self.assertRaisesRegex(TypeError, 'must be async'):
            @app.get('/')  # type: ignore[type-var]
            def invalid() -> str:
                return 'no'

    async def test_duplicate_routes_and_endpoints_are_rejected(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/one')
        async def one(_req) -> str:
            return 'one'

        with self.assertRaisesRegex(ValueError, 'Duplicate endpoint'):
            async def another(_req) -> str:
                return 'another'

            app.add_url_rule('/two', another, endpoint='one')

    async def test_same_handler_can_be_registered_for_multiple_methods(self) -> None:
        app = Ryuuseigun(__name__)

        async def shared(_req) -> str:
            return 'shared'

        app.add_url_rule('/shared', shared, methods=('GET',))
        app.add_url_rule('/shared', shared, methods=('POST',))

        client = app.test_client()
        self.assertEqual((await client.get('/shared')).status_code, 200)
        self.assertEqual((await client.post('/shared')).status_code, 200)

    async def test_sync_route_middleware_is_rejected(self) -> None:
        def sync_middleware(req: Request, next: Next) -> Response:
            return Response()

        with self.assertRaisesRegex(TypeError, 'must be async'):
            async def route(_req):
                return 'ok'

            Ryuuseigun(__name__).get('/', middlewares=(sync_middleware,))(route)
