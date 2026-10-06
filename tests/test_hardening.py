from typing import Optional
from orjson import OPT_SORT_KEYS
from unittest import IsolatedAsyncioTestCase
from ryuuseigun import Next, abort, Config, Module, Ryuuseigun, Request, Response, MediaType


class PipelineTests(IsolatedAsyncioTestCase):
    async def test_handled_module_error_unwinds_through_after_hooks_and_middleware(self) -> None:
        app = Ryuuseigun(__name__)
        module = Module('api', url_prefix='/api')
        calls: list[str] = []

        @module.middleware
        async def around(req: Request, next: Next) -> None:
            calls.append('middleware-before')
            await next(req)
            calls.append('middleware-after')

        @module.after_request
        async def after(_req, res: Response) -> Response:
            calls.append('after')
            res.headers['X-After'] = 'yes'
            return res

        @module.errorhandler(ValueError)
        async def value_error(_req, error: Exception) -> tuple[dict[str, str], int]:
            calls.append('error')
            return {'error': str(error)}, 422

        @module.get('/broken')
        async def broken(_req) -> None:
            calls.append('handler')
            raise ValueError('broken')

        app.register_module(module)
        response = await app.test_client().get('/api/broken')

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.headers['X-After'], 'yes')
        self.assertEqual(calls, ['middleware-before', 'handler', 'error', 'after', 'middleware-after'])

    async def test_outer_route_middleware_receives_handled_inner_failure(self) -> None:
        app = Ryuuseigun(__name__)
        calls: list[str] = []

        async def outer(req: Request, next: Next) -> None:
            calls.append('outer-before')
            await next(req)
            calls.append('outer-after')

        async def failing(req: Request, next: Next) -> None:
            raise LookupError('middleware failed')

        @app.errorhandler(LookupError)
        async def lookup_error(_req, error: Exception) -> tuple[dict[str, str], int]:
            return {'error': str(error)}, 409

        @app.get('/route', middlewares=(outer, failing,))
        async def route(_req) -> str:
            return 'unreachable'

        response = await app.test_client().get('/route')

        self.assertEqual(response.status_code, 409)
        self.assertEqual(calls, ['outer-before', 'outer-after'])

    async def test_nested_module_error_precedence_prefers_deepest_scope(self) -> None:
        app = Ryuuseigun(__name__)
        parent = Module('parent')
        child = Module('child')

        @parent.errorhandler(LookupError)
        async def parent_error(_req, error: Exception) -> tuple[dict[str, str], int]:
            return {'scope': 'parent'}, 409

        @child.errorhandler(LookupError)
        async def child_error(_req, error: Exception) -> tuple[dict[str, str], int]:
            return {'scope': 'child'}, 404

        @child.get('/missing')
        async def missing(_req) -> None:
            raise LookupError

        parent.register_module(child)
        app.register_module(parent)
        response = await app.test_client().get('/missing')

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {'scope': 'child'})

    async def test_module_pipeline_processes_method_errors_and_automatic_options(self) -> None:
        app = Ryuuseigun(__name__)
        module = Module('api')
        calls: list[str] = []

        @module.middleware
        async def around(req: Request, next: Next) -> None:
            calls.append(f'before:{req.method}')
            await next(req)
            calls.append(f'after:{req.method}')

        @module.after_request
        async def after(req: Request, res: Response) -> Response:
            res.headers['X-Method'] = req.method
            return res

        @module.errorhandler(405)
        async def method_error(_req, error: Exception) -> tuple[dict[str, str], int]:
            return {'scope': 'module'}, 405

        @module.get('/resource')
        async def resource(_req) -> str:
            return 'ok'

        app.register_module(module)
        client = app.test_client()
        rejected = await client.post('/resource')
        options = await client.request('OPTIONS', '/resource')

        self.assertEqual(rejected.json(), {'scope': 'module'})
        self.assertEqual(rejected.headers['X-Method'], 'POST')
        self.assertEqual(options.status_code, 204)
        self.assertEqual(options.headers['X-Method'], 'OPTIONS')
        self.assertEqual(calls, ['before:POST', 'after:POST', 'before:OPTIONS', 'after:OPTIONS'])


class ConfigurationTests(IsolatedAsyncioTestCase):
    async def test_pretty_json_and_debug_details(self) -> None:
        app = Ryuuseigun(__name__, config=Config(pretty_json=True, debug=True, expose_error_details=True))

        @app.get('/json')
        async def json_route(_req) -> dict[str, int]:
            return {'answer': 42}

        @app.get('/error')
        async def error_route(_req) -> None:
            raise RuntimeError('visible')

        client = app.test_client()
        json_response = await client.get('/json')
        error_response = await client.get('/error')

        self.assertIn(b'  "answer": 42', json_response.body)
        self.assertTrue(json_response.body.endswith(b'\n'))
        self.assertEqual(error_response.json()['error']['message'], 'RuntimeError: visible')

    async def test_constructor_options_override_config(self) -> None:
        app = Ryuuseigun(
            __name__,
            config=Config(debug=False, strict_slashes=False),
            debug=True,
            strict_slashes=True,
        )

        self.assertTrue(app.debug)
        self.assertTrue(app.config.strict_slashes)

    async def test_invalid_body_limit_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, 'non-negative'):
            Config(max_request_body_size=-1)

    async def test_query_limits_are_enforced_before_routing(self) -> None:
        size_limited = Ryuuseigun(__name__, config=Config(max_query_string_size=3))
        count_limited = Ryuuseigun(__name__, config=Config(max_query_parameters=1))

        size_response = await size_limited.test_client().get('/missing?abcd')
        count_response = await count_limited.test_client().get('/missing?a=1&b=2')

        self.assertEqual(size_response.status_code, 414)
        self.assertEqual(size_response.json()['error']['message'], 'Query string is too large')
        self.assertEqual(count_response.status_code, 414)
        self.assertEqual(count_response.json()['error']['message'], 'Too many query parameters')

    async def test_trusted_hosts_support_exact_and_wildcard_hosts(self) -> None:
        app = Ryuuseigun(__name__, config=Config(trusted_hosts=('example.com', '*.example.org')))

        @app.get('/')
        async def index(_req) -> str:
            return 'ok'

        client = app.test_client()
        exact = await client.get('/', headers={'Host': 'example.com:8443'})
        wildcard = await client.get('/', headers={'Host': 'api.example.org'})
        wildcard_root = await client.get('/', headers={'Host': 'example.org'})
        untrusted = await client.get('/', headers={'Host': 'attacker.example'})

        self.assertEqual(exact.status_code, 200)
        self.assertEqual(wildcard.status_code, 200)
        self.assertEqual(wildcard_root.status_code, 400)
        self.assertEqual(untrusted.status_code, 400)

    async def test_automatic_head_and_options_can_be_disabled(self) -> None:
        app = Ryuuseigun(__name__, config=Config(automatic_head=False, automatic_options=False))

        @app.get('/resource')
        async def resource(_req) -> str:
            return 'ok'

        client = app.test_client()
        head = await client.request('HEAD', '/resource')
        options = await client.request('OPTIONS', '/resource')

        self.assertEqual(head.status_code, 405)
        self.assertEqual(options.status_code, 405)
        self.assertEqual(head.headers['Allow'], 'GET')
        self.assertEqual(options.headers['Allow'], 'GET')

    async def test_slash_redirects_preserve_query_strings(self) -> None:
        app = Ryuuseigun(__name__, config=Config(strict_slashes=True, redirect_slashes=True))

        @app.get('/resource/')
        async def resource(_req) -> str:
            return 'ok'

        res = await app.test_client().get('/resource?view=compact')

        self.assertEqual(res.status_code, 308)
        self.assertEqual(res.headers['Location'], '/resource/?view=compact')

    async def test_exception_propagation_and_detail_exposure_are_independent(self) -> None:
        hidden = Ryuuseigun(__name__, config=Config(debug=True))
        visible = Ryuuseigun(__name__, config=Config(expose_error_details=True))
        propagated = Ryuuseigun(__name__, config=Config(propagate_exceptions=True))

        async def fail(_req) -> None:
            raise RuntimeError('sensitive')

        hidden.add_url_rule('/', fail)
        visible.add_url_rule('/', fail)
        propagated.add_url_rule('/', fail)

        hidden_response = await hidden.test_client().get('/')
        visible_response = await visible.test_client().get('/')

        self.assertEqual(hidden_response.json()['error']['message'], 'Internal Server Error')
        self.assertEqual(visible_response.json()['error']['message'], 'RuntimeError: sensitive')
        with self.assertRaisesRegex(RuntimeError, 'sensitive'):
            await propagated.test_client().get('/')

    async def test_problem_details_json_and_serialization_options(self) -> None:
        problem_app = Ryuuseigun(__name__, config=Config(error_media_type=MediaType.PROBLEM_JSON))
        sorted_app = Ryuuseigun(__name__, config=Config(json_options=OPT_SORT_KEYS))

        @sorted_app.get('/')
        async def sorted_json(_req) -> dict[str, int]:
            return {'z': 1, 'a': 2}

        problem = await problem_app.test_client().get('/missing')
        sorted_response = await sorted_app.test_client().get('/')

        self.assertEqual(problem.headers['Content-Type'], MediaType.PROBLEM_JSON)
        self.assertEqual(
            problem.json(),
            {'type': 'about:blank', 'title': 'Not Found', 'status': 404, 'detail': 'Not Found'},
        )
        self.assertEqual(sorted_response.body, b'{"a":2,"z":1}')

    async def test_default_response_headers_do_not_override_route_headers(self) -> None:
        app = Ryuuseigun(
            __name__,
            config=Config(default_response_headers=(('X-Framework', 'ryuuseigun'), ('Cache-Control', 'private'))),
        )

        @app.get('/')
        async def index(_req) -> Response:
            return Response('ok', headers={'Cache-Control': 'no-store'})

        response = await app.test_client().get('/')
        missing = await app.test_client().get('/missing')

        self.assertEqual(response.headers['X-Framework'], 'ryuuseigun')
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertEqual(missing.headers['X-Framework'], 'ryuuseigun')

    async def test_configuration_values_are_validated(self) -> None:
        for name in ('max_query_string_size', 'max_query_parameters'):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, 'non-negative'):
                Config(**{name: -1})  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, 'error_media_type'):
            Config(error_media_type='text/plain')
        with self.assertRaisesRegex(ValueError, 'unsupported orjson'):
            Config(json_options=1 << 30)
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            Config(default_response_headers=(('X-Test', 'one'), ('x-test', 'two')))
        with self.assertRaisesRegex(ValueError, 'trusted host'):
            Config(trusted_hosts=('bad host',))


class RegistrationTests(IsolatedAsyncioTestCase):
    async def test_converter_precedence_and_method_fallback(self) -> None:
        app = Ryuuseigun(__name__)

        @app.post('/values/<int:value>')
        async def integer(_req, value: int) -> dict[str, object]:
            return {'kind': 'int', 'value': value}

        @app.get('/values/<value>')
        async def string(_req, value: str) -> dict[str, str]:
            return {'kind': 'string', 'value': value}

        numeric_get = await app.test_client().get('/values/42')
        numeric_post = await app.test_client().post('/values/42')

        self.assertEqual(numeric_get.json(), {'kind': 'string', 'value': '42'})
        self.assertEqual(numeric_post.json(), {'kind': 'int', 'value': 42})

    async def test_same_converter_shape_conflicts_even_with_different_names(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/items/<int:item_id>')
        async def first(_req, item_id: int) -> dict[str, int]:
            return {'id': item_id}

        with self.assertRaisesRegex(ValueError, 'Duplicate route'):
            @app.get('/items/<int:other_id>')
            async def second(_req, other_id: int) -> dict[str, int]:
                return {'id': other_id}

    async def test_failed_multi_method_registration_is_atomic(self) -> None:
        app = Ryuuseigun(__name__)

        @app.get('/resource')
        async def existing(_req) -> str:
            return 'existing'

        async def conflicting(_req) -> str:
            return 'conflicting'

        with self.assertRaisesRegex(ValueError, 'Duplicate route'):
            app.add_url_rule('/resource', conflicting, methods=('GET', 'POST'))

        response = await app.test_client().post('/resource')
        self.assertEqual(response.status_code, 405)

    async def test_failed_module_registration_adds_no_partial_routes(self) -> None:
        app = Ryuuseigun(__name__)
        module = Module('feature')

        @app.get('/taken')
        async def taken(_req) -> str:
            return 'app'

        @module.get('/new')
        async def new(_req) -> str:
            return 'new'

        @module.get('/taken')
        async def conflict(_req) -> str:
            return 'module'

        with self.assertRaisesRegex(ValueError, 'Duplicate route'):
            app.register_module(module)

        self.assertEqual((await app.test_client().get('/new')).status_code, 404)

    async def test_duplicate_mount_is_rejected_and_registered_module_is_frozen(self) -> None:
        app = Ryuuseigun(__name__)
        module = Module('feature')

        @module.get('/route')
        async def route(_req) -> str:
            return 'ok'

        app.register_module(module, url_prefix='/one')

        with self.assertRaisesRegex(ValueError, 'Duplicate endpoint'):
            app.register_module(module, url_prefix='/two')
        with self.assertRaisesRegex(RuntimeError, 'cannot be changed'):
            @module.get('/late')
            async def late(_req) -> str:
                return 'late'

        self.assertEqual((await app.test_client().get('/one/route')).status_code, 200)
        self.assertEqual((await app.test_client().get('/two/route')).status_code, 404)

    async def test_cyclic_module_registration_is_rejected_atomically(self) -> None:
        app = Ryuuseigun(__name__)
        first = Module('first')
        second = Module('second')

        @first.get('/first')
        async def first_route(_req) -> str:
            return 'first'

        first.register_module(second)
        second.register_module(first)

        with self.assertRaisesRegex(ValueError, 'Cyclic'):
            app.register_module(first)

        self.assertEqual((await app.test_client().get('/first')).status_code, 404)


class SignatureTests(IsolatedAsyncioTestCase):
    async def test_sync_module_route_is_rejected_during_definition(self) -> None:
        module = Module('feature')

        with self.assertRaisesRegex(TypeError, 'must be async'):
            @module.get('/')  # type: ignore[type-var]
            def invalid() -> str:
                return 'invalid'

        with self.assertRaisesRegex(TypeError, 'must be async'):
            @module.websocket('/')  # type: ignore[type-var]
            def invalid_socket() -> None:
                return None

    async def test_error_handler_keys_are_validated(self) -> None:
        app = Ryuuseigun(__name__)

        async def handler(_req, error: Exception) -> Response:
            return Response(str(error))

        with self.assertRaisesRegex(ValueError, 'between 100 and 599'):
            app.register_error_handler(99, handler)
        with self.assertRaisesRegex(TypeError, 'status codes or exception types'):
            app.register_error_handler(True, handler)
        with self.assertRaisesRegex(TypeError, 'status codes or exception types'):
            app.register_error_handler(str, handler)  # type: ignore[arg-type]

    async def test_invalid_hook_signatures_fail_during_registration(self) -> None:
        app = Ryuuseigun(__name__)

        async def invalid_before(one: Request, two: Request) -> Optional[Response]:
            return None

        async def invalid_after() -> Response:
            return Response()

        async def invalid_error() -> Response:
            return Response()

        with self.assertRaisesRegex(TypeError, 'must accept'):
            app.before_request(invalid_before)  # type: ignore[type-var]
        with self.assertRaisesRegex(TypeError, 'must accept'):
            app.after_request(invalid_after)  # type: ignore[type-var]
        with self.assertRaisesRegex(TypeError, 'must accept'):
            app.register_error_handler(Exception, invalid_error)  # type: ignore[arg-type]

    async def test_request_and_websocket_parameters_must_accept_positional_values(self) -> None:
        app = Ryuuseigun(__name__)

        with self.assertRaisesRegex(TypeError, 'positional'):
            @app.get('/')  # type: ignore[type-var]
            async def invalid_route(*, req: Request) -> str:
                return req.path

        with self.assertRaisesRegex(TypeError, 'positional'):
            @app.websocket('/')  # type: ignore[type-var]
            async def invalid_socket(*, socket) -> None:
                return None

    async def test_status_handler_handles_abort_and_specific_exception_beats_500(self) -> None:
        app = Ryuuseigun(__name__)

        @app.errorhandler(418)
        async def teapot(_req, error: Exception) -> tuple[dict[str, str], int]:
            return {'handler': 'teapot'}, 418

        @app.errorhandler(500)
        async def internal(_req, error: Exception) -> tuple[dict[str, str], int]:
            return {'handler': '500'}, 500

        @app.errorhandler(ValueError)
        async def value_error(_req, error: Exception) -> tuple[dict[str, str], int]:
            return {'handler': 'value'}, 422

        @app.get('/teapot')
        async def abort_route(_req) -> None:
            abort(418)

        @app.get('/value')
        async def value_route(_req) -> None:
            raise ValueError

        client = app.test_client()

        self.assertEqual((await client.get('/teapot')).json(), {'handler': 'teapot'})
        self.assertEqual((await client.get('/value')).json(), {'handler': 'value'})
