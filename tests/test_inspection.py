from io import StringIO
from json import loads
from unittest import TestCase
from unittest.mock import patch
from types import SimpleNamespace
from ryuuseigun import Ryuuseigun
from ryuuseigun.__main__ import main
from contextlib import redirect_stdout

class InspectionTests(TestCase):
    def test_inspection_lists_routes_and_mounts_without_starting_resources(self):
        app = Ryuuseigun('inspect')
        started = []

        @app.startup
        async def startup():
            started.append(True)

        @app.middleware
        async def outer(req, next):
            await next(req)

        @app.get('/hello')
        async def hello(req):
            return 'hello'

        @app.websocket('/socket')
        async def socket(ws):
            await ws.accept()

        app.mount('/child', Ryuuseigun('child'), name='child', lifespan=True)
        output = StringIO()
        with patch('sys.argv', ['ryuuseigun', 'inspect', 'sample:app', '--json']):
            with patch('ryuuseigun.__main__.import_module', return_value=SimpleNamespace(app=app)):
                with redirect_stdout(output):
                    main()
        result = loads(output.getvalue())
        self.assertEqual([route['path'] for route in result['routes']], ['/hello', '/socket'])
        self.assertTrue(result['routes'][0]['middleware'][0].endswith('.outer'))
        self.assertEqual(result['routes'][1]['protocol'], 'websocket')
        self.assertEqual(result['mounts'], [{'path': '/child', 'name': 'child', 'lifespan': True}])
        self.assertEqual(started, [])
        with self.assertRaises(RuntimeError):
            app.add_url_rule('/late', hello)
