import sys
import subprocess
from functools import wraps
from unittest import IsolatedAsyncioTestCase
from pydantic import BaseModel, Field
from openapi_spec_validator import validate
from ryuuseigun import Ryuuseigun, Config, Module, JSONResponse
from ryuuseigun.openapi import OpenAPI
from ryuuseigun.validation import parse_json, parse_query

class Payload(BaseModel):
    title: str = Field(min_length=1)

class Result(BaseModel):
    id: int
    title: str

class Query(BaseModel):
    limit: int = Field(default=10, ge=1, le=100)

class SchemaTests(IsolatedAsyncioTestCase):
    async def test_schema_runtime_validation_wrappers_and_cache(self):
        app = Ryuuseigun('schema', config=Config(propagate_exceptions=True))
        api = OpenAPI(app, title='Test', version='1')
        module = Module('api', url_prefix='/api')

        def wrapper(handler):
            @wraps(handler)
            async def wrapped(*args, **kwargs):
                return await handler(*args, **kwargs)
            return wrapped

        @module.post('/items/<int:item_id>')
        @wrapper
        @api.schema(body=Payload, responses={201: Result})
        async def item(req, item_id):
            payload = await parse_json(req, Payload)
            self.assertIs(payload, await parse_json(req, Payload))
            return JSONResponse({'id': item_id, 'title': payload.title}, status_code=201)

        @module.get('/page')
        @api.schema(query=Query, responses={200: None})
        async def page(req):
            return {'limit': parse_query(req, Query).limit}

        app.register_module(module)
        document = api.document()
        validate(document)
        operation = document['paths']['/api/items/{item_id}']['post']
        self.assertEqual(operation['parameters'][0]['schema']['type'], 'integer')
        self.assertIn('422', operation['responses'])
        document['info']['title'] = 'mutated'
        self.assertEqual(api.document()['info']['title'], 'Test')

        client = app.test_client()
        self.assertEqual((await client.post('/api/items/1', json={'title': 'Hello'})).json(), {'id': 1, 'title': 'Hello'})
        invalid = await client.post('/api/items/1', json={'title': ''})
        self.assertEqual(invalid.status_code, 422)
        self.assertNotIn('input', invalid.json()['errors'][0])
        self.assertEqual((await client.post('/api/items/1', body='{', headers={'Content-Type': 'application/json'})).status_code, 400)
        self.assertEqual((await client.post('/api/items/1', body='{}')).status_code, 415)
        self.assertEqual((await client.get('/api/page?limit=3')).json(), {'limit': 3})
        self.assertEqual((await client.get('/api/page?limit=0')).status_code, 422)
        first = await client.get('/openapi.json')
        self.assertEqual(first.body, (await client.get('/openapi.json')).body)
        self.assertIn('Download schema', (await client.get('/docs')).text)

    async def test_response_validation_and_duplicate_identifiers(self):
        app = Ryuuseigun('response-validation', config=Config(propagate_exceptions=True))
        api = OpenAPI(app, title='Test', version='1')

        @app.get('/')
        @api.schema(responses={200: Result})
        async def wrong(req):
            return {'id': 'not-an-integer', 'title': 'Hello'}

        with self.assertRaises(ValueError):
            await app.test_client().get('/')

        duplicate = Ryuuseigun('duplicates')
        schema = OpenAPI(duplicate, title='Duplicates', version='1')

        @duplicate.get('/one')
        @schema.schema(responses={200: None}, operation_id='same')
        async def one(req):
            return None

        @duplicate.get('/two')
        @schema.schema(responses={200: None}, operation_id='same')
        async def two(req):
            return None

        with self.assertRaisesRegex(ValueError, 'Duplicate OpenAPI'):
            duplicate.finalize()

    def test_core_import_does_not_import_optional_dependencies(self):
        code = '''
import asyncio, sys
from importlib.abc import MetaPathFinder
class RejectOptional(MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname.split('.')[0] in {'pydantic', 'sqlalchemy', 'starlette'}:
            raise ImportError('Optional dependency is unavailable')
sys.meta_path.insert(0, RejectOptional())
from ryuuseigun import Ryuuseigun
app = Ryuuseigun('core-only')
@app.get('/')
async def route(req):
    return {'ok': True}
async def check():
    async with app.test_client() as client:
        assert (await client.get('/')).json() == {'ok': True}
asyncio.run(check())
assert not {'pydantic', 'sqlalchemy', 'ryuuseigun.openapi'} & sys.modules.keys()
'''
        subprocess.run([sys.executable, '-c', code], check=True)
