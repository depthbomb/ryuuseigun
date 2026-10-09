from subprocess import run
from sys import executable
from unittest import TestCase

class DependencyTests(TestCase):
    def test_core_runs_without_optional_dependencies(self):
        run([executable, '-c', '''
import sys
from asyncio import run
from importlib.abc import MetaPathFinder

class RejectOptional(MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname.split('.')[0] in {'sqrrl', 'starlette', 'granian', 'brotli'}:
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

run(check())
'''], check=True)
