"""Inspect an application's public routes without starting its resources.

Run `python -m ryuuseigun inspect package.module:app`, adding --json for tools.
Importing the module runs its Python code; use application modules you trust.
The command finalizes registration and never starts an ASGI server or lifespan.
"""
from json import dumps
from argparse import ArgumentParser
from importlib import import_module
from ryuuseigun.app import Ryuuseigun

def main() -> None:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('inspect',))
    parser.add_argument('application', help='Python module:application')
    parser.add_argument('--json', action='store_true', help='Print machine-readable route information')
    options = parser.parse_args()
    module, separator, attribute = options.application.partition(':')
    if not separator or not module or not attribute:
        parser.error('Use module:application')
    application = getattr(import_module(module), attribute)
    if not isinstance(application, Ryuuseigun):
        parser.error('The application must be a Ryuuseigun instance')
    routes = [
        {
            'path': route.path, 'endpoint': route.endpoint, 'protocol': route.protocol,
            'methods': sorted(route.methods), 'modules': route.modules, 'middleware': route.middleware,
        }
        for route in application.inspect_routes()
    ]
    mounts = [{'path': mount.path or '/', 'name': mount.name, 'lifespan': mount.lifespan} for mount in application.mounts]
    if options.json:
        print(dumps({'routes': routes, 'mounts': mounts}, indent=2))
    else:
        for route in routes:
            print(f'{route["protocol"]:9} {",".join(route["methods"]):20} {route["path"]}  ({route["endpoint"]})')
            for middleware in route['middleware']:
                print(f'  {middleware}')
        for mount in mounts:
            print(f'mount     {mount["path"]}  ({mount["name"]}, lifespan={mount["lifespan"]})')

if __name__ == '__main__':
    main()
