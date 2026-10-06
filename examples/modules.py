from typing import Optional
from ryuuseigun import Module, Request, Response, Ryuuseigun

app = Ryuuseigun(__name__)
api = Module('api', url_prefix='/api')
users = Module('users', url_prefix='/users')


@api.before_request
async def require_client_header(req: Request) -> Optional[tuple[dict[str, str], int]]:
    if req.headers.get('X-Client') is None:
        return {'error': 'X-Client is required'}, 401
    return None


@api.after_request
async def identify_api(_req: Request, res: Response) -> Response:
    res.headers['X-API-Version'] = '1'
    return res


@api.after_request
async def identify_module(req: Request, res: Response) -> Response:
    res.headers['X-Module'] = 'api'
    return res


@api.errorhandler(LookupError)
async def lookup_error(req: Request, error: Exception) -> tuple[dict[str, str], int]:
    return {'error': str(error), 'path': req.path}, 404


@users.get('')
async def list_users(_req: Request) -> dict[str, list[dict[str, object]]]:
    return {'users': [{'id': 1, 'name': 'Ada'}, {'id': 2, 'name': 'Grace'}]}


@users.get('/<int:user_id>')
async def get_user(_req: Request, user_id: int) -> dict[str, object]:
    if user_id not in {1, 2}:
        raise LookupError(f'User {user_id} was not found')
    return {'id': user_id, 'name': 'Ada' if user_id == 1 else 'Grace'}


api.register_module(users)
app.register_module(api, url_prefix='/v1')


if __name__ == '__main__':
    print('Run with: granian --interface asgi examples.modules:app')
