from uuid import UUID
from ryuuseigun import abort, Config, Request, request, Response, JSONValue, Ryuuseigun

app = Ryuuseigun(__name__, config=Config(max_request_body_size=1024 * 1024))


@app.get('/')
async def index(_req: Request) -> dict[str, object]:
    return {
        'framework': 'ryuuseigun',
        'examples': [
            '/users/42',
            '/search?q=fast',
            '/objects/12345678-1234-5678-1234-567812345678',
            '/teapot',
        ],
    }


@app.get('/users/<int:user_id>')
async def get_user(_req: Request, user_id: int) -> dict[str, object]:
    return {'id': user_id, 'active': True}


@app.get('/objects/<uuid:object_id>')
async def get_object(_req: Request, object_id: UUID) -> dict[str, str]:
    return {'id': str(object_id)}


@app.get('/search')
async def search(_req: Request) -> dict[str, object]:
    return {
        'query': request.args.get('q', ''),
        'tags': request.args.getlist('tag'),
    }


@app.post('/echo')
async def echo(req: Request) -> tuple[JSONValue, int]:
    return await req.json(), 201


@app.post('/profile')
async def profile(req: Request) -> dict[str, str]:
    form = await req.form()
    display_name = form.get('display_name')
    return {'display_name': display_name if isinstance(display_name, str) else ''}


@app.get('/custom-response')
async def custom_response(_req: Request) -> Response:
    return Response('Ryuuseigun response', headers={'X-Framework': 'ryuuseigun'})


@app.get('/teapot')
async def teapot(_req: Request) -> None:
    abort(418, 'Short and stout')


if __name__ == '__main__':
    print('Run with: granian --interface asgi examples.basic:app')
