"""Equivalent JSON, 64 KiB streaming and 64 KiB upload endpoints.

Each factory imports only its own framework so RSS includes the dependencies
that framework actually uses. All responses are checked by the load generator.
"""

async def chunks():
    for _ in range(16):
        yield b'x' * 4096

def create_ryuuseigun():
    from ryuuseigun import Ryuuseigun, StreamingResponse

    app = Ryuuseigun('benchmark')

    @app.get('/health')
    async def health(req):
        return {'ok': True}

    @app.get('/users/<int:user_id>')
    async def user(req, user_id):
        return {'id': user_id}

    @app.get('/stream')
    async def stream(req):
        return StreamingResponse(chunks(), media_type='application/octet-stream')

    @app.post('/upload')
    async def upload(req):
        return {'size': len(await req.body())}

    return app

def create_starlette():
    from starlette.routing import Route
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse, StreamingResponse

    async def health(req):
        return JSONResponse({'ok': True})

    async def user(req):
        return JSONResponse({'id': req.path_params['user_id']})

    async def stream(req):
        return StreamingResponse(chunks(), media_type='application/octet-stream')

    async def upload(req):
        return JSONResponse({'size': len(await req.body())})

    return Starlette(routes=[
        Route('/health', health), Route('/users/{user_id:int}', user),
        Route('/stream', stream), Route('/upload', upload, methods=['POST']),
    ])

def create_quart():
    from quart import Quart, request, Response

    app = Quart('benchmark')

    @app.get('/health')
    async def health():
        return {'ok': True}

    @app.get('/users/<int:user_id>')
    async def user(user_id):
        return {'id': user_id}

    @app.get('/stream')
    async def stream():
        return Response(chunks(), content_type='application/octet-stream')

    @app.post('/upload')
    async def upload():
        return {'size': len(await request.get_data())}

    return app
