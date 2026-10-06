from uuid import uuid4
from time import perf_counter
from secrets import compare_digest
from ryuuseigun.openapi import OpenAPI
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from examples.gallery.storage import Storage
from examples.gallery.rate_limits import Bucket
from sqlalchemy.ext.asyncio import create_async_engine
from examples.gallery.auth import requires_authentication
from ryuuseigun.validation import parse_json, parse_query
from examples.gallery.models import Page, Login, ImageList, CreateImage, ImageRecord, GalleryState
from ryuuseigun import (
    Next,
    abort,
    Config,
    Module,
    Request,
    Response,
    redirect,
    Ryuuseigun,
    UploadFile,
    JSONResponse,
    StreamingResponse,
)

def create_app(
    *, password: str, database_url: str = 'sqlite+aiosqlite:///gallery.db',
    secure_cookies: bool = True, allowed_origin: str = 'https://testserver',
) -> Ryuuseigun[GalleryState]:
    if not password:
        raise ValueError('Provide a non-empty gallery password')
    app = Ryuuseigun[GalleryState](
        __name__, request_state_factory=GalleryState,
        config=Config(max_request_body_size=1024 * 1024),
    )
    api = Module[GalleryState]('api', url_prefix='/api')
    schema = OpenAPI(app, title='Gallery', version='0.1', security_schemes={
        'session': {'type': 'apiKey', 'in': 'cookie', 'name': 'session'},
    })
    images_bucket = Bucket(2, 1)

    @app.middleware
    async def request_logging(req: Request[GalleryState], next: Next[GalleryState]) -> None:
        started = perf_counter()
        try:
            await next(req)
        finally:
            app.logger.info('Request %s finished in %.3f ms', req.state.request_id, (perf_counter() - started) * 1000)

    @app.lifespan
    @asynccontextmanager
    async def resources(app: Ryuuseigun[GalleryState]) -> AsyncIterator[None]:
        engine = create_async_engine(database_url)
        try:
            app.state.storage = Storage(engine)
            await app.state.storage.initialize()
            app.state.ready = True
            yield
        finally:
            app.state.ready = False
            await engine.dispose()

    @app.before_request
    async def session(req: Request[GalleryState]) -> None:
        req.state.request_id = uuid4().hex
        if req.method in {'POST', 'PUT', 'PATCH', 'DELETE'}:
            origin = req.headers.get('Origin')
            if origin is not None and origin != allowed_origin:
                abort(403, 'Origin is not allowed')
        token = req.cookies.get('session')
        if token:
            storage: Storage = app.state.storage
            csrf = await storage.session(token)
            if csrf is not None:
                req.state.user = 'demo'
                req.state.csrf_token = csrf
                if req.method in {'POST', 'PUT', 'PATCH', 'DELETE'} and not compare_digest(
                    req.headers.get('X-CSRF-Token', ''), csrf,
                ):
                    abort(403, 'Missing or invalid CSRF token')

    @app.after_request
    async def request_headers(req: Request[GalleryState], res: Response) -> Response:
        res.headers['X-Request-ID'] = req.state.request_id
        res.headers['X-Content-Type-Options'] = 'nosniff'
        return res

    @api.after_request
    async def private_headers(req: Request[GalleryState], res: Response) -> Response:
        res.headers['Cache-Control'] = 'no-store'
        return res

    @app.post('/login')
    @schema.schema(body=Login, responses={303: None, 401: None})
    async def login(req: Request[GalleryState]) -> Response:
        credentials = await parse_json(req, Login)
        if credentials.username != 'demo' or not compare_digest(credentials.password.encode(), password.encode()):
            abort(401, 'Invalid credentials')
        storage: Storage = app.state.storage
        token, _ = await storage.login()
        res = redirect('/account', 303)
        res.set_cookie('session', token, max_age=3600, secure=secure_cookies, httponly=True)
        return res

    @app.get('/account')
    @requires_authentication()
    async def account(req: Request[GalleryState]) -> dict[str, str | None]:
        return {'user': req.state.user, 'csrf_token': req.state.csrf_token}

    @app.post('/logout')
    @requires_authentication()
    async def logout(req: Request[GalleryState]) -> Response:
        storage: Storage = app.state.storage
        await storage.logout(req.cookies['session'])
        res = Response(status_code=204)
        res.delete_cookie('session', secure=secure_cookies, httponly=True)
        return res

    @api.post('/images')
    @requires_authentication()
    @schema.schema(body=CreateImage, responses={201: ImageRecord, 401: None, 403: None, 429: None}, security=('session',))
    @images_bucket.consume(cost=2)
    async def create_image(req: Request[GalleryState]) -> JSONResponse:
        payload = await parse_json(req, CreateImage)
        storage: Storage = app.state.storage
        record = await storage.create(payload.title)
        return JSONResponse(record.model_dump(mode='json'), status_code=201)

    @api.get('/images')
    @requires_authentication()
    @schema.schema(query=Page, responses={200: ImageList, 401: None}, security=('session',))
    async def images(req: Request[GalleryState]) -> JSONResponse:
        page = parse_query(req, Page)
        storage: Storage = app.state.storage
        records = await storage.list_images(page.limit)
        return JSONResponse(ImageList(images=records).model_dump(mode='json'))

    @api.put('/images/<int:image_id>/content')
    @requires_authentication()
    async def upload(req: Request[GalleryState], image_id: int) -> Response:
        form = await req.form()
        file = form.get('image')
        if not isinstance(file, UploadFile):
            abort(400, 'Provide an image file')
        storage: Storage = app.state.storage
        if not await storage.put_content(image_id, await file.read()):
            abort(404)
        return Response(status_code=204)

    @api.get('/images/<int:image_id>/content')
    @requires_authentication()
    async def download(req: Request[GalleryState], image_id: int) -> StreamingResponse:
        storage: Storage = app.state.storage
        content = await storage.content(image_id)
        if content is None:
            abort(404)

        async def chunks() -> AsyncIterator[bytes]:
            for offset in range(0, len(content), 65536):
                yield content[offset:offset + 65536]

        return StreamingResponse(chunks(), media_type='application/octet-stream', headers={
            'Content-Disposition': f'attachment; filename="image-{image_id}.bin"',
        })

    app.register_module(api)
    return app
