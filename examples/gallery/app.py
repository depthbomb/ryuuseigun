"""A small gallery with developer-owned sessions, CSRF checks and sqrrl storage.

For this single-process demo, startup applies the checked-in migrations. In a
deployment with several workers, apply migrations separately before starting
them. The rate limiter is process-local; replace it with your own shared store
when requests can land on more than one worker.
"""
from uuid import uuid4
from pathlib import Path
from sqrrl import Database
from time import perf_counter
from secrets import compare_digest
from sqrrl.migrate import load, apply
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from examples.gallery.storage import Storage
from examples.gallery.rate_limits import Bucket
from examples.gallery.auth import requires_authentication
from examples.gallery.models import GalleryState, GalleryResources
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
    *, password: str, database_path: str | Path = 'gallery.db',
    secure_cookies: bool = True, allowed_origin: str = 'https://testserver',
) -> Ryuuseigun[GalleryState, GalleryResources]:
    if not password:
        raise ValueError('Provide a non-empty gallery password')
    app = Ryuuseigun[GalleryState, GalleryResources](
        __name__, request_state_factory=GalleryState,
        app_state_factory=GalleryResources,
        config=Config(max_request_body_size=1024 * 1024),
    )
    api = Module[GalleryState]('api', url_prefix='/api')
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
    async def resources(app: Ryuuseigun[GalleryState, GalleryResources]) -> AsyncIterator[None]:
        async with await Database.create(database_path) as database:
            await apply(database, load(Path(__file__).with_name('migrations')))
            app.state.storage = Storage(database)
            app.state.ready = True
            try:
                yield
            finally:
                app.state.ready = False
                app.state.storage = None

    @app.before_request
    async def session(req: Request[GalleryState]) -> None:
        req.state.request_id = uuid4().hex
        if req.method in {'POST', 'PUT', 'PATCH', 'DELETE'}:
            origin = req.headers.get('Origin')
            if origin is not None and origin != allowed_origin:
                abort(403, 'Origin is not allowed')
        token = req.cookies.get('session')
        if token:
            storage = app.state.get_storage()
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
    async def login(req: Request[GalleryState]) -> Response:
        credentials = await req.json()
        if not isinstance(credentials, dict):
            abort(400, 'Provide a username and password')
        supplied_password = credentials.get('password')
        if not isinstance(supplied_password, str):
            abort(400, 'Provide a password')
        if credentials.get('username') != 'demo' or not compare_digest(supplied_password.encode(), password.encode()):
            abort(401, 'Invalid credentials')
        storage = app.state.get_storage()
        token, _ = await storage.login()
        res = redirect(req.url_for('account'), 303)
        res.set_cookie('session', token, max_age=3600, secure=secure_cookies, httponly=True)
        return res

    @app.get('/account')
    @requires_authentication()
    async def account(req: Request[GalleryState]) -> dict[str, str | None]:
        return {'user': req.state.user, 'csrf_token': req.state.csrf_token}

    @app.post('/logout')
    @requires_authentication()
    async def logout(req: Request[GalleryState]) -> Response:
        storage = app.state.get_storage()
        await storage.logout(req.cookies['session'])
        res = Response(status_code=204)
        res.delete_cookie('session', secure=secure_cookies, httponly=True)
        return res

    @api.post('/images')
    @requires_authentication()
    async def create_image(req: Request[GalleryState]) -> JSONResponse:
        payload = await req.json()
        title = payload.get('title') if isinstance(payload, dict) else None
        if not isinstance(title, str) or not 1 <= len(title) <= 120:
            abort(422, 'Title must be a string between 1 and 120 characters')
        return await save_image(req, title)

    @images_bucket.consume(cost=2)
    async def save_image(req: Request[GalleryState], title: str) -> JSONResponse:
        record = await app.state.get_storage().create(title)
        return JSONResponse({'id': record.id, 'title': record.title}, status_code=201)

    @api.get('/images')
    @requires_authentication()
    async def images(req: Request[GalleryState]) -> JSONResponse:
        try:
            limit = int(req.query.get('limit', '20') or '')
        except ValueError:
            abort(422, 'Limit must be an integer')
        if not 1 <= limit <= 100:
            abort(422, 'Limit must be between 1 and 100')
        records = await app.state.get_storage().list_images(limit)
        return JSONResponse({'images': [{'id': record.id, 'title': record.title} for record in records]})

    @api.put('/images/<int:image_id>/content')
    @requires_authentication()
    async def upload(req: Request[GalleryState], image_id: int) -> Response:
        form = await req.form()
        file = form.get('image')
        if not isinstance(file, UploadFile):
            abort(400, 'Provide an image file')
        storage = app.state.get_storage()
        if not await storage.put_content(image_id, await file.read()):
            abort(404)
        return Response(status_code=204)

    @api.get('/images/<int:image_id>/content')
    @requires_authentication()
    async def download(req: Request[GalleryState], image_id: int) -> StreamingResponse:
        storage = app.state.get_storage()
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
