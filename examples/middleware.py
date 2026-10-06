from uuid import uuid4
from hashlib import sha256
from time import perf_counter_ns
from ryuuseigun import Next, Request, request, Response, asyncify, Ryuuseigun


class DomainError(Exception):
    pass


app = Ryuuseigun(__name__)


@app.middleware
async def measure_request(req: Request, next: Next) -> None:
    req.state.started = perf_counter_ns()
    await next(req)


@app.after_request
async def server_timing(req: Request, res: Response) -> Response:
    res.headers['Server-Timing'] = f'app;dur={(perf_counter_ns() - req.state.started) / 1_000_000:.3f}'
    return res


@app.before_request
async def assign_request_id(req: Request) -> None:
    req.state.request_id = uuid4().hex


@app.after_request
async def add_request_id(req: Request, res: Response) -> Response:
    res.headers['X-Request-ID'] = req.state.request_id
    return res


async def require_api_key(req: Request, next: Next) -> None:
    if req.headers.get('X-API-Key') != 'example-key':
        await req.respond(Response('Invalid API key', 401))
        return
    await next(req)


@app.get('/private', middlewares=(require_api_key,))
async def private_route(_req: Request) -> dict[str, object]:
    return {'authenticated': True, 'request_id': request.state.request_id}


@asyncify
def digest(value: str) -> str:
    return sha256(value.encode()).hexdigest()


@app.get('/digest/<value>')
async def create_digest(_req: Request, value: str) -> dict[str, str]:
    return {'value': value, 'sha256': await digest(value)}


@app.get('/domain-error')
async def domain_error(_req: Request) -> None:
    raise DomainError('A domain rule rejected this request')


@app.errorhandler(DomainError)
async def handle_domain_error(_req: Request, error: DomainError) -> tuple[dict[str, str], int]:
    return {'error': str(error)}, 422


if __name__ == '__main__':
    print('Run with: granian --interface asgi examples.middleware:app')
