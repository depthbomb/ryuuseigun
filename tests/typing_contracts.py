from dataclasses import dataclass
from ryuuseigun import Next, Module, Ryuuseigun, Request, Response

@dataclass
class State:
    name: str = ''

@dataclass
class OtherState:
    count: int = 0

app = Ryuuseigun[State](__name__, request_state_factory=State)
module = Module[State]('api')

@app.get('/<int:value>')
async def route(req: Request[State], value: int) -> str:
    return req.state.name * value

@module.before_request
async def before(req: Request[State]) -> None:
    req.state.name = 'ready'

@app.after_request
async def after(req: Request[State], response: Response) -> Response:
    response.headers['X-Name'] = req.state.name
    return response

@app.errorhandler(ValueError)
async def error(req: Request[State], exception: ValueError) -> str:
    return req.state.name + str(exception)

@app.middleware
async def middleware(req: Request[State], next: Next[State]) -> None:
    await next(req)

async def wrong_route(req: Request[OtherState]) -> str:
    return str(req.state.count)

async def wrong_after(req: Request[OtherState], response: Response) -> Response:
    return response

async def wrong_error(req: Request[OtherState], exception: ValueError) -> str:
    return str(exception)

async def wrong_middleware(req: Request[OtherState], next: Next[OtherState]) -> None:
    await next(req)

# Strict mypy reports unused ignores if any registration loses its state constraint.
app.get('/wrong')(wrong_route)  # type: ignore[arg-type]
module.get('/wrong')(wrong_route)  # type: ignore[arg-type]
app.add_url_rule('/wrong-direct', wrong_route)  # type: ignore[arg-type]
module.add_url_rule('/wrong-direct', wrong_route)  # type: ignore[arg-type]
app.before_request(wrong_route)  # type: ignore[arg-type]
module.before_request(wrong_route)  # type: ignore[arg-type]
app.after_request(wrong_after)  # type: ignore[arg-type]
module.after_request(wrong_after)  # type: ignore[arg-type]
app.errorhandler(ValueError)(wrong_error)  # type: ignore[arg-type]
module.errorhandler(ValueError)(wrong_error)  # type: ignore[arg-type]
app.middleware(wrong_middleware)  # type: ignore[arg-type]
module.middleware(wrong_middleware)  # type: ignore[arg-type]
app.get('/wrong-middleware', middlewares=(wrong_middleware,))(route)  # type: ignore[arg-type]

async def client_options() -> None:
    client = app.test_client(base_url='https://testserver')
    await client.post('/', form={'field': 'value'}, files={'file': ('a.txt', b'a', 'text/plain')})
    await client.post('/', jsno={})  # type: ignore[call-arg]
