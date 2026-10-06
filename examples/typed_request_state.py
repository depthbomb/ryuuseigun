from uuid import uuid4
from typing import Optional
from dataclasses import dataclass
from ryuuseigun import Next, Request, Response, Ryuuseigun


@dataclass(slots=True)
class RequestState:
    request_id: str = ''
    user_id: Optional[int] = None


app = Ryuuseigun[RequestState](__name__, request_state_factory=RequestState)


@app.middleware
async def identify(req: Request[RequestState], next: Next[RequestState]) -> None:
    req.state.request_id = uuid4().hex
    supplied_user = req.headers.get('X-User-ID')
    if supplied_user is not None:
        req.state.user_id = int(supplied_user)

    await next(req)


@app.after_request
async def identify_response(req: Request[RequestState], res: Response) -> Response:
    res.headers['X-Request-ID'] = req.state.request_id
    return res


@app.get('/session')
async def session(req: Request[RequestState]) -> dict[str, object]:
    return {
        'request_id': req.state.request_id,
        'user_id': req.state.user_id,
    }
