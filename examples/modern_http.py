from pathlib import Path
from asyncio import sleep
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from ryuuseigun import (
    Module,
    Request,
    Response,
    MediaType,
    WebSocket,
    Ryuuseigun,
    Compression,
    FileResponse,
    ServerSentEvent,
    StreamingResponse,
    EventStreamResponse,
)

app = Ryuuseigun(__name__)
app.after_request(Compression())
chat = Module('chat', url_prefix='/chat')


@app.lifespan
@asynccontextmanager
async def resources(current: Ryuuseigun) -> AsyncIterator[None]:
    current.state.service = 'ready'
    yield


@app.post('/upload')
async def upload(req: Request) -> dict[str, int]:
    size = 0
    async for chunk in req.stream():
        size += len(chunk)
    return {'bytes_received': size}


@app.query('/records', accepts=(MediaType.JSON,))
async def query_records(req: Request) -> dict[str, object]:
    return {'query': await req.json(), 'safe': True, 'idempotent': True}


@app.get('/stream')
async def stream(_req: Request) -> StreamingResponse:
    async def chunks() -> AsyncIterator[str]:
        for number in range(3):
            yield f'chunk {number}\n'
            await sleep(0.1)

    return StreamingResponse(chunks(), media_type=MediaType.TEXT_UTF8)


@app.get('/events')
async def events(_req: Request) -> EventStreamResponse:
    async def updates() -> AsyncIterator[ServerSentEvent]:
        for number in range(3):
            yield ServerSentEvent(str(number), event='counter', id=str(number))
            await sleep(1)

    return EventStreamResponse(updates())


@app.get('/source')
async def source(_req: Request) -> FileResponse:
    return FileResponse(Path(__file__), as_attachment=True)


@app.get('/extended')
async def extended(_req: Request) -> Response:
    res = Response(
        'Inspect this response with a server that supports the ASGI extensions.',
        trailers={'Digest': 'example-digest'},
        early_hints=('</static/app.css>; rel=preload; as=style',),
    )

    @res.after_send
    async def record_completion() -> None:
        app.state.last_response_completed = True

    return res


@chat.websocket('/<int:room_id>')
async def room(socket: WebSocket, room_id: int) -> None:
    await socket.accept('json')
    while True:
        message = await socket.receive_json()
        await socket.send_json({'room_id': room_id, 'message': message})


app.register_module(chat)
