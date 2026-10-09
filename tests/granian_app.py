from os import environ
from pathlib import Path
from asyncio import Event
from contextlib import asynccontextmanager
from ryuuseigun import Next, Request, Ryuuseigun, StreamingResponse

app = Ryuuseigun(__name__)
child = Ryuuseigun('child')
events = []

@child.lifespan
@asynccontextmanager
async def resources(app):
    events.append('startup')
    try:
        yield
    finally:
        marker = environ.get('RYUUSEIGUN_SHUTDOWN_MARKER')
        if marker:
            Path(marker).write_text('closed', encoding='utf-8')

@child.middleware
async def lifetime(req: Request, next: Next):
    if req.path == '/stream':
        events.append('enter')
        try:
            await next(req)
        finally:
            events.append('exit')
    else:
        await next(req)

@child.get('/health')
async def health(req):
    return {'events': events, 'url': req.url_for('health')}

@child.get('/stream')
async def stream(req):
    async def chunks():
        try:
            yield b'first\n'
            await Event().wait()
        finally:
            events.append('generator closed')
    return StreamingResponse(chunks())

@child.post('/upload')
async def upload(req):
    size = 0
    async for chunk in req.stream():
        size += len(chunk)
    return {'size': size}

app.mount('/nested', child, name='child', lifespan=True)

@app.get('/health')
async def root_health(_req) -> dict[str, bool]:
    return {'healthy': True}
