"""Run `python -m examples.testing_streams` after installing .[examples].

TestClient accepts the outer ASGI application, including third-party middleware.
The client context starts lifespan and closes active streams before shutdown.
Each stream holds at most one outgoing ASGI message; its consumer controls when
the producer can continue. Leaving the stream context disconnects and waits for
cleanup. Buffered get/post helpers still work for ordinary, finite responses.
"""
from asyncio import run, Event
from collections.abc import AsyncIterator
from ryuuseigun.testing import TestClient
from starlette.middleware.cors import CORSMiddleware
from ryuuseigun import Next, Request, Ryuuseigun, StreamingResponse

async def main() -> None:
    app = Ryuuseigun(__name__)
    closed = Event()

    @app.middleware
    async def resource(req: Request, next: Next) -> None:
        try:
            await next(req)
        finally:
            closed.set()

    @app.get('/events')
    async def events(req: Request) -> StreamingResponse:
        async def chunks() -> AsyncIterator[bytes]:
            yield b'data: hello\n\n'
            await Event().wait()
        return StreamingResponse(chunks(), media_type='text/event-stream')

    application = CORSMiddleware(app, allow_origins=['https://example.com'])
    async with TestClient(application) as client:
        async with client.stream('GET', '/events', headers={'Origin': 'https://example.com'}) as response:
            assert response.status_code == 200
            assert response.headers['Access-Control-Allow-Origin'] == 'https://example.com'
            assert await anext(response.iter_bytes()) == b'data: hello\n\n'
            assert not closed.is_set()
        assert closed.is_set()
    print('Stream, CORS headers and disconnect cleanup passed.')

if __name__ == '__main__':
    run(main())
