from ryuuseigun import Request, Ryuuseigun
from starlette.middleware.cors import CORSMiddleware

app = Ryuuseigun(__name__)

@app.get('/data')
async def data(req: Request) -> dict[str, bool]:
    return {'available': True}

# Wrap the entire ASGI app so framework-generated error responses also receive CORS headers.
application = CORSMiddleware(
    app,
    allow_origins=['https://frontend.example'],
    allow_credentials=True,
    allow_methods=['GET', 'POST'],
    allow_headers=['Content-Type', 'X-CSRF-Token'],
)
