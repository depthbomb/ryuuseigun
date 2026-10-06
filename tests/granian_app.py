from ryuuseigun import Ryuuseigun

app = Ryuuseigun(__name__)


@app.get('/health')
async def health(_req) -> dict[str, bool]:
    return {'healthy': True}
