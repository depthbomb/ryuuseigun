from typing import assert_type
from ryuuseigun.openapi import OpenAPI
from examples.gallery.models import GalleryState
from examples.gallery.rate_limits import Bucket
from examples.gallery.auth import requires_authentication
from ryuuseigun import Ryuuseigun, Request

app = Ryuuseigun[GalleryState](__name__, request_state_factory=GalleryState)
api = OpenAPI(app, title='Typing', version='1')

@app.get('/<int:item_id>')
@requires_authentication()
@Bucket(2, 1).consume()
@api.schema(responses={200: None})
async def route(req: Request[GalleryState], item_id: int) -> dict[str, int]:
    return {'id': item_id}

async def contracts(req: Request[GalleryState]) -> None:
    assert_type(await route(req, item_id=1), dict[str, int])
    await route(req, item_id='bad')  # type: ignore[arg-type]

@api.schema(responses={200: None})
async def schema_only(req: Request[GalleryState], item_id: int) -> str:
    return str(item_id)

async def state_contract(req: Request[int]) -> None:
    await schema_only(req, 1)  # type: ignore[arg-type]
