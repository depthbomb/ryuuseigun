from typing import assert_type
from ryuuseigun import Request, Ryuuseigun
from examples.gallery.rate_limits import Bucket
from examples.gallery.models import GalleryState
from examples.gallery.auth import requires_authentication

app = Ryuuseigun[GalleryState](__name__, request_state_factory=GalleryState)

@app.get('/<int:item_id>')
@requires_authentication()
@Bucket(2, 1).consume()
async def route(req: Request[GalleryState], item_id: int) -> dict[str, int]:
    return {'id': item_id}

async def contracts(req: Request[GalleryState]) -> None:
    assert_type(await route(req, item_id=1), dict[str, int])
    await route(req, item_id='bad')  # type: ignore[arg-type]

async def state_contract(req: Request[int]) -> None:
    await route(req, 1)  # type: ignore[arg-type]
