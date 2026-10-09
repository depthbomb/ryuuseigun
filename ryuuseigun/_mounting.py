from dataclasses import dataclass
from ryuuseigun.types import ASGIApplication

@dataclass(frozen=True, slots=True)
class Mount:
    """An independent ASGI app; native request hooks stay inside each app.

    Set lifespan=True when the child supports the ASGI lifespan protocol.
    Wrap the outer app with ASGI middleware for policies shared by all children.
    Parent trusted-host checks also apply to mounted requests.
    """
    path: str
    app: ASGIApplication
    name: str
    lifespan: bool = False
    lifespan_timeout: float = 10
