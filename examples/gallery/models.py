from dataclasses import dataclass
from examples.gallery.storage import Storage

@dataclass(slots=True)
class GalleryState:
    request_id: str = ''
    user: str | None = None
    csrf_token: str = ''

@dataclass(slots=True)
class GalleryResources:
    ready: bool = False
    storage: Storage | None = None

    def get_storage(self) -> Storage:
        if self.storage is None:
            raise RuntimeError('The gallery lifespan has not started')

        return self.storage
