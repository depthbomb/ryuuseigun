from dataclasses import dataclass
from pydantic import Field, BaseModel

@dataclass(slots=True)
class GalleryState:
    request_id: str = ''
    user: str | None = None
    csrf_token: str = ''

class Login(BaseModel):
    username: str
    password: str

class CreateImage(BaseModel):
    title: str = Field(min_length=1, max_length=120)

class ImageRecord(BaseModel):
    id: int
    title: str

class ImageList(BaseModel):
    images: list[ImageRecord]

class Page(BaseModel):
    limit: int = Field(default=20, ge=1, le=100)
