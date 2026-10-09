from time import time
from hashlib import sha256
from secrets import token_urlsafe
from sqrrl import Database, NotFoundError
from examples.gallery.database_models import Image, Client, ImageColumns, SessionColumns

class Storage:
    def __init__(self, database: Database) -> None:
        self.client = Client(database)

    async def create(self, title: str) -> Image:
        return await self.client.images.create(title=title)

    async def list_images(self, limit: int) -> list[Image]:
        return await self.client.images.query().order_by(ImageColumns.id.asc()).limit(limit).all()

    async def put_content(self, identifier: int, content: bytes) -> bool:
        try:
            await self.client.images.update(identifier, content=content)
        except NotFoundError:
            return False

        return True

    async def content(self, identifier: int) -> bytes | None:
        try:
            return (await self.client.images.get(identifier)).content
        except NotFoundError:
            return None

    async def login(self) -> tuple[str, str]:
        token, csrf = token_urlsafe(32), token_urlsafe(32)
        async with self.client.transaction() as transaction:
            await transaction.sessions.delete_where(SessionColumns.expires.le(time()))
            await transaction.sessions.create(
                token=sha256(token.encode()).hexdigest(), csrf=csrf, expires=time() + 3600,
            )

        return token, csrf

    async def session(self, token: str) -> str | None:
        record = await self.client.sessions.query().where(
            SessionColumns.token.eq(sha256(token.encode()).hexdigest()), SessionColumns.expires.gt(time()),
        ).first()
        return None if record is None else record.csrf

    async def logout(self, token: str) -> None:
        await self.client.sessions.delete_where(SessionColumns.token.eq(sha256(token.encode()).hexdigest()))
