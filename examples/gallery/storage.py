from time import time
from hashlib import sha256
from sqlalchemy import text
from secrets import token_urlsafe
from sqlalchemy.ext.asyncio import AsyncEngine
from examples.gallery.models import ImageRecord

class Storage:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    async def initialize(self) -> None:
        async with self.engine.begin() as connection:
            await connection.execute(text(
                'CREATE TABLE IF NOT EXISTS images (id INTEGER PRIMARY KEY, title TEXT NOT NULL, content BLOB)'
            ))
            await connection.execute(text(
                'CREATE TABLE IF NOT EXISTS sessions (token TEXT PRIMARY KEY, csrf TEXT NOT NULL, expires REAL NOT NULL)'
            ))

    async def create(self, title: str) -> ImageRecord:
        async with self.engine.begin() as connection:
            result = await connection.execute(text('INSERT INTO images (title) VALUES (:title) RETURNING id'), {'title': title})
            identifier = int(result.scalar_one())
        return ImageRecord(id=identifier, title=title)

    async def list_images(self, limit: int) -> list[ImageRecord]:
        async with self.engine.connect() as connection:
            result = await connection.execute(text('SELECT id, title FROM images ORDER BY id LIMIT :limit'), {'limit': limit})
            return [ImageRecord(id=row.id, title=row.title) for row in result]

    async def put_content(self, identifier: int, content: bytes) -> bool:
        async with self.engine.begin() as connection:
            result = await connection.execute(
                text('UPDATE images SET content=:content WHERE id=:id RETURNING id'),
                {'id': identifier, 'content': content},
            )
            return result.scalar_one_or_none() is not None

    async def content(self, identifier: int) -> bytes | None:
        async with self.engine.connect() as connection:
            result = await connection.execute(text('SELECT content FROM images WHERE id=:id'), {'id': identifier})
            value = result.scalar_one_or_none()
            return None if value is None else bytes(value)

    async def login(self) -> tuple[str, str]:
        token, csrf = token_urlsafe(32), token_urlsafe(32)
        async with self.engine.begin() as connection:
            await connection.execute(text('DELETE FROM sessions WHERE expires <= :now'), {'now': time()})
            await connection.execute(
                text('INSERT INTO sessions (token, csrf, expires) VALUES (:token, :csrf, :expires)'),
                {'token': sha256(token.encode()).hexdigest(), 'csrf': csrf, 'expires': time() + 3600},
            )
        return token, csrf

    async def session(self, token: str) -> str | None:
        async with self.engine.connect() as connection:
            result = await connection.execute(
                text('SELECT csrf FROM sessions WHERE token=:token AND expires>:now'),
                {'token': sha256(token.encode()).hexdigest(), 'now': time()},
            )
            value = result.scalar_one_or_none()
            return None if value is None else str(value)

    async def logout(self, token: str) -> None:
        async with self.engine.begin() as connection:
            await connection.execute(text('DELETE FROM sessions WHERE token=:token'), {'token': sha256(token.encode()).hexdigest()})
