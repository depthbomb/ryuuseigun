from asyncio import Lock
from typing import IO, Optional
from ryuuseigun._tasks import run_sync
from ryuuseigun.types import ASGIMessage
from tempfile import SpooledTemporaryFile
from ryuuseigun.constants import ASGIMessageType

class RequestBodyBuffer:
    def __init__(self, spool_threshold: int) -> None:
        self._file: Optional[IO[bytes]] = None
        self._lock = Lock()
        self._spool_threshold = spool_threshold
        self._read_position = 0
        self._write_position = 0
        self._complete = False
        self._closed = False

    async def append(self, body: bytes, *, more_body: bool) -> None:
        async with self._lock:
            if self._closed:
                raise RuntimeError('Request body buffer is closed')

            if body:
                await run_sync(self._write, body, more_body)
            else:
                self._complete = not more_body

    async def read(self) -> Optional[ASGIMessage]:
        async with self._lock:
            if self._closed:
                raise RuntimeError('Request body buffer is closed')

            if self._read_position == self._write_position and not self._complete:
                return None

            body = await run_sync(self._read) if self._read_position < self._write_position else b''
            return {
                'type': ASGIMessageType.HTTP_REQUEST,
                'body': body,
                'more_body': self._read_position < self._write_position or not self._complete,
            }

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            if self._file is not None:
                await run_sync(self._file.close)

    def _write(self, body: bytes, more_body: bool) -> None:
        if self._file is None:
            file = SpooledTemporaryFile(max_size=max(self._spool_threshold, 1), mode='w+b')
            self._file = file
            if self._spool_threshold == 0:
                file.rollover()

        self._file.seek(self._write_position)
        self._file.write(body)
        self._write_position += len(body)
        self._complete = not more_body

    def _read(self) -> bytes:
        assert self._file is not None
        self._file.seek(self._read_position)
        body = self._file.read(min(64 * 1024, self._write_position - self._read_position))
        self._read_position += len(body)
        if self._read_position == self._write_position:
            self._file.seek(0)
            self._file.truncate()
            self._read_position = 0
            self._write_position = 0

        return body
