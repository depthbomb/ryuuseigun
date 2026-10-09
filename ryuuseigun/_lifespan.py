from typing import Any
from ryuuseigun._tasks import cancel_and_join
from ryuuseigun.types import ASGIMessage, ASGIApplication
from asyncio import Task, wait, Queue, timeout, create_task, FIRST_COMPLETED

_owners: set[int] = set()

class LifespanSession:
    def __init__(self, app: ASGIApplication, timeout_seconds: float) -> None:
        self.app = app
        self.timeout = timeout_seconds
        self.incoming: Queue[ASGIMessage] = Queue()
        self.outgoing: Queue[ASGIMessage] = Queue()
        self.task: Task[None] | None = None
        self.state: dict[str, Any] = {}

    async def start(self) -> None:
        if id(self.app) in _owners:
            raise RuntimeError('This application already has an active test-client lifespan')
        _owners.add(id(self.app))
        try:
            self.task = create_task(self.app(
                {'type': 'lifespan', 'asgi': {'version': '3.0', 'spec_version': '2.0'}, 'state': self.state},
                self.incoming.get, self.outgoing.put,
            ))
            await self._exchange('startup')
        except BaseException:
            await self.close()
            raise

    async def stop(self) -> None:
        try:
            await self._exchange('shutdown')
            if self.task is not None:
                async with timeout(self.timeout):
                    await self.task
        finally:
            await self.close()

    async def close(self) -> None:
        try:
            if self.task is not None:
                await cancel_and_join(self.task)
        finally:
            _owners.discard(id(self.app))

    async def _exchange(self, phase: str) -> None:
        if self.task is None:
            raise RuntimeError('Lifespan is not running')
        receiver = create_task(self.outgoing.get())
        try:
            async with timeout(self.timeout):
                await self.incoming.put({'type': f'lifespan.{phase}'})
                await wait((receiver, self.task), return_when=FIRST_COMPLETED)
                if not receiver.done():
                    self.task.result()
                    if self.outgoing.empty():
                        raise RuntimeError(f'Application exited before lifespan {phase} completed')
                    message = self.outgoing.get_nowait()
                else:
                    message = receiver.result()
                if message['type'] != f'lifespan.{phase}.complete':
                    raise RuntimeError(f'Lifespan {phase} failed: {message.get("message", message["type"])}')
        except TimeoutError as error:
            raise TimeoutError(f'Lifespan {phase} timed out after {self.timeout:g} seconds') from error
        finally:
            await cancel_and_join(receiver)
