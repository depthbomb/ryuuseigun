from typing import Any
from sys import exception
from collections.abc import Callable
from asyncio import Future, gather, shield, to_thread, ensure_future, CancelledError

async def run_sync[**Parameters, Result](
    function: Callable[Parameters, Result], *args: Parameters.args, **kwargs: Parameters.kwargs,
) -> Result:
    task = ensure_future(to_thread(function, *args, **kwargs))
    try:
        return await shield(task)
    except CancelledError:
        # A thread cannot be cancelled. Join it before its caller releases resources.
        joined = gather(task, return_exceptions=True)
        while not joined.done():
            try:
                await shield(joined)
            except CancelledError:
                pass
        raise

async def cancel_and_join(*tasks: Future[Any]) -> None:
    original_error = exception()
    for task in tasks:
        if not task.done():
            task.cancel()
    joined = gather(*tasks, return_exceptions=True)
    interrupted = False
    while not joined.done():
        try:
            await shield(joined)
        except CancelledError:
            interrupted = True
    joined.result()
    if interrupted and original_error is None:
        raise CancelledError
