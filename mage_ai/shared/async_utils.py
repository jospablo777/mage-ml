import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Coroutine


def run_sync(coroutine: Coroutine) -> Any:
    """
    Run a coroutine to completion from synchronous code and return its result.

    asyncio.run raises RuntimeError when the calling thread already runs an event loop,
    as Mage's server thread does, and the coroutine is never awaited. In that case the
    coroutine runs in its own event loop on a worker thread, and this call waits for it.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coroutine)

    with ThreadPoolExecutor(max_workers=1) as executor:
        return executor.submit(asyncio.run, coroutine).result()
