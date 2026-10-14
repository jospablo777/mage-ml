import asyncio
from concurrent.futures import ThreadPoolExecutor
from queue import Empty

from mage_ai.server.active_kernel import (
    close_retired_clients,
    get_active_kernel_client,
    set_output_reader_running,
)
from mage_ai.server.logger import Logger

logger = Logger().new_server_logger(__name__)


async def get_messages(callback=None):
    loop = asyncio.get_running_loop()
    set_output_reader_running(True)
    try:
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix='kernel-output') as executor:
            await _read(loop, executor, callback)
    finally:
        set_output_reader_running(False)
        close_retired_clients()


async def _read(loop, executor, callback):
    def read():
        # A client a restart replaced is closed here, on the only thread that reads clients,
        # once no read is using it.
        close_retired_clients()
        return get_active_kernel_client().get_iopub_msg(timeout=1)

    while True:
        try:
            message = await loop.run_in_executor(executor, read)
            if message.get('content'):
                if callback:
                    callback(message)
                else:
                    logger.warning('No callback for kernel output')
        except Empty:
            continue
        except Exception:
            logger.exception('Failed to process kernel output')
            await asyncio.sleep(1)
