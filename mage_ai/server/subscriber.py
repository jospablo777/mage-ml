import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from queue import Empty

from mage_ai.server.active_kernel import get_active_kernel_client
from mage_ai.server.logger import Logger

logger = Logger().new_server_logger(__name__)


async def get_messages(callback=None):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix='kernel-output') as executor:
        while True:
            try:
                client = get_active_kernel_client()
                message = await loop.run_in_executor(
                    executor, partial(client.get_iopub_msg, timeout=1),
                )
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
