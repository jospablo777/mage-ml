import os

from mage_ai.server.logger import Logger
from mage_ai.shared.dates import utc_now

LOGGER = Logger().new_server_logger(__name__)


def debug(text):
    if not os.getenv('DISABLE_API_TERMINAL_OUTPUT'):
        now = utc_now().isoformat()
        LOGGER.debug(f'[{now}][api.views] {text}')


def error(text):
    if not os.getenv('DISABLE_API_TERMINAL_OUTPUT'):
        now = utc_now().isoformat()
        LOGGER.error(f'[{now}][api.views] {text}')


def info(text):
    if not os.getenv('DISABLE_API_TERMINAL_OUTPUT'):
        now = utc_now().isoformat()
        LOGGER.info(f'[{now}][api.views] {text}')
