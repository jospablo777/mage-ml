"""
Update check for the header badge.

The badge used to appear whenever the version string on PyPI differed from the
installed one. This fork carries a local version segment and can be ahead of the
release it was forked from, so any difference marked it as out of date.
"""
import os
from typing import Optional

import aiohttp
from packaging.version import InvalidVersion, Version

from mage_ai.server.constants import VERSION

PYPI_TIMEOUT_SECONDS = 3
# The distribution this build is published as, not the project it was forked from.
UPDATE_CHECK_PACKAGE = os.getenv('MAGE_UPDATE_CHECK_PACKAGE', 'mage-ml')
UPDATE_CHECK_DISABLED_VALUES = ('0', 'false', 'False', 'no')


def update_check_enabled() -> bool:
    return os.getenv('MAGE_UPDATE_CHECK_ENABLED', '1') not in UPDATE_CHECK_DISABLED_VALUES


def newer_version(installed: str, candidate: Optional[str]) -> str:
    """
    Return candidate only when it is a strictly newer release than installed.

    Comparison follows PEP 440, so a local segment such as 0.9.79+ml.2 counts as
    newer than 0.9.79. An unparseable version on either side keeps the installed one.
    """
    if not candidate or candidate == installed:
        return installed

    try:
        if Version(candidate) > Version(installed):
            return candidate
    except InvalidVersion:
        pass

    return installed


async def fetch_published_version(package_name: str = UPDATE_CHECK_PACKAGE) -> Optional[str]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f'https://pypi.org/pypi/{package_name}/json',
                timeout=PYPI_TIMEOUT_SECONDS,
            ) as response:
                response_json = await response.json()

                return response_json.get('info', {}).get('version')
    except Exception:
        return None


async def get_latest_version() -> str:
    """The version the header should offer, which is the installed one when current."""
    if not update_check_enabled():
        return VERSION

    return newer_version(VERSION, await fetch_published_version())
