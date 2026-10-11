"""
Update check for the header badge.

MageML is published as GitHub releases (do_not_commit/github_release_instructions), so the
check reads the repository's latest release instead of PyPI, where the name is not
MageML's. The badge appears only for a strictly newer version (PEP 440). MageML has its
own versions, starting at 0.1.0, independent of the Mage release it is based on
(constants.UPSTREAM_MAGE_VERSION).
"""
import os
from typing import Optional

import aiohttp
from packaging.version import InvalidVersion, Version

from mage_ai.server.constants import VERSION

TIMEOUT_SECONDS = 3
# MageML is published as GitHub releases of this repository, not on PyPI.
UPDATE_CHECK_REPOSITORY = os.getenv('MAGE_UPDATE_CHECK_REPOSITORY', 'jospablo777/mage-ml')
UPDATE_CHECK_DISABLED_VALUES = ('0', 'false', 'False', 'no')


def update_check_enabled() -> bool:
    return os.getenv('MAGE_UPDATE_CHECK_ENABLED', '1') not in UPDATE_CHECK_DISABLED_VALUES


def newer_version(installed: str, candidate: Optional[str]) -> str:
    """
    Return candidate only when it is a strictly newer release than installed.

    Comparison follows PEP 440, so a local segment such as 0.1.0+build.2 counts as
    newer than 0.1.0. An unparseable version on either side keeps the installed one.
    """
    if not candidate or candidate == installed:
        return installed

    try:
        if Version(candidate) > Version(installed):
            return candidate
    except InvalidVersion:
        pass

    return installed


async def fetch_published_version(
    repository: str = UPDATE_CHECK_REPOSITORY,
) -> Optional[str]:
    """The version of the repository's latest GitHub release (its tag without a leading
    v), or None when there is none or GitHub cannot be reached."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f'https://api.github.com/repos/{repository}/releases/latest',
                headers={'Accept': 'application/vnd.github+json'},
                timeout=TIMEOUT_SECONDS,
            ) as response:
                if response.status != 200:
                    return None
                response_json = await response.json()
                tag = response_json.get('tag_name') or ''
                return tag[1:] if tag[:1] in ('v', 'V') else tag or None
    except Exception:
        return None


def release_url(version: str, repository: str = UPDATE_CHECK_REPOSITORY) -> str:
    """The page of a release, for the header's version link and Update badge."""
    return f'https://github.com/{repository}/releases/tag/v{version}'


async def get_latest_version() -> str:
    """The version the header should offer, which is the installed one when current."""
    if not update_check_enabled():
        return VERSION

    return newer_version(VERSION, await fetch_published_version())
