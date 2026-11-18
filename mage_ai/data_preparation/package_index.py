"""
A package's releases on the package index the server's uv uses, for the Workers page's
version picker: PyPI unless UV_DEFAULT_INDEX (or UV_INDEX_URL) names another. Read with the
simple repository API in JSON (PEP 691 and PEP 700), which PyPI and private indexes
(Artifactory, Nexus, devpi, GitLab) serve.
"""
import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
from typing import Dict, List, Optional

from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

DEFAULT_INDEX = 'https://pypi.org/simple'
CACHE_SECONDS = 3600
TIMEOUT_SECONDS = 10

_cache: Dict[str, Dict] = {}
_lock = threading.Lock()


class PackageIndexError(Exception):
    def __init__(self, message: str, missing: bool = False):
        super().__init__(message)
        self.missing = missing


def index_url() -> str:
    url = os.getenv('UV_DEFAULT_INDEX') or os.getenv('UV_INDEX_URL') or DEFAULT_INDEX
    return url.rstrip('/')


def _shown(url: str) -> str:
    """The index without credentials, as messages name it."""
    return re.sub(r'//[^/@]+@', '//', url).replace('https://', '').replace('/simple', '')


def releases(name: str) -> Dict:
    """
    The package's releases, newest first, without yanked ones: {name, latest, versions,
    prereleases}. latest is the newest final release, the one uv picks for a requirement
    without a version. Kept for CACHE_SECONDS.
    """
    package = canonicalize_name(name or '')
    if not package or not re.fullmatch(r'[a-z0-9][a-z0-9._-]*', package):
        raise PackageIndexError(f'"{name}" is not a package name.')
    url = index_url()
    key = f'{url}|{package}'
    with _lock:
        cached = _cache.get(key)
    if cached and time.time() - cached['at'] < CACHE_SECONDS:
        return cached['value']
    request = urllib.request.Request(
        f'{url}/{package}/', headers={'Accept': 'application/vnd.pypi.simple.v1+json'},
    )
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read())
    except urllib.error.HTTPError as error:
        if error.code == 404:
            raise PackageIndexError(f'No package named {name} on {_shown(url)}.', missing=True)
        raise PackageIndexError(f'{_shown(url)} answered {error.code} for {name}.')
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        raise PackageIndexError(
            f'The package index ({_shown(url)}) could not be reached: '
            f'{getattr(error, "reason", error)}.')
    except ValueError:
        raise PackageIndexError(
            f'{_shown(url)} did not answer in the JSON index format (PEP 691).')
    value = _summarize(package, payload)
    with _lock:
        _cache[key] = dict(at=time.time(), value=value)
    return value


def _summarize(package: str, payload: Dict) -> Dict:
    yanked = set()
    by_version: Dict[str, List[Dict]] = {}
    for file in payload.get('files') or []:
        version = _file_version(package, file.get('filename') or '')
        if version:
            by_version.setdefault(version, []).append(file)
    for version, files in by_version.items():
        if files and all(f.get('yanked') for f in files):
            yanked.add(version)
    listed = payload.get('versions') or list(by_version)
    parsed = []
    for text in listed:
        if text in yanked:
            continue
        try:
            parsed.append((Version(text), text))
        except InvalidVersion:
            continue
    parsed.sort(reverse=True)
    finals = [text for version, text in parsed if not version.is_prerelease]
    return dict(
        name=payload.get('name') or package,
        latest=finals[0] if finals else (parsed[0][1] if parsed else None),
        versions=finals,
        prereleases=[text for version, text in parsed if version.is_prerelease][:20],
    )


def _file_version(package: str, filename: str) -> Optional[str]:
    """A distribution file's version: name-1.2.3-....whl or name-1.2.3.tar.gz."""
    stem = re.sub(r'\.(whl|tar\.gz|zip|tar\.bz2)$', '', filename)
    parts = stem.split('-')
    if len(parts) < 2:
        return None
    # Names in file names use _ for - and keep other characters.
    if canonicalize_name(parts[0]) != package:
        return None
    return parts[1]
