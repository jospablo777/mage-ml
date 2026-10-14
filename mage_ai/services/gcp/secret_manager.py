"""
Google Cloud Secret Manager for io_config.yaml: `{{ gcp_secret_var('<secret>') }}`, or
`{{ gcp_secret_var('<secret>', 'username') }}` for one key of a JSON payload. A secret is a
name in GOOGLE_CLOUD_PROJECT (or the credentials' project), or
`projects/<project>/secrets/<secret>[/versions/<version>]`; the latest version by default.

Credentials are Google's application default credentials, through google-auth, which the
Google Cloud extras install.
"""
import base64
import json
import os
import re
import threading
import time
from typing import Dict, Optional, Tuple

API = 'https://secretmanager.googleapis.com/v1'
CACHE_SECONDS = 300
NAME = re.compile(r'^[A-Za-z0-9_-]+$')

_session = None
_project: Optional[str] = None
_cache: Dict[str, Tuple[float, str]] = {}
_lock = threading.Lock()


def _resource(secret: str, project: Optional[str]) -> str:
    parts = secret.split('/')
    if len(parts) == 1 and NAME.match(secret):
        if not project:
            raise ValueError(
                f'gcp_secret_var({secret!r}) needs a project: set GOOGLE_CLOUD_PROJECT, or '
                f'name it: projects/<project>/secrets/{secret}.'
            )
        return f'projects/{project}/secrets/{secret}/versions/latest'
    if len(parts) == 4 and parts[0] == 'projects' and parts[2] == 'secrets':
        return f'{secret}/versions/latest'
    if len(parts) == 6 and parts[0] == 'projects' and parts[2] == 'secrets' \
            and parts[4] == 'versions':
        return secret
    raise ValueError(
        f'{secret!r} is not a secret: a name, or projects/<project>/secrets/<secret>'
        '[/versions/<version>].'
    )


def _client():
    global _session, _project
    if _session is None:
        try:
            import google.auth
            from google.auth.transport.requests import AuthorizedSession
        except ImportError as error:
            raise ImportError(
                'gcp_secret_var needs google-auth: install mage-ml[bigquery] or '
                f'mage-ml[google-cloud-storage]. ({error})'
            ) from error
        credentials, project = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform'],
        )
        _session = AuthorizedSession(credentials)
        _project = os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('GCLOUD_PROJECT') or project
    return _session, _project


def get_secret(secret: str, field: Optional[str] = None) -> str:
    with _lock:
        session, project = _client()
        resource = _resource(secret, project)
        cached = _cache.get(resource)
        if cached and time.monotonic() - cached[0] < CACHE_SECONDS:
            payload = cached[1]
        else:
            response = session.get(f'{API}/{resource}:access', timeout=30)
            if response.status_code in (401, 403):
                raise PermissionError(
                    f'Secret Manager denied {resource} ({response.status_code}); give the '
                    'account the Secret Manager Secret Accessor role.'
                )
            if response.status_code == 404:
                raise KeyError(f'Secret Manager has no {resource}.')
            response.raise_for_status()
            payload = base64.b64decode(response.json()['payload']['data']).decode('utf-8')
            _cache[resource] = (time.monotonic(), payload)
    if field is None:
        return payload
    try:
        value = json.loads(payload)[field]
    except (ValueError, KeyError, TypeError):
        raise KeyError(f'Secret {secret} has no key {field} (its payload is not JSON with it).')
    return value if isinstance(value, str) else json.dumps(value)
