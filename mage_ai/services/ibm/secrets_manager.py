"""
IBM Cloud Secrets Manager for io_config.yaml: `{{ ibm_secret_var('<secret id>') }}`, or
`{{ ibm_secret_var('<secret id>', 'username') }}` for one field.

The instance is IBM_SECRETS_MANAGER_URL (or MAGE_SERVICE_IBM_SECRETS_MANAGER_URL),
`https://<instance id>.<region>.secrets-manager.appdomain.cloud`, and the credentials those of
`TokenManager.from_environment`: IBM_CLOUD_API_KEY or IBM_CLOUD_TRUSTED_PROFILE.

Fields follow the exported services' `ibm:` secrets: an arbitrary secret gives its payload, a
username and password secret its password, an IAM credentials secret its API key; a key-value
secret needs the key.
"""
import os
import threading
import time
from typing import Dict, Optional, Tuple

import requests

from .iam import TokenManager

URL_VARIABLES = ('IBM_SECRETS_MANAGER_URL', 'MAGE_SERVICE_IBM_SECRETS_MANAGER_URL')
DEFAULT_FIELDS = {'arbitrary': 'payload', 'username_password': 'password',
                  'iam_credentials': 'api_key'}
# A secret read again within this many seconds comes from the cache: io_config.yaml is
# rendered for every block.
CACHE_SECONDS = 300


class SecretsManagerError(Exception):
    pass


def value(secret: Dict, field: Optional[str], secret_id: str) -> str:
    kind = secret.get('secret_type') or 'arbitrary'
    name = secret.get('name') or secret_id
    if field is None:
        if kind == 'kv':
            raise SecretsManagerError(
                f'Secret {name} is a key-value secret; pick a key: '
                f"ibm_secret_var('{secret_id}', '<key>')."
            )
        field = DEFAULT_FIELDS.get(kind)
        if field is None:
            raise SecretsManagerError(
                f"Secret {name} is a {kind} secret; pick a field: "
                f"ibm_secret_var('{secret_id}', '<field>')."
            )
    found = secret.get(field)
    if found is None:
        found = (secret.get('data') or {}).get(field)
    if found is None:
        raise SecretsManagerError(f'Secret {name} has no field {field}.')
    return found if isinstance(found, str) else str(found)


class SecretsManager:
    def __init__(self, url: str, tokens: TokenManager, timeout: float = 30):
        url = url.rstrip('/')
        if not url.startswith('https://') and not url.startswith(('http://127.0.0.1',
                                                                   'http://localhost')):
            raise SecretsManagerError('The Secrets Manager URL must be an https:// address.')
        self.url = url
        self.tokens = tokens
        self.timeout = timeout
        self._cache: Dict[str, Tuple[float, Dict]] = {}
        self._lock = threading.Lock()

    def secret(self, secret_id: str) -> Dict:
        with self._lock:
            cached = self._cache.get(secret_id)
            if cached and time.monotonic() - cached[0] < CACHE_SECONDS:
                return cached[1]
        try:
            response = requests.get(
                f'{self.url}/api/v2/secrets/{secret_id}',
                headers={'Authorization': f'Bearer {self.tokens.token()}',
                         'Accept': 'application/json'},
                timeout=self.timeout,
            )
        except requests.RequestException as error:
            raise SecretsManagerError(f'Secrets Manager could not be reached: {error}') from None
        if response.status_code in (401, 403):
            raise SecretsManagerError(
                f'Secrets Manager denied secret {secret_id} ({response.status_code}); grant the '
                'service ID or trusted profile the SecretsReader role.'
            )
        if response.status_code == 404:
            raise SecretsManagerError(f'Secrets Manager has no secret {secret_id}.')
        if response.status_code != 200:
            try:
                reason = response.json().get('message')
            except ValueError:
                reason = None
            raise SecretsManagerError(
                f'Secrets Manager failed for secret {secret_id} ({response.status_code}): '
                f'{reason or response.reason}'
            )
        body = response.json()
        with self._lock:
            self._cache[secret_id] = (time.monotonic(), body)
        return body

    def get(self, secret_id: str, field: Optional[str] = None) -> str:
        return value(self.secret(secret_id), field, secret_id)


_default: Optional[SecretsManager] = None
_default_lock = threading.Lock()


def _manager() -> SecretsManager:
    global _default
    with _default_lock:
        if _default is None:
            url = next((os.environ[v] for v in URL_VARIABLES if os.environ.get(v)), None)
            if not url:
                raise SecretsManagerError(
                    'ibm_secret_var needs IBM_SECRETS_MANAGER_URL, the Secrets Manager '
                    'instance\'s endpoint, and IBM_CLOUD_API_KEY or IBM_CLOUD_TRUSTED_PROFILE.'
                )
            _default = SecretsManager(url, TokenManager.from_environment())
        return _default


def get_secret(secret_id: str, field: Optional[str] = None) -> str:
    """The io_config.yaml template function `ibm_secret_var`."""
    if os.environ.get('MAGE_SERVICE'):
        # Exported services fetch these secrets at start-up into the environment.
        from mage_ai.pipeline_services.secrets import service_cloud_secret

        return service_cloud_secret('ibm', secret_id, field)
    return _manager().get(secret_id, field)
