"""
IBM Cloud IAM access tokens, for the services that take `Authorization: Bearer <token>`.

A token comes from an API key (a user's or a service ID's) or from a trusted profile with the
compute resource token that Code Engine and IKS mount, so no key is stored at all. Tokens are
cached and replaced a few minutes before they expire; one manager is safe to share between
threads.
"""
import os
import threading
import time
from typing import Dict, Optional

import requests

DEFAULT_IAM_URL = 'https://iam.cloud.ibm.com'
# Where Code Engine and IKS mount the compute resource token of a trusted profile.
CR_TOKEN_PATHS = (
    '/var/run/secrets/codeengine.cloud.ibm.com/compute-resource-token/token',
    '/var/run/secrets/tokens/vault-token',
)
# A token is replaced this long before it expires; IAM tokens last an hour.
REFRESH_BEFORE_SECONDS = 300


class IAMError(Exception):
    pass


class TokenManager:
    def __init__(
        self,
        api_key: Optional[str] = None,
        trusted_profile: Optional[str] = None,
        cr_token_file: Optional[str] = None,
        iam_url: Optional[str] = None,
        timeout: float = 30,
    ):
        if not api_key and not trusted_profile:
            raise IAMError(
                'IBM Cloud IAM needs an API key or a trusted profile (with the compute '
                'resource token Code Engine or IKS mounts).'
            )
        self.api_key = api_key
        self.trusted_profile = trusted_profile
        self.cr_token_file = cr_token_file
        self.iam_url = (iam_url or DEFAULT_IAM_URL).rstrip('/')
        self.timeout = timeout
        self._token: Optional[str] = None
        self._expires_at = 0.0
        self._lock = threading.Lock()

    @classmethod
    def from_environment(cls, environ: Optional[Dict[str, str]] = None) -> 'TokenManager':
        """
        IBM_CLOUD_API_KEY (or IBM_CLOUD_API_KEY_FILE), or IBM_CLOUD_TRUSTED_PROFILE;
        IBM_CLOUD_IAM_URL for a private IAM endpoint.
        """
        environ = os.environ if environ is None else environ
        api_key = environ.get('IBM_CLOUD_API_KEY')
        if not api_key and environ.get('IBM_CLOUD_API_KEY_FILE'):
            with open(environ['IBM_CLOUD_API_KEY_FILE']) as file:
                api_key = file.read().strip()
        return cls(
            api_key=api_key,
            trusted_profile=environ.get('IBM_CLOUD_TRUSTED_PROFILE'),
            cr_token_file=environ.get('IBM_CLOUD_CR_TOKEN_FILE'),
            iam_url=environ.get('IBM_CLOUD_IAM_URL'),
        )

    def token(self) -> str:
        with self._lock:
            if self._token is None or time.time() >= self._expires_at - REFRESH_BEFORE_SECONDS:
                self._token, self._expires_at = self._request()
            return self._token

    def _form(self) -> Dict[str, str]:
        if self.api_key:
            return dict(grant_type='urn:ibm:params:oauth:grant-type:apikey', apikey=self.api_key)
        path = self.cr_token_file or next((p for p in CR_TOKEN_PATHS if os.path.exists(p)), None)
        if not path:
            raise IAMError(
                'No compute resource token is mounted for the trusted profile: enable the '
                'trusted profile on the Code Engine app or IKS pod, or set '
                'IBM_CLOUD_CR_TOKEN_FILE.'
            )
        with open(path) as file:
            cr_token = file.read().strip()
        key = 'profile_id' if self.trusted_profile.startswith('Profile-') else 'profile_name'
        return {
            'grant_type': 'urn:ibm:params:oauth:grant-type:cr-token',
            'cr_token': cr_token,
            key: self.trusted_profile,
        }

    def _request(self):
        try:
            response = requests.post(
                f'{self.iam_url}/identity/token',
                data=self._form(),
                headers={'Accept': 'application/json'},
                timeout=self.timeout,
            )
        except requests.RequestException as error:
            raise IAMError(f'IBM Cloud IAM could not be reached: {error}') from None
        try:
            body = response.json()
        except ValueError:
            body = {}
        if response.status_code != 200:
            reason = body.get('errorMessage') or body.get('message') or response.reason
            raise IAMError(
                f'IBM Cloud IAM refused the credentials ({response.status_code}): {reason}'
            )
        token = body.get('access_token')
        if not token:
            raise IAMError('IBM Cloud IAM sent no access token.')
        now = time.time()
        expires_at = body.get('expiration') or now + float(body.get('expires_in') or 3600)
        return token, float(expires_at)
