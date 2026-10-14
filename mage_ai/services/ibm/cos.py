"""
IBM Cloud Object Storage with plain boto3.

COS speaks the S3 API with two differences that break boto3 as it comes:

- With an API key or a trusted profile, COS takes `Authorization: Bearer <IAM token>`, not
  an AWS signature. IBM's SDK is a fork of boto3 for this (`signature_version='oauth'`).
  Here, botocore hooks scoped to the COS endpoint's host skip the signature and add the
  token on every attempt, so retries after an expiry carry a fresh token.
- boto3 1.36 and later add CRC checksums to every upload by default, which COS fails. COS
  clients compute checksums only when an operation requires them.

HMAC keys (from service credentials created with HMAC on) make COS take AWS signatures; then
any S3 tool works, such as pyarrow, DuckDB and Polars, and presigned URLs.

`install_boto3_hook` puts the same hooks on boto3's default session, for libraries that make
their own clients, such as MLflow's artifact store. Clients for other hosts, AWS S3 included,
are left as they are.
"""
import os
import threading
from typing import Dict, Optional
from urllib.parse import urlparse

from .iam import TokenManager

# The checksum behavior of boto3 before 1.36, which COS supports.
CHECKSUM_SETTINGS = dict(
    request_checksum_calculation='when_required',
    response_checksum_validation='when_required',
)
CHECKSUM_ENVIRONMENT = dict(
    AWS_REQUEST_CHECKSUM_CALCULATION='when_required',
    AWS_RESPONSE_CHECKSUM_VALIDATION='when_required',
)
_CONTEXT_KEY = 'mage_ibm_cos_iam'
_session = None
_session_lock = threading.Lock()


def _authority(url: str) -> str:
    """host[:port], lowercased, without the scheme's default port."""
    parsed = urlparse(url if '://' in url else f'https://{url}')
    host = (parsed.hostname or '').lower()
    default = {'http': 80, 'https': 443}.get(parsed.scheme)
    return host if parsed.port in (None, default) else f'{host}:{parsed.port}'


def endpoint_host(endpoint_url: str) -> str:
    authority = _authority(endpoint_url)
    if not authority or authority.startswith(':'):
        raise ValueError(f'{endpoint_url!r} is not a COS endpoint URL.')
    return authority


# IBM's public, private and direct COS endpoints: s3.<region>.cloud-object-storage...
COS_DOMAIN = 'cloud-object-storage.appdomain.cloud'


def is_cos_endpoint(endpoint_url: str) -> bool:
    """An IBM COS endpoint: IBM's domain, or the IBM_COS_ENDPOINT this process was given."""
    host = endpoint_host(endpoint_url)
    if host.split(':')[0].endswith(COS_DOMAIN):
        return True
    configured = os.getenv('IBM_COS_ENDPOINT')
    return bool(configured) and endpoint_host(configured) == host


def _host_matches(url: str, host: str) -> bool:
    # Path-style URLs carry the endpoint's host; virtual-hosted ones put the bucket before it.
    actual = _authority(url)
    return actual == host or actual.endswith(f'.{host}')


class IAMHooks:
    """botocore handlers that authenticate requests to one COS endpoint with IAM tokens."""

    def __init__(self, host: str, tokens: TokenManager, instance_id: Optional[str] = None):
        self.host = host
        self.tokens = tokens
        self.instance_id = instance_id

    def register(self, events) -> None:
        # Unique ids, so registering twice on a session does not add the hooks twice.
        suffix = f'{self.host}-{id(self)}'
        events.register('before-call.s3', self.tag, unique_id=f'mage-cos-tag-{suffix}')
        events.register('choose-signer.s3', self.unsigned, unique_id=f'mage-cos-signer-{suffix}')
        events.register('before-send.s3', self.authorize, unique_id=f'mage-cos-auth-{suffix}')

    def tag(self, params=None, context=None, **kwargs):
        if params is not None and context is not None:
            if _host_matches(params.get('url') or '', self.host):
                context[_CONTEXT_KEY] = True

    def unsigned(self, context=None, **kwargs):
        if context and context.get(_CONTEXT_KEY):
            from botocore import UNSIGNED

            return UNSIGNED
        return None

    def authorize(self, request=None, **kwargs):
        if request is None or not _host_matches(request.url, self.host):
            return None
        request.headers['Authorization'] = f'Bearer {self.tokens.token()}'
        if self.instance_id:
            # Listing and creating buckets name the COS instance.
            request.headers['ibm-service-instance-id'] = self.instance_id
        return None


def token_manager(
    api_key: Optional[str] = None,
    trusted_profile: Optional[str] = None,
    iam_url: Optional[str] = None,
) -> TokenManager:
    return TokenManager(api_key=api_key, trusted_profile=trusted_profile, iam_url=iam_url)


def client(
    endpoint_url: str,
    api_key: Optional[str] = None,
    instance_id: Optional[str] = None,
    hmac_access_key_id: Optional[str] = None,
    hmac_secret_access_key: Optional[str] = None,
    trusted_profile: Optional[str] = None,
    iam_url: Optional[str] = None,
    region_name: Optional[str] = None,
    config=None,
    tokens: Optional[TokenManager] = None,
):
    """
    A boto3 S3 client for a COS endpoint, such as
    `https://s3.us-south.cloud-object-storage.appdomain.cloud` (or its private or direct
    endpoint). HMAC keys win when both kinds of credentials are given.
    """
    import boto3
    from botocore.config import Config

    if '://' not in endpoint_url:
        endpoint_url = f'https://{endpoint_url}'
    settings = Config(**CHECKSUM_SETTINGS, s3=dict(addressing_style='path'))
    if config is not None:
        settings = settings.merge(config)
    if hmac_access_key_id and hmac_secret_access_key:
        return boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=hmac_access_key_id,
            aws_secret_access_key=hmac_secret_access_key,
            region_name=region_name or 'us-east-1',
            config=settings,
        )
    if tokens is None:
        if not api_key and not trusted_profile:
            raise ValueError(
                'IBM COS needs HMAC keys (access key id and secret), an API key, or a '
                'trusted profile.'
            )
        tokens = token_manager(api_key, trusted_profile, iam_url)
    # Hooks go on the client's own copy of the events, so clients can share a session; one
    # session loads the S3 model once. Sessions are not safe to create clients from in
    # parallel.
    with _session_lock:
        global _session
        if _session is None:
            _session = boto3.session.Session()
        s3 = _session.client(
            's3', endpoint_url=endpoint_url, region_name=region_name or 'us-east-1',
            config=settings,
        )
    IAMHooks(endpoint_host(endpoint_url), tokens, instance_id).register(s3.meta.events)
    return s3


_installed: Dict[str, IAMHooks] = {}
_install_lock = threading.Lock()


def install_boto3_hook(
    endpoint_url: str,
    api_key: Optional[str] = None,
    instance_id: Optional[str] = None,
    trusted_profile: Optional[str] = None,
    iam_url: Optional[str] = None,
    tokens: Optional[TokenManager] = None,
) -> None:
    """
    Makes boto3 clients that the default session creates from now on authenticate to this
    COS endpoint with IAM tokens, for libraries that create their own clients (MLflow).
    Sets boto3's checksum mode to `when_required` through the environment unless it is set.
    """
    import boto3

    host = endpoint_host(endpoint_url)
    for name, value in CHECKSUM_ENVIRONMENT.items():
        os.environ.setdefault(name, value)
    with _install_lock:
        if host in _installed:
            return
        if tokens is None:
            tokens = token_manager(api_key, trusted_profile, iam_url)
        hooks = IAMHooks(host, tokens, instance_id)
        if boto3.DEFAULT_SESSION is None:
            boto3.setup_default_session()
        hooks.register(boto3.DEFAULT_SESSION.events)
        _installed[host] = hooks


def install_boto3_hook_from_environment(environ: Optional[Dict[str, str]] = None) -> bool:
    """
    From IBM_COS_ENDPOINT with IBM_COS_API_KEY (or IBM_CLOUD_API_KEY) or
    IBM_CLOUD_TRUSTED_PROFILE, and IBM_COS_INSTANCE_ID. Returns whether it installed.
    """
    environ = os.environ if environ is None else environ
    endpoint = environ.get('IBM_COS_ENDPOINT')
    api_key = environ.get('IBM_COS_API_KEY') or environ.get('IBM_CLOUD_API_KEY')
    profile = environ.get('IBM_CLOUD_TRUSTED_PROFILE')
    if not endpoint or not (api_key or profile):
        return False
    install_boto3_hook(
        endpoint,
        api_key=api_key,
        instance_id=environ.get('IBM_COS_INSTANCE_ID'),
        trusted_profile=profile,
        iam_url=environ.get('IBM_CLOUD_IAM_URL'),
    )
    return True
