"""
MLflow artifacts in IBM Cloud Object Storage, through MLflow's artifact repository plugins.

MageML registers the `cos` scheme (pyproject: `mlflow.artifact_repository`), so wherever
mage-ml is installed, MLflow clients and servers store artifacts at `cos://<bucket>/<path>`:

    mlflow server --artifacts-destination cos://models/mlflow ...

The settings come from the environment: IBM_COS_ENDPOINT, with IBM_COS_HMAC_ACCESS_KEY_ID and
IBM_COS_HMAC_SECRET_ACCESS_KEY, or IBM_COS_API_KEY (or IBM_CLOUD_API_KEY), or
IBM_CLOUD_TRUSTED_PROFILE.

Experiments that already use `s3://` artifact locations on COS (MLFLOW_S3_ENDPOINT_URL set
to the COS endpoint) keep them: when MLflow loads its artifact plugins and those settings
include an API key or a trusted profile, the boto3 hook for that endpoint is installed, so
MLflow's own S3 client authenticates with IAM tokens. No `sitecustomize.py` and no swap of
boto3 for IBM's fork.
"""
import os
import threading
import urllib.parse
from typing import Dict, Optional, Tuple

from mlflow.store.artifact.s3_artifact_repo import S3ArtifactRepository

from . import cos

_clients: Dict[Tuple, object] = {}
_clients_lock = threading.Lock()


def _settings(environ: Optional[Dict[str, str]] = None) -> Dict[str, Optional[str]]:
    environ = os.environ if environ is None else environ
    endpoint = environ.get('IBM_COS_ENDPOINT')
    if not endpoint:
        raise ValueError(
            'cos:// artifact locations need IBM_COS_ENDPOINT, such as '
            'https://s3.us-south.cloud-object-storage.appdomain.cloud, and HMAC keys, an API key '
            'or a trusted profile in the environment.'
        )
    return dict(
        endpoint_url=endpoint,
        hmac_access_key_id=environ.get('IBM_COS_HMAC_ACCESS_KEY_ID'),
        hmac_secret_access_key=environ.get('IBM_COS_HMAC_SECRET_ACCESS_KEY'),
        api_key=environ.get('IBM_COS_API_KEY') or environ.get('IBM_CLOUD_API_KEY'),
        instance_id=environ.get('IBM_COS_INSTANCE_ID'),
        trusted_profile=environ.get('IBM_CLOUD_TRUSTED_PROFILE'),
        iam_url=environ.get('IBM_CLOUD_IAM_URL'),
    )


def _client():
    settings = _settings()
    key = tuple(sorted(settings.items()))
    with _clients_lock:
        if key not in _clients:
            _clients[key] = cos.client(**settings)
        return _clients[key]


class COSArtifactRepository(S3ArtifactRepository):
    """Artifacts at cos://<bucket>/<path>, with the S3 repository's uploads and downloads."""

    def _get_s3_client(self):
        return _client()

    def parse_s3_compliant_uri(self, uri):
        parsed = urllib.parse.urlparse(uri)
        if parsed.scheme not in ('cos', 's3'):
            raise ValueError(f'Not a COS URI: {uri}')
        return parsed.netloc, parsed.path.removeprefix('/')


def _install_for_s3_locations() -> None:
    endpoint = os.environ.get('MLFLOW_S3_ENDPOINT_URL')
    if not endpoint or 'IBM_COS_ENDPOINT' not in os.environ:
        return
    if cos.endpoint_host(endpoint) != cos.endpoint_host(os.environ['IBM_COS_ENDPOINT']):
        return
    cos.install_boto3_hook_from_environment()


_install_for_s3_locations()
