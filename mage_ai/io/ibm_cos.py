from typing import Optional

from mage_ai.io.base import BaseFile
from mage_ai.io.config import BaseConfigLoader, ConfigKey
from mage_ai.io.s3 import S3
from mage_ai.services.ibm import cos


class IBMCloudObjectStorage(S3):
    """
    Handles data transfer between an IBM Cloud Object Storage bucket and the MageML app, with
    the file formats of `S3` (CSV, JSON, Parquet, HDF5).

    Authenticates with HMAC keys, an IBM Cloud API key, or a trusted profile (Code Engine,
    IKS). API keys and trusted profiles use IAM tokens over plain boto3, without IBM's fork of
    it; see `mage_ai.services.ibm.cos`.
    """

    def __init__(
        self,
        endpoint_url: str,
        api_key: Optional[str] = None,
        instance_id: Optional[str] = None,
        hmac_access_key_id: Optional[str] = None,
        hmac_secret_access_key: Optional[str] = None,
        trusted_profile: Optional[str] = None,
        iam_url: Optional[str] = None,
        verbose: bool = False,
        **kwargs,
    ) -> None:
        """
        - `endpoint_url`: the bucket's regional endpoint, such as
          `https://s3.us-south.cloud-object-storage.appdomain.cloud` (or its `private` or
          `direct` endpoint inside IBM Cloud)
        - `hmac_access_key_id` and `hmac_secret_access_key`: HMAC keys from service credentials
        - or `api_key`: an IBM Cloud API key, and `instance_id`: the COS instance CRN, needed to
          list or create buckets
        - or `trusted_profile`: a trusted profile id or name, on Code Engine or IKS
        """
        BaseFile.__init__(self, verbose=verbose)
        self.client = cos.client(
            endpoint_url,
            api_key=api_key,
            instance_id=instance_id,
            hmac_access_key_id=hmac_access_key_id,
            hmac_secret_access_key=hmac_secret_access_key,
            trusted_profile=trusted_profile,
            iam_url=iam_url,
            **kwargs,
        )

    @classmethod
    def with_config(cls, config: BaseConfigLoader, **kwargs) -> 'IBMCloudObjectStorage':
        """
        From IBM_COS_ENDPOINT, with IBM_COS_HMAC_ACCESS_KEY_ID and
        IBM_COS_HMAC_SECRET_ACCESS_KEY, or IBM_COS_API_KEY (or IBM_CLOUD_API_KEY) and
        IBM_COS_INSTANCE_ID, or IBM_CLOUD_TRUSTED_PROFILE.
        """
        endpoint = config[ConfigKey.IBM_COS_ENDPOINT]
        if not endpoint:
            raise ValueError(
                'IBM_COS_ENDPOINT is not set: add the bucket\'s endpoint to io_config.yaml, '
                'such as https://s3.us-south.cloud-object-storage.appdomain.cloud.'
            )
        return cls(
            endpoint,
            api_key=config[ConfigKey.IBM_COS_API_KEY] or config[ConfigKey.IBM_CLOUD_API_KEY],
            instance_id=config[ConfigKey.IBM_COS_INSTANCE_ID],
            hmac_access_key_id=config[ConfigKey.IBM_COS_HMAC_ACCESS_KEY_ID],
            hmac_secret_access_key=config[ConfigKey.IBM_COS_HMAC_SECRET_ACCESS_KEY],
            trusted_profile=config[ConfigKey.IBM_CLOUD_TRUSTED_PROFILE],
            iam_url=config[ConfigKey.IBM_CLOUD_IAM_URL],
            **kwargs,
        )
