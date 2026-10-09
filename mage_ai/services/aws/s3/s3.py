from typing import Dict, Iterable, Iterator, List, Optional

import boto3
import boto3.s3.transfer as s3transfer
import botocore
from botocore.exceptions import ClientError

MAX_POOL_CONNECTIONS = 100
# S3 returns at most 1,000 keys per listing and deletes at most 1,000 keys per request.
PAGE_SIZE = 1000


class Client:
    def __init__(self, bucket, **kwargs):
        self.bucket = bucket
        self.settings = {
            key: kwargs.get(key)
            for key in (
                'aws_access_key_id',
                'aws_secret_access_key',
                'aws_session_token',
                'endpoint_url',
                'region_name',
            )
            if kwargs.get(key)
        }
        self.client = kwargs.get('client') or boto3.client(
            's3',
            aws_access_key_id=kwargs.get('aws_access_key_id'),
            aws_secret_access_key=kwargs.get('aws_secret_access_key'),
            aws_session_token=kwargs.get('aws_session_token'),
            config=botocore.client.Config(max_pool_connections=MAX_POOL_CONNECTIONS),
            endpoint_url=kwargs.get('endpoint_url'),
            region_name=kwargs.get('region_name'),
        )
        self.transfer_config = s3transfer.TransferConfig(
            use_threads=True,
            max_concurrency=MAX_POOL_CONNECTIONS,
        )

    def polars_storage_options(self) -> Optional[Dict[str, str]]:
        """
        Storage options that give Polars the settings this client was created with.
        None when it was created without any: Polars then reads the same environment
        variables and AWS configuration as boto3.
        """
        if not self.settings:
            return None
        options = {
            'aws_endpoint_url' if key == 'endpoint_url' else
            'aws_region' if key == 'region_name' else key: value
            for key, value in self.settings.items()
        }
        if options.get('aws_endpoint_url', '').startswith('http://'):
            options['aws_allow_http'] = 'true'
        return options

    def download_file(self, object_key: str, filename_destination):
        return self.client.download_file(
            self.bucket,
            object_key,
            filename_destination,
            Config=self.transfer_config,
        )

    def read(self, object_key: str):
        return self.get_object(object_key).read()

    def get_object(self, object_key: str):
        return self.client.get_object(Bucket=self.bucket, Key=object_key)['Body']

    def exists(self, object_key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=object_key)
        except ClientError as err:
            if err.response.get('Error', {}).get('Code') in ('404', 'NoSuchKey', 'NotFound'):
                return False
            raise
        return True

    def delete_keys(self, keys: Iterable[str]) -> None:
        keys = list(keys)
        for start in range(0, len(keys), PAGE_SIZE):
            response = self.client.delete_objects(
                Bucket=self.bucket,
                Delete={
                    'Objects': [{'Key': key} for key in keys[start:start + PAGE_SIZE]],
                    'Quiet': True,
                },
            )
            errors = response.get('Errors')
            if errors:
                raise RuntimeError(f'Failed to delete {len(errors)} objects: {errors[:3]}')

    def delete_objects(self, prefix: str):
        """
        Delete every key that starts with the prefix. A prefix without a trailing slash
        also matches sibling keys, so 'a/output_1' deletes 'a/output_10'.
        """
        self.delete_keys(self.list_objects(prefix, max_keys=None))

    def _pages(
        self,
        prefix: str,
        delimiter: Optional[str] = None,
        max_keys: Optional[int] = None,
    ) -> Iterator[dict]:
        params = dict(Bucket=self.bucket, Prefix=prefix)
        if delimiter:
            params['Delimiter'] = delimiter
        if max_keys is not None:
            params['MaxKeys'] = min(max_keys, PAGE_SIZE)
        while True:
            response = self.client.list_objects_v2(**params)
            yield response
            if not response.get('IsTruncated'):
                return
            params['ContinuationToken'] = response['NextContinuationToken']

    def listdir(
        self,
        prefix: str,
        delimiter: str = '/',
        suffix: str = None,
        max_results: int = None,
    ) -> List[str]:
        keys = []
        for response in self._pages(prefix, delimiter=delimiter, max_keys=max_results):
            for obj in response.get('Contents', []):
                if suffix is None or obj['Key'].endswith(suffix):
                    keys.append(obj['Key'])
            for obj in response.get('CommonPrefixes', []):
                keys.append(obj['Prefix'])
            if max_results is not None and len(keys) >= max_results:
                return keys[:max_results]
        return keys

    def list_objects(
        self,
        prefix: str,
        max_keys: Optional[int] = None,
        suffix: str = None,
    ) -> List[str]:
        keys = []
        for response in self._pages(prefix, max_keys=max_keys):
            for obj in response.get('Contents', []):
                if suffix is None or obj['Key'].endswith(suffix):
                    keys.append(obj['Key'])
            if max_keys is not None and len(keys) >= max_keys:
                return keys[:max_keys]
        return keys

    def upload(self, object_key: str, content):
        return self.client.put_object(
            Body=content,
            Bucket=self.bucket,
            Key=object_key,
        )

    def upload_object(self, object_key: str, file):
        return self.client.upload_fileobj(
            file,
            Bucket=self.bucket,
            Key=object_key,
            Config=self.transfer_config,
        )
