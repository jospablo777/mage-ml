import os
import posixpath
from typing import Dict

import boto3
from botocore.config import Config

from mage_integrations.destinations.delta_lake.base import DeltaLake as BaseDeltaLake
from mage_integrations.destinations.delta_lake.base import main


class DeltaLakeS3(BaseDeltaLake):
    """
    WARNING:
    If you get this error EndpointConnectionError occasionally,
    it’s because you have an ~/.aws/credentials file. Remove that file or else this error
    occurs occasionally from boto3.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.aws_region = None

    @property
    def bucket(self) -> str:
        return self.config['bucket']

    @property
    def region(self) -> str:
        return self.config.get('aws_region', 'us-west-2')

    @property
    def delta_log_object_key_path(self) -> str:
        return f'{self.table_object_key_path}/_delta_log'

    @property
    def table_object_key_path(self) -> str:
        return f"{self.config['object_key_path']}/{self.config['table']}"

    # PyDeltaTableError: Failed to load checkpoint: Failed to read checkpoint content:
    # Generic S3 error: Error performing get request ../_delta_log/_last_checkpoint:
    # response error "Received redirect without LOCATION,
    # this normally indicates an incorrectly configured region", after 0 retries
    # The above error is caused having AWS environment variables with values that
    # don’t match the S3 storage options when using Delta Lake to read and write from S3.
    def before_process(self) -> None:
        self.aws_region = os.getenv('AWS_DEFAULT_REGION')
        if self.aws_region:
            os.environ['AWS_DEFAULT_REGION'] = self.region

    def after_process(self) -> None:
        if self.aws_region:
            os.environ['AWS_DEFAULT_REGION'] = self.aws_region
        self.aws_region = None

    @property
    def endpoint(self) -> str:
        return self.config.get('aws_endpoint')

    def build_storage_options(self) -> Dict:
        options = {
            'AWS_ACCESS_KEY_ID': self.config['aws_access_key_id'],
            'AWS_REGION': self.region,
            'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
            'AWS_SECRET_ACCESS_KEY': self.config['aws_secret_access_key'],
        }
        if self.endpoint:
            options['AWS_ENDPOINT_URL'] = self.endpoint
            if self.endpoint.startswith('http://'):
                options['AWS_ALLOW_HTTP'] = 'true'
        return options

    def build_table_uri(self, stream: str) -> str:
        # posixpath.join took the parts as a list, which raised TypeError on every
        # export.
        return posixpath.join(
            f"s3://{self.config['bucket']}",
            self.config['object_key_path'],
            self.table_name,
        )

    def build_client(self):
        config = Config(
           retries={
              'max_attempts': 10,
              'mode': 'standard',
           },
        )

        return boto3.client(
            's3',
            aws_access_key_id=self.config['aws_access_key_id'],
            aws_secret_access_key=self.config['aws_secret_access_key'],
            config=config,
            region_name=self.region,
            endpoint_url=self.endpoint,
        )

    def check_and_create_delta_log(self, stream: str) -> bool:
        """
        Whether the table has a Delta log. Without one, objects left under the table's
        path are removed. The prefix had no trailing slash, so a table named orders also
        removed orders_archive, and only the first 1,000 objects were listed.
        """
        client = self.build_client()
        table_prefix = f'{self.table_object_key_path}/'

        resp = client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=f'{self.delta_log_object_key_path}/',
            MaxKeys=1,
        )
        has_logs = resp.get('KeyCount', 0) > 0
        if not has_logs:
            for page in client.get_paginator('list_objects_v2').paginate(
                Bucket=self.bucket,
                Prefix=table_prefix,
            ):
                keys = [{'Key': obj['Key']} for obj in page.get('Contents', [])]
                if keys:
                    client.delete_objects(Bucket=self.bucket, Delete={'Objects': keys})

        return has_logs

    def test_connection(self) -> None:
        client = self.build_client()
        client.head_bucket(Bucket=self.bucket)


if __name__ == '__main__':
    main(DeltaLakeS3)
