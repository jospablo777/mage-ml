"""
IBM Cloud support never changes how MageML talks to AWS, Google Cloud or Azure: S3 clients
for other endpoints stay plain boto3 clients with boto3's defaults, also with IBM settings in
the environment, and importing the IBM modules sets nothing.
"""
import os
import unittest
from unittest.mock import patch

ENDPOINTS = (
    None,
    'https://s3.eu-west-1.amazonaws.com',
    'https://storage.googleapis.com',
    'http://127.0.0.1:9000',  # MinIO
)


class CloudIsolationTest(unittest.TestCase):
    def test_s3_clients_for_other_clouds_are_plain(self):
        from mage_ai.services.aws.s3 import s3

        environ = dict(IBM_COS_ENDPOINT='https://s3.us-south.cloud-object-storage.appdomain.cloud',
                       IBM_CLOUD_API_KEY='key', AWS_ACCESS_KEY_ID='AKIA' + 'X' * 16,
                       AWS_SECRET_ACCESS_KEY='s' * 40, AWS_DEFAULT_REGION='eu-west-1')
        with patch.dict('os.environ', environ):
            for endpoint in ENDPOINTS:
                client = s3.Client('bucket', endpoint_url=endpoint)
                self.assertFalse(client.is_cos, endpoint)
                self.assertNotEqual(
                    client.client.meta.config.request_checksum_calculation, 'when_required',
                    endpoint,
                )

    def test_importing_the_ibm_modules_changes_no_environment(self):
        before = dict(os.environ)
        import mage_ai.services.ibm.cos  # noqa: F401
        import mage_ai.services.ibm.secrets_manager  # noqa: F401

        self.assertEqual(dict(os.environ), before)
