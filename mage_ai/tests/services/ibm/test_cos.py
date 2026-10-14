"""
COS clients on plain boto3: IAM requests carry a bearer token (refreshed before it expires)
and the instance id, not an AWS signature; no request carries the CRC checksums boto3 adds by
default, which COS fails; HMAC requests are signed; and the default-session hook leaves other
hosts alone.
"""
import unittest
from unittest.mock import patch

import boto3

from mage_ai.services.ibm import cos
from mage_ai.services.ibm.iam import IAMError, TokenManager
from mage_ai.tests.services.ibm.fixtures import FakeIBMCloud

CHECKSUM_HEADERS = ('x-amz-checksum-crc32', 'x-amz-sdk-checksum-algorithm', 'x-amz-trailer')


def lower(headers):
    return {k.lower(): v for k, v in headers.items()}


class COSClientTest(unittest.TestCase):
    def setUp(self):
        self.cloud = FakeIBMCloud()
        self.addCleanup(self.cloud.close)

    def iam_client(self, **kwargs):
        return cos.client(
            self.cloud.url, api_key='key-1', instance_id='crn:v1:bluemix:public:cos:1',
            iam_url=self.cloud.url, **kwargs,
        )

    def test_an_iam_client_sends_a_bearer_token_and_the_instance_not_a_signature(self):
        s3 = self.iam_client()
        s3.put_object(Bucket='models', Key='churn/model.pkl', Body=b'\x00\x01weights')
        body = s3.get_object(Bucket='models', Key='churn/model.pkl')['Body'].read()

        self.assertEqual(body, b'\x00\x01weights')
        self.assertEqual(self.cloud.token_requests, [dict(
            grant_type='urn:ibm:params:oauth:grant-type:apikey', apikey='key-1',
        )])
        for method, path, headers, _ in self.cloud.requests:
            headers = lower(headers)
            self.assertEqual(headers['authorization'], 'Bearer token-1', (method, path))
            self.assertEqual(headers['ibm-service-instance-id'], 'crn:v1:bluemix:public:cos:1')
            self.assertFalse([h for h in CHECKSUM_HEADERS if h in headers], headers)
            self.assertNotIn('aws-chunked', headers.get('content-encoding', ''))

    def test_an_hmac_client_signs_its_requests_without_default_checksums(self):
        s3 = cos.client(self.cloud.url, hmac_access_key_id='a' * 32,
                        hmac_secret_access_key='s' * 48)
        s3.put_object(Bucket='models', Key='data.parquet', Body=b'PAR1')

        headers = lower(self.cloud.requests[0][2])
        self.assertTrue(headers['authorization'].startswith('AWS4-HMAC-SHA256'))
        self.assertFalse([h for h in CHECKSUM_HEADERS if h in headers], headers)
        self.assertEqual(self.cloud.token_requests, [])

    def test_a_token_is_replaced_before_it_expires(self):
        # Tokens that expire within the refresh margin are replaced on the next request.
        self.cloud.token_lifetime = 60
        s3 = self.iam_client()
        s3.put_object(Bucket='b', Key='k1', Body=b'1')
        s3.put_object(Bucket='b', Key='k2', Body=b'2')

        auth = [lower(h)['authorization'] for _, _, h, _ in self.cloud.requests]
        self.assertEqual(auth, ['Bearer token-1', 'Bearer token-2'])

    def test_a_token_is_reused_while_it_is_valid(self):
        s3 = self.iam_client()
        for i in range(3):
            s3.put_object(Bucket='b', Key=f'k{i}', Body=b'x')
        self.assertEqual(len(self.cloud.token_requests), 1)

    def test_a_rejected_key_says_why(self):
        s3 = cos.client(self.cloud.url, api_key='bad-key', iam_url=self.cloud.url)
        with self.assertRaises(IAMError) as caught:
            s3.put_object(Bucket='b', Key='k', Body=b'x')
        self.assertIn('Provided API key could not be found', str(caught.exception))
        self.assertIn('400', str(caught.exception))
        self.assertNotIn('bad-key', str(caught.exception))

    def test_no_credentials_is_explained(self):
        with self.assertRaises(ValueError) as caught:
            cos.client(self.cloud.url)
        self.assertIn('HMAC keys', str(caught.exception))

    def test_a_trusted_profile_exchanges_the_mounted_compute_resource_token(self):
        import tempfile

        with tempfile.NamedTemporaryFile('w', suffix='.token', delete=False) as file:
            file.write('cr-token-value\n')
        tokens = TokenManager(trusted_profile='Profile-1234', cr_token_file=file.name,
                              iam_url=self.cloud.url)
        s3 = cos.client(self.cloud.url, tokens=tokens)
        s3.put_object(Bucket='b', Key='k', Body=b'x')

        self.assertEqual(self.cloud.token_requests, [{
            'grant_type': 'urn:ibm:params:oauth:grant-type:cr-token',
            'cr_token': 'cr-token-value',
            'profile_id': 'Profile-1234',
        }])


class DefaultSessionHookTest(unittest.TestCase):
    def setUp(self):
        self.cloud = FakeIBMCloud()
        self.other = FakeIBMCloud()
        self.addCleanup(self.cloud.close)
        self.addCleanup(self.other.close)
        saved = boto3.DEFAULT_SESSION
        self.addCleanup(setattr, boto3, 'DEFAULT_SESSION', saved)
        boto3.DEFAULT_SESSION = None
        installed = dict(cos._installed)
        self.addCleanup(lambda: (cos._installed.clear(), cos._installed.update(installed)))
        cos._installed.clear()
        environ = patch.dict('os.environ', {}, clear=False)
        environ.start()
        self.addCleanup(environ.stop)

    def test_clients_from_the_default_session_use_iam_for_cos_only(self):
        import os

        cos.install_boto3_hook(self.cloud.url, api_key='key-1', iam_url=self.cloud.url)
        cos.install_boto3_hook(self.cloud.url, api_key='key-1', iam_url=self.cloud.url)
        # As MLflow makes its client: boto3.client with its own settings, no credentials.
        library = boto3.client('s3', endpoint_url=self.cloud.url, region_name='us-east-1')
        library.put_object(Bucket='mlflow', Key='artifacts/model.pkl', Body=b'm')
        aws = boto3.client('s3', endpoint_url=self.other.url, region_name='us-east-1',
                           aws_access_key_id='AKIA' + 'X' * 16, aws_secret_access_key='s' * 40)
        aws.put_object(Bucket='other', Key='k', Body=b'x')

        cos_headers = lower(self.cloud.requests[0][2])
        self.assertEqual(cos_headers['authorization'], 'Bearer token-1')
        self.assertEqual(len(self.cloud.requests), 1)
        other_headers = lower(self.other.requests[0][2])
        self.assertTrue(other_headers['authorization'].startswith('AWS4-HMAC-SHA256'))
        self.assertEqual(os.environ['AWS_REQUEST_CHECKSUM_CALCULATION'], 'when_required')

    def test_install_from_the_environment_needs_an_endpoint_and_credentials(self):
        self.assertFalse(cos.install_boto3_hook_from_environment({}))
        self.assertFalse(cos.install_boto3_hook_from_environment(
            dict(IBM_COS_ENDPOINT=self.cloud.url),
        ))
        self.assertTrue(cos.install_boto3_hook_from_environment(dict(
            IBM_COS_ENDPOINT=self.cloud.url, IBM_COS_API_KEY='key-1',
            IBM_CLOUD_IAM_URL=self.cloud.url,
        )))


class SharedSessionTest(unittest.TestCase):
    def test_clients_for_two_endpoints_keep_their_own_hooks(self):
        first, second = FakeIBMCloud(), FakeIBMCloud()
        self.addCleanup(first.close)
        self.addCleanup(second.close)
        a = cos.client(first.url, api_key='key-a', iam_url=first.url)
        b = cos.client(second.url, hmac_access_key_id='a' * 32, hmac_secret_access_key='s' * 48)
        a.put_object(Bucket='b', Key='k', Body=b'1')
        b.put_object(Bucket='b', Key='k', Body=b'2')

        self.assertEqual(lower(first.requests[0][2])['authorization'], 'Bearer token-1')
        self.assertTrue(lower(second.requests[0][2])['authorization'].startswith('AWS4'))
        self.assertEqual(second.token_requests, [])


class MageStorageOnCOSTest(unittest.TestCase):
    """MageML's own S3 storage (remote variables, logs) on COS, chosen by the endpoint."""

    def setUp(self):
        self.cloud = FakeIBMCloud()
        self.addCleanup(self.cloud.close)

    def test_variables_in_cos_use_iam_when_the_endpoint_is_cos(self):
        from mage_ai.data_preparation.storage.s3_storage import S3Storage

        environ = dict(IBM_COS_ENDPOINT=self.cloud.url, AWS_ENDPOINT_URL_S3=self.cloud.url,
                       IBM_CLOUD_API_KEY='key-1', IBM_CLOUD_IAM_URL=self.cloud.url)
        with patch.dict('os.environ', environ):
            storage = S3Storage(dirpath='s3://mage-vars/project')
            storage.write_json_file('s3://mage-vars/project/pipelines/p/output.json',
                                    dict(rows=3, city='Zürich'))
            data = storage.read_json_file('s3://mage-vars/project/pipelines/p/output.json')

        self.assertEqual(data, dict(rows=3, city='Zürich'))
        # Lazy Polars outputs go through the COS client, which Polars cannot replace.
        self.assertIsNone(storage.polars_location('s3://mage-vars/project/x.parquet'))
        headers = lower(self.cloud.requests[0][2])
        self.assertEqual(headers['authorization'], 'Bearer token-1')
        self.assertFalse([h for h in CHECKSUM_HEADERS if h in headers], headers)

    def test_other_endpoints_keep_a_plain_client(self):
        from mage_ai.services.aws.s3 import s3

        with patch.dict('os.environ', {'IBM_COS_ENDPOINT': self.cloud.url}):
            client = s3.Client('b', endpoint_url='https://s3.eu-west-1.amazonaws.com',
                               aws_access_key_id='AKIA' + 'X' * 16,
                               aws_secret_access_key='s' * 40)
        # boto3's own checksum default: not a COS client.
        self.assertNotEqual(client.client.meta.config.request_checksum_calculation,
                            'when_required')
        self.assertTrue(cos.is_cos_endpoint(
            'https://s3.private.eu-de.cloud-object-storage.appdomain.cloud'))
        self.assertFalse(cos.is_cos_endpoint('https://s3.amazonaws.com'))
