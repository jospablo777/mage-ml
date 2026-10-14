"""
The IBM Cloud Object Storage io class: a data frame written and read back through COS with an
IAM API key keeps its values, and its settings come from io_config.yaml.
"""
import unittest

import pandas as pd

from mage_ai.io.config import ConfigKey
from mage_ai.io.ibm_cos import IBMCloudObjectStorage
from mage_ai.tests.services.ibm.fixtures import FakeIBMCloud


class Config(dict):
    def __getitem__(self, key):
        return self.get(key)


class IBMCloudObjectStorageTest(unittest.TestCase):
    def setUp(self):
        self.cloud = FakeIBMCloud()
        self.addCleanup(self.cloud.close)

    def test_a_data_frame_round_trips_through_cos_with_an_api_key(self):
        storage = IBMCloudObjectStorage.with_config(Config({
            ConfigKey.IBM_COS_ENDPOINT: self.cloud.url,
            ConfigKey.IBM_CLOUD_API_KEY: 'key-1',
            ConfigKey.IBM_COS_INSTANCE_ID: 'crn:v1:cos:1',
            ConfigKey.IBM_CLOUD_IAM_URL: self.cloud.url,
        }))
        df = pd.DataFrame(dict(id=[1, 2, 2**62], city=['Zürich', 'São Paulo', None]))
        storage.export(df, 'lake', 'orders/2026/orders.parquet')
        loaded = storage.load('lake', 'orders/2026/orders.parquet')

        pd.testing.assert_frame_equal(loaded, df)
        headers = {k.lower(): v for k, v in self.cloud.requests[0][2].items()}
        self.assertEqual(headers['authorization'], 'Bearer token-1')

    def test_hmac_keys_win_over_an_api_key(self):
        storage = IBMCloudObjectStorage.with_config(Config({
            ConfigKey.IBM_COS_ENDPOINT: self.cloud.url,
            ConfigKey.IBM_COS_HMAC_ACCESS_KEY_ID: 'a' * 32,
            ConfigKey.IBM_COS_HMAC_SECRET_ACCESS_KEY: 's' * 48,
            ConfigKey.IBM_CLOUD_API_KEY: 'key-1',
        }))
        storage.export(pd.DataFrame(dict(a=[1])), 'lake', 'a.csv')

        self.assertEqual(self.cloud.token_requests, [])

    def test_a_missing_endpoint_says_what_to_add(self):
        with self.assertRaises(ValueError) as caught:
            IBMCloudObjectStorage.with_config(Config({ConfigKey.IBM_CLOUD_API_KEY: 'k'}))
        self.assertIn('IBM_COS_ENDPOINT', str(caught.exception))
