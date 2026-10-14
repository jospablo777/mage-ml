"""
ibm_secret_var in io_config.yaml: MageML reads IBM Cloud Secrets Manager with an IAM token,
picks each secret type's value as the exported services do, caches it while io_config.yaml
is rendered for every block, and explains what is missing.
"""
import unittest
from unittest.mock import patch

from mage_ai.services.ibm import secrets_manager
from mage_ai.services.ibm.iam import TokenManager
from mage_ai.services.ibm.secrets_manager import (
    SecretsManager,
    SecretsManagerError,
    value,
)
from mage_ai.tests.services.ibm.fixtures import FakeIBMCloud


class SecretValuesTest(unittest.TestCase):
    def test_each_secret_type_gives_its_value(self):
        self.assertEqual(value(dict(secret_type='arbitrary', payload='p'), None, 'a'), 'p')
        self.assertEqual(value(dict(secret_type='username_password', username='u',
                                    password='pw'), None, 'a'), 'pw')  # pragma: allowlist secret
        self.assertEqual(value(dict(secret_type='username_password', username='u'),
                               'username', 'a'), 'u')
        self.assertEqual(value(dict(secret_type='iam_credentials', api_key='k'), None, 'a'), 'k')
        self.assertEqual(value(dict(secret_type='kv', data=dict(port=5432)), 'port', 'a'), '5432')
        with self.assertRaisesRegex(SecretsManagerError, 'pick a key'):
            value(dict(secret_type='kv', data={}), None, 'a')
        with self.assertRaisesRegex(SecretsManagerError, 'has no field host'):
            value(dict(secret_type='kv', data={}), 'host', 'a')


class SecretsManagerTest(unittest.TestCase):
    def setUp(self):
        self.cloud = FakeIBMCloud()
        self.addCleanup(self.cloud.close)
        password = 's3cret'  # pragma: allowlist secret
        self.cloud.secrets['db'] = dict(secret_type='username_password', name='db',
                                        username='shop', password=password)
        tokens = TokenManager(api_key='key-1', iam_url=self.cloud.url)
        self.manager = SecretsManager(self.cloud.url, tokens)

    def test_secrets_are_read_with_a_token_and_cached(self):
        self.assertEqual(self.manager.get('db'), 's3cret')
        self.assertEqual(self.manager.get('db', 'username'), 'shop')
        self.assertEqual([r[0] for r in self.cloud.secret_requests], ['db'])
        self.assertEqual(self.cloud.secret_requests[0][1], 'Bearer token-1')

    def test_a_missing_secret_says_so(self):
        with self.assertRaisesRegex(SecretsManagerError, 'has no secret nope'):
            self.manager.get('nope')

    def test_the_template_function_reads_the_environment(self):
        environ = dict(IBM_SECRETS_MANAGER_URL=self.cloud.url, IBM_CLOUD_API_KEY='key-1',
                       IBM_CLOUD_IAM_URL=self.cloud.url)
        with patch.dict('os.environ', environ), patch.object(secrets_manager, '_default', None):
            from mage_ai.data_preparation.shared.utils import get_template_vars_no_db

            ibm_secret_var = get_template_vars_no_db()['ibm_secret_var']
            self.assertEqual(ibm_secret_var('db', 'username'), 'shop')

    def test_without_a_url_the_function_says_what_to_set(self):
        with patch.dict('os.environ', {}, clear=True), \
                patch.object(secrets_manager, '_default', None):
            with self.assertRaisesRegex(SecretsManagerError, 'IBM_SECRETS_MANAGER_URL'):
                secrets_manager.get_secret('db')

    def test_inside_a_service_the_value_comes_from_its_environment(self):
        environ = dict(MAGE_SERVICE='1', IBM_SECRET_DB_USERNAME='shop')
        with patch.dict('os.environ', environ, clear=True):
            self.assertEqual(secrets_manager.get_secret('db', 'username'), 'shop')
            with self.assertRaisesRegex(KeyError, 'IBM_SECRET_DB'):
                secrets_manager.get_secret('db')
