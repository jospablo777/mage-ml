import importlib
import sys
import unittest
from unittest.mock import MagicMock, patch


class SecretClientsTest(unittest.TestCase):
    """
    Each secret client was made when its module was imported. Off AWS, making the boto3
    client asked the instance metadata address for a region and waited 2 seconds, in every
    process that rendered a config file; the first export on a fresh server paid it too.
    """

    def fresh(self, name):
        sys.modules.pop(name, None)
        self.addCleanup(sys.modules.pop, name, None)
        return importlib.import_module(name)

    def test_aws_client_is_made_on_first_use_and_once(self):
        with patch('mage_ai.services.aws.get_aws_boto3_client') as make_client:
            module = self.fresh('mage_ai.services.aws.secrets_manager.secrets_manager')
            self.assertEqual(make_client.call_count, 0)

            cache = MagicMock()
            cache.get_secret_string.return_value = 'value'
            with patch.dict(sys.modules, aws_secretsmanager_caching=MagicMock(
                SecretCache=MagicMock(return_value=cache),
            )):
                self.assertEqual(module.get_secret('a'), 'value')
                self.assertEqual(module.get_secret('b'), 'value')
            self.assertEqual(make_client.call_count, 1)

    def test_azure_vault_is_not_required_until_used(self):
        with patch.dict('os.environ', {}, clear=True):
            module = self.fresh('mage_ai.services.azure.key_vault.key_vault')
            with patch.dict(sys.modules, {
                'azure.identity': MagicMock(),
                'azure.keyvault.secrets': MagicMock(),
            }):
                with self.assertRaisesRegex(Exception, 'AZURE_KEY_VAULT_URL'):
                    module.get_secret('a')

    def test_template_variables_offer_both_without_making_a_client(self):
        from mage_ai.data_preparation.shared.utils import get_template_vars_no_db

        self.fresh('mage_ai.services.aws.secrets_manager.secrets_manager')
        self.fresh('mage_ai.services.azure.key_vault.key_vault')
        with patch('mage_ai.services.aws.get_aws_boto3_client') as make_client:
            variables = get_template_vars_no_db()
        self.assertIn('aws_secret_var', variables)
        self.assertIn('azure_secret_var', variables)
        make_client.assert_not_called()
