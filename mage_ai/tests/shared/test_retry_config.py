from mage_ai.data_preparation.shared.retry import RetryConfig, resolve_retry_config
from mage_ai.tests.base_test import TestCase


class RetryConfigTests(TestCase):
    def test_project_pipeline_and_block_in_order(self):
        self.assertEqual(
            resolve_retry_config(
                dict(retries=1, delay=10, max_delay=100),
                dict(retries=3),
                dict(delay=1),
            ),
            dict(retries=3, delay=1, max_delay=100),
        )

    def test_blank_and_null_values_inherit(self):
        self.assertEqual(
            resolve_retry_config(dict(retries=2, delay=7), None, dict(retries=None, delay='')),
            dict(retries=2, delay=7),
        )

    def test_zero_turns_retries_off(self):
        self.assertEqual(resolve_retry_config(dict(retries=2), dict(retries=0))['retries'], 0)

    def test_null_values_load_as_defaults(self):
        config = RetryConfig.load(config=dict(retries=None, delay=None, max_delay=None))

        self.assertEqual((config.retries, config.delay, config.max_delay), (0, 5, 60))
