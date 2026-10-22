from unittest.mock import MagicMock

from mage_ai.data_integrations.utils.scheduler import build_block_run_metadata
from mage_ai.data_preparation.models.block.data_integration.utils import (
    get_streams_from_catalog,
)
from mage_ai.tests.base_test import TestCase


class StreamsFromCatalogTest(TestCase):
    def test_a_block_without_a_catalog_has_no_stream_settings(self):
        """A batch pipeline destination block with no catalog of its own failed its
        controller."""
        self.assertEqual(get_streams_from_catalog(None, ['users']), [])
        self.assertEqual(get_streams_from_catalog({}, ['users']), [])
        self.assertEqual(
            get_streams_from_catalog(dict(streams=[dict(tap_stream_id='users')]), ['users']),
            [dict(tap_stream_id='users')],
        )


class BuildBlockRunMetadataTest(TestCase):
    def test_reads_the_settings_when_none_are_given(self):
        """The settings were read and dropped, and the call failed on None."""
        block = MagicMock()
        block.is_data_integration.return_value = True
        block.is_source.return_value = True
        block.get_data_integration_settings.return_value = dict(
            catalog=dict(streams=[dict(tap_stream_id='users', stream='users')]),
            config=dict(batch_fetch_limit=10),
            data_integration_uuid='amplitude',
        )

        metadata = build_block_run_metadata(block, MagicMock())

        self.assertEqual(
            [(m['stream'], m['index'], m['number_of_batches']) for m in metadata],
            [('users', 0, 1)],
        )
