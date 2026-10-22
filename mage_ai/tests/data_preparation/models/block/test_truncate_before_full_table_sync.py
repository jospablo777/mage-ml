import os
from unittest.mock import MagicMock, patch

from mage_ai.data_integrations.destinations.truncate import should_truncate
from mage_ai.data_preparation.models.block.data_integration.utils import (
    truncate_destination_before_sync,
)
from mage_ai.data_preparation.models.block.integration import DestinationBlock
from mage_ai.data_preparation.models.constants import BlockType
from mage_ai.tests.base_test import TestCase

TRUNCATE = dict(truncate_before_full_table_sync=True)


def finished_process():
    proc = MagicMock()
    proc.stdout = []
    proc.returncode = 0
    return proc


class ShouldTruncateTest(TestCase):
    def test_only_full_table_streams_with_the_setting(self):
        self.assertTrue(should_truncate(TRUNCATE, 'FULL_TABLE'))
        self.assertFalse(should_truncate(TRUNCATE, 'INCREMENTAL'))
        self.assertFalse(should_truncate(TRUNCATE, 'LOG_BASED'))
        self.assertFalse(should_truncate({}, 'FULL_TABLE'))
        self.assertFalse(should_truncate(None, 'FULL_TABLE'))


class IntegrationPipelineTest(TestCase):
    """A stream's batches run in order in an integration pipeline: the first truncates."""

    def run_exporter(self, index: int, config: dict, replication_method='FULL_TABLE'):
        pipeline = MagicMock()
        pipeline.source_output_file_path.return_value = os.path.join(self.repo_path, 'out.json')
        with open(pipeline.source_output_file_path.return_value, 'w') as f:
            f.write('')
        pipeline.source_state_file_path.return_value = os.path.join(self.repo_path, 'in.json')
        pipeline.destination_state_file_path.return_value = os.path.join(
            self.repo_path, 'out_state.json',
        )
        block = DestinationBlock('dest', 'dest', BlockType.DATA_EXPORTER, pipeline=pipeline)
        block.template_runtime_configuration = dict(index=index, selected_streams=['users'])
        module = 'mage_ai.data_preparation.models.block.integration'
        calls = []
        with patch(f'{module}.get_catalog_by_stream', return_value=dict(
            replication_method=replication_method,
        )), patch(f'{module}.build_config', return_value=(config, '{}')), patch(
            f'{module}.truncate_destination_table',
            side_effect=lambda args, *a, **k: calls.append(('truncate', args)),
        ), patch(
            f'{module}.subprocess.Popen',
            side_effect=lambda args, **k: calls.append(('write', args)) or finished_process(),
        ):
            block._execute_block([])
        return calls

    def test_the_first_batch_truncates_before_it_writes(self):
        calls = self.run_exporter(0, TRUNCATE)

        self.assertEqual([kind for kind, _ in calls], ['truncate', 'write'])
        self.assertNotIn('--input_file_path', calls[0][1])
        self.assertIn('--input_file_path', calls[1][1])

    def test_later_batches_do_not_truncate(self):
        self.assertEqual([kind for kind, _ in self.run_exporter(1, TRUNCATE)], ['write'])

    def test_without_the_setting_or_on_incremental_streams(self):
        self.assertEqual([kind for kind, _ in self.run_exporter(0, {})], ['write'])
        self.assertEqual(
            [kind for kind, _ in self.run_exporter(0, TRUNCATE, 'INCREMENTAL')],
            ['write'],
        )


class BatchPipelineTest(TestCase):
    """Batches run in parallel in a batch pipeline: the stream's controller truncates."""

    def block(self, config: dict, replication_method='FULL_TABLE'):
        block = MagicMock()
        block.uuid = 'dest'
        block.template_runtime_configuration = {}
        block.configuration_data_integration = {}
        block.pipeline.pipeline_variables_dir = self.repo_path
        block.pipeline.get_block.return_value.is_source.return_value = False
        block.get_data_integration_settings.return_value = dict(
            catalog=dict(streams=[dict(
                destination_table='people',
                replication_method=replication_method,
                schema=dict(properties={}),
                stream='users',
                tap_stream_id='users',
            )]),
            config=dict(config),
            data_integration_uuid='postgresql',
        )
        return block

    def truncate(self, block):
        with patch(
            'mage_ai.data_integrations.destinations.truncate.subprocess.Popen',
            return_value=finished_process(),
        ) as popen:
            truncated = truncate_destination_before_sync(block, 'users', logger=MagicMock())
        return truncated, popen

    def test_truncates_the_stream_table_without_input(self):
        truncated, popen = self.truncate(self.block(TRUNCATE))

        self.assertTrue(truncated)
        args = popen.call_args[0][0]
        self.assertEqual(args[-1], '--truncate_table')
        self.assertNotIn('--input_file_path', args)
        self.assertIn('"table": "people"', args[args.index('--config_json') + 1])

    def test_without_the_setting_nothing_runs(self):
        truncated, popen = self.truncate(self.block({}))

        self.assertFalse(truncated)
        popen.assert_not_called()

    def test_incremental_streams_are_not_truncated(self):
        truncated, popen = self.truncate(self.block(TRUNCATE, 'INCREMENTAL'))

        self.assertFalse(truncated)
        popen.assert_not_called()
