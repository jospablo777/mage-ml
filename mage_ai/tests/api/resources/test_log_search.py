import asyncio
from pathlib import Path

from mage_ai.api.resources.LogResource import LogResource, matching_lines
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.tests.base_test import DBTestCase, TestCase
from mage_ai.tests.orchestration import test_flow_execution as flows


class MatchingLinesTest(TestCase):
    def test_only_lines_with_the_text_are_kept_ignoring_case(self):
        item = dict(path='a.log', content='start\nOrder 42 FAILED\nend\norder 43 failed')

        self.assertEqual(
            matching_lines(item, 'failed')['content'], 'Order 42 FAILED\norder 43 failed',
        )
        self.assertIsNone(matching_lines(item, 'missing'))


class LogSearchTest(DBTestCase):
    # The flow tests' pipeline harness, without their tests.
    block = flows.FlowExecutionTest.block
    execute_run = flows.FlowExecutionTest.execute_run

    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)
        self.output = Path(self.repo_path) / f'{self.pipeline.uuid}.parquet'

    def test_a_text_search_returns_only_matching_lines(self):
        """Pipeline logs had no text search (upstream #6105)."""
        self.block('load', 'data_loader', '''
            @data_loader
            def load(**kwargs):
                kwargs['logger'].info('needle-7f3a found the order')
                kwargs['logger'].info('nothing to see')
                return 1
        ''')
        run = trigger_pipeline(self.pipeline.uuid, variables={'output_path': str(self.output)})
        self.execute_run(run)

        def search(text):
            result = asyncio.run(LogResource.collection(
                {'text': [text]} if text else {}, {}, None, parent_model=self.pipeline,
            ))
            logs = result[0].model
            items = logs['block_run_logs'] + logs['pipeline_run_logs']
            return '\n'.join(item['content'] for item in items)

        everything = search(None)
        found = search('NEEDLE-7F3A')

        self.assertIn('nothing to see', everything)
        self.assertIn('needle-7f3a found the order', found)
        self.assertNotIn('nothing to see', found)
        self.assertEqual(search('absent-text'), '')
