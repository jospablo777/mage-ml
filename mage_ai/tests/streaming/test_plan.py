"""The compile panel's plan of a stream: runs, boundaries, sinks, positions and problems."""
import textwrap
from pathlib import Path

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.constants import PipelineType
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import compilation
from mage_ai.streaming import plan
from mage_ai.tests.base_test import DBTestCase

PYTHON = '''
@transformer
def t(messages, *args, **kwargs):
    return messages
'''


class StreamPlanTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path,
                                        pipeline_type=PipelineType.STREAMING)
        self.addCleanup(self.pipeline.delete)

    def add(self, name, kind, content, upstream=(), language='yaml'):
        block = Block.create(f'{self.pipeline.uuid}_{name}', kind, self.repo_path,
                             language=language)
        Path(block.file_path).write_text(textwrap.dedent(content))
        self.pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    def test_the_plan_shows_runs_sinks_positions_and_the_dead_letter_sink(self):
        source = self.add('source', 'data_loader', '''
            connector_type: kafka
            bootstrap_server: "{{ env_var('KAFKA_BOOTSTRAP') }}"
            topic: orders
            consumer_group: scoring
        ''')
        first = self.add('clean', 'transformer', PYTHON, [source], language='python')
        second = self.add('score', 'transformer', PYTHON, [first], language='python')
        sink = self.add('out', 'data_exporter', 'connector_type: dummy\n', [second])
        dead = self.add('dead', 'data_exporter', '''
            connector_type: kafka
            bootstrap_server: "{{ env_var('KAFKA_BOOTSTRAP') }}"
            topic: failed
            dead_letter: true
        ''', [second])

        found = compilation.summary_for(Pipeline.get(self.pipeline.uuid,
                                                     repo_path=self.repo_path))['stream']

        self.assertEqual(found['problems'], [])
        self.assertEqual(found['source'], dict(block=source.uuid, kind='kafka'))
        self.assertEqual(found['runs'], [dict(
            language='python', blocks=[first.uuid, second.uuid],
            runs_in=plan.RUNS_IN['python'],
        )])
        self.assertEqual(found['boundaries'], 0)
        self.assertEqual(found['sinks'], [dict(block=sink.uuid, kind='stdout',
                                               after=second.uuid)])
        self.assertEqual(found['dead_letter'], dict(block=dead.uuid, kind='kafka'))
        self.assertIn('scoring-dev-', found['development'][0])

    def test_problems_that_stop_the_stream_are_shown_before_start(self):
        source = self.add('source', 'data_loader', 'connector_type: kinesis\nstream_name: x\n')
        self.add('out', 'data_exporter', 'connector_type: dummy\n', [source])

        found = plan.stream_plan(Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path))

        self.assertTrue(any('kinesis' in p for p in found['problems']), found)

    def test_languages_split_into_runs_with_boundaries(self):
        self.assertEqual(
            [(r['language'], r['blocks']) for r in plan._runs([
                dict(uuid='a', language='python'), dict(uuid='b', language='rust'),
                dict(uuid='c', language='rust'), dict(uuid='d', language='python'),
            ])],
            [('python', ['a']), ('rust', ['b', 'c']), ('python', ['d'])],
        )
