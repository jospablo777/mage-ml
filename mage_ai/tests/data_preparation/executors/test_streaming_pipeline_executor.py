import shutil
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

from mage_ai.data_preparation.executors.streaming_pipeline_executor import (
    StreamingPipelineExecutor,
)
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.constants import BlockLanguage, PipelineType
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.streaming.sources.base import SourceConsumeMethod
from mage_ai.tests.base_test import DBTestCase


class FakeSource:
    """Hands over one batch and, as RabbitMQ does, reads its list after the handler."""

    consume_method = SourceConsumeMethod.BATCH_READ

    def __init__(self):
        self.messages = [dict(id=1), dict(id=2)]
        self.after_handler = None

    def batch_read(self, handler):
        handler(self.messages)
        self.after_handler = [dict(m) for m in self.messages]

    def destroy(self):
        pass


class FakeSink:
    def __init__(self):
        self.batches = []

    def batch_write(self, messages):
        self.batches.append(messages)

    def destroy(self):
        pass


class StreamingPipelineExecutorTest(DBTestCase):
    def add(self, name, kind, content, upstream=(), language=BlockLanguage.PYTHON):
        block = Block.create(
            f'{self.pipeline.uuid}_{name}', kind, self.repo_path, language=language,
        )
        Path(block.file_path).write_text(textwrap.dedent(content))
        self.pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    def run_pipeline(self, sink_names):
        source = FakeSource()
        sinks = {name: FakeSink() for name in sink_names}
        pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        with patch(
            'mage_ai.streaming.sources.source_factory.SourceFactory.get_source',
            return_value=source,
        ), patch(
            'mage_ai.streaming.sinks.sink_factory.SinkFactory.get_sink',
            side_effect=lambda config, **kwargs: sinks[config['name']],
        ):
            StreamingPipelineExecutor(pipeline).execute(retry_config=dict(retries=0))
        return source, sinks

    def build(self, mutate, transformer_last):
        """
        A source with a transformer, which changes the messages, and a sink; the block
        added last is the last to get the messages.
        """
        self.pipeline = Pipeline.create(
            f'{self._testMethodName}_{transformer_last}', repo_path=self.repo_path,
            pipeline_type=PipelineType.STREAMING,
        )
        self.addCleanup(self.pipeline.delete)
        source = self.add('source', 'data_loader', 'connector_type: kafka\n',
                          language=BlockLanguage.YAML)

        def sink(name, upstream):
            self.add(name, 'data_exporter', f'connector_type: dummy\nname: {name}\n',
                     upstream=[upstream], language=BlockLanguage.YAML)

        if transformer_last:
            sink('direct', source)
        transformer = self.add('mutate', 'transformer', f'''
            @transformer
            def transform(messages, *args, **kwargs):
                {mutate}
                return messages
        ''', upstream=[source])
        sink('after', transformer)
        if not transformer_last:
            sink('direct', source)
        last = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path).get_block(
            source.uuid,
        ).downstream_blocks[-1]
        self.assertEqual(last.uuid.endswith('mutate'), transformer_last)

    def test_a_transformer_that_changes_messages_does_not_change_other_branches(self):
        for transformer_last in (False, True):
            with self.subTest(transformer_last=transformer_last):
                self.build("for m in messages:\n                    m['changed'] = True",
                           transformer_last)

                source, sinks = self.run_pipeline(['after', 'direct'])

                self.assertEqual(sinks['after'].batches, [[
                    dict(id=1, changed=True), dict(id=2, changed=True),
                ]])
                self.assertEqual(sinks['direct'].batches, [[dict(id=1), dict(id=2)]])

    def test_the_source_keeps_its_list_when_a_transformer_empties_it(self):
        for transformer_last in (False, True):
            with self.subTest(transformer_last=transformer_last):
                self.build('messages.clear()', transformer_last)

                source, sinks = self.run_pipeline(['after', 'direct'])

                self.assertEqual(source.after_handler, [dict(id=1), dict(id=2)])
                self.assertEqual(sinks['direct'].batches, [[dict(id=1), dict(id=2)]])
                self.assertEqual(sinks['after'].batches, [[]])

    def test_a_chain_with_one_block_after_each_copies_nothing(self):
        self.pipeline = Pipeline.create(
            self._testMethodName, repo_path=self.repo_path, pipeline_type=PipelineType.STREAMING,
        )
        self.addCleanup(self.pipeline.delete)
        upstream = self.add('source', 'data_loader', 'connector_type: kafka\n',
                            language=BlockLanguage.YAML)
        for name in ['first', 'second']:
            upstream = self.add(name, 'transformer', '''
                @transformer
                def transform(messages, *args, **kwargs):
                    return [dict(m, seen=m.get('seen', 0) + 1) for m in messages]
            ''', upstream=[upstream])
        self.add('sink', 'data_exporter', 'connector_type: dummy\nname: sink\n',
                 upstream=[upstream], language=BlockLanguage.YAML)

        with patch(
            'mage_ai.data_preparation.executors.streaming_pipeline_executor.copy_messages',
        ) as copy_messages:
            _, sinks = self.run_pipeline(['sink'])

        copy_messages.assert_not_called()
        self.assertEqual(sinks['sink'].batches, [[dict(id=1, seen=2), dict(id=2, seen=2)]])

    def test_the_dead_letter_sink_is_not_written_in_the_editor(self):
        """It takes only failing messages, in an exported service."""
        self.pipeline = Pipeline.create(
            self._testMethodName, repo_path=self.repo_path, pipeline_type=PipelineType.STREAMING,
        )
        self.addCleanup(self.pipeline.delete)
        source = self.add('source', 'data_loader', 'connector_type: kafka\n',
                          language=BlockLanguage.YAML)
        self.add('sink', 'data_exporter', 'connector_type: dummy\nname: sink\n',
                 upstream=[source], language=BlockLanguage.YAML)
        self.add('dead', 'data_exporter',
                 'connector_type: dummy\nname: dead\ndead_letter: true\n',
                 upstream=[source], language=BlockLanguage.YAML)

        source, sinks = self.run_pipeline(['sink', 'dead'])

        self.assertEqual(sinks['sink'].batches, [[dict(id=1), dict(id=2)]])
        self.assertEqual(sinks['dead'].batches, [])

    def test_rust_transformers_run_in_their_programs(self):
        """Execute pipeline built them as batch blocks, which a stream block cannot be."""
        self.pipeline = Pipeline.create(
            self._testMethodName, repo_path=self.repo_path, pipeline_type=PipelineType.STREAMING,
        )
        self.addCleanup(self.pipeline.delete)
        source = self.add('source', 'data_loader', 'connector_type: kafka\n',
                          language=BlockLanguage.YAML)
        rust = self.add('rust', 'transformer', '''
            use mage::prelude::*;

            fn transform(messages: Vec<Value>) -> Vec<Value> {
                println!("rust saw {}", messages.len());
                messages.into_iter().map(|mut m| { m["rust"] = json!(true); m }).collect()
            }
        ''', upstream=[source], language=BlockLanguage.RUST)
        self.add('sink', 'data_exporter', 'connector_type: dummy\nname: sink\n',
                 upstream=[rust], language=BlockLanguage.YAML)

        _, sinks = self.run_pipeline(['sink'])

        self.assertEqual(sinks['sink'].batches,
                         [[dict(id=1, rust=True), dict(id=2, rust=True)]])

    @unittest.skipUnless(shutil.which('Rscript'), 'needs Rscript, with arrow and jsonlite')
    def test_r_transformers_run_in_their_programs(self):
        self.pipeline = Pipeline.create(
            self._testMethodName, repo_path=self.repo_path, pipeline_type=PipelineType.STREAMING,
        )
        self.addCleanup(self.pipeline.delete)
        source = self.add('source', 'data_loader', 'connector_type: kafka\n',
                          language=BlockLanguage.YAML)
        r = self.add('r', 'transformer', '''
            #* @transformer
            transform <- function(messages, ...) {
              messages$r <- TRUE
              messages
            }
        ''', upstream=[source], language=BlockLanguage.R)
        self.add('sink', 'data_exporter', 'connector_type: dummy\nname: sink\n',
                 upstream=[r], language=BlockLanguage.YAML)

        _, sinks = self.run_pipeline(['sink'])

        self.assertEqual(sinks['sink'].batches, [[dict(id=1, r=True), dict(id=2, r=True)]])


class DevelopmentRunTest(DBTestCase):
    """A stream run from the editor never moves production's consumer group (SAFETY.md 3)."""

    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(
            self._testMethodName, repo_path=self.repo_path, pipeline_type=PipelineType.STREAMING,
        )
        self.addCleanup(self.pipeline.delete)
        source = Block.create(f'{self.pipeline.uuid}_source', 'data_loader', self.repo_path,
                              language=BlockLanguage.YAML)
        Path(source.file_path).write_text(
            'connector_type: kafka\nbootstrap_server: localhost:9092\ntopic: orders\n'
            'consumer_group: scoring\n')
        self.pipeline.add_block(source)
        sink = Block.create(f'{self.pipeline.uuid}_sink', 'data_exporter', self.repo_path,
                            language=BlockLanguage.YAML)
        Path(sink.file_path).write_text('connector_type: dummy\nname: sink\n')
        self.pipeline.add_block(sink, upstream_block_uuids=[source.uuid])

    def run_with(self, run):
        seen = {}

        def get_source(config, **kwargs):
            seen.update(config=config, **kwargs)
            return FakeSource()

        with patch('mage_ai.streaming.sources.source_factory.SourceFactory.get_source',
                   side_effect=get_source), \
                patch('mage_ai.streaming.sinks.sink_factory.SinkFactory.get_sink',
                      return_value=FakeSink()):
            run(Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path))
        return seen

    def test_the_editor_reads_with_a_development_group_and_checkpoints(self):
        printed = []

        # As the notebook's StreamBlockOutputToQueue: write, and nothing else.
        class Output:
            def write(self, text):
                printed.append(text)

        seen = self.run_with(lambda p: p.execute_sync(
            build_block_output_stdout=lambda block_uuid, **kwargs: Output(),
            retry_config=dict(), development_user='Ada Lovelace',
        ))

        self.assertEqual(seen['config']['consumer_group'], 'scoring-dev-ada-lovelace')
        self.assertTrue(seen['checkpoint_path'].endswith('streaming_checkpoint_dev'))
        self.assertIn('the group scoring keeps its position', ''.join(printed))

    def test_a_trigger_run_keeps_the_production_group(self):
        seen = self.run_with(
            lambda p: StreamingPipelineExecutor(p).execute(retry_config=dict(retries=0)))

        self.assertEqual(seen['config']['consumer_group'], 'scoring')
        self.assertTrue(seen['checkpoint_path'].endswith('streaming_checkpoint'))

    def test_the_sinks_a_run_writes_are_named_for_confirmation(self):
        from mage_ai.streaming.development import sinks_written

        source = self.pipeline.get_block(f'{self.pipeline.uuid}_source')
        for name, language, content in (
            ('scored', BlockLanguage.YAML, 'connector_type: kafka\ntopic: scored\n'),
            ('failures', BlockLanguage.YAML,
             'connector_type: kafka\ntopic: failed\ndead_letter: true\n'),
            ('custom', BlockLanguage.PYTHON, '@streaming_sink\nclass Sink:\n    pass\n'),
        ):
            block = Block.create(f'{self.pipeline.uuid}_{name}', 'data_exporter',
                                 self.repo_path, language=language)
            Path(block.file_path).write_text(content)
            self.pipeline.add_block(block, upstream_block_uuids=[source.uuid])

        found = sinks_written(Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path))

        self.assertEqual(sorted(found), sorted([
            (f'{self.pipeline.uuid}_scored', 'kafka scored'),
            (f'{self.pipeline.uuid}_custom', 'a Python sink'),
        ]))
