import asyncio
import copy
import logging
import os
import time
import traceback
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from typing import Callable, Dict, List, Optional, Union

import pytz

from mage_ai.data_preparation.executors.pipeline_executor import PipelineExecutor
from mage_ai.data_preparation.logging.logger import DictLogger
from mage_ai.data_preparation.models.block.streaming import streaming_config
from mage_ai.data_preparation.models.constants import BlockLanguage, BlockType
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.data_preparation.shared.retry import RetryConfig
from mage_ai.data_preparation.shared.stream import StreamToLogger
from mage_ai.orchestration.db import safe_db_query
from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.shared.hash import merge_dict
from mage_ai.shared.retry import retry


def copy_messages(data):
    """A deep copy of a batch; items that cannot be deep copied are copied shallowly."""
    if data is None:
        return data
    if type(data) is list:
        return [copy_messages(item) for item in data]
    try:
        return copy.deepcopy(data)
    except Exception:
        return copy.copy(data)


class StreamingPipelineExecutor(PipelineExecutor):
    def __init__(self, pipeline: Pipeline, development: bool = False,
                 development_user: str = None, **kwargs):
        super().__init__(pipeline, **kwargs)
        # A run from the editor: positions of its own (mage_ai/streaming/development.py).
        self.development = development
        self.development_user = development_user
        # TODO: Support custom log destination for streaming pipelines
        self.parse_and_validate_blocks()
        self.retry_metadata = dict(attempts=0)

    def parse_and_validate_blocks(self):
        """
        Find the first valid streaming pipeline is in the structure:
        source -> transformer1 -> sink2
               -> transformer2 -> sink2
               -> transformer3 -> sink3
        """
        blocks = self.pipeline.blocks_by_uuid.values()
        source_blocks = []
        sink_blocks = []
        transformer_blocks = []
        for b in blocks:
            if b.type == BlockType.DATA_LOADER:
                # Data loader block should be root block
                if len(b.upstream_blocks or []) > 0:
                    raise Exception(f'Data loader {b.uuid} can\'t have upstream blocks.')
                if len(b.downstream_blocks or []) < 1:
                    raise Exception(f'Data loader {b.uuid} must have at least one transformer or'
                                    ' data exporter as the downstream block.')
                source_blocks.append(b)
            if b.type == BlockType.DATA_EXPORTER:
                # Data exporter block should be leaf block
                if len(b.downstream_blocks or []) > 0:
                    raise Exception(f'Data expoter {b.uuid} can\'t have downstream blocks.')
                if len(b.upstream_blocks or []) != 1:
                    raise Exception(f'Data exporter {b.uuid} must have a transformer or data'
                                    ' loader as the upstream block.')
                sink_blocks.append(b)
            if b.type == BlockType.TRANSFORMER:
                # Each transformer block can only have one upstream block
                if len(b.upstream_blocks or []) != 1:
                    raise Exception(f'Transformer {b.uuid} should (only) have one upstream block.')
                transformer_blocks.append(b)

        if len(source_blocks) != 1:
            raise Exception('Please provide (only) one data loader block as the source.')

        self.source_block = source_blocks[0]
        self.sink_blocks = sink_blocks

    def execute(
        self,
        build_block_output_stdout: Callable[..., object] = None,
        global_vars: Dict = None,
        pipeline_run_id: int = None,
        retry_config: Dict = None,
        **kwargs,
    ) -> None:
        # TODOs:
        # 1. Support multiple sources and sinks
        # 2. Support flink pipeline

        tags = self.build_tags(pipeline_run_id=pipeline_run_id, **kwargs)
        self.logging_tags = tags
        if build_block_output_stdout:
            stdout_logger = logging.getLogger('streaming_pipeline_executor')
            self.logger = DictLogger(stdout_logger)
            stdout = build_block_output_stdout(self.pipeline.uuid)
        else:
            self.logger = DictLogger(self.logger_manager.logger, logging_tags=tags)
            stdout = StreamToLogger(self.logger, logging_tags=tags)
        try:
            if retry_config is None:
                retry_config = self.pipeline.retry_config or dict()
            infinite_retries = False if retry_config else True

            if type(retry_config) is not RetryConfig:
                retry_config = RetryConfig.load(config=retry_config)

            @retry(
                retries=retry_config.retries,
                delay=retry_config.delay,
                max_delay=retry_config.max_delay,
                exponential_backoff=retry_config.exponential_backoff,
                logger=self.logger,
                logging_tags=self.logging_tags,
                retry_metadata=self.retry_metadata,
            )
            def __execute_with_retry():
                try:
                    with redirect_stdout(stdout):
                        with redirect_stderr(stdout):
                            self.__execute_in_python(
                                build_block_output_stdout=build_block_output_stdout,
                                global_vars=global_vars,
                                pipeline_run_id=pipeline_run_id,
                            )
                finally:
                    if isinstance(stdout, StreamToLogger):
                        stdout.flush()
            __execute_with_retry()
        except Exception as e:
            if not build_block_output_stdout:
                self.logger.exception(
                        f'Failed to execute streaming pipeline {self.pipeline.uuid}',
                        **merge_dict(dict(error=e), tags),
                    )
            if not infinite_retries:
                # If pipeline retry config is present, fail the pipeline after the retries
                self.__update_pipeline_run_status(
                    pipeline_run_id,
                    PipelineRun.PipelineRunStatus.FAILED,
                    error=e,
                )

            raise e

    def __tell(self, build_block_output_stdout, block_uuid: str, message: str) -> None:
        if build_block_output_stdout:
            build_block_output_stdout(block_uuid).write(message + '\n')
        else:
            self.logger.info(message, **(getattr(self, 'logging_tags', None) or {}))

    def __execute_in_python(
        self,
        build_block_output_stdout: Callable[..., object] = None,
        global_vars: Dict = None,
        pipeline_run_id: int = None,

    ):
        from mage_ai.streaming.sinks.sink_factory import SinkFactory
        from mage_ai.streaming.sources.base import SourceConsumeMethod
        from mage_ai.streaming.sources.source_factory import SourceFactory

        if global_vars is None:
            global_vars = dict()

        # Initialize source block
        if self.source_block.language == BlockLanguage.PYTHON:
            source = SourceFactory.get_python_source(
                self.source_block.content,
                global_vars=global_vars,
            )
        else:
            # Default to YAML config
            source_config = self.__interpolate_vars(
                self.source_block.content,
                global_vars=global_vars,
            )
            if self.development:
                from mage_ai.streaming.development import for_development

                source_config, notes = for_development(source_config, self.development_user)
                for note in notes:
                    self.__tell(build_block_output_stdout, self.source_block.uuid, note)
            source = SourceFactory.get_source(
                source_config,
                checkpoint_path=os.path.join(
                    self.pipeline.pipeline_variables_dir,
                    'streaming_checkpoint_dev' if self.development else 'streaming_checkpoint',
                ),
            )

        # Initialize destination blocks
        sinks_by_uuid = dict()
        for sink_block in self.sink_blocks:
            if sink_block.language == BlockLanguage.PYTHON:
                sinks_by_uuid[sink_block.uuid] = SinkFactory.get_python_sink(
                    sink_block.content,
                    global_vars=global_vars,
                )
            else:
                sink_config = self.__interpolate_vars(sink_block.content, global_vars=global_vars)
                if isinstance(sink_config, dict) and sink_config.get('dead_letter'):
                    # Only messages that fail go there, in an exported service; a run here
                    # stops on a failure instead.
                    self.logger.info(
                        f'Sink {sink_block.uuid} is the dead-letter sink: it is not written '
                        'here.',
                        **(getattr(self, 'logging_tags', None) or {}),
                    )
                    continue
                sinks_by_uuid[sink_block.uuid] = SinkFactory.get_sink(
                    sink_config,
                    buffer_path=os.path.join(
                        self.pipeline.pipeline_variables_dir,
                        'buffer',
                    ),
                )

        # Rust and R transformers: one program per block for the run (rust_sample).
        rust_programs: Dict = dict()
        # Keyed state and timers of the blocks with state_key, in memory for the run: they
        # start empty and are not saved, unlike a service's. Timers follow the time batches
        # arrive and fire with the next batch.
        states: Dict[str, Dict] = dict()
        block_timers: Dict[str, Dict] = dict()

        def block_state(block) -> Optional[Dict]:
            """The block's state, timers, the timers that fire now and the time, or None."""
            if not (block.configuration or {}).get('state_key'):
                return None
            timers = block_timers.setdefault(block.uuid, dict())
            now = int(time.time() * 1000)
            fired = {key: at for key, at in timers.items() if at <= now}
            for key in fired:
                del timers[key]
            return dict(state=states.setdefault(block.uuid, dict()), timers=timers,
                        fired=fired, watermark=now)

        def run_rust(block, data, variables: Dict):
            from mage_ai.streaming import rust_sample

            program = rust_programs.get(block.uuid)
            if program is None and block.language == BlockLanguage.R:
                program = rust_sample.RStreamProgram(
                    block, self.pipeline.uuid, variables, self.pipeline.repo_path,
                    float(block.timeout or 3600),
                )
                rust_programs[block.uuid] = program
            elif program is None:
                program = rust_sample.RustStreamProgram(
                    rust_sample.build_stream_program(block, self.pipeline.repo_path),
                    block.uuid, self.pipeline.uuid, variables, self.pipeline.repo_path,
                    rust_sample.program_env(block), float(block.timeout or 3600),
                    state_key=(block.configuration or {}).get('state_key'),
                )
                rust_programs[block.uuid] = program
            messages = data if isinstance(data, list) else [data]
            held = block_state(block)
            try:
                reply = program.run(
                    [rust_sample.encode(m) for m in messages],
                    **({} if held is None else {
                        name: value if name == 'watermark' else {block.uuid: value}
                        for name, value in held.items()
                    }),
                )
            except rust_sample.RustSampleError:
                rust_programs.pop(block.uuid, None)
                raise
            finally:
                for line in program.take_lines():
                    if not line.startswith('\x1emage-block '):
                        print(line)
            if not reply.get('ok'):
                error = reply.get('error') or {}
                raise Exception(f'{block.uuid}: {error.get("message")}')
            for kind, updates in (('state', 'state_updates'), ('timers', 'timer_updates')):
                changed = (reply.get(updates) or {}).get(block.uuid) or {}
                for key, value in changed.items():
                    if value is None:
                        held[kind].pop(key, None)
                    else:
                        held[kind][key] = value
            return [rust_sample.decode(m) for m in reply['messages']]

        def handle_batch_events_recursively(curr_block, outputs_by_block: Dict, **kwargs):
            curr_block_output = outputs_by_block[curr_block.uuid]
            downstream_blocks = curr_block.downstream_blocks
            for index, downstream_block in enumerate(downstream_blocks):
                # Each block but the last gets a copy, so that one that changes the
                # messages does not change those of the others. The last one gets them
                # as they are, in a list of its own: the source can read its list after
                # the handler, as RabbitMQ does to acknowledge the messages. Copying for
                # every block cost more than most transformers.
                if index < len(downstream_blocks) - 1:
                    data = copy_messages(curr_block_output)
                elif type(curr_block_output) is list:
                    data = list(curr_block_output)
                else:
                    data = curr_block_output
                if (downstream_block.type == BlockType.TRANSFORMER
                        and downstream_block.language in (BlockLanguage.RUST, BlockLanguage.R)):
                    outputs_by_block[downstream_block.uuid] = run_rust(
                        downstream_block, data, kwargs,
                    )
                elif downstream_block.type == BlockType.TRANSFORMER:
                    held = block_state(downstream_block)
                    execute_block_kwargs = dict(
                        global_vars=kwargs if held is None else dict(kwargs, **held),
                        input_args=[data],
                        logger=self.logger,
                    )
                    if build_block_output_stdout:
                        execute_block_kwargs['build_block_output_stdout'] = \
                            build_block_output_stdout
                    outputs_by_block[downstream_block.uuid] = \
                        downstream_block.execute_block(
                            **execute_block_kwargs,
                    )['output']
                elif downstream_block.type == BlockType.DATA_EXPORTER:
                    if downstream_block.uuid in sinks_by_uuid:
                        sinks_by_uuid[downstream_block.uuid].batch_write(data)
                if downstream_block.downstream_blocks:
                    handle_batch_events_recursively(
                        downstream_block,
                        outputs_by_block,
                        **kwargs,
                    )

        def handle_batch_events(messages: List[Union[Dict, str]], **kwargs):
            # Handle the events with DFS

            outputs_by_block = dict()
            outputs_by_block[self.source_block.uuid] = messages

            handle_batch_events_recursively(
                self.source_block,
                outputs_by_block,
                **merge_dict(global_vars, kwargs),
            )

        async def handle_event_async(message, **kwargs):
            outputs_by_block = dict()
            outputs_by_block[self.source_block.uuid] = [message]

            handle_batch_events_recursively(
                self.source_block,
                outputs_by_block,
                **merge_dict(global_vars, kwargs),
            )

        def handle_event(message, **kwargs):
            outputs_by_block = dict()
            outputs_by_block[self.source_block.uuid] = [message]

            handle_batch_events_recursively(
                self.source_block,
                outputs_by_block,
                **merge_dict(global_vars, kwargs),
            )

        # Long running method
        try:
            if source.consume_method == SourceConsumeMethod.BATCH_READ:
                source.batch_read(handler=handle_batch_events)
            elif source.consume_method == SourceConsumeMethod.READ_ASYNC:
                loop = asyncio.get_event_loop()
                if loop is not None:
                    loop.run_until_complete(source.read_async(handler=handle_event_async))
                else:
                    asyncio.run(source.read_async(handler=handle_event_async))
            elif source.consume_method == SourceConsumeMethod.READ:
                source.read(handler=handle_event)
        finally:
            source.destroy()
            for sink in sinks_by_uuid.values():
                sink.destroy()
            for program in rust_programs.values():
                program.close()

    @safe_db_query
    def __update_pipeline_run_status(
        self,
        pipeline_run_id: int,
        status: PipelineRun.PipelineRunStatus,
        error: Exception = None,
    ):
        if not pipeline_run_id or not status:
            return
        pipeline_run = PipelineRun.get_by_id(pipeline_run_id)
        pipeline_run.update(
            status=status,
            completed_at=datetime.now(tz=pytz.UTC),
        )
        if status == PipelineRun.PipelineRunStatus.FAILED:
            error_msg = None
            stacktrace = None
            if error is not None:
                error_msg = str(error)
                stacktrace = traceback.format_exc()
            notification_sender = self.pipeline.get_notification_sender()
            notification_sender.send_pipeline_run_failure_message(
                pipeline=self.pipeline,
                pipeline_run=pipeline_run,
                error=error_msg,
                stacktrace=stacktrace,
            )

    def __execute_in_flink(self):
        """
        TODO: Implement this method
        """
        pass

    def __interpolate_vars(self, content: str, global_vars: Dict = None):
        return streaming_config(content, global_vars)
