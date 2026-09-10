import asyncio
import traceback
from datetime import datetime
from typing import Dict, List

import pytz

from mage_ai.data_preparation.executors.block_executor import BlockExecutor
from mage_ai.data_preparation.logging.logger import DictLogger
from mage_ai.data_preparation.logging.logger_manager_factory import LoggerManagerFactory
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration.db.models.schedules import BlockRun, PipelineRun
from mage_ai.shared.hash import merge_dict
from mage_ai.usage_statistics.constants import EventNameType, EventObjectType
from mage_ai.usage_statistics.logger import UsageStatisticLogger


class PipelineExecutor:
    def __init__(self, pipeline: Pipeline, execution_partition: str = None):
        self.pipeline = pipeline
        self.execution_partition = execution_partition
        self.logger_manager = LoggerManagerFactory.get_logger_manager(
            pipeline_uuid=self.pipeline.uuid,
            partition=self.execution_partition,
            repo_config=self.pipeline.repo_config,
        )
        self.logger = DictLogger(self.logger_manager.logger)

    def cancel(self, **kwargs):
        pass

    def execute(
        self,
        allow_blocks_to_fail: bool = False,
        analyze_outputs: bool = False,
        global_vars: Dict = None,
        pipeline_run_id: int = None,
        run_sensors: bool = True,
        run_tests: bool = True,
        update_status: bool = False,
        **kwargs,
    ) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise RuntimeError('Use await PipelineExecutor.execute_async() inside an event loop.')
        return asyncio.run(self.execute_async(
            allow_blocks_to_fail=allow_blocks_to_fail,
            analyze_outputs=analyze_outputs,
            global_vars=global_vars,
            pipeline_run_id=pipeline_run_id,
            run_sensors=run_sensors,
            run_tests=run_tests,
            update_status=update_status,
            **kwargs,
        ))

    async def execute_async(
        self,
        allow_blocks_to_fail: bool = False,
        analyze_outputs: bool = False,
        global_vars: Dict = None,
        pipeline_run_id: int = None,
        run_sensors: bool = True,
        run_tests: bool = True,
        update_status: bool = False,
        **kwargs,
    ) -> None:
        try:
            if pipeline_run_id is None:
                await self.pipeline.execute(
                    analyze_outputs=analyze_outputs,
                    global_vars=global_vars,
                    run_sensors=run_sensors,
                    run_tests=run_tests,
                    update_status=update_status,
                )
            else:
                pipeline_run = PipelineRun.query.get(pipeline_run_id)
                if pipeline_run.status != PipelineRun.PipelineRunStatus.RUNNING:
                    return
                await self.__run_blocks(
                    pipeline_run,
                    allow_blocks_to_fail=allow_blocks_to_fail,
                    global_vars=global_vars,
                )
        finally:
            self.logger_manager.output_logs_to_destination()

    async def __run_blocks(
        self,
        pipeline_run: PipelineRun,
        allow_blocks_to_fail: bool = False,
        global_vars: Dict = None
    ):
        if global_vars is None:
            global_vars = dict()

        def create_block_task(
            block_run: BlockRun,
            block_run_outputs_cache: Dict[str, List],
        ) -> asyncio.Task:
            async def execute_block() -> None:
                pipeline_run.refresh()
                if pipeline_run.status != PipelineRun.PipelineRunStatus.RUNNING:
                    return
                executor_kwargs = dict(
                    pipeline=self.pipeline,
                    block_uuid=block_run.block_uuid,
                    execution_partition=self.execution_partition,
                )
                block_run_data = dict(status=BlockRun.BlockRunStatus.RUNNING)
                if not block_run.started_at or \
                        (block_run.metrics and not block_run.metrics.get('controller')):
                    block_run_data['started_at'] = datetime.now(tz=pytz.UTC)

                block_run.update(**block_run_data)

                try:
                    return BlockExecutor(block_run_id=block_run.id, **executor_kwargs).execute(
                        block_run_id=block_run.id,
                        block_run_outputs_cache=block_run_outputs_cache,
                        cache_block_output_in_memory=self.pipeline.cache_block_output_in_memory,
                        global_vars=global_vars,
                        pipeline_run_id=pipeline_run.id,
                        skip_logging=True,
                    )
                except Exception as error:
                    errors = traceback.format_stack()

                    await UsageStatisticLogger().error(
                        event_name=EventNameType.BLOCK_RUN_ERROR,
                        errors='\n'.join(errors or []),
                        message=str(error),
                        resource=EventObjectType.BLOCK_RUN,
                        resource_id=block_run.block_uuid,
                        resource_parent=EventObjectType.PIPELINE if self.pipeline else None,
                        resource_parent_id=self.pipeline.uuid if self.pipeline else None,
                    )

                    raise

            return asyncio.create_task(execute_block())

        block_run_outputs_cache = dict()

        while True:
            pipeline_run.refresh()
            if (
                pipeline_run.status != PipelineRun.PipelineRunStatus.RUNNING
                or pipeline_run.all_blocks_completed(allow_blocks_to_fail)
            ):
                return
            # Update the statuses of the block runs to CONDITION_FAILED or UPSTREAM_FAILED.
            pipeline_run.update_block_run_statuses(pipeline_run.initial_block_runs)
            executable_block_runs = pipeline_run.executable_block_runs(
                allow_blocks_to_fail=allow_blocks_to_fail,
            )
            if not executable_block_runs:
                return
            block_run_tasks = [
                create_block_task(b, block_run_outputs_cache=block_run_outputs_cache)
                for b in executable_block_runs]
            block_run_outputs = await asyncio.gather(
                *block_run_tasks, return_exceptions=allow_blocks_to_fail,
            )
            if self.pipeline.cache_block_output_in_memory:
                for idx, block_run in enumerate(executable_block_runs):
                    result = block_run_outputs[idx]
                    if isinstance(result, dict):
                        block_run_outputs_cache[block_run.block_uuid] = result.get('output', [])

    def build_tags(self, **kwargs):
        default_tags = dict(
            pipeline_uuid=self.pipeline.uuid,
        )
        if kwargs.get('pipeline_run_id'):
            default_tags['pipeline_run_id'] = kwargs.get('pipeline_run_id')
        return merge_dict(kwargs.get('tags', {}), default_tags)

    def _run_commands(
        self,
        global_vars: Dict = None,
        pipeline_run_id: int = None,
        **kwargs,
    ) -> List[str]:
        cmd = [
            '/app/run_app.sh',
            'mage',
            'run',
            self.pipeline.repo_config.repo_path,
            self.pipeline.uuid,
        ]
        options = [
            '--executor-type',
            'local_python',
        ]
        if self.execution_partition is not None:
            options += [
                '--execution-partition',
                f'{self.execution_partition}',
            ]
        if pipeline_run_id is not None:
            options += [
                '--pipeline-run-id',
                f'{pipeline_run_id}',
            ]
        return cmd + options
