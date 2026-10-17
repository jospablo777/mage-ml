"""
Pipeline callbacks: callback blocks attached to no block run once when a pipeline run
completes or fails, with the run's status (upstream #5438). Callbacks attached to blocks run
for each of those blocks, so a failure of several blocks called them several times.

    @callback('success', 'failure')
    def notify(status, **kwargs):
        ...  # kwargs: pipeline_run, pipeline_uuid, failed_blocks, error

They run in a thread, so a slow callback does not hold up the scheduler, and once per run.
"""
import threading
from typing import Callable, List, Optional

from mage_ai.shared.hash import merge_dict

METRICS_KEY = 'pipeline_callbacks'


def pipeline_callback_blocks(pipeline) -> List:
    """The pipeline's callback blocks that are attached to no block."""
    return [
        block for block in (getattr(pipeline, 'callbacks_by_uuid', None) or {}).values()
        if not block.upstream_block_uuids
    ]


def _start(target: Callable) -> None:
    threading.Thread(target=target, name='mage-pipeline-callbacks', daemon=True).start()


def run_pipeline_callbacks(
    pipeline,
    pipeline_run,
    status: str,
    error: Optional[str] = None,
    logger=None,
    logging_tags: Optional[dict] = None,
) -> bool:
    """
    Starts the pipeline's callbacks for a run that ended with status, 'success' or
    'failure'. Returns whether they were started: not again for the same run.
    """
    blocks = pipeline_callback_blocks(pipeline)
    if not blocks:
        return False
    metrics = pipeline_run.metrics or {}
    if metrics.get(METRICS_KEY):
        return False
    pipeline_run.update(metrics=merge_dict(metrics, {METRICS_KEY: status}))

    pipeline_run_id = pipeline_run.id
    failed_blocks = [b.block_uuid for b in pipeline_run.failed_block_runs] \
        if status == 'failure' else []
    callback = 'on_success' if status == 'success' else 'on_failure'
    block_uuids = [block.uuid for block in blocks]

    def target():
        from mage_ai.orchestration.db.models.schedules import PipelineRun

        run = PipelineRun.get_by_id(pipeline_run_id)
        callback_kwargs = dict(
            error=error,
            failed_blocks=failed_blocks,
            pipeline_run=run,
            pipeline_uuid=pipeline.uuid,
            status=status,
        )
        for uuid in block_uuids:
            block = pipeline.callbacks_by_uuid.get(uuid)
            try:
                block.execute_callback(
                    callback,
                    callback_kwargs=callback_kwargs,
                    execution_partition=run.execution_partition if run else None,
                    global_vars=run.get_variables() if run else {},
                    logger=logger,
                    logging_tags=logging_tags,
                )
            except Exception as callback_error:
                message = f'Pipeline callback {uuid} failed: {callback_error}'
                if logger is not None:
                    logger.exception(message, **(logging_tags or {}))
                else:
                    print(message)

    _start(target)
    return True
