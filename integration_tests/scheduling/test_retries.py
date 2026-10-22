"""
Retrying a failed run, as the run page does (pipeline_run_action=retry_blocks): the failed
block and the ones after it run again, the completed ones before it keep their outputs;
from a block, that block and every block after it run again.
"""
import textwrap
import uuid
from pathlib import Path
from unittest.mock import patch

BLOCK = '''
from pathlib import Path


@{kind}
def step(*args, **kwargs):
    folder = Path(kwargs['out_dir'])
    folder.mkdir(parents=True, exist_ok=True)
    runs = folder / '{name}.runs'
    runs.write_text(runs.read_text() + 'x' if runs.exists() else 'x')
    if '{name}' == 'middle' and not (folder / 'fixed').exists():
        raise ValueError('the middle block fails until it is fixed')
    return [len(runs.read_text())]
'''


def drive(run, passes: int = 60):
    """The run's scheduler and the jobs it queues, in this process, until it finishes."""
    from mage_ai.orchestration import pipeline_scheduler_original
    from mage_ai.orchestration.db.models.schedules import PipelineRun
    from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler

    jobs = []
    job_manager = pipeline_scheduler_original.get_job_manager()
    with patch.object(job_manager, 'add_job', side_effect=lambda _type, _uid, target, *a, **k:
                      jobs.append((target, a, k))), \
            patch.object(job_manager, 'has_pipeline_run_job', return_value=False), \
            patch.object(job_manager, 'has_block_run_job', return_value=False), \
            patch.object(job_manager, 'kill_block_run_job'):
        scheduler = PipelineScheduler(run)
        if run.status == PipelineRun.PipelineRunStatus.INITIAL:
            scheduler.start(should_schedule=False)
        for _ in range(passes):
            scheduler.schedule()
            run.refresh()
            if run.status != PipelineRun.PipelineRunStatus.RUNNING:
                break
            while jobs:
                target, args, kwargs = jobs.pop(0)
                try:
                    target(*args, **kwargs)
                except Exception:
                    # A failed block is recorded on its block run; the next pass sees it.
                    pass
    run.refresh()
    return run


def runs_of(folder: Path):
    return {p.stem: len(p.read_text()) for p in folder.glob('*.runs')}


def test_a_failed_run_retried_and_retried_from_a_block(mage_project, tmp_path):
    from mage_ai.api.resources.PipelineRunResource import PipelineRunResource
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.orchestration.db.models.schedules import PipelineRun
    from mage_ai.orchestration.triggers.api import trigger_pipeline

    name = f'retry_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, repo_path=mage_project)
    upstream = []
    for step, kind in (('first', 'data_loader'), ('middle', 'transformer'),
                       ('last', 'data_exporter')):
        block = Block.create(f'{name}_{step}', kind, mage_project, pipeline=pipeline,
                             upstream_block_uuids=upstream)
        Path(block.file_path).write_text(textwrap.dedent(BLOCK.format(kind=kind, name=step)))
        upstream = [block.uuid]
    out = tmp_path / 'out'

    run = drive(trigger_pipeline(pipeline.uuid, variables=dict(out_dir=str(out))))
    assert run.status == PipelineRun.PipelineRunStatus.FAILED
    assert runs_of(out) == {'first': 1, 'middle': 1}

    # Fixed, then retried as the run page does: the failed block and the one after it.
    (out / 'fixed').write_text('')
    PipelineRunResource(run, None).update(dict(pipeline_run_action='retry_blocks'))
    run = drive(PipelineRun.query.get(run.id))
    assert run.status == PipelineRun.PipelineRunStatus.COMPLETED, \
        {b.block_uuid: b.status for b in run.block_runs}
    assert runs_of(out) == {'first': 1, 'middle': 2, 'last': 1}

    # From the first block: all of them again.
    PipelineRunResource(run, None).update(dict(
        pipeline_run_action='retry_blocks', from_block_uuid=f'{name}_first',
    ))
    run = drive(PipelineRun.query.get(run.id))
    assert run.status == PipelineRun.PipelineRunStatus.COMPLETED
    assert runs_of(out) == {'first': 2, 'middle': 3, 'last': 2}
