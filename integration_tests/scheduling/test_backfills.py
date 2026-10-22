"""
A backfill run by MageML's scheduler: one run per date, each with its date's variables
(`ds`, `execution_date`, the interval), and the backfill completed once they are.
"""
import json
import textwrap
import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytz

BLOCK = '''
import json
from pathlib import Path


@data_loader
def record(*args, **kwargs):
    folder = Path(kwargs['out_dir'])
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{kwargs['ds']}.json").write_text(json.dumps({
        key: str(kwargs.get(key)) for key in (
            'ds', 'execution_date', 'interval_start_datetime', 'interval_end_datetime',
            'interval_seconds',
        )
    }))
    return [kwargs['ds']]
'''


def run_scheduler(until, passes: int = 60):
    """schedule_all and the jobs it queues, in this process, until `until()` is true."""
    from mage_ai.orchestration import pipeline_scheduler_original
    from mage_ai.orchestration.pipeline_scheduler_original import schedule_all

    jobs = []
    job_manager = pipeline_scheduler_original.get_job_manager()
    with patch.object(job_manager, 'add_job', side_effect=lambda _type, _uid, target, *a, **k:
                      jobs.append((target, a, k))), \
            patch.object(job_manager, 'has_pipeline_run_job', return_value=False), \
            patch.object(job_manager, 'has_block_run_job', return_value=False):
        for _ in range(passes):
            if until():
                return True
            schedule_all()
            while jobs:
                target, args, kwargs = jobs.pop(0)
                target(*args, **kwargs)
    return until()


def test_a_backfill_runs_each_date_with_its_variables(mage_project, tmp_path):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.orchestration.backfills.service import start_backfill
    from mage_ai.orchestration.db.models.schedules import Backfill, PipelineRun

    name = f'backfill_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, repo_path=mage_project)
    block = Block.create(f'{name}_record', 'data_loader', mage_project, pipeline=pipeline)
    Path(block.file_path).write_text(textwrap.dedent(BLOCK))
    out = tmp_path / 'seen'
    backfill = Backfill.create(
        name=name, pipeline_uuid=pipeline.uuid,
        start_datetime=datetime(2026, 1, 1, tzinfo=pytz.UTC),
        end_datetime=datetime(2026, 1, 3, tzinfo=pytz.UTC),
        interval_type=Backfill.IntervalType.DAY, interval_units=1,
        variables=dict(out_dir=str(out)),
    )
    runs = start_backfill(backfill)
    backfill.update(status=Backfill.Status.INITIAL)
    assert len(runs) == 3

    def finished():
        statuses = {r.status for r in PipelineRun.query.filter(
            PipelineRun.backfill_id == backfill.id,
        )}
        return statuses == {PipelineRun.PipelineRunStatus.COMPLETED}

    assert run_scheduler(finished), [
        (r.id, r.status) for r in PipelineRun.query.filter(PipelineRun.backfill_id == backfill.id)
    ]
    seen = {p.stem: json.loads(p.read_text()) for p in out.glob('*.json')}
    assert sorted(seen) == ['2026-01-01', '2026-01-02', '2026-01-03']
    assert seen['2026-01-02']['interval_seconds'] == '86400.0'
    assert seen['2026-01-02']['interval_start_datetime'].startswith('2026-01-02')
    assert seen['2026-01-02']['interval_end_datetime'].startswith('2026-01-03')
    assert seen['2026-01-03']['execution_date'].startswith('2026-01-03')

    run_scheduler(lambda: Backfill.query.get(backfill.id).status == Backfill.Status.COMPLETED)
    assert Backfill.query.get(backfill.id).status == Backfill.Status.COMPLETED
