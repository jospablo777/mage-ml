"""
Trigger rules (upstream #4704), run by Mage's scheduler with a process per block, in one
process, and by `mage run`: a cleanup block (all_done) runs after a failed block and gets
None from it; a join (none_failed_min_one_success) runs after a branch that was skipped
(a disabled block) and gets None from it. The failed block still fails the run.
"""
import json
import textwrap
import uuid
from pathlib import Path

import pytest

from integration_tests.scheduling.test_retries import drive

LOAD = '''
@data_loader
def load(*args, **kwargs):
    return dict(rows=3)
'''

FAIL = '''
@transformer
def fail(data, *args, **kwargs):
    raise ValueError('this block fails')
'''

PASS = '''
@transformer
def step(data, *args, **kwargs):
    return dict(rows=data['rows'] + 1)
'''

RECORD = '''
import json
from pathlib import Path


@data_exporter
def record(*inputs, **kwargs):
    Path(kwargs['out_dir'], '{name}.json').write_text(json.dumps(inputs))
'''


def build(project, *, one_process=False):
    """
    load -> fail -> after_fail
    load, fail -> cleanup (all_done)
    load -> left (disabled), load -> right; left, right -> join (none_failed_min_one_success)
    """
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'rules_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, repo_path=project)
    blocks = {}

    def add(step, kind, content, upstream=(), configuration=None):
        block = Block.create(
            f'{name}_{step}', kind, project, pipeline=pipeline,
            upstream_block_uuids=[blocks[u].uuid for u in upstream],
        )
        Path(block.file_path).write_text(textwrap.dedent(content))
        if configuration:
            pipeline.get_block(block.uuid).update(dict(configuration=configuration))
        blocks[step] = block

    add('load', 'data_loader', LOAD)
    add('fail', 'transformer', FAIL, ['load'])
    add('after_fail', 'transformer', PASS, ['fail'])
    add('cleanup', 'data_exporter', RECORD.format(name='cleanup'), ['fail', 'load'],
        dict(trigger_rule='all_done'))
    add('left', 'transformer', PASS, ['load'], dict(disabled=True))
    add('right', 'transformer', PASS, ['load'])
    add('join', 'data_exporter', RECORD.format(name='join'), ['left', 'right'],
        dict(trigger_rule='none_failed_min_one_success'))
    if one_process:
        pipeline = Pipeline.get(pipeline.uuid, repo_path=project)
        pipeline.run_pipeline_in_one_process = True
        pipeline.save()
    return name, Pipeline.get(pipeline.uuid, repo_path=project)


def check_outputs(out: Path):
    assert json.loads((out / 'cleanup.json').read_text()) == [None, dict(rows=3)]
    assert json.loads((out / 'join.json').read_text()) == [None, dict(rows=4)]


@pytest.mark.parametrize('one_process', [False, True], ids=['block_processes', 'one_process'])
def test_a_scheduled_run(mage_project, tmp_path, one_process):
    from mage_ai.orchestration.db.models.schedules import PipelineRun
    from mage_ai.orchestration.triggers.api import trigger_pipeline

    name, pipeline = build(mage_project, one_process=one_process)
    out = tmp_path / 'out'
    out.mkdir()

    run = drive(trigger_pipeline(pipeline.uuid, variables=dict(out_dir=str(out))))

    statuses = {b.block_uuid.removeprefix(f'{name}_'): b.status.value for b in run.block_runs}
    assert statuses == dict(
        load='completed', fail='failed', after_fail='upstream_failed', cleanup='completed',
        left='condition_failed', right='completed', join='completed',
    )
    assert run.status == PipelineRun.PipelineRunStatus.FAILED
    check_outputs(out)


def test_mage_run(mage_project, tmp_path, capsys):
    from mage_ai.data_preparation.executors.executor_factory import ExecutorFactory

    name, pipeline = build(mage_project)
    out = tmp_path / 'out'
    out.mkdir()

    with pytest.raises(ValueError, match='this block fails'):
        ExecutorFactory.get_pipeline_executor(pipeline).execute(
            global_vars=dict(out_dir=str(out)), update_status=False,
        )

    check_outputs(out)
    summary = capsys.readouterr().out
    assert f'{name}_after_fail' in summary and 'not run: upstream failed' in summary
    assert 'is disabled: it and the blocks after it are skipped' in summary
