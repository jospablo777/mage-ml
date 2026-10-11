"""
Compiled chains in Mage's pipeline runs (block/plans.py): in a fused stage, consecutive
Rust blocks run as one compiled process and consecutive R blocks in one R session. Each
pipeline runs block by block and fused through the scheduler, each job in its own
process; every block must give the same output both ways.
"""
import os
import shutil
import textwrap
import uuid
from pathlib import Path

import pytest

RUST_LOAD = '''
use mage::prelude::*;

fn load_data() -> Result<DataFrame> {
    Ok(df!("id" => (0..1000i64).collect::<Vec<_>>(),
           "amount" => (0..1000).map(|i| (i % 17) as f64 * 1.5).collect::<Vec<_>>())?)
}
'''
RUST_FILTER = '''
use mage::prelude::*;

fn transform(rows: LazyFrame) -> Result<LazyFrame> {
    Ok(rows.filter(col("amount").gt(lit(3.0))))
}
'''
RUST_DOUBLE = '''
use mage::prelude::*;

fn transform(rows: LazyFrame) -> Result<LazyFrame> {
    Ok(rows.with_column((col("amount") * lit(2.0)).alias("doubled")))
}
'''
RUST_TOTAL = '''
use mage::prelude::*;

fn transform(rows: LazyFrame) -> Result<LazyFrame> {
    Ok(rows.select([col("doubled").sum().alias("total"), len().alias("rows")]))
}
'''
RUST_BROKEN = '''
use mage::prelude::*;

fn transform(rows: LazyFrame) -> Result<LazyFrame> {
    Ok(rows.with_column(col("no_such_column").alias("doubled")))
}
'''


def pipeline_of(project: str, blocks):
    """blocks: (name, type, language, code); a chain in that order."""
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    suffix = uuid.uuid4().hex[:8]
    pipeline = Pipeline.create(f'chain_{suffix}', repo_path=project)
    upstream = []
    for name, block_type, language, code in blocks:
        block = Block.create(f'{name}_{suffix}', block_type, project, language=language)
        Path(block.file_path).write_text(textwrap.dedent(code).strip() + '\n')
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        upstream = [block]
    return Pipeline.get(pipeline.uuid, repo_path=project)


def run(pipeline, mode):
    from mage_ai.orchestration import fusion_verify
    from mage_ai.shared.dates import utc_now

    return fusion_verify.run_in_process(
        fusion_verify._create_run(pipeline, mode, {}, utc_now()), log=lambda text: None,
    )


def stored(pipeline, block_uuid, pipeline_run):
    return pipeline.variable_manager.get_variable(
        pipeline.uuid, block_uuid, 'output_0', partition=pipeline_run.execution_partition,
    )


def statuses(pipeline_run):
    return {
        b.block_uuid.rsplit('_', 1)[0]: getattr(b.status, 'value', b.status)
        for b in pipeline_run.block_runs
    }


def assert_same_both_ways(pipeline):
    from mage_ai.orchestration import fusion, fusion_verify

    unfused = run(pipeline, fusion.BLOCK_FUSION_OFF)
    fused = run(pipeline, fusion.BLOCK_FUSION_CHAINS)
    results = fusion_verify.compare_runs(pipeline, unfused, fused)
    assert [r for r in results if r.result not in ('same', 'close')] == [], results
    return fused


@pytest.fixture(autouse=True)
def _rust_installed():
    if shutil.which('cargo') is None:
        pytest.skip('Rust is not installed')


def test_a_rust_chain_runs_compiled_with_the_outputs_of_block_by_block(mage_project):
    from mage_ai.data_preparation.models.block import plans

    pipeline = pipeline_of(mage_project, [
        ('load', 'data_loader', 'rust', RUST_LOAD),
        ('filter', 'transformer', 'rust', RUST_FILTER),
        ('double', 'transformer', 'rust', RUST_DOUBLE),
        ('total', 'transformer', 'rust', RUST_TOTAL),
    ])
    fused = assert_same_both_ways(pipeline)
    uuids = [b.uuid for b in pipeline.blocks_by_uuid.values()]
    # The first three were planned; the last computed the chain.
    for block_uuid in uuids[:3]:
        assert plans.is_plan(stored(pipeline, block_uuid, fused)), block_uuid
    total = stored(pipeline, uuids[3], fused)
    assert total.to_dicts() == [dict(total=total['total'][0], rows=total['rows'][0])]
    assert not plans.is_plan(total)
    # A stored plan can still be run, as a later reader would.
    resolved = plans.resolve(stored(pipeline, uuids[2], fused), pipeline.get_block(uuids[2]),
                             fused.execution_partition)
    assert 'doubled' in resolved.columns and resolved.height < 1000


def test_a_failed_planned_block_is_the_one_marked_failed(mage_project):
    from mage_ai.orchestration import fusion

    pipeline = pipeline_of(mage_project, [
        ('load', 'data_loader', 'rust', RUST_LOAD),
        ('broken', 'transformer', 'rust', RUST_BROKEN),
        ('total', 'transformer', 'rust', RUST_TOTAL),
    ])
    fused = run(pipeline, fusion.BLOCK_FUSION_CHAINS)
    state = statuses(fused)
    assert state['load'] == 'completed', state
    assert state['broken'] == 'failed', state
    assert state['total'] != 'completed', state
    broken = next(b for b in fused.block_runs if b.block_uuid.startswith('broken_'))
    assert 'no_such_column' in (broken.metrics or {}).get('error', {}).get('message', '')


def test_blocks_whose_output_is_needed_are_not_planned(mage_project):
    from mage_ai.data_preparation.models.block import plans
    from mage_ai.orchestration import fusion

    tested = RUST_FILTER + '''
fn test_rows(output: &DataFrame) -> Result<()> {
    ensure!(output.height() > 0, "no rows");
    Ok(())
}
'''
    pipeline = pipeline_of(mage_project, [
        ('load', 'data_loader', 'rust', RUST_LOAD),
        ('filter', 'transformer', 'rust', tested),
        ('double', 'transformer', 'rust', RUST_DOUBLE),
        ('total', 'transformer', 'rust', RUST_TOTAL),
    ])
    fused = run(pipeline, fusion.BLOCK_FUSION_CHAINS)
    uuids = [b.uuid for b in pipeline.blocks_by_uuid.values()]
    assert plans.is_plan(stored(pipeline, uuids[0], fused))
    assert not plans.is_plan(stored(pipeline, uuids[1], fused)), 'a block with tests runs'
    assert plans.is_plan(stored(pipeline, uuids[2], fused))

    pipeline.keep_block_outputs = True
    pipeline.save()
    kept = run(pipeline, fusion.BLOCK_FUSION_CHAINS)
    assert not any(plans.is_plan(stored(pipeline, u, kept)) for u in uuids)


def test_a_polyglot_pipeline_plans_its_rust_chain(mage_project):
    from mage_ai.data_preparation.models.block import plans

    pipeline = pipeline_of(mage_project, [
        ('pandas_load', 'data_loader', 'python', '''
            import pandas as pd

            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({'id': range(200), 'amount': [i % 9 * 2.0 for i in range(200)]})
        '''),
        ('filter', 'transformer', 'rust', RUST_FILTER),
        ('double', 'transformer', 'rust', RUST_DOUBLE),
        ('pandas_total', 'transformer', 'python', '''
            @transformer
            def total(rows, *args, **kwargs):
                frame = rows.to_pandas() if hasattr(rows, 'to_pandas') else rows
                return frame[['doubled']].sum().to_frame('total').reset_index()
        '''),
    ])
    fused = assert_same_both_ways(pipeline)
    uuids = [b.uuid for b in pipeline.blocks_by_uuid.values()]
    assert plans.is_plan(stored(pipeline, uuids[1], fused))
    assert not plans.is_plan(stored(pipeline, uuids[2], fused))


@pytest.fixture
def r_environment(monkeypatch, tmp_path):
    path = os.getenv('MAGE_TEST_R_PROJECT_DIR')
    if not path or shutil.which('Rscript') is None:
        pytest.skip('R is not configured: MAGE_TEST_R_PROJECT_DIR unset')
    monkeypatch.setenv('MAGE_R_PROJECT_DIR', path)
    monkeypatch.setenv('MAGE_R_CACHE_DIR', str(tmp_path / 'mageml'))


def test_an_r_chain_runs_in_one_session_with_the_outputs_of_block_by_block(
    mage_project, r_environment,
):
    from mage_ai.data_preparation.models.block import plans

    pipeline = pipeline_of(mage_project, [
        ('load', 'data_loader', 'r', '''
            #* @data_loader
            load <- function(...) {
              tibble::tibble(id = 1:300, units = bit64::as.integer64(1:300 %% 7))
            }
        '''),
        ('scale', 'transformer', 'r', '''
            #* @transformer
            scale <- function(df_1, ...) {
              dplyr::mutate(df_1, value = units * 1.5)
            }
        '''),
        ('total', 'transformer', 'r', '''
            #* @transformer
            total <- function(df_1, ...) {
              dplyr::summarise(df_1, total = sum(value), rows = dplyr::n())
            }
        '''),
    ])
    fused = assert_same_both_ways(pipeline)
    uuids = [b.uuid for b in pipeline.blocks_by_uuid.values()]
    assert plans.is_plan(stored(pipeline, uuids[0], fused))
    assert plans.is_plan(stored(pipeline, uuids[1], fused))
    total = stored(pipeline, uuids[2], fused)
    assert float(total['total'].iloc[0]) == pytest.approx(1.5 * sum(i % 7 for i in range(1, 301)))
