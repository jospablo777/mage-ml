"""
Block fusion against block by block, on a 5-block pandas chain (load, clean, enrich,
aggregate, export). Both runs go through the scheduler with each job in its own process,
as on a worker. The test fails when the fused run stops being faster, or when the two runs
store different outputs.

MAGE_BENCHMARK_ROWS sets the rows (1,000,000 by default; the numbers in
docs/development/block-fusion.md are from 3,000,000). MAGE_BENCHMARK_RESULTS names a JSON
file for the timings.
"""
import json
import os
import textwrap
import time
from pathlib import Path

import pytest

ROWS = int(os.getenv('MAGE_BENCHMARK_ROWS', '1000000'))
# The fused run must take at most this share of the block by block run.
MAX_SHARE = float(os.getenv('MAGE_BENCHMARK_MAX_SHARE', '0.75'))

BLOCKS = [
    ('load', 'data_loader', '''
        import numpy as np
        import pandas as pd

        @data_loader
        def load(*args, **kwargs):
            rng = np.random.default_rng(11)
            rows = int(kwargs['rows'])
            return pd.DataFrame({
                'order_id': np.arange(rows, dtype='int64'),
                'customer_id': rng.integers(0, rows // 20 + 1, rows),
                'region': pd.Categorical(rng.choice(['north', 'south', 'east', 'west'], rows)),
                'amount': rng.gamma(2.0, 40.0, rows).round(2),
                'units': rng.integers(1, 12, rows),
                'ordered_at': pd.Timestamp('2025-01-01')
                + pd.to_timedelta(rng.integers(0, 365 * 24 * 3600, rows), unit='s'),
            })
    '''),
    ('clean', 'transformer', '''
        @transformer
        def clean(frame, *args, **kwargs):
            frame = frame[(frame['amount'] > 1) & (frame['units'] > 0)]
            return frame.reset_index(drop=True)
    '''),
    ('enrich', 'transformer', '''
        @transformer
        def enrich(frame, *args, **kwargs):
            return frame.assign(
                revenue=frame['amount'] * frame['units'],
                month=frame['ordered_at'].dt.month.astype('int64'),
            )
    '''),
    ('aggregate', 'transformer', '''
        @transformer
        def aggregate(frame, *args, **kwargs):
            return (
                frame.groupby(['region', 'month'], observed=True)
                .agg(revenue=('revenue', 'sum'), orders=('order_id', 'count'))
                .reset_index()
            )
    '''),
    ('export', 'data_exporter', '''
        @data_exporter
        def export(frame, *args, **kwargs):
            frame.to_parquet(kwargs['output_path'], index=False)
    '''),
]


@pytest.fixture(scope='module')
def chain(mage_project):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = Pipeline.create('fusion_benchmark', repo_path=mage_project)
    upstream = []
    for name, kind, code in BLOCKS:
        block = Block.create(
            f'benchmark_{name}', kind, mage_project, language='python',
        )
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        upstream = [block]
    yield Pipeline.get(pipeline.uuid, repo_path=mage_project)
    pipeline.delete()


def _timed_run(pipeline, mode, variables, execution_date):
    from mage_ai.orchestration import fusion_verify

    run = fusion_verify._create_run(pipeline, mode, variables, execution_date)
    started = time.perf_counter()
    run = fusion_verify.run_in_process(run, log=lambda text: None)
    seconds = time.perf_counter() - started
    blocks = {
        b.block_uuid: round((b.completed_at - b.started_at).total_seconds(), 2)
        for b in run.block_runs if b.completed_at and b.started_at
    }
    return run, seconds, blocks


def test_fused_runs_are_faster_with_the_same_outputs(chain, tmp_path):
    from mage_ai.orchestration import fusion, fusion_verify
    from mage_ai.orchestration.db.models.schedules import PipelineRun
    from mage_ai.shared.dates import utc_now

    variables = dict(output_path=str(tmp_path / 'summary.parquet'), rows=ROWS)
    execution_date = utc_now()
    # The first run pays for cold file caches; the fused run goes first, so the
    # comparison does not favor it.
    fused, fused_seconds, fused_blocks = _timed_run(
        chain, fusion.BLOCK_FUSION_CHAINS, variables, execution_date,
    )
    unfused, unfused_seconds, unfused_blocks = _timed_run(
        chain, fusion.BLOCK_FUSION_OFF, variables, execution_date,
    )

    results = dict(
        block_by_block=dict(blocks=unfused_blocks, seconds=unfused_seconds),
        fused=dict(blocks=fused_blocks, seconds=fused_seconds),
        rows=ROWS,
        share=fused_seconds / unfused_seconds,
    )
    print(
        f'\n{ROWS:,} rows: block by block {unfused_seconds:.1f} s '
        f'(blocks {sum(unfused_blocks.values()):.1f} s), fused {fused_seconds:.1f} s '
        f'(blocks {sum(fused_blocks.values()):.1f} s), {results["share"]:.0%} of the time',
    )
    for uuid in unfused_blocks:
        print(f'  {uuid}: {unfused_blocks[uuid]} s, fused {fused_blocks.get(uuid)} s')
    if os.getenv('MAGE_BENCHMARK_RESULTS'):
        Path(os.environ['MAGE_BENCHMARK_RESULTS']).write_text(json.dumps(results, indent=2))

    for run in (unfused, fused):
        assert run.status == PipelineRun.PipelineRunStatus.COMPLETED, run.id
    differences = [
        b for b in fusion_verify.compare_runs(chain, unfused, fused)
        if b.result not in ('same', 'close')
    ]
    assert not differences, differences
    assert fused_seconds <= unfused_seconds * MAX_SHARE, results
