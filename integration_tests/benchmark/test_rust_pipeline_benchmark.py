"""
The 5-block chain of test_fusion_benchmark.py (load, clean, enrich, aggregate, export)
written in Rust, run through the scheduler block by block and fused, each job in its own
process as on a worker. It measures what a Rust pipeline costs besides its blocks' work:
scheduling, processes, and passing outputs between blocks. The numbers are in
docs/development/rust-pipeline-optimization.md.

MAGE_BENCHMARK_ROWS sets the rows (1,000,000 by default). MAGE_BENCHMARK_RESULTS names a
JSON file for the timings.
"""
import json
import os
import shutil
import textwrap
from pathlib import Path

import pytest

from integration_tests.benchmark.test_fusion_benchmark import _timed_run

ROWS = int(os.getenv('MAGE_BENCHMARK_ROWS', '1000000'))

BLOCKS = [
    ('load', 'data_loader', '''
        use mage::prelude::*;

        /// Deterministic orders: the same rows on every run, without a random crate.
        fn load_data(vars: Vars) -> Result<DataFrame> {
            let rows: i64 = vars.get("rows")?;
            let mut state: u64 = 11;
            let mut next = move || {
                state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
                let mut z = state;
                z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                z ^ (z >> 31)
            };
            let regions = ["north", "south", "east", "west"];
            let n = rows as usize;
            let (mut customer, mut region, mut amount, mut units, mut at) = (
                Vec::with_capacity(n), Vec::with_capacity(n), Vec::with_capacity(n),
                Vec::with_capacity(n), Vec::with_capacity(n),
            );
            for _ in 0..n {
                customer.push((next() % (rows as u64 / 20 + 1)) as i64);
                region.push(regions[(next() % 4) as usize]);
                amount.push(((next() % 20_000) as f64) / 100.0);
                units.push((next() % 11 + 1) as i64);
                // Seconds into 2025, as microseconds since the epoch.
                at.push((1_735_689_600 + (next() % (365 * 24 * 3600)) as i64) * 1_000_000);
            }
            let frame = df!(
                "order_id" => (0..rows).collect::<Vec<i64>>(),
                "customer_id" => customer,
                "region" => region,
                "amount" => amount,
                "units" => units,
                "ordered_at" => at,
            )?;
            Ok(frame.lazy().with_column(
                col("ordered_at").cast(DataType::Datetime(TimeUnit::Microseconds, None)),
            ).collect()?)
        }
    '''),
    ('clean', 'transformer', '''
        use mage::prelude::*;

        fn transform(orders: LazyFrame) -> Result<LazyFrame> {
            Ok(orders.filter(col("amount").gt(lit(1.0)).and(col("units").gt(lit(0)))))
        }
    '''),
    ('enrich', 'transformer', '''
        use mage::prelude::*;

        fn transform(orders: LazyFrame) -> Result<LazyFrame> {
            Ok(orders.with_columns([
                (col("amount") * col("units").cast(DataType::Float64)).alias("revenue"),
                col("ordered_at").dt().month().cast(DataType::Int64).alias("month"),
            ]))
        }
    '''),
    ('aggregate', 'transformer', '''
        use mage::prelude::*;

        fn transform(orders: LazyFrame) -> Result<LazyFrame> {
            Ok(orders
                .group_by([col("region"), col("month")])
                .agg([col("revenue").sum().alias("revenue"), len().alias("orders")])
                .sort(["region", "month"], SortMultipleOptions::default()))
        }
    '''),
    ('export', 'data_exporter', '''
        use mage::prelude::*;

        fn export_data(mut summary: DataFrame, vars: Vars) -> Result<()> {
            let path: String = vars.get("output_path")?;
            ParquetWriter::new(std::fs::File::create(path)?).finish(&mut summary)?;
            Ok(())
        }
    '''),
]


@pytest.fixture(scope='module')
def chain(mage_project):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = Pipeline.create('rust_benchmark', repo_path=mage_project)
    upstream = []
    for name, kind, code in BLOCKS:
        block = Block.create(f'rust_benchmark_{name}', kind, mage_project, language='rust')
        Path(block.file_path).write_text(textwrap.dedent(code).strip() + '\n')
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        upstream = [block]
    yield Pipeline.get(pipeline.uuid, repo_path=mage_project)
    pipeline.delete()


def test_rust_chain_block_by_block_and_fused(chain, tmp_path):
    from mage_ai.orchestration import fusion, fusion_verify
    from mage_ai.orchestration.db.models.schedules import PipelineRun
    from mage_ai.shared.dates import utc_now

    variables = dict(output_path=str(tmp_path / 'summary.parquet'), rows=ROWS)
    execution_date = utc_now()
    # Compiles every block once, so neither timed run pays for it.
    warm, _, _ = _timed_run(chain, fusion.BLOCK_FUSION_CHAINS, variables, execution_date)
    assert warm.status == PipelineRun.PipelineRunStatus.COMPLETED, warm.id

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
    )
    print(
        f'\n{ROWS:,} rows, Rust: block by block {unfused_seconds:.1f} s '
        f'(blocks {sum(unfused_blocks.values()):.1f} s), fused {fused_seconds:.1f} s '
        f'(blocks {sum(fused_blocks.values()):.1f} s)',
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


@pytest.fixture(autouse=True)
def _rust_installed():
    if shutil.which('cargo') is None:
        pytest.skip('Rust is not installed')
