"""
The workload every benchmarked pipeline runs, written once per language:

1. load: read orders.parquet;
2. clean: drop orders without an amount, with an amount of 1 or less, or without units;
3. enrich: revenue = amount * units, the order's month (UTC), and the customer's segment
   from customers.parquet (a left join);
4. aggregate: revenue, order count and mean amount per region, segment and month;
5. export: write the summary to output_path as Parquet.

The input files are generated once per size with a fixed seed, so every pipeline reads
the same bytes. Every pipeline must write the same summary (checked after each run).
"""
import textwrap
from pathlib import Path
from typing import Dict, List, Tuple

STEPS = ('load', 'clean', 'enrich', 'aggregate', 'export')
STEP_TYPES = {
    'load': 'data_loader',
    'clean': 'transformer',
    'enrich': 'transformer',
    'aggregate': 'transformer',
    'export': 'data_exporter',
}
KEYS = ['region', 'segment', 'month']
REGIONS = ['north', 'south', 'east', 'west']
SEGMENTS = ['consumer', 'business', 'enterprise']


def write_inputs(directory: Path, rows: int, seed: int = 11) -> Dict[str, str]:
    """orders.parquet and customers.parquet with `rows` orders; kept when present."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    directory.mkdir(parents=True, exist_ok=True)
    orders_path = directory / 'orders.parquet'
    customers_path = directory / 'customers.parquet'
    if not (orders_path.is_file() and customers_path.is_file()):
        rng = np.random.default_rng(seed)
        customers = max(rows // 20, 1)
        amount = rng.gamma(2.0, 40.0, rows).round(2)
        missing = rng.random(rows) < 0.01
        start = np.datetime64('2025-01-01T00:00:00', 'us')
        offsets = rng.integers(0, 365 * 24 * 3600, rows).astype('timedelta64[s]')
        table = pa.table({
            'order_id': pa.array(np.arange(rows, dtype='int64')),
            'customer_id': pa.array(rng.integers(0, customers, rows)),
            'region': pa.array(np.array(REGIONS)[rng.integers(0, 4, rows)]),
            'amount': pa.array(amount, mask=missing),
            'units': pa.array(rng.integers(0, 12, rows)),
            'ordered_at': pa.array(start + offsets, pa.timestamp('us', tz='UTC')),
        })
        pq.write_table(table, orders_path)
        pq.write_table(pa.table({
            'customer_id': pa.array(np.arange(customers, dtype='int64')),
            'segment': pa.array(np.array(SEGMENTS)[rng.integers(0, 3, customers)]),
            'signup_year': pa.array(rng.integers(2015, 2026, customers)),
        }), customers_path)
    return dict(orders_path=str(orders_path), customers_path=str(customers_path))


PANDAS = {
    'load': '''
        import pandas as pd


        @data_loader
        def load(*args, **kwargs):
            return pd.read_parquet(kwargs['orders_path'])
    ''',
    'clean': '''
        import pandas as pd


        @transformer
        def clean(orders, *args, **kwargs):
            if not isinstance(orders, pd.DataFrame):
                # A Rust upstream block returns a Polars DataFrame.
                orders = orders.to_pandas()
            kept = orders[(orders['amount'] > 1) & (orders['units'] > 0)]
            return kept.reset_index(drop=True)
    ''',
    'enrich': '''
        import pandas as pd


        @transformer
        def enrich(orders, *args, **kwargs):
            if not isinstance(orders, pd.DataFrame):
                orders = orders.to_pandas()
            customers = pd.read_parquet(
                kwargs['customers_path'], columns=['customer_id', 'segment'],
            )
            orders = orders.assign(
                revenue=orders['amount'] * orders['units'],
                month=orders['ordered_at'].dt.month.astype('int64'),
            )
            return orders.merge(customers, on='customer_id', how='left')
    ''',
    'aggregate': '''
        import pandas as pd


        @transformer
        def aggregate(orders, *args, **kwargs):
            if not isinstance(orders, pd.DataFrame):
                orders = orders.to_pandas()
            summary = (
                orders.groupby(['region', 'segment', 'month'], observed=True)
                .agg(
                    revenue=('revenue', 'sum'),
                    orders=('order_id', 'count'),
                    avg_amount=('amount', 'mean'),
                )
                .reset_index()
            )
            summary['orders'] = summary['orders'].astype('int64')
            return summary.sort_values(['region', 'segment', 'month']).reset_index(drop=True)
    ''',
    'export': '''
        import pandas as pd


        @data_exporter
        def export(summary, *args, **kwargs):
            if not isinstance(summary, pd.DataFrame):
                summary = summary.to_pandas()
            summary.to_parquet(kwargs['output_path'], index=False)
    ''',
}

POLARS = {
    'load': '''
        import polars as pl


        @data_loader
        def load(*args, **kwargs):
            return pl.read_parquet(kwargs['orders_path'])
    ''',
    'clean': '''
        import polars as pl


        @transformer
        def clean(orders, *args, **kwargs):
            return orders.filter((pl.col('amount') > 1) & (pl.col('units') > 0))
    ''',
    'enrich': '''
        import polars as pl


        @transformer
        def enrich(orders, *args, **kwargs):
            customers = pl.read_parquet(
                kwargs['customers_path'], columns=['customer_id', 'segment'],
            )
            return orders.with_columns(
                revenue=pl.col('amount') * pl.col('units'),
                month=pl.col('ordered_at').dt.month().cast(pl.Int64),
            ).join(customers, on='customer_id', how='left')
    ''',
    'aggregate': '''
        import polars as pl


        @transformer
        def aggregate(orders, *args, **kwargs):
            return (
                orders.group_by(['region', 'segment', 'month'])
                .agg(
                    revenue=pl.col('revenue').sum(),
                    orders=pl.len().cast(pl.Int64),
                    avg_amount=pl.col('amount').mean(),
                )
                .sort(['region', 'segment', 'month'])
            )
    ''',
    'export': '''
        @data_exporter
        def export(summary, *args, **kwargs):
            summary.write_parquet(kwargs['output_path'])
    ''',
}

R = {
    'load': '''
        #* @data_loader
        load <- function(...) {
          arrow::read_parquet(variable("orders_path"))
        }
    ''',
    'clean': '''
        suppressPackageStartupMessages(library(dplyr))

        #* @transformer
        clean <- function(df_1, ...) {
          df_1 |> filter(amount > 1, units > 0)
        }
    ''',
    'enrich': '''
        suppressPackageStartupMessages(library(dplyr))

        #* @transformer
        enrich <- function(df_1, ...) {
          customers <- arrow::read_parquet(
            variable("customers_path"), col_select = c("customer_id", "segment")
          )
          df_1 |>
            mutate(
              revenue = amount * units,
              month = as.integer(lubridate::month(ordered_at))
            ) |>
            left_join(customers, by = "customer_id")
        }
    ''',
    'aggregate': '''
        suppressPackageStartupMessages(library(dplyr))

        #* @transformer
        aggregate <- function(df_1, ...) {
          df_1 |>
            group_by(region, segment, month) |>
            summarise(
              revenue = sum(revenue),
              orders = n(),
              avg_amount = mean(amount),
              .groups = "drop"
            ) |>
            arrange(region, segment, month)
        }
    ''',
    'export': '''
        #* @data_exporter
        export <- function(df_1, ...) {
          arrow::write_parquet(df_1, variable("output_path"))
        }
    ''',
}

RUST = {
    'load': '''
        use mage::prelude::*;

        fn load_data(vars: Vars) -> Result<LazyFrame> {
            let path: String = vars.get("orders_path")?;
            Ok(LazyFrame::scan_parquet(
                PlRefPath::try_from_path(std::path::Path::new(&path))?,
                Default::default(),
            )?)
        }
    ''',
    'clean': '''
        use mage::prelude::*;

        fn transform(orders: LazyFrame) -> Result<LazyFrame> {
            Ok(orders.filter(col("amount").gt(lit(1.0)).and(col("units").gt(lit(0)))))
        }
    ''',
    'enrich': '''
        use mage::prelude::*;

        fn transform(orders: LazyFrame, vars: Vars) -> Result<LazyFrame> {
            let path: String = vars.get("customers_path")?;
            let customers = LazyFrame::scan_parquet(
                PlRefPath::try_from_path(std::path::Path::new(&path))?,
                Default::default(),
            )?
            .select([col("customer_id"), col("segment")]);
            Ok(orders
                .with_columns([
                    (col("amount") * col("units").cast(DataType::Float64)).alias("revenue"),
                    col("ordered_at").dt().month().cast(DataType::Int64).alias("month"),
                ])
                .left_join(customers, col("customer_id"), col("customer_id")))
        }
    ''',
    'aggregate': '''
        use mage::prelude::*;

        fn transform(orders: LazyFrame) -> Result<LazyFrame> {
            Ok(orders
                .group_by([col("region"), col("segment"), col("month")])
                .agg([
                    col("revenue").sum().alias("revenue"),
                    len().cast(DataType::Int64).alias("orders"),
                    col("amount").mean().alias("avg_amount"),
                ])
                .sort(["region", "segment", "month"], SortMultipleOptions::default()))
        }
    ''',
    'export': '''
        use mage::prelude::*;

        fn export_data(mut summary: DataFrame, vars: Vars) -> Result<()> {
            let path: String = vars.get("output_path")?;
            ParquetWriter::new(std::fs::File::create(path)?).finish(&mut summary)?;
            Ok(())
        }
    ''',
}

LANGUAGES = {
    'python': ('python', PANDAS, 'py'),
    'polars': ('python', POLARS, 'py'),
    'r': ('r', R, 'r'),
    'rust': ('rust', RUST, 'rs'),
}


def pipelines() -> Dict[str, List[str]]:
    """
    Pipeline name to the language of each step: one per language, and the staggered
    series that replaces pandas blocks with Rust from the first block on.
    """
    plans = {
        'pandas': ['python'] * 5,
        'polars': ['polars'] * 5,
        'r': ['r'] * 5,
        'rust': ['rust'] * 5,
    }
    for rust_blocks in range(1, 5):
        plans[f'rust_{rust_blocks}_of_5'] = ['rust'] * rust_blocks + ['python'] * (5 - rust_blocks)
    return plans


def block_files(languages: List[str]) -> List[Tuple[str, str, str, str]]:
    """(block uuid, block type, Mage language, code) for each step of a pipeline."""
    blocks = []
    for step, language in zip(STEPS, languages):
        mage_language, code, _ = LANGUAGES[language]
        blocks.append((
            f'{language}_{step}',
            STEP_TYPES[step],
            mage_language,
            textwrap.dedent(code[step]).strip() + '\n',
        ))
    return blocks
