import re

import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Cleans the rows: snake_case column names, trimmed text with single spaces, no row
    whose values are all missing and no duplicate rows. Takes a Polars or pandas frame.
    """
    rows = data.collect() if isinstance(data, pl.LazyFrame) else data
    if not isinstance(rows, pl.DataFrame):
        rows = pl.from_pandas(rows)
    rows = rows.rename(
        {name: re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_') for name in rows.columns},
    )
    rows = rows.with_columns(
        pl.col(pl.String).str.strip_chars().str.replace_all(r'\s+', ' '),
    )
    return rows.filter(~pl.all_horizontal(pl.all().is_null())).unique(maintain_order=True)


@test
def test_output_has_rows(output, *args) -> None:
    assert len(output) > 0, 'The output is empty'
