import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(data, *args, **kwargs):
    """
    Checks the rows before they go on and fails with every problem found: the columns of
    the required variable (id by default) have no missing values, the column of the unique
    variable (id by default) has no repeated values, and amount, when the data has it, is
    not negative. The rows pass on unchanged.
    """
    rows = data.collect() if isinstance(data, pl.LazyFrame) else data
    if not isinstance(rows, pl.DataFrame):
        rows = pl.from_pandas(rows)
    required = kwargs.get('required', ['id'])
    required = [required] if isinstance(required, str) else list(required)
    unique = kwargs.get('unique', 'id')
    problems = []
    for column in required:
        missing = rows[column].null_count()
        if missing:
            problems.append(f'{column} is missing in {missing} rows')
    keys = rows[unique].drop_nulls()
    repeated = len(keys) - keys.n_unique()
    if repeated:
        problems.append(f'{unique} repeats in {repeated} rows')
    if 'amount' in rows.columns:
        negative = rows.filter(pl.col('amount') < 0).height
        if negative:
            problems.append(f'amount is negative in {negative} rows')
    if problems:
        raise ValueError('The rows failed their checks: ' + '; '.join(problems))
    return rows
