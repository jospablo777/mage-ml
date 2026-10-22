import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(data, *args, **kwargs):
    """
    Adds features of a date or date-time column, the one the date_column variable names
    (created_at by default): its day, its ISO week day (1 is Monday), its month and hour,
    and whether the day falls on a weekend. A text column is read as ISO 8601.
    """
    rows = data.collect() if isinstance(data, pl.LazyFrame) else data
    if not isinstance(rows, pl.DataFrame):
        rows = pl.from_pandas(rows)
    column = kwargs.get('date_column', 'created_at')
    at = pl.col(column)
    if rows.schema[column] == pl.String:
        at = at.str.to_datetime()
    elif rows.schema[column] == pl.Date:
        at = at.cast(pl.Datetime)
    return rows.with_columns(
        day=at.dt.date(),
        weekday=at.dt.weekday(),
        month=at.dt.month(),
        hour=at.dt.hour(),
        weekend=at.dt.weekday() >= 6,
    )
