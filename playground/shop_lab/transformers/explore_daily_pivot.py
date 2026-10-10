import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def daily_pivot(events: pl.DataFrame, *args, **kwargs) -> pl.DataFrame:
    """A wide output: one row per day, one column per page and device (about 60)."""
    return (
        events.with_columns(
            pl.col('occurred_at').dt.date().alias('event_date'),
            (pl.col('page') + ' · ' + pl.col('device')).alias('page_device'),
        )
        .group_by('event_date', 'page_device')
        .agg(pl.len().alias('events'))
        .pivot(on='page_device', index='event_date', values='events')
        .fill_null(0)
        .sort('event_date')
    )
