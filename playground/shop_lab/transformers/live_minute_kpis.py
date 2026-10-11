import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def live_minute_kpis(events: pl.DataFrame, *args, **kwargs) -> pl.DataFrame:
    """Events, purchases, conversion and revenue per minute."""
    return (
        events.group_by(pl.col('occurred_at').dt.truncate('1m').alias('minute_start'))
        .agg(
            pl.len().alias('events'),
            (pl.col('event_type') == 'purchase').sum().alias('purchases'),
            pl.col('amount').sum().round(2).alias('revenue'),
        )
        .with_columns((pl.col('purchases') / pl.col('events')).round(4).alias('conversion_rate'))
        .sort('minute_start')
    )
