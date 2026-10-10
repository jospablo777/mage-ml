import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def sessions(events: pl.DataFrame, *args, **kwargs) -> pl.DataFrame:
    """
    One row per session. With block fusion, this block gets the events from memory. Every
    value and the row order are deterministic, so the fused and unfused runs give the same
    output (`mage verify-fusion`): ties of the mode go to the first device by name.
    """
    return (
        events.group_by('session_id')
        .agg(
            pl.col('customer_id').max(),
            pl.col('occurred_at').min().alias('started_at'),
            (pl.col('occurred_at').max() - pl.col('occurred_at').min()).alias('length'),
            pl.len().alias('events'),
            (pl.col('event_type') == 'purchase').sum().alias('purchases'),
            pl.col('device').mode().sort().first().alias('device'),
            pl.col('duration_ms').median().alias('median_duration_ms'),
            pl.col('scroll_depth').fill_nan(None).mean().alias('mean_scroll_depth'),
        )
        .sort('started_at', 'session_id')
    )
