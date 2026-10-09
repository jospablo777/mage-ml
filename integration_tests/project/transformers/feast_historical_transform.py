import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(frame: pl.DataFrame, *args, **kwargs) -> pl.DataFrame:
    # New feature rows for other drivers, two years later.
    return frame.select(
        driver_id=pl.col('driver_id') + int(kwargs['driver_offset']),
        event_timestamp=pl.col('event_timestamp').dt.offset_by('2y'),
        created=pl.col('event_timestamp').dt.offset_by('2y'),
        conv_rate=pl.col('conv_rate') + 0.25,
        acc_rate=pl.col('acc_rate') * 2,
        avg_daily_trips=pl.col('avg_daily_trips') * 2,
        city=pl.col('city').str.reverse(),
        active=pl.col('active').fill_null(True),
    )
