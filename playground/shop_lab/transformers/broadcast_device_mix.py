import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def device_mix(events, *args, **kwargs):
    """Share of each event type on each device, in Polars."""
    return (
        pl.from_pandas(events)
        .group_by('device', 'event_type')
        .len()
        .with_columns((pl.col('len') / pl.col('len').sum().over('device')).alias('share'))
        .rename({'len': 'events'})
        .sort('device', 'event_type')
    )
