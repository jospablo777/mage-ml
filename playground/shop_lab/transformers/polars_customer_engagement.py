import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def customer_engagement(sessions: pl.DataFrame, *args, **kwargs) -> pl.DataFrame:
    """Engagement per known customer, from their sessions."""
    return (
        sessions.filter(pl.col('customer_id').is_not_null())
        .group_by('customer_id')
        .agg(
            pl.len().alias('sessions'),
            pl.col('events').sum().alias('events'),
            pl.col('purchases').sum().alias('purchases'),
            pl.col('started_at').max().alias('last_seen_at'),
            pl.col('device').mode().sort().first().alias('main_device'),
            pl.col('length').mean().alias('mean_session_length'),
        )
        .with_columns(
            (pl.col('purchases') / pl.col('sessions')).round(3).alias('conversion_rate'),
        )
        .sort('customer_id')
    )


@test
def one_row_per_customer(output, *args) -> None:
    assert output['customer_id'].n_unique() == len(output), 'A customer appears twice'
