import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def customer_360(customers: pl.DataFrame, engagement: pl.DataFrame, *args, **kwargs):
    """Customers joined with their web engagement; customers never seen online keep nulls."""
    return (
        customers.join(engagement, on='customer_id', how='left')
        .with_columns(
            pl.col('sessions').fill_null(0),
            pl.when(pl.col('orders') == 0)
            .then(pl.lit('never ordered'))
            .when(pl.col('purchases').fill_null(0) == 0)
            .then(pl.lit('offline only'))
            .otherwise(pl.lit('online buyer'))
            .cast(pl.Categorical)
            .alias('profile'),
        )
        .sort('lifetime_value', descending=True)
    )
