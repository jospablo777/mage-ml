import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def store_revenue(lines, *args, **kwargs):
    """One branch of the Diamond, in Polars: orders and revenue per store."""
    return (
        pl.from_pandas(lines)
        .group_by('store_id')
        .agg(
            pl.col('order_id').n_unique().alias('orders'),
            (pl.col('quantity') * pl.col('unit_price')).sum().alias('revenue'),
        )
        .sort('store_id')
    )
