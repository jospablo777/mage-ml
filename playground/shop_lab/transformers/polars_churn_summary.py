import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def churn_summary(scores, *args, **kwargs) -> pl.DataFrame:
    """
    The R block's output arrives here as a pandas frame (the factor as a category);
    this block works in Polars.
    """
    frame = pl.from_pandas(scores)
    return (
        frame.group_by('segment', 'churn_risk')
        .agg(
            pl.len().alias('customers'),
            pl.col('spend').sum().round(2).alias('spend'),
            pl.col('days_since_last_order').median().alias('median_days_since_last_order'),
        )
        .with_columns(
            (pl.col('customers') / pl.col('customers').sum().over('segment'))
            .round(4)
            .alias('share_of_segment'),
        )
        .sort('segment', 'churn_risk')
    )
