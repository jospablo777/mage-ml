import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def daily_revenue(lines: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """Revenue per day and channel, without cancelled orders."""
    kept = lines[lines['status'] != 'cancelled'].copy()
    kept['revenue'] = (
        kept['quantity'] * kept['unit_price'].astype('float64')
        * (1 - kept['discount_pct'] / 100)
    )
    kept['order_date'] = kept['ordered_at'].dt.tz_convert('America/Costa_Rica').dt.date

    daily = (
        kept.groupby(['order_date', 'channel'], observed=True)
        .agg(
            orders=('order_id', 'nunique'),
            units=('quantity', 'sum'),
            revenue=('revenue', 'sum'),
            mean_rating=('rating', 'mean'),
        )
        .reset_index()
    )
    daily['revenue'] = daily['revenue'].round(2)
    daily['revenue_per_order'] = (daily['revenue'] / daily['orders']).round(2)
    daily['channel'] = daily['channel'].astype('category')
    print(f'{len(daily):,} days x channels, revenue {daily["revenue"].sum():,.2f}')
    return daily.sort_values(['order_date', 'channel'], ignore_index=True)


@test
def revenue_is_positive(output, *args) -> None:
    assert (output['revenue'] >= 0).all(), 'A day has negative revenue'
