from os import path

import pandas as pd

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def category_margin(lines: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """Margin per product category and customer segment. Runs next to daily_revenue."""
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        products = loader.load(
            'SELECT product_id, category, cost FROM shop.products', exact_types=True,
        )

    joined = lines.merge(products, on='product_id', how='left', validate='many_to_one')
    joined['sales'] = joined['quantity'] * joined['unit_price'].astype('float64')
    joined['costs'] = joined['quantity'] * joined['cost'].astype('float64')
    margin = (
        joined.groupby(['category', 'segment'], observed=True)
        .agg(order_lines=('order_id', 'size'), sales=('sales', 'sum'), costs=('costs', 'sum'))
        .reset_index()
    )
    margin['margin_pct'] = ((1 - margin['costs'] / margin['sales']) * 100).round(1)
    return margin.sort_values('sales', ascending=False, ignore_index=True)
