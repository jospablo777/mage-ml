from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

QUERY = """
SELECT
    o.order_id,
    o.ordered_at,
    o.status,
    o.channel,
    o.discount_pct,
    o.shipping_cost,
    o.rating,
    i.line_number,
    i.product_id,
    i.quantity,
    i.unit_price,
    c.segment,
    c.country
FROM shop.orders o
JOIN shop.order_items i USING (order_id)
JOIN shop.customers c USING (customer_id)
WHERE o.ordered_at >= TIMESTAMPTZ '2024-07-01'
"""


@data_loader
def load_order_lines(*args, **kwargs):
    """
    Order lines of the last year of the shop, with pandas. exact_types keeps nullable
    integers (rating), decimals (prices) and zoned timestamps as PostgreSQL has them.
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        return loader.load(QUERY, exact_types=True)


@test
def has_rows(output, *args) -> None:
    assert len(output) > 0, 'No order lines were loaded'
    assert output['quantity'].min() >= 1, 'A quantity is below 1'
