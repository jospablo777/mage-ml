from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_customer_activity(*args, **kwargs):
    """Recency, frequency and spend of each customer who ordered, with pandas."""
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        return loader.load(
            """
            SELECT o.customer_id,
                   c.segment,
                   c.country,
                   DATE '2025-06-30' - max(o.ordered_at)::date AS days_since_last_order,
                   count(*) AS orders,
                   sum(i.quantity * i.unit_price)::double precision AS spend,
                   avg(o.rating)::double precision AS mean_rating
            FROM shop.orders o
            JOIN shop.customers c USING (customer_id)
            JOIN (
                SELECT order_id, sum(quantity) AS quantity, avg(unit_price) AS unit_price
                FROM shop.order_items GROUP BY order_id
            ) i USING (order_id)
            WHERE o.status <> 'cancelled'
            GROUP BY o.customer_id, c.segment, c.country
            """,
            exact_types=True,
        )
