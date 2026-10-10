from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_customers(*args, **kwargs):
    """Customers with their order totals, as Polars: decimals stay decimals."""
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        return loader.load(
            """
            SELECT c.customer_id, c.full_name, c.city, c.country, c.segment,
                   c.signed_up_at, c.is_active, c.lifetime_value,
                   count(o.order_id) AS orders,
                   max(o.ordered_at) AS last_order_at
            FROM shop.customers c
            LEFT JOIN shop.orders o USING (customer_id)
            GROUP BY c.customer_id
            """,
            polars=True,
        )
