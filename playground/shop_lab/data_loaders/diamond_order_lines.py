from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_order_lines(*args, **kwargs):
    """Store order lines that were not cancelled: the top of a Diamond."""
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        return loader.load(
            """
            SELECT o.order_id, o.store_id, o.channel, i.quantity,
                   i.unit_price::double precision AS unit_price,
                   p.cost::double precision AS cost
            FROM shop.order_items i
            JOIN shop.orders o USING (order_id)
            JOIN shop.products p USING (product_id)
            WHERE o.status <> 'cancelled' AND o.store_id IS NOT NULL
            """,
        )
