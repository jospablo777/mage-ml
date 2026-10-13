"""
The retail dataset of the tutorials: a shop's customers and a year of orders, written as
Parquet files into a project's data/shop folder. The data is random but the same on every
run, and it has what real orders have: duplicated rows, cancelled and returned orders,
missing amounts and three currencies.

    mage examples retail path/to/project
"""
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

SEED = 2026
REGIONS = ('north', 'south', 'east', 'west')
SEGMENTS = ('consumer', 'business')
STATUSES = ('completed', 'cancelled', 'returned')
# What one unit of each currency is worth in USD. The tutorials convert with these rates.
RATES_TO_USD = {'USD': 1.0, 'EUR': 1.08, 'CRC': 0.0019}
START = np.datetime64('2024-07-01T00:00:00', 's')
END = np.datetime64('2025-07-01T00:00:00', 's')
FOLDER = Path('data') / 'shop'


@dataclass
class Written:
    folder: Path
    customers: int
    orders: int
    duplicates: int


def tables(customers: int = 2_000, orders: int = 30_000) -> Dict[str, pa.Table]:
    """The customers and orders tables."""
    rng = np.random.default_rng(SEED)

    customer_ids = np.arange(1, customers + 1, dtype=np.int32)
    signed_up = (
        np.datetime64('2023-01-01', 'D')
        + rng.integers(0, 548, customers).astype('timedelta64[D]')
    )
    customer_table = pa.table({
        'customer_id': pa.array(customer_ids, pa.int32()),
        'region': pa.array(rng.choice(REGIONS, customers, p=[0.3, 0.25, 0.25, 0.2])),
        'segment': pa.array(rng.choice(SEGMENTS, customers, p=[0.8, 0.2])),
        'signed_up_on': pa.array(signed_up, pa.date32()),
    })

    # Some customers buy far more often than others.
    weights = rng.gamma(0.6, 1.0, customers)
    weights /= weights.sum()
    span = int((END - START) / np.timedelta64(1, 's'))
    ordered_at = START + np.sort(rng.integers(0, span, orders)).astype('timedelta64[s]')
    currency = rng.choice(list(RATES_TO_USD), orders, p=[0.7, 0.2, 0.1])
    amount_usd = rng.gamma(2.0, 35.0, orders)
    amount = amount_usd / np.array([RATES_TO_USD[c] for c in currency])
    amount = np.round(amount, 2)
    missing = rng.random(orders) < 0.01
    order_table = pa.table({
        'order_id': pa.array(np.arange(1, orders + 1, dtype=np.int32), pa.int32()),
        'customer_id': pa.array(rng.choice(customer_ids, orders, p=weights), pa.int32()),
        'ordered_at': pa.array(ordered_at, pa.timestamp('us', tz='UTC')),
        'status': pa.array(rng.choice(STATUSES, orders, p=[0.92, 0.05, 0.03])),
        'currency': pa.array(currency),
        'amount': pa.array(np.where(missing, np.nan, amount), pa.float64(), mask=missing),
        'units': pa.array(rng.integers(1, 6, orders), pa.int32()),
    })

    # An export that ran twice: about 1% of the orders appear two times.
    repeated = rng.choice(orders, orders // 100, replace=False)
    order_table = pa.concat_tables([order_table, order_table.take(np.sort(repeated))])
    order_table = order_table.take(rng.permutation(order_table.num_rows))
    return dict(customers=customer_table, orders=order_table)


def write(project_path: str, customers: int = 2_000, orders: int = 30_000) -> Written:
    """Writes customers.parquet and orders.parquet into <project>/data/shop."""
    project = Path(project_path)
    if not project.is_dir():
        raise FileNotFoundError(f'{project} is not a folder. Pass the path of a MageML project.')
    folder = project / FOLDER
    folder.mkdir(parents=True, exist_ok=True)
    data = tables(customers=customers, orders=orders)
    for name, table in data.items():
        pq.write_table(table, folder / f'{name}.parquet', compression='zstd')
    return Written(
        folder=folder,
        customers=data['customers'].num_rows,
        orders=data['orders'].num_rows,
        duplicates=data['orders'].num_rows - orders,
    )
