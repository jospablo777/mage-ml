"""
`mage data sample` against ClickHouse: samples were drawn from PostgreSQL, MySQL and DuckDB
only. A masked sample of customers and their orders, drawn by ClickHouse's hash of the
key, keeps its joins on the hashed keys.
"""
from pathlib import Path

import pandas as pd
import yaml

KEY = 'integration-test-masking-key'  # pragma: allowlist secret


def test_a_clickhouse_sample_keeps_its_joins(mage_project, ch, monkeypatch, tmp_path):
    from mage_ai import data_samples

    ch.command('CREATE TABLE customers (id UInt32, email String) ENGINE = MergeTree ORDER BY id')
    ch.insert('customers', [(i, f'person{i}@corp.org') for i in range(1, 401)],
              column_names=['id', 'email'])
    ch.command(
        'CREATE TABLE orders (id UInt32, customer_id UInt32) ENGINE = MergeTree ORDER BY id',
    )
    ch.insert('orders', [(i, 1 + i % 400) for i in range(1, 2001)],
              column_names=['id', 'customer_id'])
    monkeypatch.setenv('MAGE_SAMPLE_KEY', KEY)
    folder = Path(mage_project) / 'data_samples'
    folder.mkdir(exist_ok=True)
    (folder / 'dev_clickhouse.yaml').write_text(yaml.safe_dump(dict(
        source=dict(profile='clickhouse', kind='clickhouse'),
        tables=[
            dict(table='customers', key='id', sample=dict(fraction=0.25),
                 columns=dict(id='hash', email=dict(fake='email'))),
            dict(table='orders', follow=dict(table='customers', column='customer_id'),
                 columns=dict(id='hash', customer_id='hash')),
        ],
    )))

    record = data_samples.run(mage_project, 'dev_clickhouse', out_dir=str(tmp_path / 'dev'))

    customers = pd.read_parquet(tmp_path / 'dev' / 'customers.parquet')
    orders = pd.read_parquet(tmp_path / 'dev' / 'orders.parquet')
    assert record['source'] == dict(profile='clickhouse', kind='clickhouse')
    assert 50 < len(customers) < 150, len(customers)
    assert not customers['email'].str.contains('corp.org').any()
    assert set(orders['customer_id']) <= set(customers['id'])
    assert len(orders) == 5 * len(customers)


def test_a_stratified_clickhouse_sample(mage_project, ch, monkeypatch, tmp_path):
    from mage_ai import data_samples

    ch.command('CREATE TABLE events (id UInt32, kind String) ENGINE = MergeTree ORDER BY id')
    ch.insert('events', [(i, 'abc'[i % 3]) for i in range(1, 301)], column_names=['id', 'kind'])
    monkeypatch.setenv('MAGE_SAMPLE_KEY', KEY)
    folder = Path(mage_project) / 'data_samples'
    folder.mkdir(exist_ok=True)
    (folder / 'strata_clickhouse.yaml').write_text(yaml.safe_dump(dict(
        source=dict(profile='clickhouse', kind='clickhouse'),
        tables=[dict(table='events', key='id', sample=dict(rows=10, stratify='kind'),
                     keep=dict(kind='a category'))],
    )))

    data_samples.run(mage_project, 'strata_clickhouse', out_dir=str(tmp_path / 's'))

    events = pd.read_parquet(tmp_path / 's' / 'events.parquet')
    assert events['kind'].value_counts().to_dict() == dict(a=10, b=10, c=10)
