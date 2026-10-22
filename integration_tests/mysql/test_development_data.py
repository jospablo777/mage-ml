"""
`mage data sample` against MySQL (item 83): samples were drawn from PostgreSQL only. A
masked sample of customers and their orders, drawn by MySQL's hash of the key, keeps its
joins on the hashed keys.
"""
from pathlib import Path

import pandas as pd
import yaml

KEY = 'integration-test-masking-key'  # pragma: allowlist secret


def test_a_mysql_sample_keeps_its_joins(mage_project, my, mysql_database, monkeypatch,
                                        tmp_path):
    from mage_ai import data_samples

    with my.cursor() as cursor:
        cursor.execute('CREATE TABLE customers (id INT PRIMARY KEY, email TEXT)')
        cursor.executemany('INSERT INTO customers VALUES (%s, %s)',
                           [(i, f'person{i}@corp.org') for i in range(1, 401)])
        cursor.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT)')
        cursor.executemany('INSERT INTO orders VALUES (%s, %s)',
                           [(i, 1 + i % 400) for i in range(1, 2001)])
    monkeypatch.setenv('MAGE_TEST_MYSQL_DATABASE', mysql_database)
    monkeypatch.setenv('MAGE_SAMPLE_KEY', KEY)
    folder = Path(mage_project) / 'data_samples'
    folder.mkdir(exist_ok=True)
    (folder / 'dev.yaml').write_text(yaml.safe_dump(dict(
        source=dict(profile='mysql', kind='mysql'),
        tables=[
            dict(table='customers', key='id', sample=dict(fraction=0.25),
                 columns=dict(id='hash', email=dict(fake='email'))),
            dict(table='orders', follow=dict(table='customers', column='customer_id'),
                 columns=dict(id='hash', customer_id='hash')),
        ],
    )))

    record = data_samples.run(mage_project, 'dev', out_dir=str(tmp_path / 'dev'))

    customers = pd.read_parquet(tmp_path / 'dev' / 'customers.parquet')
    orders = pd.read_parquet(tmp_path / 'dev' / 'orders.parquet')
    assert record['source'] == dict(profile='mysql', kind='mysql')
    assert 50 < len(customers) < 150, len(customers)
    assert not customers['email'].str.contains('corp.org').any()
    assert set(orders['customer_id']) <= set(customers['id'])
    assert len(orders) == 5 * len(customers)
