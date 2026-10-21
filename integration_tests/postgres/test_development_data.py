"""
`mage data sample` against PostgreSQL (item 83): a masked sample of customers and their
orders. Masked values never equal the originals, the sampled orders are the sampled
customers', joins on the hashed keys still match, and a personal column without a rule
stops the copy.
"""
import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

KEY = 'integration-test-masking-key'  # pragma: allowlist secret


@pytest.fixture
def customers_and_orders(pg, profile_schema):
    with pg.cursor() as cursor:
        cursor.execute(f'''
            CREATE TABLE {profile_schema}.customers (
                id integer PRIMARY KEY, email text, full_name text, country text,
                birth_date date);
            INSERT INTO {profile_schema}.customers
            SELECT i, 'person' || i || '@corp.org', 'Real Person ' || i,
                   CASE WHEN i % 2 = 0 THEN 'CR' ELSE 'MX' END,
                   DATE '1990-01-01' + i
            FROM generate_series(1, 400) AS i;
            CREATE TABLE {profile_schema}.orders (
                id integer PRIMARY KEY, customer_id integer, total numeric);
            INSERT INTO {profile_schema}.orders
            SELECT i, 1 + i % 400, i * 1.5 FROM generate_series(1, 2000) AS i;
        ''')
    pg.commit()
    return profile_schema


def write(project: str, name: str, value: dict):
    folder = Path(project) / 'data_samples'
    folder.mkdir(exist_ok=True)
    (folder / f'{name}.yaml').write_text(yaml.safe_dump(value))


def test_a_masked_sample_keeps_its_joins(mage_project, customers_and_orders, monkeypatch,
                                         tmp_path):
    from mage_ai import data_samples

    schema = customers_and_orders
    monkeypatch.setenv('MAGE_SAMPLE_KEY', KEY)
    write(mage_project, 'dev', dict(
        source=dict(profile='test_schema'),
        tables=[
            dict(table=f'{schema}.customers', key='id', sample=dict(fraction=0.25),
                 columns=dict(id='hash', email=dict(fake='email'), full_name=dict(fake='name'),
                              birth_date=dict(shift_days=30, by='id')),
                 keep=dict(country='a country is not personal here')),
            dict(table=f'{schema}.orders', key='id',
                 follow=dict(table=f'{schema}.customers', column='customer_id'),
                 columns=dict(customer_id='hash')),
        ],
    ))

    record = data_samples.run(mage_project, 'dev', out_dir=str(tmp_path / 'dev'))
    assert '.mage/dev_data/' in (Path(mage_project) / '.gitignore').read_text().splitlines()

    customers = pd.read_parquet(tmp_path / 'dev' / f'{schema}.customers.parquet')
    orders = pd.read_parquet(tmp_path / 'dev' / f'{schema}.orders.parquet')
    assert 50 < len(customers) < 150, len(customers)
    assert not set(customers['id']) & set(range(1, 401))
    assert not customers['email'].str.contains('corp.org').any()
    assert not customers['full_name'].str.startswith('Real').any()
    # Each sampled customer's orders came along, and only theirs: the hashed keys join.
    assert set(orders['customer_id']) <= set(customers['id'])
    assert len(orders) == 5 * len(customers)
    assert set(customers['country']) == {'CR', 'MX'}
    tables = {t['table']: t for t in record['tables']}
    assert tables[f'{schema}.customers']['rules']['email'] == 'fake'
    assert tables[f'{schema}.customers']['kept'] == {'country': 'a country is not personal here'}
    assert json.loads((tmp_path / 'dev' / 'record.json').read_text())['key_env'] == \
        'MAGE_SAMPLE_KEY'
    import duckdb
    found = duckdb.connect(str(tmp_path / 'dev' / 'dev.duckdb')).execute(
        f'SELECT count(*) FROM "{schema}".orders o JOIN "{schema}".customers c '
        'ON o.customer_id = c.id').fetchone()[0]
    assert found == len(orders)

    # The same key gives the same hashes on the next copy.
    again = data_samples.run(mage_project, 'dev', out_dir=str(tmp_path / 'again'))
    assert again['tables'][0]['rows'] == len(customers)
    assert set(pd.read_parquet(tmp_path / 'again' / f'{schema}.customers.parquet')['id']) == \
        set(customers['id'])


def test_a_personal_column_without_a_rule_stops_the_copy(mage_project, customers_and_orders,
                                                         monkeypatch, tmp_path):
    from mage_ai import data_samples

    monkeypatch.setenv('MAGE_SAMPLE_KEY', KEY)
    write(mage_project, 'leaky', dict(
        source=dict(profile='test_schema'),
        tables=[dict(table=f'{customers_and_orders}.customers', key='id',
                     sample=dict(rows=10), columns=dict(id='hash'))],
    ))
    with pytest.raises(data_samples.SampleError, match='email, full_name, birth_date look'):
        data_samples.run(mage_project, 'leaky', out_dir=str(tmp_path / 'leaky'))
    assert not (tmp_path / 'leaky').exists()
