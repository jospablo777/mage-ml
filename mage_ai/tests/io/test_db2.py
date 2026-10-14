"""
Db2 statements and values without a server: names fold as Db2 folds them, unique columns are
NOT NULL, upserts are MERGE statements, types keep values exact, and the connection string
turns TLS on with the credentials' certificate and never shows the password.
"""
import base64
import unittest
from datetime import datetime
from decimal import Decimal

import numpy as np
import pandas as pd

from mage_ai.io.config import ConfigKey
from mage_ai.io.db2 import Db2, _stored_name, _value, quote
from mage_ai.io.export_utils import infer_dtypes

PEM = '-----BEGIN CERTIFICATE-----\nMIIBszCCAVmgAwIBAgIU\n-----END CERTIFICATE-----\n'


class Config(dict):
    def __getitem__(self, key):
        return self.get(key)


def db2(**settings):
    return Db2.with_config(Config({
        ConfigKey.DB2_DATABASE: 'bludb', ConfigKey.DB2_HOST: 'db2.example.com',
        ConfigKey.DB2_PORT: 50001, ConfigKey.DB2_USER: 'svc',
        ConfigKey.DB2_PASSWORD: 'p@ss',  # pragma: allowlist secret
        **settings,
    }))


class Db2NamesTest(unittest.TestCase):
    def test_simple_names_stay_plain_and_fold_to_upper_case(self):
        self.assertEqual(quote('orders'), 'orders')
        self.assertEqual(_stored_name('orders'), 'ORDERS')
        self.assertEqual(quote('order total'), '"order total"')
        self.assertEqual(quote('Orders', case_sensitive=True), '"Orders"')
        self.assertEqual(_stored_name('Orders', case_sensitive=True), 'Orders')
        self.assertEqual(quote('a"b'), '"a""b"')
        # A leading underscore reads as a directive, reserved words as SQL: both quoted.
        self.assertEqual(quote('_a'), '"_a"')
        self.assertEqual(quote('select'), '"select"')
        self.assertEqual(_stored_name('select'), 'select')
        # Words other databases reserve but Db2 does not stay plain.
        self.assertEqual(quote('a'), 'a')
        self.assertEqual(db2()._clean_column_name('a'), 'a')


class Db2StatementsTest(unittest.TestCase):
    def test_unique_columns_are_not_null_in_the_table(self):
        sql = db2().build_create_table_command(
            {'id': 'BIGINT', 'city': 'VARCHAR(255)'}, 'shop', 'orders',
            unique_constraints=['id'],
        )
        self.assertEqual(sql, (
            'CREATE TABLE shop.orders (\n    id BIGINT NOT NULL,\n    city VARCHAR(255),\n'
            '    UNIQUE (id)\n)'
        ))

    def test_upserts_merge_on_the_unique_columns(self):
        sql = db2()._merge('shop.orders', ['id', 'city'], ['id', 'city'], ['id'], 'UPDATE',
                           False)
        self.assertEqual(sql, (
            'MERGE INTO shop.orders AS t USING (VALUES (?, ?)) AS s (id, city) ON t.id = s.id'
            ' WHEN MATCHED THEN UPDATE SET t.city = s.city'
            ' WHEN NOT MATCHED THEN INSERT (id, city) VALUES (s.id, s.city)'
        ))
        ignore = db2()._merge('t', ['id', 'city'], ['id', 'city'], ['id'], 'IGNORE', False)
        self.assertIn('WHEN MATCHED THEN UPDATE SET t.city = t.city', ignore)

    def test_queries_are_limited_with_fetch_first(self):
        self.assertEqual(
            db2()._enforce_limit('SELECT * FROM orders;', 10),
            'SELECT * FROM (\nSELECT * FROM orders\n) AS mage_query FETCH FIRST 10 ROWS ONLY',
        )


class Db2TypesTest(unittest.TestCase):
    def types(self, df):
        client = db2()
        dtypes = infer_dtypes(df)
        return {c: client.get_type(df[c], dtypes[c]) for c in df.columns}

    def test_types_keep_values_exact(self):
        df = pd.DataFrame(dict(
            id=[1, 2], huge=[2**70, 1], price=[Decimal('0.10'), Decimal('12.345')],
            ratio=[0.5, 1.25], flag=[True, False],
            at=pd.to_datetime(['2026-01-01', '2026-01-02']),
            city=['Zürich', 'São Paulo'], long=['x' * 40000, 'y'],
            attrs=[{'a': 1}, {'b': [1, 2]}],
        ))
        types = self.types(df)
        self.assertEqual(types['id'], 'BIGINT')
        self.assertEqual(types['huge'], 'DECFLOAT(34)')
        self.assertEqual(types['price'], 'DECFLOAT(34)')
        self.assertEqual(types['ratio'], 'DOUBLE')
        self.assertEqual(types['flag'], 'BOOLEAN')
        self.assertEqual(types['at'], 'TIMESTAMP(6)')
        self.assertEqual(types['city'], 'VARCHAR(255)')
        self.assertEqual(types['long'], 'CLOB(2G)')
        self.assertTrue(types['attrs'].startswith('VARCHAR'))

    def test_values_bind_as_the_driver_expects(self):
        self.assertIsNone(_value(np.nan))
        self.assertIsNone(_value(pd.NaT))
        self.assertIsNone(_value(None))
        self.assertEqual(_value(np.int64(7)), 7)
        self.assertEqual(_value(2**70), str(2**70))
        self.assertEqual(_value(-(2**63)), -(2**63))
        self.assertIs(_value(np.bool_(True)), True)
        self.assertEqual(_value({'a': [1, 'ñ']}), '{"a": [1, "ñ"]}')
        self.assertEqual(
            _value(pd.Timestamp('2026-01-01 12:00', tz='Europe/Zurich')),
            datetime(2026, 1, 1, 11, 0),
        )
        self.assertEqual(_value(Decimal('0.10')), Decimal('0.10'))


class Db2ConnectionTest(unittest.TestCase):
    def test_tls_uses_the_credentials_certificate(self):
        text = db2(**{
            ConfigKey.DB2_SSL: True,
            ConfigKey.DB2_SSL_CERTIFICATE: base64.b64encode(PEM.encode()).decode(),
        }).connection_string()
        self.assertIn('SECURITY=SSL;', text)
        path = text.split('SSLServerCertificate=')[1].split(';')[0]
        with open(path) as file:
            self.assertEqual(file.read(), PEM)

    def test_credentials_stay_out_of_the_dsn_and_semicolons_there_are_refused(self):
        client = db2(**{ConfigKey.DB2_PASSWORD: 'a;b'})  # pragma: allowlist secret
        text = client.connection_string()
        self.assertNotIn('a;b', text)
        self.assertNotIn('UID', text)
        self.assertNotIn('SECURITY', text)
        with self.assertRaises(ValueError):
            db2(**{ConfigKey.DB2_HOST: 'h;SECURITY=NONE'}).connection_string()
