"""
Which upstream outputs a service's SQL block writes to tables first, as Mage decides it:
an upstream SQL block on the same database is read as it is (I-267).
"""
import tempfile
import unittest
from pathlib import Path

from mage_ai.pipeline_services.runtime import sql

IO_CONFIG = '''
duckdb:
  DUCKDB_DATABASE: shop.duckdb
duckdb_too:
  DUCKDB_DATABASE: shop.duckdb
duckdb_other:
  DUCKDB_DATABASE: finance.duckdb
redshift:
  REDSHIFT_HOST: cluster.example.com
  REDSHIFT_PORT: 5439
  REDSHIFT_DBNAME: dev
'''


class UpstreamTablesTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.io_config = str(Path(directory.name) / 'io_config.yaml')
        Path(self.io_config).write_text(IO_CONFIG)

    def copies(self, provider, profile, upstream_profile):
        return sql.should_upload(
            dict(data_provider=provider, data_provider_profile=profile),
            dict(language='sql', configuration=dict(
                data_provider=provider, data_provider_profile=upstream_profile,
            )),
            self.io_config,
            sql.PROVIDERS[provider]['profile_keys'],
        )

    def test_a_profile_without_an_optional_key_reads_its_own_tables(self):
        # No DUCKDB_SCHEMA: the same database all the same.
        self.assertFalse(self.copies('duckdb', 'duckdb', 'duckdb'))
        self.assertFalse(self.copies('duckdb', 'duckdb', 'duckdb_too'))
        self.assertTrue(self.copies('duckdb', 'duckdb', 'duckdb_other'))

    def test_other_outputs_are_written(self):
        python = dict(language='python', configuration={})
        self.assertTrue(sql.should_upload(
            dict(data_provider='duckdb', data_provider_profile='duckdb'), python,
            self.io_config, sql.PROVIDERS['duckdb']['profile_keys'],
        ))
        sensor = dict(type='sensor', language='python', configuration={})
        self.assertFalse(sql.should_upload(
            dict(data_provider='duckdb', data_provider_profile='duckdb'), sensor,
            self.io_config, sql.PROVIDERS['duckdb']['profile_keys'],
        ))
