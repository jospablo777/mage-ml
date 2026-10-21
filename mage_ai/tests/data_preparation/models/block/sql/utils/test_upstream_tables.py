"""
How a SQL block refers to its upstream blocks: which {{ df_N }} it uses, whether an
upstream block's data is uploaded, and which table each upstream block's data is in.
"""
from pathlib import Path

import pandas as pd
import polars as pl

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.block.sql.utils.shared import (
    blocks_in_query,
    build_dynamic_table_name,
    should_cache_data_from_upstream,
    table_name_parts,
    upstream_has_no_data,
)
from mage_ai.data_preparation.models.constants import BlockLanguage, BlockType
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.tests.base_test import DBTestCase

PG = dict(data_provider='postgres', data_provider_profile='default')


class UpstreamTablesTest(DBTestCase):
    def setUp(self):
        super().setUp()
        io_config = Path(self.repo_path) / 'io_config.yaml'
        if not io_config.exists():
            io_config.write_text('version: 0.1.1\ndefault:\n  POSTGRES_HOST: localhost\n')
            self.addCleanup(io_config.unlink, missing_ok=True)
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)

    def block(self, uuid, language=BlockLanguage.SQL, configuration=None, upstream=()):
        block = Block.create(
            f'{self.pipeline.uuid}_{uuid}', BlockType.TRANSFORMER, self.repo_path,
            pipeline=self.pipeline, language=language, configuration=configuration,
            upstream_block_uuids=[b.uuid for b in upstream],
        )
        self.pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        return self.pipeline.get_block(block.uuid)

    def test_only_the_upstream_blocks_a_query_names_are_used(self):
        first = self.block('first', BlockLanguage.PYTHON)
        second = self.block('second', BlockLanguage.PYTHON)
        sql = self.block('sql', configuration=PG, upstream=[first, second])

        self.assertEqual(list(blocks_in_query(sql, 'SELECT * FROM {{df_2}}')), [second.uuid])
        self.assertEqual(
            sorted(blocks_in_query(sql, 'SELECT * FROM {{ df_1 }} JOIN {{   df_2 }}')),
            sorted([first.uuid, second.uuid]),
        )
        # df_10 is not df_1.
        self.assertEqual(blocks_in_query(sql, 'SELECT * FROM {{ df_10 }}'), {})
        self.assertEqual(blocks_in_query(sql, ''), {})

    def test_python_upstream_data_is_uploaded_and_same_database_sql_is_not(self):
        python = self.block('python', BlockLanguage.PYTHON)
        same = self.block('same', configuration=dict(PG, data_provider_schema='s'))
        other = self.block('other', configuration=dict(
            data_provider='mysql', data_provider_profile='default',
        ))
        sql = self.block('sql', configuration=dict(PG, data_provider_schema='s'),
                         upstream=[python, same, other])

        keys = ['data_provider', 'data_provider_profile']
        self.assertTrue(should_cache_data_from_upstream(sql, python, keys, []))
        self.assertFalse(should_cache_data_from_upstream(sql, same, keys, []))
        self.assertTrue(should_cache_data_from_upstream(sql, other, keys, []))

    def test_an_upstream_table_name_comes_from_the_configuration_or_the_block(self):
        upstream = self.block('up', configuration=dict(PG, data_provider_schema='up_schema'))
        configuration = dict(PG, data_provider_schema='mine')

        self.assertEqual(
            table_name_parts(configuration, upstream),
            (None, 'up_schema', upstream.table_name),
        )
        overridden = dict(configuration, upstream_block_configuration={
            upstream.uuid: dict(table_name='db.s.t'),
        })
        self.assertEqual(table_name_parts(overridden, upstream), ('db', 's', 't'))
        self.assertEqual(
            table_name_parts(dict(configuration, upstream_block_configuration={
                upstream.uuid: dict(table_name='s.t'),
            }), upstream, no_schema=True),
            ('s', None, 't'),
        )
        python = self.block('python', BlockLanguage.PYTHON)
        self.assertEqual(table_name_parts(configuration, python)[1], 'mine')
        self.assertEqual(
            table_name_parts(configuration, python, dynamic_block_index=2)[2],
            build_dynamic_table_name(python.table_name, 2),
        )
        self.assertTrue(build_dynamic_table_name('t', 2).endswith('__dynamic_2'))

    def test_an_empty_frame_is_written_and_empty_values_are_skipped(self):
        empty = pd.DataFrame({'id': pd.Series([], dtype='Int64')})
        self.assertFalse(upstream_has_no_data(empty))
        self.assertFalse(upstream_has_no_data(pl.DataFrame({'id': []})))
        # BigQuery and Snowflake skip it: their exports were not tested with one.
        self.assertTrue(upstream_has_no_data(empty, empty_frames_have_data=False))
        # The truthiness of a polars frame raises.
        self.assertTrue(upstream_has_no_data(pl.DataFrame(), empty_frames_have_data=False))
        for value in ({}, [], None, pd.Series([], dtype='float64')):
            self.assertTrue(upstream_has_no_data(value), value)
        self.assertFalse(upstream_has_no_data([1]))

    def test_two_trino_blocks_on_the_same_profile_share_tables(self):
        from mage_ai.data_preparation.models.block.sql.trino import CACHE_KEYS

        io_config = Path(self.repo_path) / 'io_config.yaml'
        original = io_config.read_text()
        self.addCleanup(io_config.write_text, original)
        io_config.write_text(original + (
            'trino:\n  TRINO_CATALOG: memory\n  TRINO_HOST: localhost\n'
            '  TRINO_PORT: 8080\n  TRINO_SCHEMA: public\n'
        ))
        trino = dict(data_provider='trino', data_provider_profile='trino')
        first = self.block('first', configuration=trino)
        second = self.block('second', configuration=trino, upstream=[first])
        self.assertFalse(should_cache_data_from_upstream(
            second, first, ['data_provider'], CACHE_KEYS,
        ))

    def test_bigquery_blocks_of_one_project_share_tables(self):
        import json
        import tempfile

        from mage_ai.data_preparation.models.block.sql.bigquery import (
            reads_upstream_table,
        )

        key_file = tempfile.NamedTemporaryFile('w', suffix='.json', delete=False)
        json.dump(dict(type='service_account', project_id='shop'), key_file)
        key_file.close()
        self.addCleanup(Path(key_file.name).unlink)
        io_config = Path(self.repo_path) / 'io_config.yaml'
        original = io_config.read_text()
        self.addCleanup(io_config.write_text, original)
        # Profiles seldom set both the key and its file; one of them is enough.
        io_config.write_text(original + (
            'bq_file:\n'
            f'  GOOGLE_SERVICE_ACC_KEY_FILEPATH: {key_file.name}\n'
            'bq_key:\n'
            '  GOOGLE_SERVICE_ACC_KEY:\n'
            '    type: service_account\n'
            '    project_id: shop\n'
            'bq_other:\n'
            '  GOOGLE_SERVICE_ACC_KEY:\n'
            '    project_id: finance\n'
            'bq_adc:\n'
            '  GOOGLE_LOCATION: EU\n'
        ))

        def bq(profile):
            return dict(data_provider='bigquery', data_provider_profile=profile)

        first = self.block('first', configuration=bq('bq_file'))
        same_profile = self.block('same_profile', configuration=bq('bq_file'), upstream=[first])
        same_project = self.block('same_project', configuration=bq('bq_key'), upstream=[first])
        other = self.block('other', configuration=bq('bq_other'), upstream=[first])
        adc = self.block('adc', configuration=bq('bq_adc'), upstream=[first])

        self.assertTrue(reads_upstream_table(same_profile, first))
        self.assertTrue(reads_upstream_table(same_project, first))
        self.assertFalse(reads_upstream_table(other, first))
        # Application default credentials name no project: the table is copied.
        self.assertFalse(reads_upstream_table(adc, first))

    def test_a_bigquery_sql_upstream_is_not_written_over_itself(self):
        from unittest.mock import MagicMock, patch

        from mage_ai.data_preparation.models.block.sql.bigquery import (
            create_upstream_block_tables,
        )

        io_config = Path(self.repo_path) / 'io_config.yaml'
        original = io_config.read_text()
        self.addCleanup(io_config.write_text, original)
        io_config.write_text(original + 'bq:\n  GOOGLE_LOCATION: US\n')
        bq = dict(data_provider='bigquery', data_provider_profile='bq',
                  data_provider_schema='analytics')
        sql = self.block('sql_up', configuration=bq)
        python = self.block('python_up', BlockLanguage.PYTHON)
        block = self.block('down', configuration=bq, upstream=[sql, python])
        frame = pd.DataFrame({'a': [1]})
        loader = MagicMock()
        loader.default_database.return_value = 'shop'

        with patch.object(block, 'fetch_input_variables',
                          return_value=([frame, frame], {}, [sql.uuid, python.uuid])):
            create_upstream_block_tables(loader, block, query=None)

        written = [call.args[1] for call in loader.export.call_args_list]
        self.assertEqual(written, [f'analytics.{python.table_name}'])

    def test_a_profile_with_an_unset_key_reads_its_own_tables(self):
        """
        DUCKDB_SCHEMA and REDSHIFT_CLUSTER_ID are optional: a profile without one differed
        from itself, and the upstream SQL table was read back and written over itself.
        """
        io_config = Path(self.repo_path) / 'io_config.yaml'
        original = io_config.read_text()
        self.addCleanup(io_config.write_text, original)
        io_config.write_text(original + (
            'redshift:\n  REDSHIFT_HOST: cluster.example.com\n  REDSHIFT_PORT: 5439\n'
            '  REDSHIFT_DBNAME: dev\n'
            'redshift_too:\n  REDSHIFT_HOST: cluster.example.com\n  REDSHIFT_PORT: 5439\n'
            '  REDSHIFT_DBNAME: dev\n'
            'redshift_other:\n  REDSHIFT_HOST: other.example.com\n  REDSHIFT_PORT: 5439\n'
            '  REDSHIFT_DBNAME: dev\n'
            'duckdb:\n  DUCKDB_DATABASE: shop.duckdb\n'
        ))
        keys = ['REDSHIFT_CLUSTER_ID', 'REDSHIFT_DBNAME', 'REDSHIFT_HOST', 'REDSHIFT_PORT',
                'REDSHIFT_SCHEMA']

        def redshift(profile, upstream=()):
            return self.block(
                f'rs_{profile}_{len(upstream)}', upstream=upstream,
                configuration=dict(data_provider='redshift', data_provider_profile=profile),
            )

        first = redshift('redshift')
        for profile, copies in [('redshift', False), ('redshift_too', False),
                                ('redshift_other', True)]:
            block = redshift(profile, [first])
            self.assertEqual(
                should_cache_data_from_upstream(block, first, ['data_provider'], keys), copies,
                profile,
            )
        duckdb = dict(data_provider='duckdb', data_provider_profile='duckdb')
        up = self.block('duck_up', configuration=duckdb)
        down = self.block('duck_down', configuration=duckdb, upstream=[up])
        self.assertFalse(should_cache_data_from_upstream(
            down, up, ['data_provider'], ['DUCKDB_DATABASE', 'DUCKDB_SCHEMA'],
        ))

    def test_a_downstream_block_leaves_the_upstream_table_as_it_is(self):
        """The copy went through pandas and wrote the upstream table over itself as DOUBLE."""
        import duckdb as duckdb_client

        database = Path(self.repo_path) / f'{self.pipeline.uuid}.duckdb'
        io_config = Path(self.repo_path) / 'io_config.yaml'
        original = io_config.read_text()
        self.addCleanup(io_config.write_text, original)
        io_config.write_text(original + f'shop_duckdb:\n  DUCKDB_DATABASE: {database}\n')
        duckdb = dict(data_provider='duckdb', data_provider_profile='shop_duckdb',
                      export_write_policy='replace')
        up = self.block('amounts', configuration=duckdb)
        Path(up.file_path).write_text(
            'SELECT n, (n * 1.25)::DECIMAL(10, 2) AS amount FROM range(5) t(n)',
        )
        down = self.block('total', configuration=duckdb, upstream=[up])
        Path(down.file_path).write_text('SELECT SUM(amount) AS total FROM {{ df_1 }}')
        self.pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)

        self.pipeline.get_block(up.uuid).execute_sync()
        self.pipeline.get_block(down.uuid).execute_sync()

        with duckdb_client.connect(str(database), read_only=True) as connection:
            column_type = connection.execute(
                'SELECT data_type FROM information_schema.columns '
                "WHERE table_name = ? AND column_name = 'amount'", [up.table_name],
            ).fetchone()[0]
        self.assertEqual(column_type, 'DECIMAL(10,2)')

    def test_a_copied_sql_upstream_is_named_where_the_copy_was_written(self):
        """
        An upstream SQL block on another database of the same kind is copied with the
        block's names; {{ df_1 }} named it with the upstream's schema.
        """
        from unittest.mock import MagicMock

        from mage_ai.data_preparation.models.block.sql import postgres

        io_config = Path(self.repo_path) / 'io_config.yaml'
        original = io_config.read_text()
        self.addCleanup(io_config.write_text, original)
        io_config.write_text(original + (
            'shop:\n  POSTGRES_DBNAME: shop\n  POSTGRES_HOST: db\n  POSTGRES_PORT: 5432\n'
            'finance:\n  POSTGRES_DBNAME: finance\n  POSTGRES_HOST: db\n  POSTGRES_PORT: 5432\n'
        ))
        upstream = self.block('ledger', configuration=dict(
            data_provider='postgres', data_provider_profile='finance',
            data_provider_schema='raw',
        ))
        same = self.block('same_db', configuration=dict(
            data_provider='postgres', data_provider_profile='finance',
        ), upstream=[upstream])
        block = self.block('report', configuration=dict(
            data_provider='postgres', data_provider_profile='shop',
        ), upstream=[upstream])
        loader = MagicMock()
        loader.default_database.return_value = 'shop'
        loader.default_schema.return_value = 'public'

        query = postgres.interpolate_input_data(block, 'SELECT * FROM {{ df_1 }}', loader)
        self.assertEqual(query, f'SELECT * FROM "shop".public.{upstream.table_name}')
        # On the upstream's own database, its table is read as it is, in its schema.
        query = postgres.interpolate_input_data(same, 'SELECT * FROM {{ df_1 }}', loader)
        self.assertEqual(query, f'SELECT * FROM "shop".raw.{upstream.table_name}')
