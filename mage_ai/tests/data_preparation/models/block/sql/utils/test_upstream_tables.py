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
