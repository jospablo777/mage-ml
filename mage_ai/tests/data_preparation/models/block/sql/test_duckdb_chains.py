"""
Compiled chains of DuckDB SQL blocks: in a fused stage, a block that replaces its table
and is read only by the next SQL block of its profile becomes a view, and the block that
needs its rows writes its table from one query DuckDB plans across the views.
"""
from pathlib import Path

import duckdb

from mage_ai.data_preparation.models.block import Block, plans
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import fusion
from mage_ai.tests.base_test import DBTestCase

PROFILE = 'chains_duckdb'


class DuckDBChainTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        self.database = Path(self.repo_path) / f'{self.pipeline.uuid}.duckdb'
        io_config = Path(self.repo_path) / 'io_config.yaml'
        previous = io_config.read_text() if io_config.exists() else None
        io_config.write_text(f'{PROFILE}:\n  DUCKDB_DATABASE: {self.database}\n')
        self.addCleanup(
            lambda: io_config.write_text(previous) if previous is not None
            else io_config.unlink(),
        )

    def sql(self, name, kind, query, upstream=(), policy='replace'):
        block = Block.create(
            f'{self.pipeline.uuid}_{name}', kind, self.repo_path, pipeline=self.pipeline,
            language='sql', upstream_block_uuids=[b.uuid for b in upstream],
            configuration=dict(
                data_provider='duckdb',
                data_provider_profile=PROFILE,
                data_provider_schema='main',
                export_write_policy=policy,
            ),
        )
        Path(block.file_path).write_text(query)
        self.pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        return self.pipeline.get_block(block.uuid)

    def chain(self, transform_policy='replace'):
        load = self.sql('load', 'data_loader', 'SELECT range AS n FROM range(10)')
        double = self.sql('double', 'transformer',
                          'SELECT n * 2 AS d FROM {{ df_1 }} WHERE n > 2', [load],
                          policy=transform_policy)
        total = self.sql('total', 'data_exporter', 'SELECT sum(d) AS total FROM {{ df_1 }}',
                         [double])
        return load, double, total

    def run_stage(self, *blocks):
        pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        fusion._run_stage_in_process(
            pipeline, [b.uuid for b in blocks], {}, run_tests=False, update_status=False,
        )

    def relations(self):
        with duckdb.connect(str(self.database), read_only=True) as connection:
            return dict(connection.execute(
                "SELECT table_name, table_type FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall())

    def total(self, block):
        with duckdb.connect(str(self.database), read_only=True) as connection:
            return connection.execute(f'SELECT total FROM main.{block.table_name}').fetchone()[0]

    def test_a_chain_becomes_views_and_the_exporter_writes_its_table(self):
        load, double, total = self.chain()
        self.assertTrue(plans.chains_with(load, double))
        self.assertTrue(plans.chains_with(double, total))

        self.run_stage(load, double, total)

        self.assertEqual(self.total(total), sum(n * 2 for n in range(3, 10)))
        relations = self.relations()
        self.assertEqual(relations[load.table_name], 'VIEW')
        self.assertEqual(relations[double.table_name], 'VIEW')
        self.assertEqual(relations[total.table_name], 'BASE TABLE')
        output = self.pipeline.variable_manager.get_variable(
            self.pipeline.uuid, double.uuid, 'output_0',
        )
        self.assertIn('Compiled into a view', output[plans.VIEW_KEY]['note'])

        # Run again, compiled: the views are replaced and the total is the same.
        self.run_stage(load, double, total)
        self.assertEqual(self.total(total), 84)

        # Run in the editor, the block writes its table again in place of its view.
        self.pipeline.get_block(double.uuid).execute_sync(from_notebook=True, run_all_blocks=True)
        self.assertEqual(self.relations()[double.table_name], 'BASE TABLE')
        # And compiled again, it is a view again.
        self.run_stage(load, double, total)
        self.assertEqual(self.relations()[double.table_name], 'VIEW')
        self.assertEqual(self.total(total), 84)

    def test_a_block_that_appends_keeps_its_table(self):
        load, double, total = self.chain(transform_policy='append')
        self.assertFalse(plans.chains_with(double, total))
        self.assertIn('appends to its table', plans.sql_view_problem(double))

        self.run_stage(load, double, total)
        self.run_stage(load, double, total)

        relations = self.relations()
        self.assertEqual(relations[load.table_name], 'VIEW')
        self.assertEqual(relations[double.table_name], 'BASE TABLE')
        # The appended rows are what the exporter reads, as without compiling.
        self.assertEqual(self.total(total), 2 * 84)

    def test_the_compile_summary_lists_chains_and_blocks_left_out(self):
        from mage_ai.orchestration import compilation

        load, double, total = self.chain(transform_policy='append')
        pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        summary = compilation.summary_for(pipeline)

        self.assertEqual(summary['sql_chains'],
                         [dict(blocks=[load.uuid, double.uuid], views=[load.uuid])])
        [left_out] = summary['sql_left_out']
        self.assertEqual(left_out['block'], double.uuid)
        self.assertIn('set its write policy to replace', left_out['reason'])

    def test_postgres_blocks_chain_and_blocks_of_two_databases_do_not(self):
        def configured(block, provider, profile=PROFILE):
            block.configuration = dict(
                block.configuration, data_provider=provider, data_provider_profile=profile,
            )
            return block

        load, double, total = self.chain()
        configured(load, 'postgres')
        configured(double, 'postgres')
        self.assertTrue(plans.chains_with(load, double))
        # A DuckDB block reads PostgreSQL views it cannot see.
        self.assertFalse(plans.chains_with(double, total))
        configured(total, 'postgres', 'another')
        self.assertFalse(plans.chains_with(double, total))
        configured(total, 'mysql')
        self.assertIn('supports DuckDB and PostgreSQL', plans.sql_view_problem(total))
