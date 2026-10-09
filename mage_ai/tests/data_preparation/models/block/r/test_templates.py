from mage_ai.data_preparation.models.constants import BlockLanguage, BlockType
from mage_ai.data_preparation.templates.template import fetch_template_source
from mage_ai.tests.base_test import TestCase


def template(block_type, **config):
    return fetch_template_source(block_type, config, language=BlockLanguage.R)


class RTemplateTest(TestCase):
    def test_default_templates_register_the_block_function(self):
        self.assertIn(
            '#* @data_loader\nload_data <- function(...)', template(BlockType.DATA_LOADER),
        )
        self.assertIn(
            '#* @transformer\ntransform <- function(df_1, ...)', template(BlockType.TRANSFORMER),
        )
        self.assertIn(
            '#* @data_exporter\nexport_data <- function(df_1, ...)',
            template(BlockType.DATA_EXPORTER),
        )
        for block_type in (BlockType.DATA_LOADER, BlockType.TRANSFORMER):
            self.assertIn('library(tidyverse)', template(block_type))
            self.assertIn('#* @test\noutput_has_rows <- function(output)', template(block_type))

    def test_database_templates(self):
        for database, driver in (('postgres', 'RPostgres'), ('mysql', 'RMariaDB'),
                                 ('duckdb', 'duckdb')):
            loader = template(BlockType.DATA_LOADER, data_source=database)
            exporter = template(BlockType.DATA_EXPORTER, data_source=database)

            self.assertIn(f'database = "{database}"', loader)
            self.assertIn('read_sql(', loader)
            self.assertIn(f'rv add {driver}', loader)
            self.assertIn('write_table(', exporter)

    def test_unknown_data_sources_get_the_default_template(self):
        self.assertEqual(
            template(BlockType.DATA_LOADER, data_source='bigquery'),
            template(BlockType.DATA_LOADER),
        )

    def test_transformers_with_a_data_source_stay_in_r(self):
        """A data source gave R transformers the Python template of the warehouse."""
        self.assertEqual(
            template(BlockType.TRANSFORMER, data_source='postgres'),
            template(BlockType.TRANSFORMER),
        )
