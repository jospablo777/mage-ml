"""The block templates as data: every record renders a block of its type, and the docs
page shows the coverage the records have (ADR-0026)."""
import asyncio
from pathlib import Path

from mage_ai.data_preparation.templates import catalog
from mage_ai.data_preparation.templates.constants import PYTHON_SOURCES, TEMPLATES
from mage_ai.tests.base_test import DBTestCase, TestCase

ROOT = Path(__file__).resolve().parents[3]


class TemplateCatalogTest(TestCase):
    def test_every_record_renders_a_block_of_its_type(self):
        for record in TEMPLATES:
            with self.subTest(template=f"{record['language']} {record['block_type']} "
                                       f"{record['name']}"):
                self.assertEqual(catalog.problems(record), [])

    def test_every_python_source_file_has_a_record_with_a_family(self):
        sources = {r.get('data_source') for r in TEMPLATES if r.get('data_source')}
        for folder in ('data_loaders', 'data_exporters', 'sensors'):
            for path in (Path(catalog.TEMPLATES_DIR) / folder).glob('*.py'):
                if path.stem == 'default':
                    continue
                listed = any(r['path'] == f'{folder}/{path.name}' for r in TEMPLATES)
                self.assertTrue(listed, f'{folder}/{path.name} has no record')
        self.assertTrue(sources <= set(PYTHON_SOURCES), sources - set(PYTHON_SOURCES))

    def test_a_record_without_a_function_of_its_type_is_reported(self):
        record = dict(TEMPLATES[0], path='data_loaders/default.jinja')
        self.assertEqual(catalog.problems(record), [])
        broken = dict(record, block_type='data_exporter', name='Broken')
        [problem] = catalog.problems(broken)
        self.assertIn('no function decorated with @data_exporter', problem)
        missing = dict(record, path='data_loaders/nothing.py')
        self.assertIn('does not exist', catalog.problems(missing)[0])

    def test_the_docs_page_shows_the_records_coverage(self):
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            'block_templates', ROOT / 'scripts' / 'docs' / 'block_templates.py',
        )
        script = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(script)
        text = script.PAGE.read_text()
        self.assertEqual(
            text, script.written(text, catalog.coverage_markdown()),
            'The coverage table is out of date: run scripts/docs/block_templates.py.',
        )


class BlockTemplateApiTest(DBTestCase):
    def test_the_api_lists_python_sources_with_their_family(self):
        from mage_ai.api.operations.base import BaseOperation
        from mage_ai.api.operations.constants import OperationType
        from mage_ai.tests.factory import create_user

        result = asyncio.run(BaseOperation(
            action=OperationType.LIST, resource='block_templates',
            user=create_user(_owner=True),
        ).execute())
        qdrant = [t for t in result['block_templates']
                  if t.get('data_source') == 'qdrant' and t['block_type'] == 'data_loader']
        self.assertEqual(len(qdrant), 1)
        self.assertEqual((qdrant[0]['name'], qdrant[0]['groups']),
                         ('Qdrant', ['Vector databases']))
