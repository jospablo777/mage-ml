import json
from pathlib import Path
from unittest import TestCase

from mage_ai.api.contracts import openapi

SPEC = Path(__file__).resolve().parents[4] / 'contracts' / 'openapi.json'
OUTDATED = 'contracts/openapi.json differs from the code: run scripts/contracts/record.py ' \
    'and commit the file.'


class OpenApiTest(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.recorded = json.loads(SPEC.read_text())
        cls.static = openapi.static_spec()

    def test_the_checked_in_paths_and_operations_are_current(self):
        self.assertTrue(
            openapi.skeleton(self.recorded) == openapi.skeleton(self.static), OUTDATED,
        )
        self.assertTrue(
            self.recorded['x-mage-resources'] == self.static['x-mage-resources'], OUTDATED,
        )

    def test_every_resource_has_a_schema_or_says_why_not(self):
        schemas = self.recorded['components']['schemas']
        for resource, about in self.recorded['x-mage-resources'].items():
            with self.subTest(resource=resource):
                self.assertIn(about['schema'], schemas)
                recorded = any(
                    'type' in schema for schema in schemas[about['schema']]['properties'].values()
                )
                self.assertTrue(recorded or resource in self.recorded['x-mage-unrecorded'])

    def test_pipelines_are_recorded_from_a_response(self):
        pipeline = self.recorded['components']['schemas']['Pipeline']['properties']
        self.assertEqual(pipeline['uuid']['type'], 'string')
        self.assertEqual(pipeline['blocks']['type'], 'array')
        paths = self.recorded['paths']
        self.assertEqual(
            sorted(m for m in paths['/api/pipelines/{id}'] if m != 'parameters'),
            ['delete', 'get', 'put'],
        )
        self.assertIn('get', paths['/api/pipelines/{parent_id}/blocks'])
        self.assertIn('post', paths['/api/runs'])

    def test_the_recorded_file_holds_no_local_path(self):
        text = SPEC.read_text()
        for marker in ('/Users/', '/home/', '/private/', 'mage-contracts-', 'Traceback'):
            self.assertNotIn(marker, text)

    def test_schemas_merge_the_types_they_see(self):
        merged = openapi.merge(
            openapi.schema_of(dict(a=1, b=[dict(c='x')], d=None)),
            openapi.schema_of(dict(a=None, b=[], e=True)),
        )
        self.assertEqual(merged['properties']['a']['type'], ['integer', 'null'])
        self.assertEqual(merged['properties']['b']['items']['properties']['c'],
                         dict(type='string'))
        self.assertEqual(merged['properties']['d'], dict(type='null'))
        self.assertEqual(merged['properties']['e'], dict(type='boolean'))

    def test_frontend_pairs_are_read_from_the_api_module(self):
        pairs = openapi.frontend_pairs()
        self.assertIn(('blocks', 'pipelines'), pairs)
        self.assertIn(('blocks', 'pipelines', 'analyses'), pairs)
        self.assertNotIn(('pipelines',), pairs)

    def test_route_patterns_become_openapi_paths(self):
        self.assertEqual(
            openapi._openapi_path(r'/api/runs/(?P<token>\w+)$'), '/api/runs/{token}',
        )
        self.assertEqual(
            openapi._openapi_path(
                r'/api/pipelines/(?P<pipeline_uuid>\w+)/blocks/(?P<block_uuid>[\w\-\%2f\.]+)'
                r'/analyses$',
            ),
            '/api/pipelines/{pipeline_uuid}/blocks/{block_uuid}/analyses',
        )
        self.assertEqual(openapi._openapi_path(r'/api/status(?:es)?$'), '/api/status')
