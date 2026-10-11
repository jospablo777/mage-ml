"""
Services saved in the project (pipeline_services.yaml) and the after-run triggers that
start one pipeline after another's runs: saved and read back, checked for loops and unknown
pipelines, written as after-run triggers, and exported from the CLI with --service.
"""
import asyncio
import json
import shutil
import tempfile
from pathlib import Path

import yaml

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.pipeline_services import definitions, export
from mage_ai.tests.factory import create_user
from mage_ai.tests.pipeline_services.test_export import ExportTestCase


class DefinitionsTest(ExportTestCase):
    def setUp(self):
        super().setUp()
        self.file = Path(self.repo_path) / definitions.FILE_NAME
        self.addCleanup(lambda: self.file.unlink(missing_ok=True))

    def test_services_are_saved_renamed_and_deleted_keeping_the_others(self):
        definitions.save_definition(self.repo_path, 'first', dict(pipelines=['a', 'b']))
        definitions.save_definition(self.repo_path, 'second', dict(
            pipelines=['b', 'b', 'c'],
            after_runs=[dict(run='c', after='b')],
            max_runs=2,
        ))

        saved = definitions.load_definitions(self.repo_path)
        self.assertEqual(saved['second']['pipelines'], ['b', 'c'])
        self.assertEqual(saved['second']['after_runs'],
                         [dict(after='b', run='c', when='completed')])
        written = self.file.read_text()
        self.assertTrue(written.startswith('# Services exported from this project'))
        self.assertNotIn('include', yaml.safe_load(written)['services']['first'])

        definitions.save_definition(
            self.repo_path, 'renamed', saved['first'], previous_name='first',
        )
        self.assertEqual(sorted(definitions.load_definitions(self.repo_path)),
                         ['renamed', 'second'])
        self.assertTrue(definitions.delete_definition(self.repo_path, 'renamed'))
        self.assertEqual(definitions.names(self.repo_path), ['second'])

    def test_bad_settings_say_what_to_fix(self):
        for value, message in (
            (dict(pipelines='a'), 'pipelines must be a list'),
            (dict(pipelines=['a'], max_runs=0), 'max_runs must be'),
            (dict(pipelines=['a'], after_runs=['a']), 'after_runs must be a list'),
        ):
            with self.subTest(message=message):
                with self.assertRaisesRegex(definitions.DefinitionError, message):
                    definitions.save_definition(self.repo_path, 'svc', value)
        with self.assertRaisesRegex(definitions.DefinitionError, 'not a service name'):
            definitions.save_definition(self.repo_path, 'Bad Name', dict(pipelines=['a']))
        with self.assertRaisesRegex(definitions.DefinitionError, 'has no service nope'):
            definitions.load_definition(self.repo_path, 'nope')

    def test_after_run_problems_name_unknown_pipelines_endings_and_loops(self):
        problems = export.after_run_problems(['a', 'b', 'c'], [
            dict(run='b', after='a'),
            dict(run='c', after='b'),
            dict(run='a', after='c'),
            dict(run='x', after='a'),
            dict(run='b', after='a', when='sometimes'),
            dict(run='a', after='a'),
        ])
        text = '\n'.join(problems)
        self.assertIn('runs pipeline x, which is not in the service', text)
        self.assertIn("starts on 'sometimes'", text)
        self.assertIn('cannot start itself', text)
        self.assertIn('in a loop: a -> b -> c -> a', text)
        self.assertEqual(export.after_run_problems(['a', 'b'], [dict(run='b', after='a')]), [])

    def test_an_after_run_start_becomes_an_after_run_trigger_of_the_pipeline_it_starts(self):
        self.chain()
        follower = self.pipeline
        self.pipeline = self.create_pipeline('follower_pipeline')
        self.chain()
        first = follower.uuid

        captured = export.capture(
            self.repo_path, [first, self.pipeline.uuid],
            after_runs=[dict(run=self.pipeline.uuid, after=first, when='any')],
        )

        entry = next(p for p in captured.manifest['pipelines'] if p['uuid'] == self.pipeline.uuid)
        [trigger] = [t for t in entry['triggers'] if t['kind'] == 'after_run']
        self.assertEqual(trigger['after_pipeline'], first)
        self.assertEqual(trigger['after_status'], 'any')
        self.assertIn(f'after_run after {first} ends', export.report(captured))
        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)
        export.write(captured, str(out / 'svc'))
        self.assertIn(f'(after {first} ends)', (out / 'svc' / 'README.md').read_text())
        manifest = json.loads((out / 'svc' / 'service.json').read_text())
        written = next(p for p in manifest['pipelines'] if p['uuid'] == self.pipeline.uuid)
        self.assertEqual(written['triggers'][-1]['kind'], 'after_run')

        with self.assertRaisesRegex(export.ExportError, 'not in the service'):
            export.capture(self.repo_path, [first],
                           after_runs=[dict(run='elsewhere', after=first)])

    def create_pipeline(self, name):
        from mage_ai.data_preparation.models.pipeline import Pipeline

        pipeline = Pipeline.create(name, repo_path=self.repo_path)
        self.addCleanup(pipeline.delete)
        return pipeline

    def test_the_cli_exports_a_saved_service(self):
        from typer.testing import CliRunner

        from mage_ai.cli.main import app

        self.chain()
        definitions.save_definition(self.repo_path, 'saved-shop', dict(
            pipelines=[self.pipeline.uuid], max_runs=3,
        ))
        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)

        result = CliRunner().invoke(app, [
            'export', 'service', self.repo_path, '--service', 'saved-shop',
            '--out', str(out / 'svc'),
        ])

        self.assertEqual(result.exit_code, 0, result.output)
        manifest = json.loads((out / 'svc' / 'service.json').read_text())
        self.assertEqual(manifest['service']['name'], 'saved-shop')
        self.assertEqual(manifest['pipelines'][0]['max_concurrent_runs'], 3)

        missing = CliRunner().invoke(app, [
            'export', 'service', self.repo_path, '--service', 'nope', '--out', str(out / 'x'),
        ])
        self.assertEqual(missing.exit_code, 1)
        self.assertIn('has no service nope', missing.output)


class ServicesApiTest(ExportTestCase):
    def setUp(self):
        super().setUp()
        self.file = Path(self.repo_path) / definitions.FILE_NAME
        self.addCleanup(lambda: self.file.unlink(missing_ok=True))
        self.owner = create_user(_owner=True)

    def call(self, action, user=None, **kwargs):
        return asyncio.run(BaseOperation(
            action=action, resource='pipeline_services', user=user or self.owner, **kwargs,
        ).execute())

    def test_services_are_created_listed_changed_and_deleted(self):
        created = self.call(OperationType.CREATE, payload=dict(pipeline_service=dict(
            name='shop', pipelines=[self.pipeline.uuid],
        )))['pipeline_service']
        self.assertEqual(created['pipelines'], [self.pipeline.uuid])
        self.assertIsNone(created['exported_at'])

        duplicate = self.call(OperationType.CREATE, payload=dict(pipeline_service=dict(
            name='shop', pipelines=[],
        )))
        self.assertEqual(duplicate['error']['code'], 400)

        changed = self.call(OperationType.UPDATE, pk='shop', payload=dict(pipeline_service=dict(
            name='shop-live', max_runs=4,
        )))['pipeline_service']
        self.assertEqual((changed['name'], changed['max_runs']), ('shop-live', 4))
        self.assertEqual(changed['pipelines'], [self.pipeline.uuid])

        listed = self.call(OperationType.LIST)['pipeline_services']
        self.assertEqual([s['name'] for s in listed], ['shop-live'])

        denied = self.call(OperationType.DELETE, pk='shop-live', user=create_user())
        self.assertEqual(denied['error']['code'], 403)
        self.call(OperationType.DELETE, pk='shop-live')
        self.assertEqual(self.call(OperationType.LIST)['pipeline_services'], [])
        self.assertEqual(self.call(OperationType.DETAIL, pk='shop-live')['error']['code'], 404)
