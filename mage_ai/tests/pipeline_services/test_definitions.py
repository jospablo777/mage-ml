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
            max_runs=2,
        ))

        saved = definitions.load_definitions(self.repo_path)
        self.assertEqual(saved['second']['pipelines'], ['b', 'c'])
        self.assertEqual(saved['second']['max_runs'], 2)
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

    def test_models_and_their_registry_are_saved_with_the_service(self):
        definitions.save_definition(self.repo_path, 'scoring', dict(
            pipelines=['a'],
            model_registry='http://mlflow:5000',
            models=[dict(name='churn', uri='models:/churn-scorer@champion')],
        ))

        saved = definitions.load_definition(self.repo_path, 'scoring')
        self.assertEqual(saved['model_registry'], 'http://mlflow:5000')
        self.assertEqual(saved['models'], [dict(name='churn', uri='models:/churn-scorer@champion')])
        written = yaml.safe_load(self.file.read_text())['services']['scoring']
        self.assertEqual(written['models'][0]['uri'], 'models:/churn-scorer@champion')

    def test_bad_settings_say_what_to_fix(self):
        for value, message in (
            (dict(pipelines='a'), 'pipelines must be a list'),
            (dict(pipelines=['a'], max_runs=0), 'max_runs must be'),
            (dict(pipelines=['a'], include='models'), 'include must be a list'),
            (dict(pipelines=['a'], models=[dict(name='churn')]), 'models must be a list'),
            (dict(pipelines=['a'], model_registry=5), 'model_registry must be the address'),
            (dict(pipelines=['a'], model_registry='mlflow:5000'), 'such as http://mlflow:5000'),
        ):
            with self.subTest(message=message):
                with self.assertRaisesRegex(definitions.DefinitionError, message):
                    definitions.save_definition(self.repo_path, 'svc', value)
        with self.assertRaisesRegex(definitions.DefinitionError, 'not a service name'):
            definitions.save_definition(self.repo_path, 'Bad Name', dict(pipelines=['a']))
        with self.assertRaisesRegex(definitions.DefinitionError, 'has no service nope'):
            definitions.load_definition(self.repo_path, 'nope')

    def after_run(self, pipeline_uuid, after, status='completed'):
        from mage_ai.data_preparation.models.triggers import (
            ScheduleStatus,
            ScheduleType,
        )
        from mage_ai.orchestration.db.models.schedules import PipelineSchedule

        schedule = PipelineSchedule.create(
            name=f'after_{after}',
            pipeline_uuid=pipeline_uuid,
            repo_path=self.repo_path,
            schedule_type=ScheduleType.AFTER_RUN,
            settings=dict(after_pipeline=after, after_status=status),
            status=ScheduleStatus.ACTIVE,
        )
        self.addCleanup(schedule.delete)
        return schedule

    def test_after_run_triggers_go_into_the_service_with_the_pipeline_they_follow(self):
        self.chain()
        first = self.pipeline.uuid
        self.pipeline = self.create_pipeline('follower_pipeline')
        self.chain()
        self.after_run(self.pipeline.uuid, first, 'any')

        captured = export.capture(self.repo_path, [first, self.pipeline.uuid])

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

        # Without the pipeline it follows, the trigger could never start: it is left out.
        alone = export.capture(self.repo_path, [self.pipeline.uuid])
        self.assertEqual(alone.manifest['pipelines'][0]['triggers'], [])
        self.assertIn(f'it follows {first}, which is not in the service', '\n'.join(alone.notes))

    def test_after_run_triggers_in_a_loop_stop_the_export(self):
        self.chain()
        first = self.pipeline.uuid
        self.pipeline = self.create_pipeline('looping_pipeline')
        self.chain()
        self.after_run(self.pipeline.uuid, first)
        self.after_run(first, self.pipeline.uuid)

        with self.assertRaisesRegex(export.ExportError, 'in a loop'):
            export.capture(self.repo_path, [first, self.pipeline.uuid])

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
