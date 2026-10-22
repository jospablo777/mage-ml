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
        self.file = Path(self.repo_path) / definitions.LEGACY_FILE
        self.services = Path(self.repo_path) / definitions.SERVICES_DIR
        self.addCleanup(lambda: self.file.unlink(missing_ok=True))
        self.addCleanup(shutil.rmtree, self.services, True)
        gitignore = Path(self.repo_path) / '.gitignore'
        before = gitignore.read_text() if gitignore.exists() else None
        self.addCleanup(lambda: gitignore.write_text(before) if before is not None
                        else gitignore.unlink(missing_ok=True))

    def service_yaml(self, name: str) -> dict:
        return yaml.safe_load((self.services / name / 'service.yaml').read_text())

    def test_services_are_saved_renamed_and_deleted_keeping_the_others(self):
        definitions.save_definition(self.repo_path, 'first', dict(pipelines=['a', 'b']))
        definitions.save_definition(self.repo_path, 'second', dict(
            pipelines=['b', 'b', 'c'],
            max_runs=2,
        ))

        saved = definitions.load_definitions(self.repo_path)
        self.assertEqual(saved['second']['pipelines'], ['b', 'c'])
        self.assertEqual(saved['second']['max_runs'], 2)
        written = (self.services / 'first' / 'service.yaml').read_text()
        self.assertTrue(written.startswith('# A MageML service.'))
        self.assertEqual(self.service_yaml('first'), dict(name='first', pipelines=['a', 'b']))
        self.assertEqual(definitions.location(self.repo_path, 'first'),
                         'services/first/service.yaml')

        definitions.scaffold(self.repo_path, 'first')
        definitions.save_definition(
            self.repo_path, 'renamed', saved['first'], previous_name='first',
        )
        self.assertEqual(sorted(definitions.load_definitions(self.repo_path)),
                         ['renamed', 'second'])
        # The folder moved with its environments.
        self.assertFalse((self.services / 'first').exists())
        self.assertTrue((self.services / 'renamed' / 'environments' / 'dev.yaml').is_file())
        self.assertEqual(self.service_yaml('renamed')['name'], 'renamed')

        self.assertTrue(definitions.delete_definition(self.repo_path, 'renamed'))
        self.assertEqual(definitions.names(self.repo_path), ['second'])
        self.assertFalse((self.services / 'renamed').exists())

    def test_the_earlier_file_is_read_and_a_saved_service_moves_to_its_folder(self):
        self.file.write_text(yaml.safe_dump(dict(services=dict(
            old=dict(pipelines=['a']), other=dict(pipelines=['b']),
        ))))

        self.assertEqual(definitions.names(self.repo_path), ['old', 'other'])
        self.assertEqual(definitions.location(self.repo_path, 'old'), 'pipeline_services.yaml')

        definitions.save_definition(self.repo_path, 'old', dict(pipelines=['a', 'c']))

        self.assertEqual(self.service_yaml('old')['pipelines'], ['a', 'c'])
        self.assertEqual(list(yaml.safe_load(self.file.read_text())['services']), ['other'])

        self.assertEqual(definitions.migrate(self.repo_path), ['other'])
        self.assertFalse(self.file.exists())
        self.assertEqual(definitions.names(self.repo_path), ['old', 'other'])

    def test_a_folder_and_its_name_must_match(self):
        folder = self.services / 'shop'
        folder.mkdir(parents=True)
        (folder / 'service.yaml').write_text('name: store\npipelines: [a]\n')

        with self.assertRaisesRegex(definitions.DefinitionError,
                                    'says name: store, but its folder is shop'):
            definitions.load_definitions(self.repo_path)

    def test_generated_builds_and_local_secrets_stay_out_of_git(self):
        definitions.save_definition(self.repo_path, 'first', dict(pipelines=['a']))
        definitions.save_definition(self.repo_path, 'second', dict(pipelines=['a']))

        lines = (Path(self.repo_path) / '.gitignore').read_text().splitlines()
        self.assertEqual(lines.count('.mage/build/'), 1)
        self.assertEqual(lines.count('services/*/.env*'), 1)

    def test_a_project_with_its_own_ignores_gets_only_what_it_misses(self):
        folder = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, folder, True)
        (folder / '.gitignore').write_text('secrets/\n.mage/build/')

        definitions.ensure_gitignore(str(folder))
        definitions.ensure_gitignore(str(folder))

        lines = (folder / '.gitignore').read_text().splitlines()
        self.assertEqual(lines[:2], ['secrets/', '.mage/build/'])
        # Development runs from the editor write under .mage/dev/ (I-204).
        for line in definitions.GITIGNORE_LINES:
            self.assertEqual(lines.count(line), 1, line)
        self.assertIn('.mage/dev/', lines)

    def test_a_delete_keeps_files_the_user_added(self):
        definitions.save_definition(self.repo_path, 'keep', dict(pipelines=['a']))
        definitions.scaffold(self.repo_path, 'keep')
        (self.services / 'keep' / 'README.md').write_text('notes')

        definitions.delete_definition(self.repo_path, 'keep')

        self.assertEqual(sorted(p.name for p in (self.services / 'keep').iterdir()),
                         ['README.md'])

    def test_models_and_their_registry_are_saved_with_the_service(self):
        definitions.save_definition(self.repo_path, 'scoring', dict(
            pipelines=['a'],
            model_registry='http://mlflow:5000',
            models=[dict(name='churn', uri='models:/churn-scorer@champion')],
        ))

        saved = definitions.load_definition(self.repo_path, 'scoring')
        self.assertEqual(saved['model_registry'], 'http://mlflow:5000')
        self.assertEqual(saved['models'], [dict(name='churn', uri='models:/churn-scorer@champion')])
        written = self.service_yaml('scoring')
        self.assertEqual(written['models'][0]['uri'], 'models:/churn-scorer@champion')

    def test_a_models_challengers_and_sample_are_saved_with_it(self):
        definitions.save_definition(self.repo_path, 'scoring', dict(
            pipelines=['a'],
            models=[
                dict(name='churn', uri='models:/churn@champion',
                     challengers=[dict(name='v4', uri='models:/churn/4')],
                     shadow=dict(sample=0.25)),
                dict(name='fraud', uri='models:/fraud/2',
                     challengers=[dict(name='next', uri='models:/fraud@next')]),
            ],
        ))

        saved = definitions.load_definition(self.repo_path, 'scoring')
        churn, fraud = saved['models']
        self.assertEqual(churn['challengers'], [dict(name='v4', uri='models:/churn/4')])
        self.assertEqual(churn['shadow'], dict(sample=0.25))
        self.assertEqual(fraud['shadow'], dict(sample=definitions.DEFAULT_SHADOW_SAMPLE))
        from mage_ai.pipeline_services.exports import (
            audit_settings,
            challenger_settings,
        )
        self.assertEqual(challenger_settings(saved['models'])['churn'], dict(
            sample=0.25, challengers=[dict(name='v4', uri='models:/churn/4')]))
        self.assertEqual(audit_settings(saved['models']), {})

    def test_a_models_audit_settings_are_saved_with_it(self):
        definitions.save_definition(self.repo_path, 'scoring', dict(pipelines=['a'], models=[
            dict(name='churn', uri='models:/churn/1',
                 audit=dict(mask=['ssn'], reasons=3)),
        ]))
        [churn] = definitions.load_definition(self.repo_path, 'scoring')['models']
        self.assertEqual(churn['audit'], dict(features='values', mask=['ssn'], hash=[],
                                              reasons=3))

    def test_alerts_are_saved_with_the_service(self):
        definitions.save_definition(self.repo_path, 'scoring', dict(pipelines=['a'], alerts=dict(
            channels=[dict(kind='slack', url_env='SLACK_WEBHOOK_URL')], run_minutes=30,
        )))
        alerts = definitions.load_definition(self.repo_path, 'scoring')['alerts']
        self.assertEqual(alerts['channels'], [dict(kind='slack', url_env='SLACK_WEBHOOK_URL')])
        self.assertEqual(alerts['on'], list(definitions.ALERT_KINDS))
        self.assertEqual(alerts['run_minutes'], 30)
        self.assertEqual(self.service_yaml('scoring')['alerts'], alerts)

    def test_alerts_go_by_email_through_a_variable_holding_the_smtp_url(self):
        definitions.save_definition(self.repo_path, 'mailed', dict(pipelines=['a'], alerts=dict(
            channels=[dict(kind='email', url_env='ALERT_SMTP_URL')],
        )))
        alerts = definitions.load_definition(self.repo_path, 'mailed')['alerts']
        self.assertEqual(alerts['channels'], [dict(kind='email', url_env='ALERT_SMTP_URL')])

    def test_a_page_save_keeps_the_settings_the_page_does_not_edit(self):
        """The Services page sends models as name and uri; challengers and audit stayed out."""
        from mage_ai.api.resources.PipelineServiceResource import (
            FIELDS,
            _keep_model_settings,
        )

        earlier = [dict(name='churn', uri='models:/churn/1', audit=dict(reasons=3),
                        challengers=[dict(name='v2', uri='models:/churn/2')],
                        shadow=dict(sample=0.5))]
        kept = _keep_model_settings([dict(name='churn', uri='models:/churn/3'),
                                     dict(name='new', uri='models:/new/1')], earlier)
        self.assertEqual(kept[0], dict(earlier[0], uri='models:/churn/3'))
        self.assertEqual(kept[1], dict(name='new', uri='models:/new/1'))
        self.assertIn('alerts', FIELDS)

    def test_an_environment_builds_for_the_services_platforms_unless_it_names_its_own(self):
        from mage_ai.pipeline_services import local_run

        definitions.save_definition(self.repo_path, 'shop', dict(
            pipelines=['a'], platforms=['linux/arm64']))
        folder = Path(self.repo_path) / 'services' / 'shop' / definitions.ENVIRONMENTS_DIR
        folder.mkdir(parents=True, exist_ok=True)
        (folder / 'prod.yaml').write_text('target: kubernetes\n')
        (folder / 'edge.yaml').write_text('target: kubernetes\nplatforms: [linux/amd64]\n')
        self.assertEqual(local_run.environment_settings(self.repo_path, 'shop', 'prod')
                         ['platforms'], ['linux/arm64'])
        self.assertEqual(local_run.environment_settings(self.repo_path, 'shop', 'edge')
                         ['platforms'], ['linux/amd64'])

    def test_bad_settings_say_what_to_fix(self):
        for value, message in (
            (dict(pipelines='a'), 'pipelines must be a list'),
            (dict(pipelines=['a'], max_runs=0), 'max_runs must be'),
            (dict(pipelines=['a'], include='models'), 'include must be a list'),
            (dict(pipelines=['a'], models=[dict(name='churn')]), 'models must be a list'),
            (dict(pipelines=['a'], model_registry=5), 'model_registry must be the address'),
            (dict(pipelines=['a'], models=[dict(name='m', uri='u', challengers=[
                dict(name='bad name', uri='x')])]), 'challengers must be a list'),
            (dict(pipelines=['a'], models=[dict(name='m', uri='u', challengers=[
                dict(name='c', uri='x'), dict(name='c', uri='y')])]), 'same name'),
            (dict(pipelines=['a'], models=[dict(name='m', uri='u', challengers=[
                dict(name='c', uri='x')], shadow=dict(sample=2))]), 'shadow.sample must be'),
            (dict(pipelines=['a'], models=[dict(name='m', uri='u', audit=dict(
                features='all'))]), 'audit.features is'),
            (dict(pipelines=['a'], models=[dict(name='m', uri='u', audit=dict(
                reasons=-1))]), 'audit.reasons is'),
            (dict(pipelines=['a'], alerts=dict(channels=[])), 'at least one channel'),
            (dict(pipelines=['a'], platforms='linux/amd64'), 'platforms must be a list'),
            (dict(pipelines=['a'], platforms=['linux/arm/v7']), 'publish no builds'),
            (dict(pipelines=['a'], alerts=dict(channels=[dict(kind='sms', url_env='X')])),
             'a channel kind is one of'),
            (dict(pipelines=['a'], alerts=dict(channels=[dict(
                kind='slack', url='https://hooks.slack.com/x')])), 'not the URL'),
            (dict(pipelines=['a'], alerts=dict(on=['always'], channels=[dict(
                kind='slack', url_env='X')])), 'on lists alerts'),
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

    def test_a_landing_time_is_named_in_the_notes(self):
        """The service starts a time trigger's runs at each interval: no landing time."""
        from mage_ai.data_preparation.models.triggers import (
            ScheduleStatus,
            ScheduleType,
        )
        from mage_ai.orchestration.db.models.schedules import PipelineSchedule

        self.chain()
        for name, landing in [('landing', True), ('plain', False)]:
            schedule = PipelineSchedule.create(
                name=name,
                pipeline_uuid=self.pipeline.uuid,
                repo_path=self.repo_path,
                schedule_interval='@weekly',
                schedule_type=ScheduleType.TIME,
                settings=dict(landing_time_enabled=landing),
                status=ScheduleStatus.ACTIVE,
            )
            self.addCleanup(schedule.delete)

        captured = export.capture(self.repo_path, [self.pipeline.uuid])

        triggers = captured.manifest['pipelines'][0]['triggers']
        self.assertEqual([t['schedule'] for t in triggers], ['@weekly', '@weekly'])
        notes = '\n'.join(captured.notes)
        self.assertIn('trigger landing: its landing time is not exported', notes)
        self.assertNotIn('trigger plain', notes)

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
        self.assertEqual(listed[0]['path'], 'services/shop-live/service.yaml')
        # The services that run a pipeline, for its pages.
        using = self.call(OperationType.LIST, query=dict(pipeline=[self.pipeline.uuid]))
        self.assertEqual([s['name'] for s in using['pipeline_services']], ['shop-live'])
        other = self.call(OperationType.LIST, query=dict(pipeline=['another']))
        self.assertEqual(other['pipeline_services'], [])

        # Where the image runs, saved with the service; alerts too, through the API.
        targeted = self.call(OperationType.UPDATE, pk='shop-live', payload=dict(
            pipeline_service=dict(
                platforms=['linux/amd64', 'linux/arm64'],
                alerts=dict(channels=[dict(kind='slack', url_env='SLACK_WEBHOOK_URL')]),
            ),
        ))['pipeline_service']
        self.assertEqual(targeted['platforms'], ['linux/amd64', 'linux/arm64'])
        refused = self.call(OperationType.UPDATE, pk='shop-live', payload=dict(
            pipeline_service=dict(platforms=['linux/s390x']),
        ))
        self.assertIn('Polars, pyarrow and the engine', refused['error']['message'])

        denied = self.call(OperationType.DELETE, pk='shop-live', user=create_user())
        self.assertEqual(denied['error']['code'], 403)
        self.call(OperationType.DELETE, pk='shop-live')
        self.assertEqual(self.call(OperationType.LIST)['pipeline_services'], [])
        self.assertEqual(self.call(OperationType.DETAIL, pk='shop-live')['error']['code'], 404)
