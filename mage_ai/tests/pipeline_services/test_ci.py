"""
mage service ci: the workflow it writes (checks on pull requests, deploys by rule with each
service's secrets), what it refuses, and the commands the workflow runs.
"""
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

import yaml

from mage_ai.pipeline_services import ci
from mage_ai.tests.base_test import TestCase


class CiWorkflowTest(TestCase):
    def setUp(self):
        super().setUp()
        self.root = Path(tempfile.mkdtemp()).resolve()
        self.addCleanup(shutil.rmtree, self.root, True)
        subprocess.run(['git', 'init', '-q'], cwd=self.root, check=True)
        self.project = self.root / 'app'
        for name, environments in (
            ('shop-live', {'dev': 'target: kubernetes\ncontext: dev\nimage: icr.io/t/shop\n',
                           'prod': 'target: kubernetes\ncontext: prod\nimage: icr.io/t/shop\n',
                           'local': 'target: docker\n'}),
            ('reports', {'dev': 'target: kubernetes\ncontext: dev\n'}),
        ):
            folder = self.project / 'services' / name / 'environments'
            folder.mkdir(parents=True)
            (folder.parent / 'service.yaml').write_text('pipelines: [orders]\n')
            for env, text in environments.items():
                (folder / f'{env}.yaml').write_text(text)

    def write(self, *rules, **kwargs):
        return ci.write(str(self.project), [ci.parse_rule(r) for r in rules],
                        mage_version='0.2.0', **kwargs)

    def test_the_workflow_checks_pull_requests_and_deploys_by_rule(self):
        path = self.write('dev=main', 'prod=tag:v*')

        self.assertEqual(path, self.root / '.github' / 'workflows' / 'mage-services.yml')
        text = path.read_text()
        flow = yaml.safe_load(text)
        on = flow[True]  # YAML reads the key on as true
        self.assertEqual(on['pull_request']['paths'][0], 'app/**')
        self.assertEqual(on['push'], dict(branches=['main'], tags=['v*']))
        self.assertEqual(flow['env']['MAGE_PROJECT'], 'app')
        self.assertEqual(set(flow['jobs']),
                         {'check', 'plan-dev', 'deploy-dev', 'plan-prod', 'deploy-prod'})
        check = flow['jobs']['check']['steps'][-1]['run']
        self.assertIn('mage service check --affected --locked', check)
        self.assertIn("refs/heads/main", flow['jobs']['plan-dev']['if'])
        self.assertIn("startsWith(github.ref, 'refs/tags/v')", flow['jobs']['plan-prod']['if'])
        deploy = flow['jobs']['deploy-prod']
        self.assertEqual(deploy['environment'], 'prod')
        self.assertEqual(deploy['strategy']['matrix']['include'],
                         '${{ fromJSON(needs.plan-prod.outputs.services) }}')
        steps = {step.get('name'): step for step in deploy['steps']}
        self.assertEqual(steps["Write the service's environment"]['env']['SERVICE_ENV'],
                         '${{ secrets[matrix.secret] }}')
        self.assertIn('--env prod --if-changed', steps['Deploy']['run'])
        self.assertIn('mage-ml==0.2.0', text)
        # Matrix values reach scripts through the environment, not the script text.
        self.assertNotIn('run: mage service deploy "${{', text)

    def test_the_plan_lists_each_service_with_its_secret_and_registry(self):
        self.assertEqual(ci.environment_plan(str(self.project), 'dev'), [
            dict(service='reports', environment='dev', target='kubernetes',
                 secret='REPORTS_DEV_ENV', registry=''),
            dict(service='shop-live', environment='dev', target='kubernetes',
                 secret='SHOP_LIVE_DEV_ENV', registry='icr.io'),
        ])

    def test_what_the_workflow_refuses(self):
        with self.assertRaisesRegex(ci.CiError, "Docker, which in CI is the runner's"):
            self.write('local=main')
        with self.assertRaisesRegex(ci.CiError, 'No service defines the environment qa'):
            self.write('qa=main')
        with self.assertRaisesRegex(ci.CiError, 'ENV=BRANCH'):
            ci.parse_rule('dev')
        with self.assertRaisesRegex(ci.CiError, 'a prefix with a final'):
            ci.parse_rule('prod=tag:v*.*')
        with self.assertRaisesRegex(ci.CiError, 'one rule'):
            self.write('dev=main', 'dev=release')
        self.write('dev=main')
        with self.assertRaisesRegex(ci.CiError, 'pass --force'):
            self.write('dev=main')
        self.write('dev=main', 'prod=tag:v*', force=True)

    def test_the_command_writes_it_and_lists_the_plan_as_json(self):
        from typer.testing import CliRunner

        from mage_ai.cli.main import app

        runner = CliRunner()
        written = runner.invoke(app, ['service', 'ci', '--deploy', 'dev=main',
                                      '--project', str(self.project)])
        self.assertEqual(written.exit_code, 0, written.output)
        self.assertIn('dev: deploys a build on a push to main', written.output)

        listed = runner.invoke(app, ['service', 'list', '--env', 'prod', '--json',
                                     '--project', str(self.project)])
        self.assertEqual(listed.exit_code, 0, listed.output)
        self.assertEqual([p['service'] for p in json.loads(listed.output)], ['shop-live'])

    def test_a_promotion_rule_deploys_the_image_another_environment_runs(self):
        rules = [ci.parse_rule('dev=main'), ci.parse_rule('prod=tag:v*')]
        ci.apply_promotions(rules, ['prod=dev'])

        flow = yaml.safe_load(ci.write(str(self.project), rules, mage_version='0.2.0')
                              .read_text())

        steps = {s.get('name'): s for s in flow['jobs']['deploy-prod']['steps']}
        self.assertIn('--env prod --if-changed --from dev', steps['Deploy']['run'])
        dev = {s.get('name'): s for s in flow['jobs']['deploy-dev']['steps']}
        self.assertNotIn('--from', dev['Deploy']['run'])
        with self.assertRaisesRegex(ci.CiError, 'needs a --deploy rule for qa'):
            ci.apply_promotions(rules, ['qa=dev'])
        with self.assertRaisesRegex(ci.CiError, 'ENV=SOURCE'):
            ci.apply_promotions(rules, ['prod'])
