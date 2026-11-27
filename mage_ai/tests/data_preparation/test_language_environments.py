"""
The R and Rust environments on the Workers page (data_preparation/language_environments.py):
what each lists from its files, and a change of rproject.toml or Cargo.toml saved only when it
resolves in a copy. The resolving tests need cargo or rv and their package indexes.
"""
import hashlib
import shlex
import shutil
import textwrap
import unittest
from pathlib import Path

from mage_ai.data_preparation import language_environments as le
from mage_ai.data_preparation.models.block.rust import workspace as ws
from mage_ai.tests.base_test import DBTestCase

RPROJECT = '''\
[project]
name = "test"
r_version = "4.6"
repositories = [{alias = "PPM", url = "https://packagemanager.posit.co/cran/latest"}]
dependencies = [
    "arrow",
    "bit64",
    "jsonlite",
    "tibble",
    {name = "glue", repository = "PPM"},
]
'''
RV_LOCK = '''\
version = 2
r_version = "4.6"
''' + ''.join(f'''
[[packages]]
name = "{name}"
version = "{version}"
source = {{ repository = "https://packagemanager.posit.co/cran/latest" }}
force_source = false
dependencies = []
''' for name, version in (('arrow', '25.0.1'), ('glue', '1.8.0'), ('cli', '3.6.5')))


class LanguageEnvironmentsTest(DBTestCase):
    def setUp(self):
        super().setUp()
        for folder in ('r', 'rust'):
            shutil.rmtree(Path(self.repo_path, folder), ignore_errors=True)

    def write(self, relative: str, text: str) -> Path:
        path = Path(self.repo_path, relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(text))
        return path

    def r_environment(self) -> Path:
        self.write('r/rv.lock', RV_LOCK)
        return self.write('r/rproject.toml', RPROJECT)

    def row(self, language):
        return next(r for r in le.summary(self.repo_path) if r['language'] == language)

    def test_a_project_without_them_lists_both_with_its_blocks(self):
        self.write('transformers/clean.R', 'x <- 1\n')
        self.write('data_loaders/rows.rs', 'fn main() {}\n')
        r, rust = self.row('r'), self.row('rust')
        self.assertEqual((r['state'], r['exists'], r['blocks']),
                         ('none', False, ['transformers/clean.R']))
        self.assertEqual((rust['state'], rust['exists'], rust['blocks']),
                         ('none', False, ['data_loaders/rows.rs']))
        # With the project's path: the terminal may be anywhere.
        self.assertIn(f'mage rust init {shlex.quote(self.repo_path)}',
                      le.detail(self.repo_path, 'rust')['commands'])

    def test_the_r_environment_lists_its_packages_with_the_locked_versions(self):
        self.r_environment()
        r = self.row('r')
        self.assertEqual((r['state'], r['version'], r['definition']), ('ready', '4.6', 'r'))
        self.assertEqual((r['package_count'], r['locked_count']), (5, 3))
        self.assertEqual(r['packages'][0], dict(name='arrow', locked='25.0.1', source=None))
        self.assertEqual(r['packages'][4], dict(name='glue', locked='1.8.0',
                                                source='repository PPM'))
        self.assertIsNone(r['packages'][1]['locked'])

    def test_the_rust_environment_lists_its_crates_with_the_locked_versions(self):
        ws.init(self.repo_path)
        rust = self.row('rust')
        self.assertEqual((rust['state'], rust['version'], rust['definition']),
                         ('ready', ws.RUST_VERSION, 'rust'))
        crates = {p['name']: p for p in rust['packages']}
        self.assertEqual(crates['mage-block']['requirement'], 'path .mage/sdk; features: postgres')
        self.assertEqual(crates['serde']['requirement'], '1; features: derive')
        self.assertTrue(crates['anyhow']['locked'].startswith('1.'))

    def test_a_file_that_is_not_toml_or_drops_what_blocks_need_is_refused(self):
        rproject = self.r_environment()
        ws.init(self.repo_path)
        cargo = Path(self.repo_path, 'rust', 'Cargo.toml')
        before = rproject.read_text(), cargo.read_text()
        for language, text, message in (
            ('r', '[project', 'rproject.toml is not valid TOML'),
            ('r', RPROJECT.replace('    "arrow",\n', ''), 'R blocks need arrow: keep it'),
            ('r', '[project]\ndependencies = "arrow"\n', 'dependencies is a list'),
            ('rust', '[workspace', 'Cargo.toml is not valid TOML'),
            ('rust', before[1].replace('mage-block =', 'mage-blocks ='),
             'must keep mage-block'),
            ('rust', '  ', 'The file is empty'),
            ('python', 'x', 'the languages are R and Rust'),
        ):
            with self.subTest(language=language, message=message):
                with self.assertRaisesRegex(le.LanguageEnvironmentError, message):
                    le.save_manifest(self.repo_path, language, text)
        self.assertEqual((rproject.read_text(), cargo.read_text()), before)

    def test_without_an_environment_there_is_nothing_to_change(self):
        for language, command in (('r', 'mage r init'), ('rust', 'mage rust init')):
            with self.subTest(language=language):
                with self.assertRaisesRegex(le.LanguageEnvironmentError, command):
                    le.save_manifest(self.repo_path, language, '[project]\n')

    @unittest.skipIf(shutil.which('cargo') is None, 'cargo is not installed')
    def test_a_cargo_toml_is_saved_only_when_its_crates_resolve(self):
        ws.init(self.repo_path)
        root = Path(self.repo_path, 'rust')
        text = (root / 'Cargo.toml').read_text()

        def digest():
            return hashlib.sha1(
                (root / 'Cargo.toml').read_bytes() + (root / 'Cargo.lock').read_bytes(),
            ).hexdigest()

        before = digest()
        for change, reason in (
            ('no-such-crate-mageml-test = "1"', 'no matching package named'),
            ('anyhow = "99"', 'failed to select a version'),
        ):
            with self.subTest(change=change):
                with self.assertRaises(le.LanguageEnvironmentError) as raised:
                    le.save_manifest(self.repo_path, 'rust', text.replace(
                        'anyhow = "1"', change if change.startswith('anyhow')
                        else f'anyhow = "1"\n{change}'))
                self.assertIn(reason, str(raised.exception))
                self.assertIn('nothing changed', str(raised.exception))
                # Only cargo's reason: not the check's own files.
                self.assertNotIn('mageml_check', str(raised.exception))
                self.assertEqual(digest(), before)
        saved = le.save_manifest(self.repo_path, 'rust',
                                 text.replace('anyhow = "1"', 'anyhow = "1"\nitoa = "1"'))
        self.assertIn('itoa', [p['name'] for p in saved['packages']])
        self.assertIn('itoa = "1"', (root / 'Cargo.toml').read_text())

    @unittest.skipIf(shutil.which('rv') is None, 'rv is not installed')
    def test_an_rproject_toml_is_saved_only_when_its_packages_resolve(self):
        rproject = self.r_environment()
        with self.assertRaises(le.LanguageEnvironmentError) as raised:
            le.save_manifest(self.repo_path, 'r', RPROJECT.replace(
                '"tibble",', '"tibble",\n    "nosuchpackagemagemltest",'))
        self.assertIn('nosuchpackagemagemltest', str(raised.exception))
        self.assertEqual(rproject.read_text(), RPROJECT)
        saved = le.save_manifest(self.repo_path, 'r', RPROJECT.replace(
            '"tibble",', '"tibble",\n    "zeallot",'))
        self.assertIn('zeallot', [p['name'] for p in saved['packages']])

    @unittest.skipIf(shutil.which('rv') is None, 'rv is not installed')
    def test_a_damaged_rv_lock_does_not_stop_a_change(self):
        self.r_environment()
        self.write('r/rv.lock', 'version = 2\n[[packages]]\nname = "arrow"\n')
        saved = le.save_manifest(self.repo_path, 'r', RPROJECT.replace(
            '"tibble",', '"tibble",\n    "zeallot",'))
        self.assertIn('zeallot', [p['name'] for p in saved['packages']])

    def test_the_api_shows_them_to_viewers_and_changes_them_for_editors(self):
        import asyncio

        from mage_ai.api.operations.base import BaseOperation
        from mage_ai.api.operations.constants import OperationType
        from mage_ai.orchestration.db.models.oauth import Role
        from mage_ai.tests.factory import create_user

        Role.create_default_roles()
        editor = create_user(roles_new=[Role.get_role('Editor')])
        viewer = create_user(roles_new=[Role.get_role('Viewer')])

        def call(user, action, pk=None, payload=None):
            return asyncio.run(BaseOperation(
                action=action, pk=pk, payload=payload or {}, query={},
                resource='language_environments', user=user,
            ).execute())

        init = dict(language_environment=dict(init=True))
        self.assertEqual(call(viewer, OperationType.UPDATE, 'rust', init)['error']['code'], 403)
        self.assertIn('mage r init',
                      call(editor, OperationType.UPDATE, 'r', init)['error']['message'])
        created = call(editor, OperationType.UPDATE, 'rust', init)['language_environment']
        self.assertTrue(created['exists'])
        listed = call(viewer, OperationType.LIST)['language_environments']
        self.assertEqual([(r['id'], r['state']) for r in listed],
                         [('r', 'none'), ('rust', 'ready')])
        shown = call(viewer, OperationType.DETAIL, 'rust')['language_environment']
        self.assertIn('mage-block', shown['manifest_text'])
        self.assertEqual(call(viewer, OperationType.DETAIL, 'go')['error']['code'], 404)

        change = dict(language_environment=dict(manifest_text='[workspace'))
        self.assertEqual(call(viewer, OperationType.UPDATE, 'rust', change)['error']['code'], 403)
        refused = call(editor, OperationType.UPDATE, 'rust', change)['error']
        self.assertEqual(refused['code'], 400)
        self.assertIn('not valid TOML', refused['message'])
        sync = call(editor, OperationType.UPDATE, 'rust',
                    dict(language_environment=dict(sync=True)))['error']
        self.assertIn('Rust needs no sync', sync['message'])


class NamedEnvironmentsTest(DBTestCase):
    """Named R and Rust environments next to the defaults (ADR-0040)."""

    write = LanguageEnvironmentsTest.write
    r_environment = LanguageEnvironmentsTest.r_environment

    def setUp(self):
        super().setUp()
        for folder in ('r', 'rust', 'r_environments', 'rust_environments', 'pipelines/churn'):
            shutil.rmtree(Path(self.repo_path, folder), ignore_errors=True)

    def pipeline(self, blocks, environment=None):
        import yaml

        self.write('pipelines/churn/metadata.yaml', yaml.safe_dump(dict(
            name='Churn', environment=environment, blocks=blocks,
        )))

    def test_an_r_environment_starts_from_the_default_or_from_what_blocks_need(self):
        self.r_environment()
        copied = le.create(self.repo_path, 'r', 'models')
        self.assertEqual((copied['id'], copied['label'], copied['environment']),
                         ('r.models', 'R models', 'models'))
        folder = Path(self.repo_path, 'r_environments', 'models')
        self.assertEqual((folder / 'rproject.toml').read_text(), RPROJECT)
        self.assertEqual((folder / 'rv.lock').read_text(), RV_LOCK)
        self.assertEqual([p['name'] for p in copied['packages']],
                         ['arrow', 'bit64', 'jsonlite', 'tibble', 'glue'])

        older = le.create(self.repo_path, 'r', 'old', r_version='4.3')
        self.assertEqual(older['version'], '4.3')
        # Another R resolves its own versions: the default's lock is not copied.
        self.assertFalse(Path(self.repo_path, 'r_environments', 'old', 'rv.lock').exists())

        minimal = le.create(self.repo_path, 'r', 'small', start_from='minimal')
        self.assertEqual([p['name'] for p in minimal['packages']],
                         ['arrow', 'bit64', 'jsonlite', 'tibble'])
        self.assertEqual([r['id'] for r in le.summary(self.repo_path)],
                         ['r', 'r.models', 'r.old', 'r.small', 'rust'])

    def test_a_rust_environment_copies_the_default_workspace_or_starts_new(self):
        ws.init(self.repo_path)
        default = ws.of_environment(self.repo_path)
        default.cargo_toml.write_text(default.cargo_toml.read_text().replace(
            'rayon = "1"\n', 'rayon = "1"\nitoa = "1"\n'))
        copied = le.create(self.repo_path, 'rust', 'fmt')
        self.assertIn('itoa', [p['name'] for p in copied['packages']])
        self.assertTrue(Path(self.repo_path, 'rust_environments', 'fmt', '.mage', 'sdk').is_dir())
        fresh = le.create(self.repo_path, 'rust', 'torch', start_from='minimal')
        self.assertNotIn('itoa', [p['name'] for p in fresh['packages']])
        self.assertIn('mage-block', [p['name'] for p in fresh['packages']])

    def test_names_that_exist_or_are_not_names_are_refused(self):
        ws.init(self.repo_path)
        le.create(self.repo_path, 'rust', 'fmt')
        for name, message in (('fmt', 'exists already'), ('default', 'another name'),
                              ('../up', 'not an environment name')):
            with self.assertRaises(le.LanguageEnvironmentError) as context:
                le.create(self.repo_path, 'rust', name)
            self.assertIn(message, str(context.exception))
        with self.assertRaises(le.LanguageEnvironmentError) as context:
            le.create(self.repo_path, 'r', 'models')
        self.assertIn('no default R environment to start from', str(context.exception))

    def test_each_environment_lists_the_blocks_and_pipelines_that_select_it(self):
        ws.init(self.repo_path)
        le.create(self.repo_path, 'rust', 'fmt')
        self.write('transformers/plain.rs', 'fn main() {}\n')
        self.write('transformers/fast.rs', 'fn main() {}\n')
        self.pipeline([
            dict(uuid='plain', language='rust'),
            dict(uuid='fast', language='rust', configuration=dict(rust_environment='fmt')),
        ])
        rows = {r['id']: r for r in le.summary(self.repo_path) if r['language'] == 'rust'}
        self.assertEqual(rows['rust']['blocks'], ['transformers/plain.rs'])
        self.assertEqual(rows['rust.fmt']['blocks'], ['transformers/fast.rs'])
        self.assertEqual(rows['rust.fmt']['pipelines'],
                         [dict(pipeline='churn', name='Churn', blocks=['fast'], default=False)])
        self.assertEqual(le.detail(self.repo_path, 'rust.fmt')['blocks'],
                         ['transformers/fast.rs'])

    def test_an_environment_in_use_is_not_removed(self):
        ws.init(self.repo_path)
        le.create(self.repo_path, 'rust', 'fmt')
        self.pipeline([dict(uuid='fast', language='rust')], environment=dict(rust='fmt'))
        with self.assertRaises(le.LanguageEnvironmentError) as context:
            le.delete(self.repo_path, 'rust.fmt')
        self.assertIn('Blocks run in fmt: churn: fast', str(context.exception))
        self.pipeline([dict(uuid='fast', language='rust')])
        self.assertTrue(le.delete(self.repo_path, 'rust.fmt')['deleted'])
        self.assertFalse(Path(self.repo_path, 'rust_environments', 'fmt').exists())
        for pk, message in (('rust', 'cannot be removed'), ('rust.fmt', 'no Rust environment')):
            with self.assertRaises(le.LanguageEnvironmentError) as context:
                le.delete(self.repo_path, pk)
            self.assertIn(message, str(context.exception))

    def test_the_api_creates_and_removes_them_for_editors(self):
        import asyncio

        from mage_ai.api.operations.base import BaseOperation
        from mage_ai.api.operations.constants import OperationType
        from mage_ai.orchestration.db.models.oauth import Role
        from mage_ai.tests.factory import create_user

        Role.create_default_roles()
        editor = create_user(roles_new=[Role.get_role('Editor')])
        viewer = create_user(roles_new=[Role.get_role('Viewer')])

        def call(user, action, pk=None, payload=None):
            return asyncio.run(BaseOperation(
                action=action, pk=pk, payload=payload or {}, query={},
                resource='language_environments', user=user,
            ).execute())

        new = dict(language_environment=dict(language='rust', name='fmt',
                                             start_from='minimal'))
        self.assertEqual(call(viewer, OperationType.CREATE, payload=new)['error']['code'], 403)
        created = call(editor, OperationType.CREATE, payload=new)['language_environment']
        self.assertEqual((created['id'], created['state']), ('rust.fmt', 'ready'))
        shown = call(viewer, OperationType.DETAIL, 'rust.fmt')['language_environment']
        self.assertIn('mage-block', shown['manifest_text'])
        self.assertEqual(call(viewer, OperationType.DETAIL, 'rust.nope')['error']['code'], 404)
        self.assertEqual(call(viewer, OperationType.DELETE, 'rust.fmt')['error']['code'], 403)
        self.assertNotIn('error', call(editor, OperationType.DELETE, 'rust.fmt'))
        self.assertEqual([r['id'] for r in call(viewer, OperationType.LIST)[
            'language_environments']], ['r', 'rust'])
