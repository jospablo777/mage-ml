"""
Pipelines exported as one zip and imported into another project: planned first, skipped or
replaced, and never written outside the project folders.
"""
import base64
import io
import json
import os
import shutil
import tempfile
import zipfile
from pathlib import Path

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation import project_archive
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user

LOAD = '''
@data_loader
def load(*args, **kwargs):
    return 1
'''


class ProjectArchiveTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        self.name = f'archived_{self.faker.unique.pyint()}'
        pipeline = Pipeline.create(self.name, repo_path=self.repo_path)
        block = Block.create(f'{self.name}_load', 'data_loader', self.repo_path,
                             pipeline=pipeline)
        Path(block.file_path).write_text(LOAD)
        Path(pipeline.dir_path, 'README.md').write_text('# Orders\n')
        self.block_path = os.path.relpath(block.file_path, self.repo_path)
        self.target = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.target, 'pipelines'))
        self.addCleanup(shutil.rmtree, self.target, True)

    def archive(self):
        return project_archive.export(self.repo_path, [self.name])

    def test_an_export_holds_the_pipeline_its_blocks_and_a_manifest(self):
        with zipfile.ZipFile(io.BytesIO(self.archive())) as archive:
            names = set(archive.namelist())
            manifest = json.loads(archive.read(project_archive.MANIFEST))

        self.assertIn(f'pipelines/{self.name}/metadata.yaml', names)
        self.assertIn(f'pipelines/{self.name}/README.md', names)
        self.assertIn(self.block_path, names)
        self.assertNotIn('io_config.yaml', names)
        self.assertEqual([p['uuid'] for p in manifest['pipelines']], [self.name])
        self.assertEqual(set(manifest['files']), names - {project_archive.MANIFEST})

    def test_an_import_creates_the_pipeline_in_another_project(self):
        content = self.archive()

        planned = project_archive.plan(self.target, content)
        self.assertEqual({f['status'] for f in planned['files']}, {'new'})
        self.assertEqual(planned['pipelines'][0]['action'], 'create')

        result = project_archive.import_archive(self.target, content)

        self.assertEqual(sorted(result['written']), sorted(f['path'] for f in planned['files']))
        imported = Pipeline.get(self.name, repo_path=self.target)
        self.assertEqual(list(imported.blocks_by_uuid), [f'{self.name}_load'])
        self.assertEqual(Path(self.target, self.block_path).read_text(), LOAD)

        again = project_archive.import_archive(self.target, content)
        self.assertEqual(again['written'], [])
        self.assertEqual({f['action'] for f in again['files']}, {'keep'})

    def test_a_changed_block_is_a_conflict_unless_replaced(self):
        content = self.archive()
        project_archive.import_archive(self.target, content)
        Path(self.target, self.block_path).write_text('# changed here\n')
        shutil.rmtree(os.path.join(self.target, 'pipelines', self.name))

        planned = project_archive.plan(self.target, content)
        self.assertEqual(planned['conflicts'], [self.block_path])
        with self.assertRaisesRegex(project_archive.ArchiveError, 'mode replace'):
            project_archive.import_archive(self.target, content)

        project_archive.import_archive(self.target, content, mode='replace')
        self.assertEqual(Path(self.target, self.block_path).read_text(), LOAD)

    def test_skip_leaves_an_existing_pipeline_and_its_blocks(self):
        content = self.archive()
        project_archive.import_archive(self.target, content)
        Path(self.target, self.block_path).write_text('# changed here\n')

        planned = project_archive.plan(self.target, content)

        self.assertEqual(planned['pipelines'][0]['action'], 'skip')
        self.assertEqual(planned['conflicts'], [])
        project_archive.import_archive(self.target, content)
        self.assertEqual(Path(self.target, self.block_path).read_text(), '# changed here\n')

    def test_archives_that_write_outside_the_project_are_refused(self):
        def zipped(files, manifest=None):
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, 'w') as archive:
                archive.writestr(project_archive.MANIFEST, json.dumps(
                    manifest or dict(format=1, pipelines=[], files={}),
                ))
                for name, data in files.items():
                    archive.writestr(name, data)
            return buffer.getvalue()

        for files, message in (
            ({'../escape.py': 'x'}, 'outside the project'),
            ({'io_config.yaml': 'x'}, 'outside the project'),
            ({'/etc/passwd': 'x'}, 'outside the project'),
        ):
            with self.assertRaisesRegex(project_archive.ArchiveError, message):
                project_archive.plan(self.target, zipped(files))
        with self.assertRaisesRegex(project_archive.ArchiveError, 'checksum'):
            project_archive.plan(self.target, zipped(
                {'transformers/x.py': 'a'},
                dict(format=1, pipelines=[], files={'transformers/x.py': 'not it'}),
            ))
        with self.assertRaisesRegex(project_archive.ArchiveError, 'not a zip'):
            project_archive.plan(self.target, b'plain text')
        with self.assertRaisesRegex(project_archive.ArchiveError, 'no mageml-project.json'):
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, 'w') as archive:
                archive.writestr('pipelines/x/metadata.yaml', 'a')
            project_archive.plan(self.target, buffer.getvalue())

    async def test_the_api_exports_plans_and_imports(self):
        owner = create_user(_owner=True, email=self.faker.unique.email())

        async def call(**payload):
            return await BaseOperation(
                action=OperationType.CREATE, payload=dict(project_archive=payload),
                resource='project_archives', user=owner,
            ).execute()

        exported = (await call(action='export', pipeline_uuids=[self.name]))['project_archive']
        content = exported['content']
        self.assertEqual(base64.b64decode(content)[:2], b'PK')

        planned = (await call(action='plan', content=content))['project_archive']['plan']
        self.assertEqual(planned['pipelines'][0]['action'], 'skip')
        refused = await call(action='plan', content='not base64!')
        self.assertEqual(refused['error']['code'], 400)
