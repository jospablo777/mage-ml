"""
Export as service from the app: the API writes the build context to the project's MageML
data folder, reports what it holds, refuses what cannot be exported, and the download
serves it as a zip under a folder named like the service.
"""
import asyncio
import shutil
import tempfile
import time
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import jwt

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.pipeline_services import exports
from mage_ai.settings import JWT_DOWNLOAD_SECRET
from mage_ai.tests.factory import create_user
from mage_ai.tests.pipeline_services.test_export import LOAD, ExportTestCase


class AppExportBase(ExportTestCase):
    def setUp(self):
        super().setUp()
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, True)
        patcher = patch.object(exports, 'exports_root', return_value=self.root)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.owner = create_user(_owner=True)
        exports._jobs.clear()
        exports._builds.clear()

    def operation(self, action, user=None, **kwargs):
        return asyncio.run(BaseOperation(
            action=action,
            resource='pipeline_service_exports',
            user=user or self.owner,
            **kwargs,
        ).execute())

    def start(self, user=None, **payload):
        return self.operation(
            OperationType.CREATE, user=user, payload=dict(pipeline_service_export=payload),
        )

    def create(self, user=None, **payload):
        """Starts an export and waits for it: the app polls its progress the same way."""
        started = self.start(user=user, **payload)
        if 'error' in started:
            return started
        job = started['pipeline_service_export']
        progress = [job['progress']]
        for _ in range(600):
            if job['state'] != 'running':
                break
            time.sleep(0.05)
            detail = self.operation(OperationType.DETAIL, pk=job['name'])
            self.assertIn('pipeline_service_export', detail, detail)
            job = detail['pipeline_service_export']
            progress.append(job['progress'])
        self.assertEqual(progress, sorted(progress))
        return dict(pipeline_service_export=job)


class AppExportTest(AppExportBase):
    def test_an_export_reports_what_the_service_runs_and_downloads_as_a_zip(self):
        self.chain()

        response = self.create(pipeline_uuids=[self.pipeline.uuid], name='Shop Orders')

        result = response['pipeline_service_export']
        self.assertEqual((result['state'], result['progress']), ('completed', 1.0))
        self.assertEqual(result['name'], 'shop-orders')
        self.assertEqual(Path(result['path']), self.root / 'shop-orders')
        self.assertTrue((self.root / 'shop-orders' / 'Dockerfile').is_file())
        self.assertTrue((self.root / 'shop-orders' / 'report.txt').is_file())
        [pipeline] = result['pipelines']
        self.assertEqual(
            [(b['language'], b['type']) for b in pipeline['blocks']],
            [('python', 'data_loader'), ('python', 'transformer'), ('python', 'data_exporter')],
        )
        self.assertIn('polars', result['packages'])
        self.assertIn('utils/rates.py', result['files'])
        token = jwt.decode(result['download_token'], JWT_DOWNLOAD_SECRET, algorithms=['HS256'])
        self.assertEqual(token['service_export'], 'shop-orders')
        self.assertEqual(token['file_name'], 'shop-orders.zip')

        with exports.zip_export(self.repo_path, 'shop-orders') as archive:
            names = zipfile.ZipFile(archive).namelist()
        self.assertIn('shop-orders/Dockerfile', names)
        self.assertIn('shop-orders/service.json', names)
        self.assertIn(f'shop-orders/{self.project.name}/utils/rates.py', names)
        self.assertTrue(all(name.startswith('shop-orders/') for name in names))

    def test_exporting_again_replaces_the_earlier_export(self):
        self.chain()
        self.create(pipeline_uuids=[self.pipeline.uuid], name='shop')
        stale = self.root / 'shop' / 'stale.txt'
        stale.write_text('from an earlier export')

        self.create(pipeline_uuids=[self.pipeline.uuid], name='shop', max_runs=3)

        self.assertFalse(stale.exists())
        service = (self.root / 'shop' / 'service.json').read_text()
        self.assertIn('"max_concurrent_runs": 3', service)

    def test_what_cannot_be_exported_comes_back_as_the_problems(self):
        load = self.add('load', 'data_loader', LOAD.format(project=self.project.name))
        self.add('sensor', 'sensor', '@sensor\ndef s(*args, **kwargs):\n    return True\n',
                 upstream=[load])

        preview = self.start(pipeline_uuids=[self.pipeline.uuid], preview=True)
        self.assertIn('is a sensor block', '\n'.join(
            preview['pipeline_service_export']['problems']))

        result = self.create(pipeline_uuids=[self.pipeline.uuid])['pipeline_service_export']

        self.assertEqual(result['state'], 'failed')
        self.assertIn('is a sensor block', '\n'.join(result['problems']))
        self.assertEqual(list(self.root.iterdir()), [])

    def test_a_preview_shows_what_the_export_would_hold_and_writes_nothing(self):
        self.chain()

        preview = self.start(pipeline_uuids=[self.pipeline.uuid], name='Shop', preview=True)

        result = preview['pipeline_service_export']
        self.assertEqual(result['name'], 'shop')
        self.assertEqual(result['problems'], [])
        self.assertEqual(len(result['pipelines'][0]['blocks']), 3)
        self.assertIn('polars', result['packages'])
        self.assertIsNone(result['path'])
        self.assertEqual(list(self.root.iterdir()), [])

    def test_progress_of_an_unknown_export_is_not_found(self):
        response = self.operation(OperationType.DETAIL, pk='never-started')
        self.assertEqual(response['error']['code'], 404)

    def test_requests_are_checked_before_exporting(self):
        self.chain()

        self.assertEqual(self.create(pipeline_uuids=[])['error']['code'], 400)
        missing = self.create(pipeline_uuids=[self.pipeline.uuid, 'no_such_pipeline'])
        self.assertEqual(missing['error']['code'], 404)
        self.assertIn('no_such_pipeline', missing['error']['message'])
        runs = self.create(pipeline_uuids=[self.pipeline.uuid], max_runs=0)
        self.assertEqual(runs['error']['code'], 400)
        denied = self.create(user=create_user(), pipeline_uuids=[self.pipeline.uuid])
        self.assertEqual(denied['error']['code'], 403)

    def test_the_list_shows_each_export_newest_first_with_a_download(self):
        self.chain()
        self.create(pipeline_uuids=[self.pipeline.uuid], name='first')
        self.create(pipeline_uuids=[self.pipeline.uuid], name='second')
        (self.root / 'not-an-export').mkdir()

        listed = asyncio.run(BaseOperation(
            action=OperationType.LIST,
            resource='pipeline_service_exports',
            user=self.owner,
        ).execute())['pipeline_service_exports']

        self.assertEqual([e['name'] for e in listed], ['second', 'first'])
        self.assertEqual(listed[0]['pipelines'], [self.pipeline.uuid])
        self.assertEqual(listed[0]['blocks'], 3)
        self.assertTrue(listed[0]['exported_at'])
        token = jwt.decode(listed[0]['download_token'], JWT_DOWNLOAD_SECRET, algorithms=['HS256'])
        self.assertEqual(token['service_export'], 'second')
        denied = asyncio.run(BaseOperation(
            action=OperationType.LIST,
            resource='pipeline_service_exports',
            user=create_user(),
        ).execute())
        self.assertEqual(denied['error']['code'], 403)

    def test_a_download_names_only_exports(self):
        with self.assertRaises(ValueError):
            exports.export_directory(self.repo_path, '../outside')
        with self.assertRaises(FileNotFoundError):
            exports.zip_export(self.repo_path, 'never-exported')


class FileOwnersCacheTest(unittest.TestCase):
    def test_the_installed_files_are_read_once_until_the_packages_change(self):
        from mage_ai.pipeline_services import export

        export._owners_cache.clear()
        self.addCleanup(export._owners_cache.clear)
        with patch.object(export, '_read_file_owners', return_value={}) as read, \
                patch.object(export, '_environment_fingerprint',
                             side_effect=[('a',), ('a',), ('b',)]):
            for _ in range(3):
                export._file_owners()

        self.assertEqual(read.call_count, 2)


class BuildTest(AppExportBase):
    def build(self, **payload):
        return asyncio.run(BaseOperation(
            action=OperationType.CREATE,
            payload=dict(pipeline_service_build=payload),
            resource='pipeline_service_builds',
            user=self.owner,
        ).execute())

    def test_a_build_runs_docker_and_keeps_every_line_it_prints(self):
        from mage_ai.pipeline_services import export

        self.chain()
        self.create(pipeline_uuids=[self.pipeline.uuid], name='shop')
        calls = []

        def fake_build(directory, image, platforms=None, push=False, on_line=None):
            calls.append((image, platforms, push))
            for line in ('#1 building', '#2 pushing'):
                on_line(line)

        with patch.object(export.shutil, 'which', return_value='/usr/bin/docker'), \
                patch.object(export, 'build_image', side_effect=fake_build):
            started = self.build(name='shop', image='icr.io/team/shop:1',
                                 platforms=['linux/amd64', 'linux/arm64'], push=True)
            job = started['pipeline_service_build']
            for _ in range(100):
                if job['state'] != 'running':
                    break
                time.sleep(0.05)
                job = asyncio.run(BaseOperation(
                    action=OperationType.DETAIL, pk='shop',
                    resource='pipeline_service_builds', user=self.owner,
                ).execute())['pipeline_service_build']

        self.assertEqual(job['state'], 'completed', job)
        self.assertEqual(job['lines'], ['#1 building', '#2 pushing'])
        self.assertIn('--push', job['command'])
        self.assertEqual(calls, [('icr.io/team/shop:1', ['linux/amd64', 'linux/arm64'], True)])

    def test_builds_need_an_export_an_image_and_one_platform_without_push(self):
        from mage_ai.pipeline_services import export

        with patch.object(export.shutil, 'which', return_value='/usr/bin/docker'):
            self.assertEqual(self.build(name='never', image='x:1')['error']['code'], 404)
            self.chain()
            self.create(pipeline_uuids=[self.pipeline.uuid], name='shop')
            self.assertEqual(self.build(name='shop', image='')['error']['code'], 400)
            several = self.build(name='shop', image='shop:1',
                                 platforms=['linux/amd64', 'linux/arm64'])
            self.assertIn('Several platforms need --push', several['error']['message'])

    def test_the_server_says_whether_it_can_build(self):
        with patch('shutil.which', return_value=None):
            status = exports.docker_status()
        self.assertEqual(status['docker'], False)
        self.assertIn('not installed', status['reason'])
