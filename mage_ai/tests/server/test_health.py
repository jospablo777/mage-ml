"""The server health report: machine, processes, history and storage sizes."""
import os
import tempfile
from unittest.mock import patch

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.server import health
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user


class HealthTest(AsyncDBTestCase):
    def test_a_report_names_the_server_process_and_the_machine(self):
        with patch.object(health, 'start'):
            report = health.report()

        self.assertEqual(report['processes'][0]['kind'], 'server')
        self.assertEqual(report['processes'][0]['pid'], os.getpid())
        self.assertGreater(report['machine']['cpu_count'], 0)
        self.assertEqual(report['sample_seconds'], health.SAMPLE_SECONDS)

    def test_a_sample_and_the_history(self):
        point = health.sample()
        self.assertGreaterEqual(point['processes'], 1)
        self.assertGreater(point['mageml_memory_mb'], 0)
        self.assertEqual(health._history.maxlen, 6 * 3600 // health.SAMPLE_SECONDS)

    def test_folder_sizes_count_files_in_subfolders(self):
        folder = tempfile.mkdtemp()
        os.makedirs(os.path.join(folder, 'a', 'b'))
        for path, size in (('x', 10), ('a/y', 20), ('a/b/z', 30)):
            with open(os.path.join(folder, path), 'wb') as file:
                file.write(b'0' * size)
        self.assertEqual(health._folder_size(folder), 60)
        self.assertIsNone(health._folder_size(os.path.join(folder, 'missing')))

        with patch.object(health, 'storage_paths', return_value=dict(
            variables=folder, database=os.path.join(folder, 'x'),
        )):
            sizes = health.measure_sizes()
        self.assertEqual(sizes['variables']['bytes'], 60)
        self.assertEqual(sizes['database']['bytes'], 10)
        self.assertGreater(sizes['disk']['total_bytes'], 0)


class HealthApiTest(AsyncDBTestCase):
    async def test_owners_read_it_and_viewers_do_not(self):
        owner = create_user(_owner=True, email=self.faker.unique.email())
        viewer = create_user(email=self.faker.unique.email(), roles=8)

        with patch.object(health, 'start'), \
                patch('mage_ai.api.policies.BasePolicy.REQUIRE_USER_AUTHENTICATION', 1):
            read = await BaseOperation(action=OperationType.DETAIL, pk='current',
                                       resource='server_health', user=owner).execute()
            refused = await BaseOperation(action=OperationType.DETAIL, pk='current',
                                          resource='server_health', user=viewer).execute()

        self.assertEqual(read['server_health']['processes'][0]['kind'], 'server')
        self.assertEqual(refused['error']['code'], 403)
