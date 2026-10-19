import asyncio
import os
import shutil
import stat

from mage_ai.shared.io import chmod, safe_write, safe_write_async
from mage_ai.tests.base_test import TestCase


class IOTests(TestCase):
    def test_chmod(self):
        path = 'test_io'
        file = 'test_chmod'
        file_path = os.path.join(path, file)

        os.mkdir(path)
        with open(file_path, 'w') as fp:
            fp.write('MESSAGE1')

        target_mode = stat.S_IRWXU
        chmod(file_path, target_mode)

        # checks whether the permission bits target_mode are set in current_mode
        # current_mode & target_mode will keep only those bits which are true for both expressions
        self.assertTrue(os.stat(path).st_mode & target_mode == target_mode)
        self.assertTrue(os.stat(file_path).st_mode & target_mode == target_mode)

        os.remove(file_path)
        os.rmdir(path)

    def test_chmod_on_a_folder_leaves_its_files_without_execute_unless_they_had_it(self):
        import tempfile

        folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, folder, True)
        os.makedirs(os.path.join(folder, 'sub'))
        data = os.path.join(folder, 'sub', 'metadata.yaml')
        script = os.path.join(folder, 'run.sh')
        for path in (data, script):
            with open(path, 'w') as fp:
                fp.write('x')
        os.chmod(data, 0o600)
        os.chmod(script, 0o700)

        chmod(folder, stat.S_IRWXU)

        self.assertEqual(os.stat(data).st_mode & 0o777, 0o600)
        self.assertEqual(os.stat(script).st_mode & 0o100, 0o100)
        self.assertEqual(os.stat(os.path.join(folder, 'sub')).st_mode & 0o700, 0o700)

    def test_safe_write(self):
        file_path = 'test_file_write'
        with open(file_path, 'w') as fp:
            fp.write('MESSAGE1')

        safe_write(file_path, 'MESSAGE2')
        with open(file_path, 'r') as fp:
            self.assertEqual(fp.read(), 'MESSAGE2')

        def write_func(fp, content):
            raise Exception('Fail to write')

        with self.assertRaises(Exception) as err:
            safe_write(file_path, 'MESSAGE3', write_func=write_func)
            self.assertTrue('Fail to write' in str(err.exception))
        with open(file_path, 'r') as fp:
            self.assertEqual(fp.read(), 'MESSAGE2')
        os.remove(file_path)

    def test_safe_write_sync(self):
        file_path = 'test_file_write_2'
        with open(file_path, 'w') as fp:
            fp.write('MESSAGE1')

        try:
            asyncio.run(safe_write_async(file_path, 'MESSAGE2'))
            with open(file_path, 'r') as fp:
                self.assertEqual(fp.read(), 'MESSAGE2')

            async def write_func_async(fp, content):
                raise Exception('Fail to write async')

            with self.assertRaises(Exception) as err:
                asyncio.run(safe_write_async(file_path, 'MESSAGE3', write_func=write_func_async))
                self.assertTrue('Fail to write async' in str(err.exception))
            with open(file_path, 'r') as fp:
                self.assertEqual(fp.read(), 'MESSAGE2')
        except RuntimeError as err:
            print(f'[WARNING] IOTests.test_safe_write_sync: {err}')
        os.remove(file_path)
