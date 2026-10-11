import os
import sys
import tempfile
from unittest.mock import patch

from mage_ai.data_preparation.git.utils import requirements_install_command
from mage_ai.tests.base_test import TestCase


class RequirementsInstallTests(TestCase):
    def test_uv_installs_into_mageml_python_within_its_constraints(self):
        with tempfile.NamedTemporaryFile() as constraints, \
                patch('shutil.which', return_value='/usr/local/bin/uv'), \
                patch.dict(os.environ, {'MAGE_RUNTIME_CONSTRAINTS': constraints.name}):
            command = requirements_install_command('/home/src/my project/requirements.txt')
        self.assertEqual(command, [
            'uv', 'pip', 'install', '--python', sys.executable,
            '--constraint', constraints.name,
            '--requirement', '/home/src/my project/requirements.txt',
        ])

    def test_without_constraints_or_uv(self):
        with patch('shutil.which', return_value='/usr/local/bin/uv'), \
                patch.dict(os.environ, {'MAGE_RUNTIME_CONSTRAINTS': ''}):
            self.assertNotIn('--constraint', requirements_install_command('r.txt'))
        with patch('shutil.which', return_value=None):
            self.assertEqual(
                requirements_install_command('r.txt'),
                [sys.executable, '-m', 'pip', 'install', '--requirement', 'r.txt'],
            )
