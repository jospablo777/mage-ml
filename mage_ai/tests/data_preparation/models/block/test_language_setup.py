"""
Whether R and Rust blocks can run here, and what the editor tells someone who creates one
when they cannot.
"""
import shutil
import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation.models.block import language_setup as setup
from mage_ai.data_preparation.models.block.r import runtime
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user


def which(*present):
    return lambda name: f'/bin/{name}' if name in present else None


class LanguageSetupTest(TestCase):
    def setUp(self):
        self.repo = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.repo, True)
        patcher = patch.dict('os.environ', {}, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)
        for name in ('MAGE_R_PROJECT_DIR', 'MAGE_RSCRIPT', 'MAGE_RV'):
            __import__('os').environ.pop(name, None)
        runtime._rscripts.clear()

    def test_r_without_r_says_to_install_it_and_how(self):
        with patch.object(shutil, 'which', which()):
            result = setup.language_setup('r', self.repo)

        self.assertFalse(result['ready'])
        self.assertIn('R is not installed, or Rscript is not on the PATH', result['problems'][0])
        self.assertTrue(result['guide'].endswith('/guides/blocks/r-setup.md'))
        self.assertTrue(result['commands'][0].startswith('mage r setup'))
        self.assertEqual(result['install'], 'uv pip install "mage-ml[r,rust,postgres]"')

    def test_system_r_without_the_exchange_packages_names_them(self):
        with patch.object(shutil, 'which', which('Rscript')), \
                patch.object(runtime, 'choose_rscript', return_value='Rscript'), \
                patch.object(runtime, 'missing_packages', return_value=['jsonlite']):
            result = setup.language_setup('r', self.repo)

        self.assertEqual(result['problems'], [
            'R is installed, but its library lacks jsonlite, which R blocks use to exchange '
            'data. Run `mage r init` for a project environment, or install.packages(c("jsonlite")) '
            'in R.'
        ])

    def test_a_project_environment_without_its_packages_says_to_sync(self):
        project = Path(self.repo) / 'r'
        project.mkdir()
        (project / 'rproject.toml').write_text('[project]\nname = "p"\nr_version = "4.6"\n')
        library = Path(self.repo) / 'library'
        library.mkdir()
        with patch.object(shutil, 'which', which('Rscript', 'rv')), \
                patch.object(runtime, 'choose_rscript', return_value='Rscript'), \
                patch.object(runtime, 'library_path', return_value=library):
            result = setup.language_setup('r', self.repo)

        self.assertIn('Run `mage r sync`', result['problems'][0])

    def test_ready_r_and_rust_have_no_problems(self):
        with patch.object(shutil, 'which', which('Rscript', 'cargo')), \
                patch.object(runtime, 'choose_rscript', return_value='Rscript'), \
                patch.object(runtime, 'missing_packages', return_value=[]):
            self.assertTrue(setup.language_setup('r', self.repo)['ready'])
            self.assertTrue(setup.language_setup('rust')['ready'])

    def test_rust_without_cargo_says_to_install_rustup(self):
        with patch.object(shutil, 'which', which()):
            result = setup.language_setup('rust')

        self.assertFalse(result['ready'])
        self.assertIn('Rust is not installed', result['problems'][0])
        self.assertIn('sh.rustup.rs', result['commands'][0])
        self.assertTrue(result['guide'].endswith('/guides/blocks/rust-setup.md'))


class LanguageSetupApiTest(AsyncDBTestCase):
    async def call(self, pk):
        return await BaseOperation(
            action=OperationType.DETAIL, pk=pk, resource='language_setups',
            user=create_user(_owner=True),
        ).execute()

    async def test_the_editor_reads_a_languages_setup(self):
        with patch.object(shutil, 'which', which()):
            result = (await self.call('rust'))['language_setup']

        self.assertEqual((result['language'], result['name'], result['ready']),
                         ('rust', 'Rust', False))
        self.assertEqual((await self.call('cobol'))['error']['code'], 404)
