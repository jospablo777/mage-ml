"""
A stream run from the editor reads the project's secrets, as a block run does: each
mage_secret_var() in the stream's files becomes the MAGE_SECRET_<NAME> variable the engine
reads (I-586).
"""
import os
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from mage_ai.streaming import dev_run

SECRETS = {'sync_source_password': 'one', 'block token': 'two'}  # pragma: allowlist secret


class ProjectSecretsTest(TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        root = Path(self.folder.name)
        self.source = root / 'source.yaml'
        self.source.write_text("password: \"{{ mage_secret_var('sync_source_password') }}\"\n"
                               "missing: \"{{ mage_secret_var('not_in_project') }}\"\n")
        self.block = root / 'transform.py'
        self.block.write_text("token = mage_secret_var('block token')\n")
        self.notes = root / 'notes.txt'
        self.notes.write_text("mage_secret_var('sync_source_password')\n")
        self.captured = SimpleNamespace(files={self.block, self.notes},
                                        stream_files={self.source})

    def secrets(self):
        with patch('mage_ai.data_preparation.shared.secrets.get_secret_value',
                   side_effect=lambda name, **_: SECRETS.get(name)) as found:
            return dev_run.project_secrets(self.captured), found

    def test_each_secret_the_stream_reads_becomes_its_variable(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('MAGE_SECRET_SYNC_SOURCE_PASSWORD', None)
            values, found = self.secrets()
        self.assertEqual(values, {
            'MAGE_SECRET_SYNC_SOURCE_PASSWORD': 'one',
            'MAGE_SECRET_BLOCK_TOKEN': 'two',
        })
        # Read once each, and a secret the project does not have is left for the engine to
        # name.
        self.assertEqual(sorted(call.args[0] for call in found.call_args_list),
                         ['block token', 'not_in_project', 'sync_source_password'])

    def test_the_server_environment_wins(self):
        with patch.dict(os.environ, {'MAGE_SECRET_SYNC_SOURCE_PASSWORD': 'from env'}):
            values, found = self.secrets()
        self.assertNotIn('MAGE_SECRET_SYNC_SOURCE_PASSWORD', values)
        self.assertNotIn('sync_source_password',
                         [call.args[0] for call in found.call_args_list])
