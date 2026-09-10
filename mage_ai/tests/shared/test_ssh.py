import os
import tempfile
import unittest
from unittest.mock import patch

import paramiko
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from mage_ai.shared.ssh import SSHTunnelForwarder


class SSHTunnelTest(unittest.TestCase):
    def test_password_authentication(self):
        tunnel = SSHTunnelForwarder(
            ('localhost', 22), ssh_username='mage', ssh_password='password',
            allow_agent=False, host_pkey_directories=[], ssh_config_file=None,
            remote_bind_address=('localhost', 5432), local_bind_address=('127.0.0.1', 0),
        )

        self.assertEqual(tunnel.ssh_password, 'password')
        self.assertEqual(tunnel.ssh_pkeys, [])

    def test_loads_rsa_ecdsa_and_ed25519_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            for name, key in [
                ('rsa', paramiko.RSAKey.generate(2048)),
                ('ecdsa', paramiko.ECDSAKey.generate()),
            ]:
                key.write_private_key_file(os.path.join(directory, f'id_{name}'))
            private_key = Ed25519PrivateKey.generate().private_bytes(
                serialization.Encoding.PEM, serialization.PrivateFormat.OpenSSH,
                serialization.NoEncryption(),
            )
            with open(os.path.join(directory, 'id_ed25519'), 'wb') as output:
                output.write(private_key)

            keys = SSHTunnelForwarder.get_keys(host_pkey_directories=[directory])

        self.assertEqual([key.get_name() for key in keys], [
            'ssh-rsa', 'ecdsa-sha2-nistp256', 'ssh-ed25519',
        ])

    def test_encrypted_private_key_authentication(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'key')
            key = paramiko.RSAKey.generate(2048)
            key.write_private_key_file(path, password='passphrase')

            password, keys = SSHTunnelForwarder._consolidate_auth(
                ssh_pkey=path, ssh_pkey_password='passphrase',
                allow_agent=False, host_pkey_directories=[],
            )

        self.assertIsNone(password)
        self.assertEqual(keys, [key])

    def test_agent_keys_are_retained(self):
        key = paramiko.RSAKey.generate(2048)
        with patch.object(SSHTunnelForwarder, 'get_agent_keys', return_value=[key]):
            _, keys = SSHTunnelForwarder._consolidate_auth(
                allow_agent=True, host_pkey_directories=[],
            )
        self.assertEqual(keys, [key])

    def test_missing_credentials_raise(self):
        with self.assertRaises(ValueError):
            SSHTunnelForwarder._consolidate_auth(allow_agent=False, host_pkey_directories=[])
