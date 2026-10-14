"""
CA certificates for database TLS from a path, a PEM text or base64 PEM (IBM Cloud Databases
credentials), written once to a private file; anything else is refused with the reason.
"""
import base64
import os
import stat
import tempfile
import unittest

from mage_ai.io.config import ConfigKey
from mage_ai.io.postgres import Postgres
from mage_ai.io.tls import ca_certificate_file

PEM = '-----BEGIN CERTIFICATE-----\nMIIBszCCAVmgAwIBAgIU\n-----END CERTIFICATE-----\n'


class Config(dict):
    def __getitem__(self, key):
        return self.get(key)


class CACertificateFileTest(unittest.TestCase):
    def test_base64_and_pem_become_the_same_private_file(self):
        from_base64 = ca_certificate_file(base64.b64encode(PEM.encode()).decode())
        from_pem = ca_certificate_file(PEM)

        self.assertEqual(from_base64, from_pem)
        with open(from_pem) as file:
            self.assertEqual(file.read(), PEM)
        self.assertEqual(stat.S_IMODE(os.stat(from_pem).st_mode), 0o600)

    def test_a_path_is_used_as_it_is(self):
        with tempfile.NamedTemporaryFile('w', suffix='.crt') as file:
            self.assertEqual(ca_certificate_file(file.name), file.name)

    def test_nothing_is_none_and_garbage_is_refused(self):
        self.assertIsNone(ca_certificate_file(None))
        self.assertIsNone(ca_certificate_file(''))
        with self.assertRaises(ValueError):
            ca_certificate_file('/no/such/ca.crt')
        with self.assertRaises(ValueError):
            ca_certificate_file(base64.b64encode(b'not a certificate').decode())


class PostgresTLSSettingsTest(unittest.TestCase):
    def test_sslmode_and_root_certificate_reach_the_connection_settings(self):
        loader = Postgres.with_config(Config({
            ConfigKey.POSTGRES_DBNAME: 'shop', ConfigKey.POSTGRES_HOST: 'db.example.com',
            ConfigKey.POSTGRES_SSLMODE: 'verify-full',
            ConfigKey.POSTGRES_SSLROOTCERT: base64.b64encode(PEM.encode()).decode(),
        }))
        self.assertEqual(loader.settings['sslmode'], 'verify-full')
        with open(loader.settings['sslrootcert']) as file:
            self.assertEqual(file.read(), PEM)

    def test_without_tls_settings_nothing_is_added(self):
        loader = Postgres.with_config(Config({ConfigKey.POSTGRES_HOST: 'db'}))
        self.assertNotIn('sslmode', loader.settings)
        self.assertNotIn('sslrootcert', loader.settings)
