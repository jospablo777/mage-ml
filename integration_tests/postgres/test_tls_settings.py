"""
POSTGRES_SSLMODE and POSTGRES_SSLROOTCERT against the test PostgreSQL, whose certificate comes
from a private CA, as IBM Cloud Databases' do: the base64 CA from service credentials
verifies the chain, verify-full checks the name, and a wrong CA or name is refused.
"""
import base64

import pytest

from integration_tests.tls_certs import CERTS, SERVER_NAME
from mage_ai.io.config import ConfigKey
from mage_ai.io.postgres import Postgres


class Config(dict):
    def __getitem__(self, key):
        return self.get(key)


def config(settings, **extra):
    return Config({
        ConfigKey.POSTGRES_DBNAME: settings['dbname'],
        ConfigKey.POSTGRES_HOST: settings['host'],
        ConfigKey.POSTGRES_PORT: settings['port'],
        ConfigKey.POSTGRES_USER: settings['user'],
        ConfigKey.POSTGRES_PASSWORD: settings['password'],
        **extra,
    })


def ca_base64() -> str:
    return base64.b64encode((CERTS / 'ca.pem').read_bytes()).decode()


def encrypted(loader) -> bool:
    with loader:
        with loader.conn.cursor() as cursor:
            cursor.execute('SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()')
            return cursor.fetchone()[0]


def test_the_base64_ca_from_credentials_verifies_the_server(postgres_settings):
    loader = Postgres.with_config(config(
        postgres_settings,
        **{ConfigKey.POSTGRES_SSLMODE: 'verify-ca', ConfigKey.POSTGRES_SSLROOTCERT: ca_base64()},
    ))
    assert encrypted(loader) is True


def test_verify_full_checks_the_name_on_the_certificate(postgres_settings):
    # The certificate names db.test: by that name it passes, by 127.0.0.1 it is refused.
    by_name = Postgres(
        **{k: postgres_settings[k] for k in ('dbname', 'user', 'password', 'port')},
        host=SERVER_NAME, hostaddr='127.0.0.1', sslmode='verify-full',
        sslrootcert=str(CERTS / 'ca.pem'), verbose=False,
    )
    assert encrypted(by_name) is True

    by_address = Postgres.with_config(config(
        postgres_settings,
        **{ConfigKey.POSTGRES_SSLMODE: 'verify-full',
           ConfigKey.POSTGRES_SSLROOTCERT: ca_base64()},
    ))
    with pytest.raises(Exception, match='does not match|certificate'):
        encrypted(by_address)


def test_a_server_from_another_ca_is_refused(postgres_settings, tmp_path):
    from integration_tests import tls_certs

    other = tls_certs.write(tmp_path / 'other')
    loader = Postgres.with_config(config(
        postgres_settings,
        **{ConfigKey.POSTGRES_SSLMODE: 'verify-ca',
           ConfigKey.POSTGRES_SSLROOTCERT: str(other / 'ca.pem')},
    ))
    with pytest.raises(Exception, match='certificate verify failed'):
        encrypted(loader)
