import os

import paramiko
from sshtunnel import SSHTunnelForwarder as BaseSSHTunnelForwarder


class SSHTunnelForwarder(BaseSSHTunnelForwarder):
    """Load SSH keys without sshtunnel 0.4's removed Paramiko DSA API."""

    @staticmethod
    def read_private_key_file(pkey_file, pkey_password=None, key_type=None, logger=None):
        key_types = (key_type,) if key_type else (
            paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key,
        )
        for key_class in key_types:
            try:
                return key_class.from_private_key_file(pkey_file, password=pkey_password)
            except paramiko.PasswordRequiredException:
                if logger:
                    logger.warning('SSH private key requires a password: %s', pkey_file)
                break
            except paramiko.SSHException:
                continue
        return None

    @classmethod
    def get_keys(cls, logger=None, host_pkey_directories=None, allow_agent=False):
        keys = cls.get_agent_keys(logger=logger) if allow_agent else []
        directories = ['~/.ssh'] if host_pkey_directories is None else host_pkey_directories
        for directory in directories:
            for name in ('rsa', 'ecdsa', 'ed25519'):
                path = os.path.expanduser(os.path.join(directory, f'id_{name}'))
                try:
                    if os.path.isfile(path):
                        key = cls.read_private_key_file(path, logger=logger)
                        if key is not None:
                            keys.append(key)
                except OSError:
                    if logger:
                        logger.warning('Cannot read SSH private key: %s', path)
        return keys

    @classmethod
    def _consolidate_auth(
        cls, ssh_password=None, ssh_pkey=None, ssh_pkey_password=None,
        allow_agent=True, host_pkey_directories=None, logger=None,
    ):
        keys = cls.get_keys(
            logger=logger, host_pkey_directories=host_pkey_directories,
            allow_agent=allow_agent,
        )
        if isinstance(ssh_pkey, (str, os.PathLike)):
            ssh_pkey = cls.read_private_key_file(
                os.path.expanduser(ssh_pkey),
                pkey_password=ssh_pkey_password or ssh_password,
                logger=logger,
            )
        if isinstance(ssh_pkey, paramiko.PKey):
            keys.insert(0, ssh_pkey)
        if not ssh_password and not keys:
            raise ValueError('No password or public key available')
        return ssh_password, keys
