"""TLS settings shared by the database clients."""
import base64
import binascii
import hashlib
import os
import tempfile
from typing import Optional

PEM_MARKER = '-----BEGIN CERTIFICATE-----'


def ca_certificate_file(value: Optional[str]) -> Optional[str]:
    """
    A CA certificate as a file path for drivers that take one. The value is a path, a PEM
    certificate, or a base64-encoded PEM certificate, as IBM Cloud Databases service
    credentials give it (`certificate.certificate_base64`). Certificates in the value are
    written once to a private file named by their hash.
    """
    if not value:
        return None
    value = value.strip()
    if PEM_MARKER in value:
        pem = value
    elif os.path.exists(os.path.expanduser(value)):
        return os.path.expanduser(value)
    else:
        try:
            pem = base64.b64decode(value, validate=True).decode('utf-8')
        except (binascii.Error, UnicodeDecodeError):
            pem = ''
        if PEM_MARKER not in pem:
            raise ValueError(
                'The CA certificate is neither a file that exists, a PEM certificate, nor a '
                'base64-encoded PEM certificate.'
            )
    pem = pem.strip() + '\n'
    digest = hashlib.sha256(pem.encode()).hexdigest()[:24]
    directory = os.path.join(tempfile.gettempdir(), f'mage-ca-{os.getuid()}')
    os.makedirs(directory, mode=0o700, exist_ok=True)
    path = os.path.join(directory, f'{digest}.pem')
    if not os.path.exists(path):
        partial = f'{path}.{os.getpid()}'
        with open(os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), 'w') as file:
            file.write(pem)
        os.replace(partial, path)
    return path
