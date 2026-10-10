"""
A test CA and a server certificate for the PostgreSQL service, so TLS connections can be
tested: `make up` writes them to .certs before starting the services.

The server certificate names only db.test, not localhost or 127.0.0.1, so verify-full
fails on the name while verify-ca passes.
"""
import datetime
import sys
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

CERTS = Path(__file__).resolve().parent / '.certs'
SERVER_NAME = 'db.test'


def _name(common_name: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])


def _write_key(key, path: Path) -> None:
    path.write_bytes(key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ))
    path.chmod(0o600)


def write(directory: Path = CERTS) -> Path:
    if (directory / 'server.pem').is_file():
        return directory
    directory.mkdir(parents=True, exist_ok=True)
    now = datetime.datetime.now(datetime.timezone.utc)
    start, end = now - datetime.timedelta(days=1), now + datetime.timedelta(days=3650)

    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca = (
        x509.CertificateBuilder()
        .subject_name(_name('Mage integration test CA'))
        .issuer_name(_name('Mage integration test CA'))
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(start).not_valid_after(end)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.KeyUsage(
            digital_signature=True, key_cert_sign=True, crl_sign=True,
            content_commitment=False, key_encipherment=False, data_encipherment=False,
            key_agreement=False, encipher_only=False, decipher_only=False,
        ), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()),
                       critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    server_key = ec.generate_private_key(ec.SECP256R1())
    server = (
        x509.CertificateBuilder()
        .subject_name(_name(SERVER_NAME))
        .issuer_name(ca.subject)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(start).not_valid_after(end)
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(SERVER_NAME)]),
                       critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH]),
                       critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
                       critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    (directory / 'ca.pem').write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    _write_key(server_key, directory / 'server.key')
    (directory / 'server.pem').write_bytes(server.public_bytes(serialization.Encoding.PEM))
    return directory


if __name__ == '__main__':
    print(write(Path(sys.argv[1]) if len(sys.argv) > 1 else CERTS))
