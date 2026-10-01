import datetime as dt
import hashlib
from dataclasses import dataclass
from ipaddress import ip_address

import jks
import keyring.backend
import keyring.errors
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import NameOID

_JKS_SIGNATURE_WHITENING = b"Mighty Aphrodite"


@dataclass
class CertBundle:
    ca_cert: x509.Certificate
    ca_key: rsa.RSAPrivateKey
    leaf_cert: x509.Certificate  # signed by CA, CN=localhost, SAN DNS:localhost, IP:127.0.0.1
    leaf_key: rsa.RSAPrivateKey


def _name(common_name: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])


@pytest.fixture(scope="session")
def certs() -> CertBundle:
    now = dt.datetime.now(dt.UTC)
    not_before = now - dt.timedelta(days=1)
    not_after = now + dt.timedelta(days=365)

    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(_name("Test CA"))
        .issuer_name(_name("Test CA"))
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )

    leaf_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    leaf_cert = (
        x509.CertificateBuilder()
        .subject_name(_name("localhost"))
        .issuer_name(ca_cert.subject)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.IPAddress(ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    return CertBundle(ca_cert, ca_key, leaf_cert, leaf_key)


def make_jks(certs: list[x509.Certificate], password: str, *, jceks: bool = False) -> bytes:
    entries = [
        jks.TrustedCertEntry.new(f"ca{i}", c.public_bytes(serialization.Encoding.DER))
        for i, c in enumerate(certs)
    ]
    data = jks.KeyStore.new("jks", entries).saves(password)
    if not jceks:
        return data
    # pyjks cannot write JCEKS. For trusted-cert entries the layout is identical to JKS apart from
    # the magic number, so swap it and recompute the trailing integrity hash (which covers it).
    body = b"\xce\xce\xce\xce" + data[4 : -hashlib.sha1().digest_size]
    digest = hashlib.sha1(password.encode("utf-16be") + _JKS_SIGNATURE_WHITENING + body)
    return body + digest.digest()


def make_pkcs12_truststore(certs: list[x509.Certificate], password: str | None) -> bytes:
    encryption = (
        serialization.BestAvailableEncryption(password.encode())
        if password
        else serialization.NoEncryption()
    )
    return pkcs12.serialize_key_and_certificates(
        name=None, key=None, cert=None, cas=certs, encryption_algorithm=encryption
    )


class MemoryKeyring(keyring.backend.KeyringBackend):
    """In-memory keyring so unit tests never touch the real OS keyring."""

    priority = 1

    def __init__(self):
        self.data: dict[tuple[str, str], str] = {}

    def get_password(self, service, username):
        return self.data.get((service, username))

    def set_password(self, service, username, password):
        self.data[(service, username)] = password

    def delete_password(self, service, username):
        if (service, username) not in self.data:
            raise keyring.errors.PasswordDeleteError()
        del self.data[(service, username)]


@pytest.fixture
def memory_keyring() -> MemoryKeyring:
    return MemoryKeyring()
