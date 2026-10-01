"""Convert JKS / JCEKS / PKCS12 / DER / PEM truststores to a PEM bundle (librdkafka only reads PEM).

Port of `truststore_b64_to_pem` from the Delta CDF notebook, made strict: every failure is a
`TruststoreError` with a stable code, and no message ever contains the password.
"""

import base64
import binascii
import re
from dataclasses import dataclass
from datetime import datetime

import jks
from cryptography import x509
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives.serialization import Encoding, pkcs12

from kafka_web.errors import ValidationFailed

_JKS_MAGICS = (b"\xfe\xed\xfe\xed", b"\xce\xce\xce\xce")
_PEM_CERT_MARKER = re.compile(rb"-----BEGIN (?:X509 )?CERTIFICATE-----")


class TruststoreError(ValidationFailed):
    """Raised for any truststore problem; always reported against the `truststore` field."""

    def __init__(self, message: str, *, code: str):
        super().__init__(message, code=code, field="truststore")


@dataclass(frozen=True)
class CertSummary:
    subject: str  # RFC 4514 string, e.g. "CN=Test CA"
    not_after: datetime  # tz-aware UTC


def truststore_b64_to_pem(b64: str, password: str | None) -> str:
    try:
        data = base64.b64decode("".join(b64.split()), validate=True)
    except (binascii.Error, ValueError) as exc:  # ValueError: non-ASCII input
        raise TruststoreError(
            "Truststore is not valid base64", code="truststore_invalid_base64"
        ) from exc
    return truststore_to_pem(data, password)


def truststore_to_pem(data: bytes, password: str | None) -> str:
    password = password or None  # an empty form field means "no password"
    magic = data[:4]

    if magic in _JKS_MAGICS:
        certs = _certs_from_jks(data, password)
    elif data.lstrip().startswith(b"-----BEGIN"):
        certs = _certs_from_pem(data)
    elif magic[:1] == b"\x30":
        certs = _certs_from_der(data, password)
    else:
        raise TruststoreError(
            f"Unrecognized truststore format, magic={magic.hex()}",
            code="truststore_unrecognized_format",
        )

    if not certs:
        raise TruststoreError("Truststore contains no certificates", code="truststore_empty")
    return "".join(c.public_bytes(Encoding.PEM).decode("ascii") for c in certs)


def summarize_pem(pem: str) -> list[CertSummary]:
    return [
        CertSummary(subject=c.subject.rfc4514_string(), not_after=c.not_valid_after_utc)
        for c in x509.load_pem_x509_certificates(pem.encode("ascii"))
    ]


def _certs_from_jks(data: bytes, password: str | None) -> list[x509.Certificate]:
    try:
        # With password=None pyjks skips the keystore integrity check. Trusted-cert entries are
        # unencrypted, so they are readable without it. Key entries are not needed (and not
        # decrypted): only trusted certificates belong in a truststore.
        store = jks.KeyStore.loads(data, password, try_decrypt_keys=False)
        return [x509.load_der_x509_certificate(entry.cert) for entry in store.certs.values()]
    except jks.KeystoreSignatureException as exc:
        raise TruststoreError(
            "Incorrect truststore password", code="truststore_invalid_password"
        ) from exc
    except Exception as exc:
        # pyjks and cryptography leak many exception shapes (struct, ValueError, ...) on corrupt
        # input. Never echo their text: it could contain data from the store.
        raise TruststoreError(
            "Unreadable JKS/JCEKS truststore", code="truststore_unrecognized_format"
        ) from exc


def _certs_from_pem(data: bytes) -> list[x509.Certificate]:
    if not _PEM_CERT_MARKER.search(data):
        return []
    try:
        return x509.load_pem_x509_certificates(data)
    except ValueError as exc:
        raise TruststoreError(
            "Malformed PEM certificate", code="truststore_unrecognized_format"
        ) from exc


def _certs_from_der(data: bytes, password: str | None) -> list[x509.Certificate]:
    try:
        bundle = pkcs12.load_pkcs12(data, password.encode() if password else None)
    except UnsupportedAlgorithm as exc:
        raise TruststoreError(
            "Truststore uses an encryption algorithm that is not supported (typically legacy "
            "RC2 PKCS12 from an old Java keytool); re-export it with modern (AES) encryption "
            "or as PEM/JKS",
            code="truststore_unrecognized_format",
        ) from exc
    except ValueError as exc:
        if "password" in str(exc).lower():
            raise _password_error(password) from exc
        # Not a PKCS12 container; it may still be a single DER certificate.
        try:
            return [x509.load_der_x509_certificate(data)]
        except ValueError as der_exc:
            raise TruststoreError(
                f"Unrecognized truststore format, magic={data[:4].hex()}",
                code="truststore_unrecognized_format",
            ) from der_exc
    certs = [c.certificate for c in bundle.additional_certs]
    if bundle.cert is not None:
        certs.insert(0, bundle.cert.certificate)
    return certs


def _password_error(password: str | None) -> TruststoreError:
    if password is None:
        return TruststoreError(
            "Truststore requires a password", code="truststore_password_required"
        )
    return TruststoreError("Incorrect truststore password", code="truststore_invalid_password")
