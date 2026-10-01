import base64
import datetime as dt
import textwrap

import jks
import pytest
from cryptography import x509
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.serialization import pkcs12

from kafka_web.config.truststore import (
    CertSummary,
    TruststoreError,
    summarize_pem,
    truststore_b64_to_pem,
    truststore_to_pem,
)
from tests.conftest import make_jks, make_pkcs12_truststore


def fingerprints(certs: list[x509.Certificate]) -> list[bytes]:
    return [c.fingerprint(hashes.SHA256()) for c in certs]


def pem_fingerprints(pem: str) -> list[bytes]:
    return fingerprints(x509.load_pem_x509_certificates(pem.encode()))


def der(cert: x509.Certificate) -> bytes:
    return cert.public_bytes(serialization.Encoding.DER)


def pem(cert: x509.Certificate) -> bytes:
    return cert.public_bytes(serialization.Encoding.PEM)


def test_jks_with_password(certs):
    out = truststore_to_pem(make_jks([certs.ca_cert], "changeit"), "changeit")

    assert pem_fingerprints(out) == fingerprints([certs.ca_cert])
    assert out.endswith("-----END CERTIFICATE-----\n")


def test_jceks_with_password(certs):
    out = truststore_to_pem(make_jks([certs.ca_cert], "changeit", jceks=True), "changeit")

    assert pem_fingerprints(out) == fingerprints([certs.ca_cert])


def test_jks_two_certs_order_independent(certs):
    data = make_jks([certs.ca_cert, certs.leaf_cert], "changeit")

    out = truststore_to_pem(data, "changeit")

    assert set(pem_fingerprints(out)) == set(fingerprints([certs.ca_cert, certs.leaf_cert]))
    assert len(pem_fingerprints(out)) == 2


def test_jks_private_key_entries_are_ignored(certs):
    key_der = certs.leaf_key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    store = jks.KeyStore.new(
        "jks",
        [
            jks.TrustedCertEntry.new("ca", der(certs.ca_cert)),
            jks.PrivateKeyEntry.new("leaf", [der(certs.leaf_cert)], key_der),
        ],
    )

    out = truststore_to_pem(store.saves("changeit"), "changeit")

    assert pem_fingerprints(out) == fingerprints([certs.ca_cert])


def test_jks_wrong_password(certs):
    with pytest.raises(TruststoreError) as exc:
        truststore_to_pem(make_jks([certs.ca_cert], "changeit"), "wrong")

    assert exc.value.code == "truststore_invalid_password"
    assert exc.value.field == "truststore"


def test_jks_missing_password(certs):
    # pyjks skips the integrity check when the password is None, and trusted-cert entries are
    # stored unencrypted, so a password-less JKS is converted rather than rejected.
    out = truststore_to_pem(make_jks([certs.ca_cert], "changeit"), None)

    assert pem_fingerprints(out) == fingerprints([certs.ca_cert])


def test_pkcs12_with_password(certs):
    data = make_pkcs12_truststore([certs.ca_cert, certs.leaf_cert], "changeit")

    out = truststore_to_pem(data, "changeit")

    assert set(pem_fingerprints(out)) == set(fingerprints([certs.ca_cert, certs.leaf_cert]))


def test_pkcs12_without_password(certs):
    out = truststore_to_pem(make_pkcs12_truststore([certs.ca_cert], None), None)

    assert pem_fingerprints(out) == fingerprints([certs.ca_cert])


def test_pkcs12_wrong_password(certs):
    with pytest.raises(TruststoreError) as exc:
        truststore_to_pem(make_pkcs12_truststore([certs.ca_cert], "changeit"), "wrong")

    assert exc.value.code == "truststore_invalid_password"
    assert exc.value.field == "truststore"


def test_pkcs12_missing_password(certs):
    with pytest.raises(TruststoreError) as exc:
        truststore_to_pem(make_pkcs12_truststore([certs.ca_cert], "changeit"), None)

    assert exc.value.code == "truststore_password_required"


def test_der_single_cert(certs):
    out = truststore_to_pem(der(certs.ca_cert), None)

    assert pem_fingerprints(out) == fingerprints([certs.ca_cert])


def test_pem_passthrough_normalized(certs):
    raw = b"  \r\n" + pem(certs.ca_cert).replace(b"\n", b"\r\n") + pem(certs.leaf_cert)

    out = truststore_to_pem(raw, None)

    assert pem_fingerprints(out) == fingerprints([certs.ca_cert, certs.leaf_cert])
    assert "\r" not in out


def test_pem_with_no_certificates():
    with pytest.raises(TruststoreError) as exc:
        truststore_to_pem(b"-----BEGIN FOO-----\n-----END FOO-----\n", None)

    assert exc.value.code == "truststore_empty"
    assert exc.value.field == "truststore"


def test_jks_with_no_trusted_certs_is_empty():
    with pytest.raises(TruststoreError) as exc:
        truststore_to_pem(jks.KeyStore.new("jks", []).saves("changeit"), "changeit")

    assert exc.value.code == "truststore_empty"


def test_corrupt_pem_certificate_is_unrecognized():
    bad = b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"

    with pytest.raises(TruststoreError) as exc:
        truststore_to_pem(bad, None)

    assert exc.value.code == "truststore_unrecognized_format"


def test_garbage_bytes():
    with pytest.raises(TruststoreError) as exc:
        truststore_to_pem(b"hello world", None)

    assert exc.value.code == "truststore_unrecognized_format"
    assert "magic=" in exc.value.message
    assert exc.value.field == "truststore"


def test_garbage_der_is_unrecognized():
    with pytest.raises(TruststoreError) as exc:
        truststore_to_pem(b"\x30\x03abc", None)

    assert exc.value.code == "truststore_unrecognized_format"


def test_truncated_jks_is_unrecognized(certs):
    with pytest.raises(TruststoreError) as exc:
        truststore_to_pem(make_jks([certs.ca_cert], "changeit")[:20], "changeit")

    assert exc.value.code == "truststore_unrecognized_format"


def test_b64_invalid():
    with pytest.raises(TruststoreError) as exc:
        truststore_b64_to_pem("not base64!!", None)

    assert exc.value.code == "truststore_invalid_base64"
    assert exc.value.field == "truststore"


def test_b64_with_newlines_ok(certs):
    encoded = base64.b64encode(make_jks([certs.ca_cert], "changeit")).decode()
    wrapped = "\n".join(textwrap.wrap(encoded, 76)) + "\n"

    out = truststore_b64_to_pem(wrapped, "changeit")

    assert pem_fingerprints(out) == fingerprints([certs.ca_cert])


def test_summarize_pem(certs):
    out = truststore_to_pem(der(certs.ca_cert), None)

    [summary] = summarize_pem(out)

    assert isinstance(summary, CertSummary)
    assert summary.subject == "CN=Test CA"
    assert summary.not_after.utcoffset() == dt.timedelta(0)
    expected = dt.datetime.now(dt.UTC) + dt.timedelta(days=365)
    assert abs(summary.not_after - expected) < dt.timedelta(days=1)


def test_error_message_never_contains_password(certs):
    secret = "s3cr3t-xyz"
    cases = [
        (make_jks([certs.ca_cert], "changeit"), secret),
        (make_pkcs12_truststore([certs.ca_cert], "changeit"), secret),
        (b"hello world", secret),
    ]
    for data, password in cases:
        with pytest.raises(TruststoreError) as exc:
            truststore_to_pem(data, password)

        assert secret not in str(exc.value)
        assert secret not in exc.value.message


def test_b64_non_ascii_is_invalid_base64():
    with pytest.raises(TruststoreError) as exc:
        truststore_b64_to_pem("é==", None)

    assert exc.value.code == "truststore_invalid_base64"
    assert exc.value.field == "truststore"


@pytest.mark.parametrize("password", ["changeit", None])
def test_jks_with_unparseable_cert_entry(password):
    store = jks.KeyStore.new("jks", [jks.TrustedCertEntry.new("bad", b"notacert")])

    with pytest.raises(TruststoreError) as exc:
        truststore_to_pem(store.saves("changeit"), password)

    assert exc.value.code == "truststore_unrecognized_format"
    assert exc.value.field == "truststore"


def test_every_truncation_of_a_jks_is_a_truststore_error(certs):
    data = make_jks([certs.ca_cert], "changeit")

    for password in (None, "changeit"):
        for cut in range(len(data) - 1):
            try:
                truststore_to_pem(data[:cut], password)
            except TruststoreError:
                continue
            except Exception as exc:
                pytest.fail(f"truncation at {cut} (password={password!r}) leaked {exc!r}")


@pytest.mark.filterwarnings("ignore::cryptography.utils.CryptographyDeprecationWarning")
def test_every_single_byte_corruption_of_a_jks_is_a_truststore_error(certs):
    data = make_jks([certs.ca_cert], "changeit")

    for password in (None, "changeit"):
        for pos in range(len(data)):
            mutated = data[:pos] + bytes([data[pos] ^ 0xFF]) + data[pos + 1 :]
            try:
                truststore_to_pem(mutated, password)
            except TruststoreError:
                continue
            except Exception as exc:
                pytest.fail(f"corruption at {pos} (password={password!r}) leaked {exc!r}")


def test_pkcs12_with_unsupported_legacy_encryption(monkeypatch):
    # Older Java keytool writes RC2-40 PKCS12; OpenSSL 3 rejects it unless the legacy provider is
    # loaded. Simulated here because a real RC2 file cannot be produced with the installed tools.
    def raise_unsupported(*args, **kwargs):
        raise UnsupportedAlgorithm("RC2 is disabled")

    monkeypatch.setattr(pkcs12, "load_pkcs12", raise_unsupported)

    with pytest.raises(TruststoreError) as exc:
        truststore_to_pem(b"\x30\x03abc", "changeit")

    assert exc.value.code == "truststore_unrecognized_format"
    assert "AES" in exc.value.message
    assert exc.value.field == "truststore"


def test_pem_with_only_trusted_certificate_block_is_empty():
    # cryptography cannot parse OpenSSL's "TRUSTED CERTIFICATE" blocks, so they do not count.
    with pytest.raises(TruststoreError) as exc:
        truststore_to_pem(
            b"-----BEGIN TRUSTED CERTIFICATE-----\nAAAA\n-----END TRUSTED CERTIFICATE-----\n",
            None,
        )

    assert exc.value.code == "truststore_empty"
