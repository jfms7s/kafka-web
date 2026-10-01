import keyring.backend
import keyring.backends.chainer
import keyring.backends.fail
import keyring.backends.null
import keyring.errors
import pytest

from kafka_web.config.secrets import KeyringUnavailable, SecretStore
from kafka_web.errors import AppError
from tests.conftest import MemoryKeyring


def test_set_get_roundtrip_uses_service_and_username(memory_keyring: MemoryKeyring):
    store = SecretStore(memory_keyring)
    store.set("stg-eu", "sasl_password", "s3cret")
    assert store.get("stg-eu") == "s3cret"
    assert store.get("stg-eu", "sasl_password") == "s3cret"
    assert memory_keyring.data == {("kafka-web", "stg-eu:sasl_password"): "s3cret"}


def test_get_missing_returns_none(memory_keyring: MemoryKeyring):
    assert SecretStore(memory_keyring).get("nope") is None


def test_set_overwrites(memory_keyring: MemoryKeyring):
    store = SecretStore(memory_keyring)
    store.set("a", "sasl_password", "one")
    store.set("a", "sasl_password", "two")
    assert store.get("a") == "two"


def test_delete_removes_secret(memory_keyring: MemoryKeyring):
    store = SecretStore(memory_keyring)
    store.set("a", "sasl_password", "x")
    store.delete("a")
    assert store.get("a") is None


def test_delete_missing_is_noop(memory_keyring: MemoryKeyring):
    SecretStore(memory_keyring).delete("never-existed")


def test_keyring_unavailable_is_an_app_error():
    assert issubclass(KeyringUnavailable, AppError)
    assert KeyringUnavailable.status == 500
    assert KeyringUnavailable.code == "keyring_unavailable"


class LowPriorityKeyring(MemoryKeyring):
    priority = 0.5


class EmptyChainer(keyring.backends.chainer.ChainerBackend):
    backends = []  # noqa: RUF012  (mirrors a chainer with no usable children)


@pytest.mark.parametrize(
    "backend",
    [
        keyring.backends.fail.Keyring(),
        keyring.backends.null.Keyring(),
        LowPriorityKeyring(),
        EmptyChainer(),
    ],
    ids=["fail", "null", "low-priority", "empty-chainer"],
)
def test_unusable_backends_raise_on_set_and_get(backend):
    store = SecretStore(backend)
    with pytest.raises(KeyringUnavailable):
        store.set("a", "sasl_password", "x")
    with pytest.raises(KeyringUnavailable):
        store.get("a")


@pytest.mark.parametrize(
    "backend", [keyring.backends.fail.Keyring(), keyring.backends.null.Keyring()]
)
def test_delete_on_unusable_backend_is_noop(backend):
    SecretStore(backend).delete("a")


def test_unavailable_error_never_contains_secret():
    with pytest.raises(KeyringUnavailable) as exc:
        SecretStore(keyring.backends.fail.Keyring()).set("a", "sasl_password", "hunter2")
    assert "hunter2" not in exc.value.message


class LockedKeyring(MemoryKeyring):
    def get_password(self, service, username):
        raise keyring.errors.KeyringLocked("locked")

    def set_password(self, service, username, password):
        raise keyring.errors.PasswordSetError("locked")

    def delete_password(self, service, username):
        raise keyring.errors.KeyringLocked("locked")


def test_runtime_keyring_errors_become_keyring_unavailable():
    store = SecretStore(LockedKeyring())
    with pytest.raises(KeyringUnavailable):
        store.get("a")
    with pytest.raises(KeyringUnavailable):
        store.set("a", "sasl_password", "x")
    with pytest.raises(KeyringUnavailable):
        store.delete("a")


def test_default_backend_comes_from_keyring_get_keyring(monkeypatch, memory_keyring):
    monkeypatch.setattr(keyring, "get_keyring", lambda: memory_keyring)
    store = SecretStore()
    store.set("a", "sasl_password", "x")
    assert memory_keyring.data[("kafka-web", "a:sasl_password")] == "x"
