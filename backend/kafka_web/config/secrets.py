"""OS-keyring storage for cluster secrets. There is deliberately no plaintext fallback."""

import keyring
import keyring.backend
import keyring.backends.chainer
import keyring.backends.fail
import keyring.backends.null
import keyring.errors

from kafka_web.errors import AppError

_NO_KEYRING_MESSAGE = (
    "No usable OS keyring is available; secrets cannot be stored. "
    "Install and unlock a keyring service (e.g. GNOME Keyring, KWallet or KeePassXC)."
)


class KeyringUnavailable(AppError):
    status = 500
    code = "keyring_unavailable"


def _is_unusable(backend: keyring.backend.KeyringBackend) -> bool:
    if isinstance(backend, keyring.backends.fail.Keyring | keyring.backends.null.Keyring):
        return True
    if isinstance(backend, keyring.backends.chainer.ChainerBackend):
        return not backend.backends
    try:
        return backend.priority < 1
    except RuntimeError:  # keyring raises this for backends that are not viable
        return True


class SecretStore:
    SERVICE = "kafka-web"

    def __init__(self, backend: keyring.backend.KeyringBackend | None = None):
        self._backend = backend if backend is not None else keyring.get_keyring()

    @staticmethod
    def _username(cluster: str, field: str) -> str:
        return f"{cluster}:{field}"

    def _require_usable(self) -> None:
        if _is_unusable(self._backend):
            raise KeyringUnavailable(_NO_KEYRING_MESSAGE)

    def get(self, cluster: str, field: str = "sasl_password") -> str | None:
        self._require_usable()
        try:
            return self._backend.get_password(self.SERVICE, self._username(cluster, field))
        except keyring.errors.KeyringError as exc:
            raise self._unavailable("read", exc) from None

    def set(self, cluster: str, field: str, value: str) -> None:
        self._require_usable()
        try:
            self._backend.set_password(self.SERVICE, self._username(cluster, field), value)
        except keyring.errors.KeyringError as exc:
            raise self._unavailable("write", exc) from None

    def delete(self, cluster: str, field: str = "sasl_password") -> None:
        if _is_unusable(self._backend):
            return  # nothing can have been stored in a keyring that was never usable
        try:
            self._backend.delete_password(self.SERVICE, self._username(cluster, field))
        except keyring.errors.PasswordDeleteError:
            return  # already absent
        except keyring.errors.KeyringError as exc:
            raise self._unavailable("delete", exc) from None

    @staticmethod
    def _unavailable(action: str, exc: keyring.errors.KeyringError) -> KeyringUnavailable:
        return KeyringUnavailable(
            f"Could not {action} the secret in the OS keyring ({type(exc).__name__})"
        )
