"""clusters.yaml + PEM truststores + keyring secrets, kept consistent behind one interface.

Layout under the config dir (directories 0700, files 0600, all writes atomic):

    clusters.yaml            hand-editable, never contains secrets
    truststores/<name>.pem   truststore converted to PEM at save time
"""

from __future__ import annotations

import contextlib
import os
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from kafka_web.config.models import ClusterBase, ClusterConfig, ClusterInput
from kafka_web.config.secrets import KeyringUnavailable, SecretStore
from kafka_web.config.truststore import CertSummary, summarize_pem, truststore_b64_to_pem
from kafka_web.errors import ConfigFileInvalid, Conflict, NotFound, ValidationFailed

_YAML_NAME = "clusters.yaml"
_TRUSTSTORE_DIR = "truststores"
_BASE_FIELDS = set(ClusterBase.model_fields)


@dataclass(frozen=True)
class Usability:
    usable: bool
    reason: str | None


class _TruststoreUnusable(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _secret_value(secret: Any) -> str | None:
    """Unwrap an optional `SecretStr`; an empty value means "not provided"."""
    return (secret.get_secret_value() if secret is not None else "") or None


def _write_atomic(path: Path, content: str) -> None:
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        tmp.chmod(0o600)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _ensure_private_dir(path: Path) -> None:
    if not path.exists():
        path.mkdir(parents=True)
        path.chmod(0o700)


class ClusterStore:
    def __init__(self, root: Path, secrets: SecretStore):
        self._root = root.absolute()
        self._secrets = secrets
        self._lock = threading.RLock()  # serializes read-modify-write cycles

    # --- reads --------------------------------------------------------------------------------

    def list(self) -> list[ClusterConfig]:
        path = self._root / _YAML_NAME
        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return []
        except (OSError, UnicodeDecodeError) as exc:
            raise ConfigFileInvalid(f"{_YAML_NAME} cannot be read: {exc}") from exc
        return self._parse(text)

    def get(self, name: str) -> ClusterConfig:
        return self._find(self.list(), name)

    def truststore_path(self, cfg: ClusterConfig) -> Path | None:
        return self._root / cfg.truststore if cfg.truststore else None

    def truststore_summary(self, cfg: ClusterConfig) -> list[CertSummary] | None:
        """Certificate summaries, or None when there is no (readable) truststore."""
        try:
            return self._read_truststore(cfg)
        except _TruststoreUnusable:
            return None

    def sasl_password(self, cfg: ClusterConfig) -> str | None:
        return self._secrets.get(cfg.name) if cfg.uses_sasl else None

    def usability(self, cfg: ClusterConfig) -> Usability:
        if cfg.uses_tls:
            try:
                self._read_truststore(cfg)
            except _TruststoreUnusable as exc:
                return Usability(False, exc.reason)
        if cfg.uses_sasl:
            try:
                password = self._secrets.get(cfg.name)
            except KeyringUnavailable as exc:
                return Usability(False, f"OS keyring unavailable: {exc.message}")
            if password is None:
                return Usability(False, "SASL password missing from keyring")
        return Usability(True, None)

    # --- writes -------------------------------------------------------------------------------

    def materialize(
        self, inp: ClusterInput, *, existing: ClusterConfig | None
    ) -> tuple[ClusterConfig, str | None, str | None]:
        """Validate and convert `inp` without persisting anything.

        Returns `(config, pem, sasl_password)`; `pem` / `sasl_password` are None when the existing
        truststore / secret are to be kept (or not applicable).
        """
        if existing is not None and existing.name != inp.name:
            raise ValidationFailed(
                "name: a cluster cannot be renamed; delete and re-add it",
                code="name_immutable",
                field="name",
            )
        fields = inp.model_dump(include=_BASE_FIELDS)

        pem: str | None = None
        truststore: str | None = None
        if inp.uses_tls:
            b64 = (inp.truststore_base64 or "").strip()
            if b64:
                pem = truststore_b64_to_pem(b64, _secret_value(inp.truststore_password))
                truststore = self._pem_relpath(inp.name)
            elif existing is not None and existing.uses_tls and existing.truststore:
                truststore = existing.truststore
            else:
                raise ValidationFailed(
                    "truststore: a truststore is required for SSL protocols",
                    code="truststore_required",
                    field="truststore",
                )

        password: str | None = None
        if inp.uses_sasl:
            password = _secret_value(inp.sasl_password)
            if password is None and not (existing is not None and existing.uses_sasl):
                raise ValidationFailed(
                    "sasl_password: required for SASL protocols",
                    code="sasl_password_required",
                    field="sasl_password",
                )

        return ClusterConfig(**fields, truststore=truststore), pem, password

    def create(self, inp: ClusterInput) -> ClusterConfig:
        with self._lock:
            clusters = self.list()
            if any(c.name == inp.name for c in clusters):
                raise Conflict(f"Cluster {inp.name!r} already exists", code="cluster_exists")
            cfg, pem, password = self.materialize(inp, existing=None)
            pem_path = self._pem_path(cfg.name)
            try:
                if pem is not None:
                    self._write_pem(cfg.name, pem)
                if password is not None:
                    self._secrets.set(cfg.name, "sasl_password", password)
                self._write_yaml([*clusters, cfg])
            except BaseException:
                pem_path.unlink(missing_ok=True)
                with contextlib.suppress(KeyringUnavailable):  # keep the original error
                    self._secrets.delete(cfg.name)
                raise
            return cfg

    def update(self, name: str, inp: ClusterInput) -> ClusterConfig:
        with self._lock:
            clusters = self.list()
            existing = self._find(clusters, name)
            cfg, pem, password = self.materialize(inp, existing=existing)
            if pem is not None:
                self._write_pem(cfg.name, pem)
            if password is not None:
                self._secrets.set(cfg.name, "sasl_password", password)
            self._write_yaml([cfg if c.name == name else c for c in clusters])
            if existing.uses_tls and not cfg.uses_tls:
                self._pem_path(name).unlink(missing_ok=True)
            if existing.uses_sasl and not cfg.uses_sasl:
                self._secrets.delete(name)
            return cfg

    def delete(self, name: str) -> None:
        with self._lock:
            clusters = self.list()
            cfg = self._find(clusters, name)
            if cfg.uses_sasl:
                self._secrets.delete(name)
            # Only the PEM this store manages; a hand-edited external truststore path is left alone.
            self._pem_path(name).unlink(missing_ok=True)
            self._write_yaml([c for c in clusters if c.name != name])

    # --- internals ----------------------------------------------------------------------------

    @staticmethod
    def _find(clusters: list[ClusterConfig], name: str) -> ClusterConfig:
        for cfg in clusters:
            if cfg.name == name:
                return cfg
        raise NotFound(f"Cluster {name!r} does not exist", code="cluster_not_found")

    @staticmethod
    def _pem_relpath(name: str) -> str:
        return f"{_TRUSTSTORE_DIR}/{name}.pem"

    def _pem_path(self, name: str) -> Path:
        return self._root / self._pem_relpath(name)

    def _write_pem(self, name: str, pem: str) -> None:
        _ensure_private_dir(self._root)
        _ensure_private_dir(self._root / _TRUSTSTORE_DIR)
        _write_atomic(self._pem_path(name), pem)

    def _write_yaml(self, clusters: list[ClusterConfig]) -> None:
        _ensure_private_dir(self._root)
        doc = {"clusters": [c.model_dump(mode="json", exclude_none=True) for c in clusters]}
        _write_atomic(self._root / _YAML_NAME, yaml.safe_dump(doc, sort_keys=False))

    def _read_truststore(self, cfg: ClusterConfig) -> list[CertSummary]:
        path = self.truststore_path(cfg)
        if path is None:
            raise _TruststoreUnusable("cluster has no truststore")
        try:
            pem = path.read_text(encoding="ascii")
        except FileNotFoundError:
            raise _TruststoreUnusable(f"truststore file missing: {cfg.truststore}") from None
        except (OSError, UnicodeDecodeError):
            raise _TruststoreUnusable(f"truststore file unreadable: {cfg.truststore}") from None
        try:
            certs = summarize_pem(pem)
        except ValueError:
            certs = []
        if not certs:
            raise _TruststoreUnusable(f"truststore file has no certificates: {cfg.truststore}")
        return certs

    @staticmethod
    def _parse(text: str) -> list[ClusterConfig]:
        try:
            doc = yaml.safe_load(text)
        except yaml.MarkedYAMLError as exc:
            line = exc.problem_mark.line + 1 if exc.problem_mark else "?"
            raise ConfigFileInvalid(
                f"{_YAML_NAME} is not valid YAML (line {line}): {exc.problem}"
            ) from None
        except yaml.YAMLError:
            raise ConfigFileInvalid(f"{_YAML_NAME} is not valid YAML") from None

        if doc is None:
            return []
        if not isinstance(doc, dict):
            raise ConfigFileInvalid(f"{_YAML_NAME} must be a mapping with a 'clusters' list")
        entries = doc.get("clusters")
        if entries is None:
            return []
        if not isinstance(entries, list):
            raise ConfigFileInvalid(f"{_YAML_NAME} must contain a 'clusters' list")

        clusters: list[ClusterConfig] = []
        for index, entry in enumerate(entries, start=1):
            if not isinstance(entry, dict):
                raise ConfigFileInvalid(f"{_YAML_NAME} entry {index} is not a mapping")
            label = f"entry {index} ({entry.get('name')!r})"
            try:
                cfg = ClusterConfig(**entry)
            except ValidationFailed as exc:
                raise ConfigFileInvalid(f"{_YAML_NAME} {label}: {exc.message}") from None
            except TypeError:  # non-string keys cannot be passed as keyword arguments
                raise ConfigFileInvalid(f"{_YAML_NAME} {label}: keys must be strings") from None
            if any(c.name == cfg.name for c in clusters):
                raise ConfigFileInvalid(f"{_YAML_NAME} {label}: duplicate cluster name")
            clusters.append(cfg)
        return clusters
