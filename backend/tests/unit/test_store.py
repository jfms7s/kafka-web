import base64
import stat
from pathlib import Path

import keyring.backends.fail
import pytest
import yaml

from kafka_web.config.models import ClusterConfig, ClusterInput
from kafka_web.config.secrets import KeyringUnavailable, SecretStore
from kafka_web.config.store import ClusterStore, Usability
from kafka_web.config.truststore import TruststoreError, summarize_pem
from kafka_web.errors import ConfigFileInvalid, Conflict, NotFound, ValidationFailed
from tests.conftest import CertBundle, MemoryKeyring, make_jks

SASL_SECRET = "sasl-s3cret-value"
TRUSTSTORE_PASSWORD = "truststore-pw-value"


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    return tmp_path / "cfg"


@pytest.fixture
def store(root: Path, memory_keyring: MemoryKeyring) -> ClusterStore:
    return ClusterStore(root, SecretStore(memory_keyring))


def jks_b64(certs: CertBundle, *, leaf: bool = False) -> str:
    cert = certs.leaf_cert if leaf else certs.ca_cert
    return b64(make_jks([cert], TRUSTSTORE_PASSWORD))


def plaintext(name: str = "dev", **kw) -> ClusterInput:
    return ClusterInput(name=name, env="dev", bootstrap_servers="b1:9092", **kw)


def sasl_ssl(certs: CertBundle, name: str = "stg-eu", **kw) -> ClusterInput:
    fields = {
        "name": name,
        "env": "stg",
        "region": "eu-west-1",
        "bootstrap_servers": "b1:9094,b2:9094",
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "SCRAM-SHA-512",
        "sasl_username": "svc",
        "sasl_password": SASL_SECRET,
        "truststore_base64": jks_b64(certs),
        "truststore_password": TRUSTSTORE_PASSWORD,
        **kw,
    }
    return ClusterInput(**fields)


def write_yaml(root: Path, text: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "clusters.yaml").write_text(text)


def yaml_text(root: Path) -> str:
    return (root / "clusters.yaml").read_text()


# --- list -------------------------------------------------------------------------------------


def test_list_missing_dir_returns_empty(store: ClusterStore, root: Path):
    assert store.list() == []
    assert not root.exists()  # reading never creates the directory


def test_list_empty_file_returns_empty(store: ClusterStore, root: Path):
    write_yaml(root, "")
    assert store.list() == []


def test_list_file_with_empty_clusters_returns_empty(store: ClusterStore, root: Path):
    write_yaml(root, "clusters:\n")
    assert store.list() == []


def test_list_malformed_yaml_raises_config_file_invalid(store: ClusterStore, root: Path):
    write_yaml(root, "clusters: [ {")
    with pytest.raises(ConfigFileInvalid) as exc:
        store.list()
    assert "clusters.yaml" in exc.value.message


def test_list_invalid_entry_raises_config_file_invalid(store: ClusterStore, root: Path):
    write_yaml(
        root,
        "clusters:\n"
        "  - {name: ok, env: dev, bootstrap_servers: 'b:1'}\n"
        "  - {name: Bad_Name, env: dev, bootstrap_servers: 'b:1'}\n",
    )
    with pytest.raises(ConfigFileInvalid) as exc:
        store.list()
    assert "Bad_Name" in exc.value.message
    assert "name" in exc.value.message
    assert exc.value.status == 500


def test_list_tls_entry_without_truststore_names_the_field(store: ClusterStore, root: Path):
    write_yaml(
        root,
        "clusters:\n  - {name: x, env: dev, bootstrap_servers: 'b:1', security_protocol: SSL}\n",
    )
    with pytest.raises(ConfigFileInvalid) as exc:
        store.list()
    assert "'x'" in exc.value.message
    assert "truststore" in exc.value.message


@pytest.mark.parametrize(
    "text",
    ["- just\n- a list\n", "clusters: nope\n", "clusters:\n  - not-a-mapping\n"],
    ids=["top-level-list", "clusters-not-a-list", "entry-not-a-mapping"],
)
def test_list_wrong_shape_raises_config_file_invalid(store: ClusterStore, root: Path, text: str):
    write_yaml(root, text)
    with pytest.raises(ConfigFileInvalid):
        store.list()


def test_list_duplicate_names_raises_config_file_invalid(store: ClusterStore, root: Path):
    entry = "  - {name: dup, env: dev, bootstrap_servers: 'b:1'}\n"
    write_yaml(root, "clusters:\n" + entry + entry)
    with pytest.raises(ConfigFileInvalid) as exc:
        store.list()
    assert "dup" in exc.value.message


def test_list_non_utf8_file_raises_config_file_invalid(store: ClusterStore, root: Path):
    root.mkdir()
    (root / "clusters.yaml").write_bytes(b"\xff\xfe\x00bad")
    with pytest.raises(ConfigFileInvalid):
        store.list()


def test_list_hand_written_minimal_entry(store: ClusterStore, root: Path):
    write_yaml(root, "clusters:\n  - {name: dev, env: dev, bootstrap_servers: 'b:1'}\n")
    [cfg] = store.list()
    assert cfg == ClusterConfig(name="dev", env="dev", bootstrap_servers="b:1")


# --- create -----------------------------------------------------------------------------------


def test_create_plaintext_persists_and_perms(store: ClusterStore, root: Path):
    cfg = store.create(plaintext(sasl_password="ignored", truststore_base64="ignored"))
    assert cfg.name == "dev"
    assert mode(root) == 0o700
    assert mode(root / "clusters.yaml") == 0o600
    doc = yaml.safe_load(yaml_text(root))
    assert doc["clusters"][0]["name"] == "dev"
    assert "sasl_password" not in yaml_text(root)
    assert "truststore_base64" not in yaml_text(root)
    assert "ignored" not in yaml_text(root)
    assert "truststore" not in doc["clusters"][0]
    assert store.list() == [cfg]
    assert store.get("dev") == cfg


def test_create_leaves_no_temp_files(store: ClusterStore, root: Path, certs: CertBundle):
    store.create(sasl_ssl(certs))
    assert sorted(p.name for p in root.iterdir()) == ["clusters.yaml", "truststores"]
    assert [p.name for p in (root / "truststores").iterdir()] == ["stg-eu.pem"]


def test_create_sasl_ssl_with_jks(
    store: ClusterStore, root: Path, certs: CertBundle, memory_keyring: MemoryKeyring
):
    cfg = store.create(sasl_ssl(certs))

    pem_path = root / "truststores" / "stg-eu.pem"
    assert pem_path.exists()
    assert mode(pem_path) == 0o600
    assert mode(root / "truststores") == 0o700
    assert [c.subject for c in summarize_pem(pem_path.read_text())] == ["CN=Test CA"]

    assert cfg.truststore == "truststores/stg-eu.pem"
    doc = yaml.safe_load(yaml_text(root))
    assert doc["clusters"][0]["truststore"] == "truststores/stg-eu.pem"
    assert memory_keyring.data == {("kafka-web", "stg-eu:sasl_password"): SASL_SECRET}
    text = yaml_text(root)
    assert SASL_SECRET not in text
    assert TRUSTSTORE_PASSWORD not in text
    assert "truststore_base64" not in text
    assert "password" not in text


def test_create_tls_without_truststore_rejected(
    store: ClusterStore, root: Path, certs: CertBundle, memory_keyring: MemoryKeyring
):
    with pytest.raises(ValidationFailed) as exc:
        store.create(sasl_ssl(certs, truststore_base64=None))
    assert exc.value.code == "truststore_required"
    assert exc.value.field == "truststore"
    assert not root.exists()
    assert memory_keyring.data == {}


def test_create_bad_truststore_password_writes_nothing(
    store: ClusterStore, root: Path, certs: CertBundle, memory_keyring: MemoryKeyring
):
    with pytest.raises(TruststoreError) as exc:
        store.create(sasl_ssl(certs, truststore_password="wrong"))
    assert exc.value.field == "truststore"
    assert not root.exists()
    assert memory_keyring.data == {}


def test_create_sasl_without_password_rejected(
    store: ClusterStore, root: Path, memory_keyring: MemoryKeyring
):
    inp = plaintext(security_protocol="SASL_PLAINTEXT", sasl_mechanism="PLAIN", sasl_username="u")
    with pytest.raises(ValidationFailed) as exc:
        store.create(inp)
    assert exc.value.code == "sasl_password_required"
    assert exc.value.field == "sasl_password"
    assert not root.exists()
    assert memory_keyring.data == {}


def test_create_with_blank_sasl_password_rejected(store: ClusterStore):
    inp = plaintext(
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_username="u",
        sasl_password="",
    )
    with pytest.raises(ValidationFailed) as exc:
        store.create(inp)
    assert exc.value.code == "sasl_password_required"


def test_create_duplicate_conflict(store: ClusterStore):
    store.create(plaintext())
    with pytest.raises(Conflict) as exc:
        store.create(plaintext())
    assert exc.value.code == "cluster_exists"
    assert len(store.list()) == 1


def test_create_keyring_unavailable_leaves_nothing_behind(root: Path, certs: CertBundle):
    store = ClusterStore(root, SecretStore(keyring.backends.fail.Keyring()))
    with pytest.raises(KeyringUnavailable):
        store.create(sasl_ssl(certs))
    assert not (root / "truststores" / "stg-eu.pem").exists()
    assert store.list() == []


def test_create_appends_preserving_order(store: ClusterStore):
    store.create(plaintext("b"))
    store.create(plaintext("a"))
    assert [c.name for c in store.list()] == ["b", "a"]


def test_get_missing_not_found(store: ClusterStore):
    with pytest.raises(NotFound) as exc:
        store.get("nope")
    assert exc.value.code == "cluster_not_found"


# --- update -----------------------------------------------------------------------------------


def test_update_keeps_secret_and_pem_when_blank(
    store: ClusterStore, root: Path, certs: CertBundle, memory_keyring: MemoryKeyring
):
    store.create(sasl_ssl(certs))
    pem_before = (root / "truststores" / "stg-eu.pem").read_text()

    updated = store.update(
        "stg-eu",
        sasl_ssl(certs, region="us-east-1", sasl_password="", truststore_base64=""),
    )

    assert updated.region == "us-east-1"
    assert store.get("stg-eu").region == "us-east-1"
    assert updated.truststore == "truststores/stg-eu.pem"
    assert (root / "truststores" / "stg-eu.pem").read_text() == pem_before
    assert memory_keyring.data == {("kafka-web", "stg-eu:sasl_password"): SASL_SECRET}


def test_update_with_new_password_replaces_secret(
    store: ClusterStore, certs: CertBundle, memory_keyring: MemoryKeyring
):
    store.create(sasl_ssl(certs))
    store.update("stg-eu", sasl_ssl(certs, sasl_password="new-one", truststore_base64=None))
    assert memory_keyring.data == {("kafka-web", "stg-eu:sasl_password"): "new-one"}


def test_update_replaces_pem(store: ClusterStore, root: Path, certs: CertBundle):
    store.create(sasl_ssl(certs))
    store.update("stg-eu", sasl_ssl(certs, truststore_base64=jks_b64(certs, leaf=True)))
    pem = (root / "truststores" / "stg-eu.pem").read_text()
    assert [c.subject for c in summarize_pem(pem)] == ["CN=localhost"]
    assert mode(root / "truststores" / "stg-eu.pem") == 0o600


def test_update_failing_validation_changes_nothing(
    store: ClusterStore, root: Path, certs: CertBundle, memory_keyring: MemoryKeyring
):
    store.create(sasl_ssl(certs))
    yaml_before = yaml_text(root)
    pem_before = (root / "truststores" / "stg-eu.pem").read_text()
    with pytest.raises(TruststoreError):
        store.update(
            "stg-eu",
            sasl_ssl(certs, sasl_password="new", env="prd", truststore_password="wrong"),
        )
    assert yaml_text(root) == yaml_before
    assert (root / "truststores" / "stg-eu.pem").read_text() == pem_before
    assert memory_keyring.data == {("kafka-web", "stg-eu:sasl_password"): SASL_SECRET}


def test_update_rename_rejected(store: ClusterStore):
    store.create(plaintext("old"))
    with pytest.raises(ValidationFailed) as exc:
        store.update("old", plaintext("new"))
    assert exc.value.code == "name_immutable"
    assert exc.value.field == "name"
    assert [c.name for c in store.list()] == ["old"]


def test_update_missing_not_found(store: ClusterStore):
    with pytest.raises(NotFound) as exc:
        store.update("ghost", plaintext("ghost"))
    assert exc.value.code == "cluster_not_found"


def test_update_tls_to_plaintext_removes_pem(store: ClusterStore, root: Path, certs: CertBundle):
    store.create(sasl_ssl(certs))
    cfg = store.update("stg-eu", plaintext("stg-eu"))
    assert cfg.truststore is None
    assert not (root / "truststores" / "stg-eu.pem").exists()
    assert "truststore" not in yaml_text(root)


def test_update_sasl_to_plaintext_removes_secret(
    store: ClusterStore, certs: CertBundle, memory_keyring: MemoryKeyring
):
    store.create(sasl_ssl(certs))
    store.update("stg-eu", plaintext("stg-eu"))
    assert memory_keyring.data == {}


def test_update_plaintext_to_sasl_requires_password(store: ClusterStore):
    store.create(plaintext())
    inp = plaintext(security_protocol="SASL_PLAINTEXT", sasl_mechanism="PLAIN", sasl_username="u")
    with pytest.raises(ValidationFailed) as exc:
        store.update("dev", inp)
    assert exc.value.code == "sasl_password_required"


def test_update_plaintext_to_ssl_requires_truststore(store: ClusterStore):
    store.create(plaintext())
    with pytest.raises(ValidationFailed) as exc:
        store.update("dev", plaintext(security_protocol="SSL"))
    assert exc.value.code == "truststore_required"


# --- delete -----------------------------------------------------------------------------------


def test_delete_removes_everything(
    store: ClusterStore, root: Path, certs: CertBundle, memory_keyring: MemoryKeyring
):
    store.create(sasl_ssl(certs))
    store.create(plaintext("other"))
    store.delete("stg-eu")
    assert [c.name for c in store.list()] == ["other"]
    assert not (root / "truststores" / "stg-eu.pem").exists()
    assert memory_keyring.data == {}


def test_delete_missing_not_found(store: ClusterStore):
    with pytest.raises(NotFound) as exc:
        store.delete("ghost")
    assert exc.value.code == "cluster_not_found"


def test_delete_never_touches_files_outside_the_managed_pem(store: ClusterStore, root: Path):
    outside = root.parent / "external-ca.pem"
    outside.write_text("keep me")
    write_yaml(
        root,
        f"clusters:\n  - {{name: x, env: dev, bootstrap_servers: 'b:1', security_protocol: SSL, "
        f"truststore: '{outside}'}}\n",
    )
    store.delete("x")
    assert outside.read_text() == "keep me"


# --- truststore / secrets accessors -------------------------------------------------------------


def test_truststore_path_is_absolute(store: ClusterStore, certs: CertBundle):
    cfg = store.create(sasl_ssl(certs))
    path = store.truststore_path(cfg)
    assert path is not None
    assert path.is_absolute()
    assert path.read_text().startswith("-----BEGIN CERTIFICATE-----")
    assert store.truststore_path(store.create(plaintext())) is None


def test_truststore_path_resolves_relative_root(
    tmp_path: Path, monkeypatch, memory_keyring: MemoryKeyring, certs: CertBundle
):
    monkeypatch.chdir(tmp_path)
    store = ClusterStore(Path("rel-cfg"), SecretStore(memory_keyring))
    cfg = store.create(sasl_ssl(certs))
    path = store.truststore_path(cfg)
    assert path is not None
    assert path.is_absolute()
    assert path.exists()


def test_truststore_summary(store: ClusterStore, certs: CertBundle):
    cfg = store.create(sasl_ssl(certs))
    summary = store.truststore_summary(cfg)
    assert summary is not None
    assert [s.subject for s in summary] == ["CN=Test CA"]
    assert summary[0].not_after == certs.ca_cert.not_valid_after_utc
    assert store.truststore_summary(store.create(plaintext())) is None


def test_truststore_summary_none_when_file_missing(
    store: ClusterStore, root: Path, certs: CertBundle
):
    cfg = store.create(sasl_ssl(certs))
    (root / "truststores" / "stg-eu.pem").unlink()
    assert store.truststore_summary(cfg) is None


def test_sasl_password_accessor(store: ClusterStore, certs: CertBundle):
    assert store.sasl_password(store.create(sasl_ssl(certs))) == SASL_SECRET
    assert store.sasl_password(store.create(plaintext())) is None


# --- usability --------------------------------------------------------------------------------


def test_usability_ok(store: ClusterStore, certs: CertBundle):
    assert store.usability(store.create(sasl_ssl(certs))) == Usability(True, None)
    assert store.usability(store.create(plaintext())) == Usability(True, None)


def test_usability_pem_deleted_on_disk(store: ClusterStore, root: Path, certs: CertBundle):
    cfg = store.create(sasl_ssl(certs))
    (root / "truststores" / "stg-eu.pem").unlink()
    result = store.usability(cfg)
    assert result == Usability(False, "truststore file missing: truststores/stg-eu.pem")


@pytest.mark.parametrize("content", ["", "not a certificate\n"], ids=["empty", "garbage"])
def test_usability_pem_without_certificates(
    store: ClusterStore, root: Path, certs: CertBundle, content: str
):
    cfg = store.create(sasl_ssl(certs))
    (root / "truststores" / "stg-eu.pem").write_text(content)
    result = store.usability(cfg)
    assert result.usable is False
    assert result.reason is not None
    assert "truststore" in result.reason


def test_usability_pem_unreadable(store: ClusterStore, root: Path, certs: CertBundle):
    cfg = store.create(sasl_ssl(certs))
    pem = root / "truststores" / "stg-eu.pem"
    pem.unlink()
    pem.mkdir()  # reading a directory raises OSError regardless of the user's privileges
    result = store.usability(cfg)
    assert result.usable is False
    assert result.reason == "truststore file unreadable: truststores/stg-eu.pem"


def test_usability_secret_missing(
    store: ClusterStore, certs: CertBundle, memory_keyring: MemoryKeyring
):
    cfg = store.create(sasl_ssl(certs))
    memory_keyring.data.clear()
    assert store.usability(cfg) == Usability(False, "SASL password missing from keyring")


def test_usability_keyring_unavailable(root: Path):
    write_yaml(
        root,
        "clusters:\n  - {name: x, env: dev, bootstrap_servers: 'b:1', "
        "security_protocol: SASL_PLAINTEXT, sasl_mechanism: PLAIN, sasl_username: u}\n",
    )
    store = ClusterStore(root, SecretStore(keyring.backends.fail.Keyring()))
    [cfg] = store.list()
    result = store.usability(cfg)
    assert result.usable is False
    assert result.reason is not None
    assert "keyring" in result.reason.lower()


# --- materialize ------------------------------------------------------------------------------


def test_materialize_does_not_persist(
    store: ClusterStore, root: Path, certs: CertBundle, memory_keyring: MemoryKeyring
):
    cfg, pem, password = store.materialize(sasl_ssl(certs), existing=None)
    assert cfg.truststore == "truststores/stg-eu.pem"
    assert pem is not None
    assert pem.startswith("-----BEGIN CERTIFICATE-----")
    assert password == SASL_SECRET
    assert not root.exists()
    assert memory_keyring.data == {}


def test_materialize_keeps_existing_truststore_and_secret(store: ClusterStore, certs: CertBundle):
    existing = store.create(sasl_ssl(certs))
    cfg, pem, password = store.materialize(
        sasl_ssl(certs, sasl_password=None, truststore_base64=None), existing=existing
    )
    assert cfg.truststore == existing.truststore
    assert pem is None
    assert password is None


def test_materialize_plaintext_returns_no_secrets(store: ClusterStore):
    cfg, pem, password = store.materialize(plaintext(sasl_password="x"), existing=None)
    assert (pem, password) == (None, None)
    assert cfg.truststore is None


def test_materialize_rejects_rename(store: ClusterStore):
    existing = store.create(plaintext("old"))
    with pytest.raises(ValidationFailed) as exc:
        store.materialize(plaintext("new"), existing=existing)
    assert exc.value.code == "name_immutable"


def test_invalid_input_that_bypasses_validation_still_surfaces_as_validation_failed(
    store: ClusterStore,
):
    bad = ClusterInput.model_construct(name="Bad Name", env="dev", bootstrap_servers="b:1")
    with pytest.raises(ValidationFailed) as exc:
        store.materialize(bad, existing=None)
    assert exc.value.field == "name"
