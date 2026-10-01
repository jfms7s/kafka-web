import pytest
from pydantic import SecretStr

from kafka_web.config.models import ClusterBase, ClusterConfig, ClusterInput
from kafka_web.errors import AppError, ValidationFailed


def make(**overrides) -> ClusterBase:
    data = {"name": "stg-eu", "env": "stg", "bootstrap_servers": "b1:9092"}
    return ClusterBase(**{**data, **overrides})


def test_valid_plaintext_minimal():
    cfg = make()
    assert cfg.name == "stg-eu"
    assert cfg.security_protocol == "PLAINTEXT"
    assert cfg.region is None
    assert cfg.read_only is False
    assert cfg.extra == {}


@pytest.mark.parametrize("name", ["Stg", "-x", "a" * 64, "a_b", "", "a b", "a\n"])
def test_bad_names_rejected(name):
    with pytest.raises(ValidationFailed) as exc:
        make(name=name)
    assert exc.value.field == "name"


@pytest.mark.parametrize("name", ["a", "0", "stg-eu", "a" * 63])
def test_good_names_accepted(name):
    assert make(name=name).name == name


def test_env_is_stripped_and_must_not_be_blank():
    assert make(env="  stg ").env == "stg"
    with pytest.raises(ValidationFailed) as exc:
        make(env="   ")
    assert exc.value.field == "env"


def test_bootstrap_servers_is_stripped_and_must_not_be_blank():
    assert make(bootstrap_servers="  b1:9092,b2:9092 ").bootstrap_servers == "b1:9092,b2:9092"
    with pytest.raises(ValidationFailed) as exc:
        make(bootstrap_servers="  ")
    assert exc.value.field == "bootstrap_servers"


def test_missing_required_field_is_validation_failed_with_field():
    with pytest.raises(ValidationFailed) as exc:
        ClusterBase(name="a", bootstrap_servers="b:1")
    assert exc.value.field == "env"


def test_invalid_protocol_is_validation_failed_with_field():
    with pytest.raises(ValidationFailed) as exc:
        make(security_protocol="TLS")
    assert exc.value.field == "security_protocol"


def test_validation_failure_is_app_error_not_value_error():
    # FastAPI/pydantic only re-raise non-ValueError exceptions unchanged from validators.
    assert not issubclass(ValidationFailed, ValueError)
    with pytest.raises(AppError):
        make(name="Bad")


def test_error_message_never_contains_input_value():
    with pytest.raises(ValidationFailed) as exc:
        ClusterInput(
            name="a",
            env="e",
            bootstrap_servers="b:1",
            security_protocol="NOPE",
            sasl_password="hunter2-secret",
        )
    assert "hunter2-secret" not in exc.value.message
    assert "hunter2-secret" not in repr(exc.value.__cause__)


@pytest.mark.parametrize("protocol", ["SASL_SSL", "SASL_PLAINTEXT"])
def test_sasl_requires_username(protocol):
    with pytest.raises(ValidationFailed) as exc:
        make(security_protocol=protocol, sasl_mechanism="PLAIN")
    assert exc.value.field == "sasl_username"


@pytest.mark.parametrize("protocol", ["SASL_SSL", "SASL_PLAINTEXT"])
def test_sasl_requires_mechanism(protocol):
    with pytest.raises(ValidationFailed) as exc:
        make(security_protocol=protocol, sasl_username="u")
    assert exc.value.field == "sasl_mechanism"


@pytest.mark.parametrize("protocol", ["PLAINTEXT", "SSL"])
def test_non_sasl_forces_sasl_fields_to_none(protocol):
    cfg = make(security_protocol=protocol, sasl_mechanism="PLAIN", sasl_username="u")
    assert cfg.sasl_mechanism is None
    assert cfg.sasl_username is None


@pytest.mark.parametrize(
    "key", ["sasl.jaas.config", "sasl.mechanism", "ssl.ca.pem", "ssl.ca.location"]
)
def test_forbidden_extra_prefixes_rejected(key):
    with pytest.raises(ValidationFailed) as exc:
        make(extra={key: "x"})
    assert exc.value.field == "extra"
    assert key in exc.value.message


@pytest.mark.parametrize("key", ["bootstrap.servers", "security.protocol"])
def test_forbidden_exact_extra_keys_rejected(key):
    with pytest.raises(ValidationFailed) as exc:
        make(extra={key: "x"})
    assert exc.value.field == "extra"


def test_other_extra_accepted():
    assert make(extra={"fetch.max.bytes": "1"}).extra == {"fetch.max.bytes": "1"}


def test_cluster_config_ssl_requires_truststore():
    with pytest.raises(ValidationFailed) as exc:
        ClusterConfig(name="a", env="e", bootstrap_servers="b:1", security_protocol="SSL")
    assert exc.value.code == "truststore_required"
    assert exc.value.field == "truststore"


def test_cluster_config_non_tls_forces_truststore_none():
    cfg = ClusterConfig(name="a", env="e", bootstrap_servers="b:1", truststore="truststores/a.pem")
    assert cfg.truststore is None


def test_cluster_config_tls_keeps_truststore():
    cfg = ClusterConfig(
        name="a",
        env="e",
        bootstrap_servers="b:1",
        security_protocol="SSL",
        truststore="truststores/a.pem",
    )
    assert cfg.truststore == "truststores/a.pem"


@pytest.mark.parametrize(
    ("protocol", "tls", "sasl"),
    [
        ("PLAINTEXT", False, False),
        ("SSL", True, False),
        ("SASL_PLAINTEXT", False, True),
        ("SASL_SSL", True, True),
    ],
)
def test_uses_tls_uses_sasl_truth_table(protocol, tls, sasl):
    cfg = make(security_protocol=protocol, sasl_mechanism="PLAIN", sasl_username="u")
    assert cfg.uses_tls is tls
    assert cfg.uses_sasl is sasl


def test_cluster_input_carries_write_only_fields():
    inp = ClusterInput(
        name="a",
        env="e",
        bootstrap_servers="b:1",
        sasl_password="pw",
        truststore_base64="AAAA",
        truststore_password="tp",
    )
    assert isinstance(inp.sasl_password, SecretStr)
    assert "pw" not in repr(inp)
    assert "tp" not in repr(inp)
