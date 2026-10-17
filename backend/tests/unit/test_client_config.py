from pathlib import Path

import pytest

from kafka_web.config.models import ClusterConfig
from kafka_web.kafka.client_config import (
    admin_config,
    base_config,
    build_client_config,
    consumer_config,
    producer_config,
)


def cfg(**kw) -> ClusterConfig:
    fields = {"name": "a", "env": "dev", "bootstrap_servers": "b1:9092", **kw}
    return ClusterConfig(**fields)


def sasl_ssl(**kw) -> ClusterConfig:
    return cfg(
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_username="svc",
        truststore="truststores/a.pem",
        **kw,
    )


def test_plaintext_has_exactly_the_basics():
    assert build_client_config(cfg(), sasl_password=None, ca_location=None) == {
        "bootstrap.servers": "b1:9092",
        "security.protocol": "PLAINTEXT",
        "client.id": "kafka-web",
    }


def test_sasl_ssl_includes_sasl_and_tls_settings():
    conf = build_client_config(
        sasl_ssl(), sasl_password="pw", ca_location=Path("/cfg/truststores/a.pem")
    )
    assert conf == {
        "bootstrap.servers": "b1:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "svc",
        "sasl.password": "pw",
        "ssl.ca.location": "/cfg/truststores/a.pem",
        "ssl.endpoint.identification.algorithm": "https",
        "enable.ssl.certificate.verification": "true",
        "client.id": "kafka-web",
    }


def test_ssl_without_sasl_has_no_sasl_keys():
    conf = build_client_config(
        cfg(security_protocol="SSL", truststore="truststores/a.pem"),
        sasl_password=None,
        ca_location=Path("/x.pem"),
    )
    assert conf["ssl.ca.location"] == "/x.pem"
    assert not any(k.startswith("sasl.") for k in conf)


def test_sasl_plaintext_has_no_tls_keys():
    conf = build_client_config(
        cfg(security_protocol="SASL_PLAINTEXT", sasl_mechanism="PLAIN", sasl_username="u"),
        sasl_password="pw",
        ca_location=None,
    )
    assert conf["sasl.mechanism"] == "PLAIN"
    assert not any(k.startswith("ssl.") for k in conf)


def test_bootstrap_servers_whitespace_is_stripped():
    conf = build_client_config(
        cfg(bootstrap_servers=" b1:9092 , b2:9092 "), sasl_password=None, ca_location=None
    )
    assert conf["bootstrap.servers"] == "b1:9092,b2:9092"


def test_extra_is_merged_last_and_may_override_defaults():
    conf = build_client_config(
        cfg(extra={"fetch.max.bytes": "1", "client.id": "mine"}),
        sasl_password=None,
        ca_location=None,
    )
    assert conf["fetch.max.bytes"] == "1"
    assert conf["client.id"] == "mine"


def test_tls_without_ca_location_is_a_programming_error():
    with pytest.raises(ValueError, match="ca_location"):
        build_client_config(sasl_ssl(), sasl_password="pw", ca_location=None)


def test_sasl_without_password_is_a_programming_error():
    with pytest.raises(ValueError, match="sasl_password"):
        build_client_config(sasl_ssl(), sasl_password=None, ca_location=Path("/x.pem"))


def test_build_refuses_code_loading_extra_that_skipped_validation():
    cfg = ClusterConfig.model_construct(
        name="dev",
        env="dev",
        region=None,
        bootstrap_servers="b:1",
        security_protocol="PLAINTEXT",
        sasl_mechanism=None,
        sasl_username=None,
        read_only=False,
        extra={"plugin.library.paths": "/tmp/x.so"},
        truststore=None,
    )
    with pytest.raises(ValueError, match=r"plugin\.library\.paths"):
        build_client_config(cfg, sasl_password=None, ca_location=None)


TLS_OPT_OUT = {
    "enable.ssl.certificate.verification": "false",
    "ssl.endpoint.identification.algorithm": "none",
}


@pytest.mark.parametrize("protocol", ["SSL", "SASL_SSL"])
def test_tls_verification_cannot_be_turned_off_through_extra(protocol):
    sasl = {"sasl_mechanism": "PLAIN", "sasl_username": "u"} if protocol == "SASL_SSL" else {}
    conf = build_client_config(
        cfg(security_protocol=protocol, truststore="truststores/a.pem", extra=TLS_OPT_OUT, **sasl),
        sasl_password="pw" if sasl else None,
        ca_location=Path("/x.pem"),
    )
    assert conf["enable.ssl.certificate.verification"] == "true"
    assert conf["ssl.endpoint.identification.algorithm"] == "https"


def test_plaintext_gets_no_tls_verification_keys():
    conf = build_client_config(cfg(), sasl_password=None, ca_location=None)
    assert "enable.ssl.certificate.verification" not in conf
    assert "ssl.endpoint.identification.algorithm" not in conf


def _on_error(err) -> None:
    pass


CLIENT_BUILDERS = {
    "admin": lambda base: admin_config(base, error_cb=_on_error),
    "producer": producer_config,
    "snapshot consumer": lambda base: consumer_config(base, partition_eof=True),
    "stream consumer": lambda base: consumer_config(base, partition_eof=False, error_cb=_on_error),
}


@pytest.mark.parametrize("build", CLIENT_BUILDERS.values(), ids=CLIENT_BUILDERS.keys())
def test_no_client_type_can_auto_create_topics_but_extra_overrides_defaults(build):
    extra = {"allow.auto.create.topics": "true", "socket.connection.setup.timeout.ms": "3000"}
    base = base_config(cfg(extra=extra), sasl_password=None, ca_location=None)

    conf = build(base)

    assert conf["allow.auto.create.topics"] == "false"
    assert conf["socket.connection.setup.timeout.ms"] == "3000"


def test_defaults_apply_when_extra_is_silent():
    base = base_config(cfg(), sasl_password=None, ca_location=None)
    assert base["socket.connection.setup.timeout.ms"] == "10000"
    assert producer_config(base)["message.timeout.ms"] == "30000"
    assert "message.timeout.ms" not in admin_config(base, error_cb=_on_error)


def test_producer_default_stays_under_extra():
    cluster = cfg(extra={"message.timeout.ms": "5000"})
    base = base_config(cluster, sasl_password=None, ca_location=None)
    assert producer_config(base)["message.timeout.ms"] == "5000"


@pytest.mark.parametrize("partition_eof", [True, False])
def test_consumer_config_has_throwaway_group_keys_over_base(partition_eof):
    base = {"bootstrap.servers": "b1:9092", "group.id": "theirs", "enable.auto.commit": "true"}
    conf = consumer_config(base, partition_eof=partition_eof)
    assert conf["group.id"].startswith("kafka-web-")
    assert conf["enable.auto.commit"] == "false"
    assert conf["auto.offset.reset"] == "earliest"
    assert conf["enable.partition.eof"] == ("true" if partition_eof else "false")
    assert "error_cb" not in conf
