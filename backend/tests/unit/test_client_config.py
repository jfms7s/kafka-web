from pathlib import Path

import pytest

from kafka_web.config.models import ClusterConfig
from kafka_web.kafka.client_config import build_client_config


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
