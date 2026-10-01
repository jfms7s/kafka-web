"""Translate a stored cluster config into a librdkafka property dict."""

from pathlib import Path

from kafka_web.config.models import ClusterConfig

CLIENT_ID = "kafka-web"


def build_client_config(
    cfg: ClusterConfig, *, sasl_password: str | None, ca_location: Path | None
) -> dict[str, str]:
    """Build the librdkafka config for `cfg`.

    Callers must check usability first: a missing CA file (TLS) or password (SASL) is a
    programming error here and raises `ValueError`.
    """
    if cfg.uses_tls and ca_location is None:
        raise ValueError(f"ca_location is required for {cfg.security_protocol}")
    if cfg.uses_sasl and sasl_password is None:
        raise ValueError(f"sasl_password is required for {cfg.security_protocol}")

    servers = ",".join(part.strip() for part in cfg.bootstrap_servers.split(","))
    conf = {"bootstrap.servers": servers, "security.protocol": cfg.security_protocol}
    if cfg.uses_sasl:
        conf["sasl.mechanism"] = cfg.sasl_mechanism
        conf["sasl.username"] = cfg.sasl_username
        conf["sasl.password"] = sasl_password
    if cfg.uses_tls:
        conf["ssl.ca.location"] = str(ca_location)
        conf["ssl.endpoint.identification.algorithm"] = "https"
    conf["client.id"] = CLIENT_ID
    conf.update(cfg.extra)
    return conf
