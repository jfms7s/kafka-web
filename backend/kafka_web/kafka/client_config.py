"""Every librdkafka property the app sets, for every client type it creates.

This module is the only place cluster config is translated into librdkafka config. Each layer
below overrides the ones before it:

1. Defaults: `CLIENT_DEFAULTS` for every client, plus `PRODUCER_DEFAULTS` for the producer.
2. Typed fields of the cluster (bootstrap servers, security protocol, SASL, CA location,
   `client.id`).
3. The user's `extra` (properties that load code are refused, never applied).
4. TLS clusters only: `TLS_VERIFICATION`, so `extra` can't turn certificate checks off.
5. Per client type: the throwaway-group consumer keys, then `FORCED`, then callbacks.

Steps 1-4 produce the base dict (`base_config`); `admin_config`, `producer_config` and
`consumer_config` apply step 5 (and the producer defaults, under the base) to it.
"""

import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from kafka_web.config.models import ClusterConfig, loads_code

CLIENT_ID = "kafka-web"
# Defaults under the user's `extra`: a TCP/TLS connect never waits longer than the admin calls.
CLIENT_DEFAULTS = {"socket.connection.setup.timeout.ms": "10000"}
PRODUCER_DEFAULTS = {"message.timeout.ms": "30000"}
# Forced over the user's `extra`: no client of ours may ever create a topic. A metadata request
# for a missing topic would otherwise create it on a broker that allows auto-creation (read-only
# clusters included), and a topic deleted between publish's existence check and produce() would
# be silently re-created.
FORCED = {"allow.auto.create.topics": "false"}
# Forced over `extra` on TLS clusters (step 4).
TLS_VERIFICATION = {
    "enable.ssl.certificate.verification": "true",
    "ssl.endpoint.identification.algorithm": "https",
}


def build_client_config(
    cfg: ClusterConfig, *, sasl_password: str | None, ca_location: Path | None
) -> dict[str, str]:
    """Steps 2-4 for `cfg`: typed fields, `extra`, TLS verification (no defaults).

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
    conf["client.id"] = CLIENT_ID
    # Validation already rejects these; refuse again so no path can hand them to librdkafka.
    if blocked := [key for key in cfg.extra if loads_code(key)]:
        raise ValueError(f"extra holds properties that load code: {', '.join(blocked)}")
    conf.update(cfg.extra)
    if cfg.uses_tls:
        # Forced after `extra`, like FORCED: TLS verification can't be turned off.
        # Old clusters.yaml entries may still carry an opt-out; this value wins over it.
        conf.update(TLS_VERIFICATION)
    return conf


def base_config(
    cfg: ClusterConfig, *, sasl_password: str | None, ca_location: Path | None
) -> dict[str, str]:
    """Steps 1-4: the shared base every client type is built from."""
    built = build_client_config(cfg, sasl_password=sasl_password, ca_location=ca_location)
    return {**CLIENT_DEFAULTS, **built}


def admin_config(base: dict[str, str], *, error_cb: Callable[[Any], None]) -> dict[str, Any]:
    return {**base, **FORCED, "error_cb": error_cb}


def producer_config(base: dict[str, str]) -> dict[str, str]:
    return {**PRODUCER_DEFAULTS, **base, **FORCED}


def consumer_config(
    base: dict[str, str],
    *,
    partition_eof: bool,
    error_cb: Callable[[Any], None] | None = None,
) -> dict[str, Any]:
    """A throwaway-group consumer: a fresh group id, no commits, `assign()` only."""
    conf: dict[str, Any] = {
        **base,
        "group.id": f"kafka-web-{uuid.uuid4()}",
        "enable.auto.commit": "false",
        "enable.partition.eof": "true" if partition_eof else "false",
        "auto.offset.reset": "earliest",
        **FORCED,
    }
    if error_cb is not None:
        conf["error_cb"] = error_cb
    return conf
