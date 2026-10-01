"""Kafka containers (podman via the Docker API) and an API client for integration tests.

Both brokers run `apache/kafka:3.9.1` in KRaft combined mode. Each listener's container port equals
its host port, so the broker can advertise `localhost:<port>` before the port mapping exists.
"""

import os

# Must be set before testcontainers is imported (it reads them at import time).
os.environ.setdefault("DOCKER_HOST", "unix:///run/user/1000/podman/podman.sock")
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

import contextlib
import socket
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from cryptography.hazmat.primitives import serialization
from fastapi.testclient import TestClient
from testcontainers.core.container import DockerContainer

from kafka_web.api.app import create_app
from kafka_web.config.secrets import SecretStore
from kafka_web.config.store import ClusterStore
from tests.conftest import CertBundle, MemoryKeyring

IMAGE = "apache/kafka:3.9.1"
READY_TIMEOUT_S = 60
SASL_USERS = {"admin": "admin-secret", "app": "app-secret"}

BASE_ENV = {
    "KAFKA_NODE_ID": "1",
    "KAFKA_PROCESS_ROLES": "broker,controller",
    "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
    "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
    "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
    "KAFKA_NUM_PARTITIONS": "3",
    "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
}


def _free_ports(count: int) -> list[int]:
    """`count` distinct free ports: all sockets stay bound until every port has been picked, so
    the OS cannot hand out the same port twice (picking them one by one could)."""
    with contextlib.ExitStack() as stack:
        ports = []
        for _ in range(count):
            sock = stack.enter_context(socket.socket())
            sock.bind(("127.0.0.1", 0))
            ports.append(sock.getsockname()[1])
    return ports


def _wait_ready(container: DockerContainer, conf: dict[str, Any]) -> None:
    """Poll `list_topics` until the broker answers; fail with the container log on timeout."""
    deadline = time.monotonic() + READY_TIMEOUT_S
    while True:
        try:
            AdminClient(conf).list_topics(timeout=2)
            return
        except KafkaException:
            pass
        container.reload()
        if container.status != "running" or time.monotonic() > deadline:
            stdout, stderr = container.get_logs()
            log = (stdout + stderr).decode(errors="replace")[-4000:]
            raise RuntimeError(f"Kafka container not ready ({container.status}):\n{log}")
        time.sleep(0.5)


def _start(env: dict[str, str], ports: list[int], files: dict[str, bytes]) -> DockerContainer:
    container = DockerContainer(IMAGE, env={**BASE_ENV, **env})
    for port in ports:
        container.with_bind_ports(port, port)
    for path, content in files.items():
        container.with_copy_into_container(content, path, 0o644)
    container.with_name(f"kafka-web-it-{ports[0]}")
    try:
        return container.start()
    except BaseException:
        with contextlib.suppress(Exception):  # remove a created-but-not-started container
            container.stop()
        raise


@pytest.fixture(scope="session")
def kafka_plaintext() -> Iterator[str]:
    """Bootstrap servers (`localhost:<port>`) of a PLAINTEXT broker."""
    [port] = _free_ports(1)
    container = _start(
        {
            "KAFKA_LISTENERS": f"PLAINTEXT://:{port},CONTROLLER://:9093",
            "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://localhost:{port}",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        },
        [port],
        {},
    )
    try:
        bootstrap = f"localhost:{port}"
        _wait_ready(container, {"bootstrap.servers": bootstrap})
        yield bootstrap
    finally:
        container.stop()


def broker_pem(certs: CertBundle) -> bytes:
    """PEM keystore for Kafka: unencrypted PKCS#8 key, leaf certificate, CA certificate."""
    key = certs.leaf_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    chain = [c.public_bytes(serialization.Encoding.PEM) for c in (certs.leaf_cert, certs.ca_cert)]
    return key + b"".join(chain)


@pytest.fixture(scope="session")
def kafka_sasl_ssl(
    certs: CertBundle, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[tuple[str, CertBundle]]:
    """`(bootstrap, certs)` of a broker whose client listener is SASL_SSL with SASL/PLAIN.

    Users: admin/admin-secret, app/app-secret. The broker certificate (CN/SAN localhost) is
    signed by `certs.ca_cert`; inter-broker traffic uses an internal PLAINTEXT listener.

    Two deliberate deviations from the obvious setup, both forced by the image's entrypoint:
    - The SASL_SSL listener is *named* EXTERNAL. The entrypoint greps the advertised listeners
      for `SSL://` / `SASL_` and then insists on JKS-style keystore credential files under
      /etc/kafka/secrets, which a PEM keystore (no keystore password) cannot satisfy.
    - The keystore is copied into the container before start (Docker API `put_archive`)
      instead of bind-mounted: no SELinux relabelling (`:Z`) and no rootless-podman UID
      mapping questions about who may read the file.
    """
    plain_port, sasl_port = _free_ports(2)
    jaas = (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        'username="admin" password="admin-secret" '
        + " ".join(f'user_{user}="{password}"' for user, password in SASL_USERS.items())
        + ";"
    )
    container = _start(
        {
            "KAFKA_LISTENERS": (
                f"PLAINTEXT://:{plain_port},EXTERNAL://:{sasl_port},CONTROLLER://:9093"
            ),
            "KAFKA_ADVERTISED_LISTENERS": (
                f"PLAINTEXT://localhost:{plain_port},EXTERNAL://localhost:{sasl_port}"
            ),
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": (
                "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:SASL_SSL"
            ),
            "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
            "KAFKA_SASL_ENABLED_MECHANISMS": "PLAIN",
            "KAFKA_LISTENER_NAME_EXTERNAL_PLAIN_SASL_JAAS_CONFIG": jaas,
            "KAFKA_SSL_KEYSTORE_TYPE": "PEM",
            "KAFKA_SSL_KEYSTORE_LOCATION": "/certs/broker.pem",
        },
        [plain_port, sasl_port],
        {"/certs/broker.pem": broker_pem(certs)},
    )
    try:
        ca_file = tmp_path_factory.mktemp("sasl-ssl") / "ca.pem"
        ca_file.write_bytes(certs.ca_cert.public_bytes(serialization.Encoding.PEM))
        bootstrap = f"localhost:{sasl_port}"
        _wait_ready(
            container,
            {
                "bootstrap.servers": bootstrap,
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": "admin",
                "sasl.password": SASL_USERS["admin"],
                "ssl.ca.location": str(ca_file),
            },
        )
        yield bootstrap, certs
    finally:
        container.stop()


def _wait_partitions_serving(bootstrap: str, topic: str, partitions: int) -> None:
    """Block until every partition answers a watermark query.

    Right after `create_topics` resolves a leader may still answer NOT_LEADER_FOR_PARTITION.
    """
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"it-wait-{uuid.uuid4()}"})
    deadline = time.monotonic() + READY_TIMEOUT_S
    try:
        for partition in range(partitions):
            while True:
                try:
                    consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=2)
                    break
                except KafkaException:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.1)
    finally:
        consumer.close()


@pytest.fixture
def topic_factory(kafka_plaintext: str) -> Iterator[Callable[..., str]]:
    """`create(partitions=3, config=None) -> name`: unique topics, deleted after the test.

    Returns once every partition is serving, so tests never race the leader election.
    """
    admin = AdminClient({"bootstrap.servers": kafka_plaintext})
    created: list[str] = []

    def create(partitions: int = 3, config: dict[str, str] | None = None) -> str:
        name = f"it-{uuid.uuid4().hex[:12]}"
        future = admin.create_topics([NewTopic(name, partitions, 1, config=config or {})])[name]
        future.result(timeout=10)
        created.append(name)
        _wait_partitions_serving(kafka_plaintext, name, partitions)
        return name

    yield create
    if created:
        for future in admin.delete_topics(created).values():
            future.result(timeout=10)


@pytest.fixture
def store(tmp_path: Path, memory_keyring: MemoryKeyring) -> ClusterStore:
    return ClusterStore(tmp_path / "cfg", SecretStore(memory_keyring))


@pytest.fixture
def api(store: ClusterStore) -> Iterator[TestClient]:
    """The real app (real registry, real Kafka clients) over a temp config dir + memory keyring."""
    with TestClient(create_app(store=store), base_url="http://127.0.0.1:8000") as client:
        yield client
