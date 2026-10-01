"""API response models. Request bodies use `ClusterInput`; no response model carries secrets."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict

from kafka_web.config.models import SaslMechanism, SecurityProtocol


class CertView(BaseModel):
    subject: str
    not_after: datetime


class ClusterView(BaseModel):
    name: str
    env: str
    region: str | None
    bootstrap_servers: str
    security_protocol: SecurityProtocol
    sasl_mechanism: SaslMechanism | None
    sasl_username: str | None
    has_sasl_password: bool
    read_only: bool
    extra: dict[str, str]
    truststore: list[CertView] | None
    usable: bool
    unusable_reason: str | None
    connected: bool


class ConnectionView(BaseModel):
    name: str
    env: str
    region: str | None
    bootstrap_servers: str
    read_only: bool
    connected_at: datetime
    active_streams: int


class StatusView(BaseModel):
    connections: list[ConnectionView]


class ConnectionTestResult(BaseModel):
    ok: bool


class TopicSummaryView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    partitions: int
    replication_factor: int
    internal: bool


class TopicListView(BaseModel):
    topics: list[TopicSummaryView]


class PartitionView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    leader: int
    replicas: list[int]
    isr: list[int]


class ConfigEntryItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    value: str | None
    display_value: str | None
    is_default: bool
    source: str
    sensitive: bool


class TopicConfigView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    partitions: list[PartitionView]
    replication_factor: int
    entries: list[ConfigEntryItem]
