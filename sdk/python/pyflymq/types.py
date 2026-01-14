# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0

"""Type definitions for FlyMQ Python SDK."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class SchemaType(str, Enum):
    """Supported schema types."""

    JSON = "json"
    AVRO = "avro"
    PROTOBUF = "protobuf"


class Compatibility(str, Enum):
    """Schema compatibility modes."""

    NONE = "none"
    BACKWARD = "backward"
    FORWARD = "forward"
    FULL = "full"


@dataclass(frozen=True, slots=True)
class ProduceResult:
    """Result of a produce operation."""

    topic: str
    offset: int
    partition: int = 0
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True, slots=True)
class ConsumedMessage:
    """A message consumed from a topic."""

    topic: str
    partition: int
    offset: int
    data: bytes
    key: bytes | None = None
    timestamp: datetime | None = None
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def value(self) -> bytes:
        """Alias for data."""
        return self.data

    def decode(self, encoding: str = "utf-8") -> str:
        """Decode message data as string."""
        return self.data.decode(encoding)

    def decode_key(self, encoding: str = "utf-8") -> str | None:
        """Decode message key as string."""
        return self.key.decode(encoding) if self.key else None


@dataclass(frozen=True, slots=True)
class FetchResult:
    """Result of a fetch operation."""

    messages: list[ConsumedMessage]
    next_offset: int


@dataclass(frozen=True, slots=True)
class TopicMetadata:
    """Metadata about a topic."""

    name: str
    partitions: int
    replication_factor: int = 1
    config: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PartitionInfo:
    """Information about a partition."""

    partition: int
    leader: str
    replicas: list[str] = field(default_factory=list)
    in_sync_replicas: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class ConsumerGroupInfo:
    """Information about a consumer group."""

    group_id: str
    state: str
    members: list[str] = field(default_factory=list)
    topics: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SchemaInfo:
    """Schema metadata."""

    id: str
    name: str
    type: SchemaType
    version: int
    definition: str
    compatibility: Compatibility = Compatibility.BACKWARD
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class DLQMessage:
    """A message in the dead letter queue."""

    id: str
    topic: str
    data: bytes
    error: str
    retries: int
    timestamp: datetime
    original_offset: int | None = None


@dataclass(frozen=True, slots=True)
class ClusterInfo:
    """Information about the FlyMQ cluster."""

    cluster_id: str
    leader_id: str
    raft_term: int
    raft_commit_index: int
    node_count: int
    topic_count: int
    total_messages: int
    nodes: list[NodeInfo] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class NodeInfo:
    """Information about a cluster node."""

    id: str
    address: str
    cluster_addr: str
    state: str
    raft_state: str
    is_leader: bool
    uptime: str
    memory_used_mb: float = 0.0
    goroutines: int = 0


@dataclass(frozen=True, slots=True)
class ConsumerGroupOffset:
    """Offset information for a consumer group."""

    topic: str
    partition: int
    offset: int
    lag: int = 0


@dataclass(frozen=True, slots=True)
class ConsumerLag:
    """Lag information for a consumer group."""

    topic: str
    partition: int
    current_offset: int
    committed_offset: int
    latest_offset: int
    lag: int


@dataclass
class ClientConfig:
    """Configuration for FlyMQ client."""

    # Bootstrap servers (comma-separated or list)
    bootstrap_servers: str | list[str] = "localhost:9092"

    # Connection settings
    connect_timeout_ms: int = 10000
    request_timeout_ms: int = 30000
    max_retries: int = 3
    retry_delay_ms: int = 1000

    # TLS settings
    tls_enabled: bool = False
    tls_ca_file: str | None = None
    tls_cert_file: str | None = None
    tls_key_file: str | None = None
    tls_insecure_skip_verify: bool = False

    # Authentication settings
    username: str | None = None
    password: str | None = None

    # Encryption settings
    encryption_key: str | None = None  # 64-char hex key for AES-256-GCM

    # Client identification
    client_id: str = "pyflymq-client"

    def get_servers(self) -> list[str]:
        """Get list of bootstrap servers."""
        if isinstance(self.bootstrap_servers, str):
            return [s.strip() for s in self.bootstrap_servers.split(",") if s.strip()]
        return list(self.bootstrap_servers)


@dataclass(frozen=True, slots=True)
class AuthResponse:
    """Response from authentication request."""

    success: bool
    error: str | None = None
    username: str | None = None
    roles: list[str] = field(default_factory=list)
    permissions: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class WhoAmIResponse:
    """Response from WhoAmI request."""

    authenticated: bool
    username: str | None = None
    roles: list[str] = field(default_factory=list)
    permissions: list[str] = field(default_factory=list)


@dataclass
class ProducerConfig:
    """Configuration for FlyMQ producer."""

    # Batching settings
    batch_size: int = 16384  # 16KB
    linger_ms: int = 0  # No delay by default
    max_batch_messages: int = 1000

    # Reliability settings
    acks: str = "all"  # "none", "leader", "all"
    retries: int = 3
    retry_backoff_ms: int = 100

    # Compression (future)
    compression_type: str = "none"  # "none", "gzip", "snappy", "lz4"


@dataclass
class ConsumerConfig:
    """Configuration for FlyMQ consumer."""

    # Consumer group settings
    group_id: str | None = None
    auto_offset_reset: str = "latest"  # "earliest", "latest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000

    # Fetch settings
    fetch_min_bytes: int = 1
    fetch_max_bytes: int = 52428800  # 50MB
    fetch_max_wait_ms: int = 500
    max_poll_records: int = 500

    # Session management
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 3000
