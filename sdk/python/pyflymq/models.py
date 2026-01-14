# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0

"""
Pydantic models for FlyMQ Python SDK.

Provides validated, serializable data models for all FlyMQ operations.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field, field_validator


# Generic type for response data
T = TypeVar("T")


class SchemaType(str, Enum):
    """Supported schema types for message validation."""
    JSON = "json"
    AVRO = "avro"
    PROTOBUF = "protobuf"


class Compatibility(str, Enum):
    """Schema compatibility modes."""
    NONE = "none"
    BACKWARD = "backward"
    FORWARD = "forward"
    FULL = "full"


class SubscribeMode(str, Enum):
    """Consumer subscribe modes."""
    EARLIEST = "earliest"
    LATEST = "latest"


class AckMode(str, Enum):
    """Producer acknowledgement modes."""
    NONE = "none"
    LEADER = "leader"
    ALL = "all"


# ============================================================================
# Configuration Models
# ============================================================================


class ClientConfig(BaseModel):
    """Configuration for FlyMQ client."""
    
    model_config = ConfigDict(validate_assignment=True)
    
    bootstrap_servers: str | list[str] = Field(
        default="localhost:9092",
        description="Comma-separated list of bootstrap servers or list of strings"
    )
    connect_timeout_ms: int = Field(default=10000, ge=100, le=300000)
    request_timeout_ms: int = Field(default=30000, ge=100, le=300000)
    max_retries: int = Field(default=3, ge=0, le=100)
    retry_delay_ms: int = Field(default=1000, ge=0, le=60000)
    
    # TLS settings
    tls_enabled: bool = False
    tls_ca_file: str | None = None
    tls_cert_file: str | None = None
    tls_key_file: str | None = None
    tls_insecure_skip_verify: bool = False
    
    # Encryption settings (data-in-motion)
    encryption_enabled: bool = False
    encryption_key: str | None = Field(
        default=None,
        description="64-character hex string for AES-256 encryption"
    )
    
    # Client identification
    client_id: str = "pyflymq-client"
    
    @field_validator("encryption_key")
    @classmethod
    def validate_encryption_key(cls, v: str | None) -> str | None:
        if v is not None:
            if len(v) != 64:
                raise ValueError("Encryption key must be 64 hex characters (256 bits)")
            try:
                bytes.fromhex(v)
            except ValueError:
                raise ValueError("Encryption key must be valid hexadecimal")
        return v
    
    def get_servers(self) -> list[str]:
        """Get list of bootstrap servers."""
        if isinstance(self.bootstrap_servers, str):
            return [s.strip() for s in self.bootstrap_servers.split(",") if s.strip()]
        return list(self.bootstrap_servers)


class ProducerConfig(BaseModel):
    """Configuration for FlyMQ producer."""
    
    model_config = ConfigDict(validate_assignment=True)
    
    batch_size: int = Field(default=16384, ge=1, description="Batch size in bytes")
    linger_ms: int = Field(default=0, ge=0, description="Time to wait for batch")
    max_batch_messages: int = Field(default=1000, ge=1)
    acks: AckMode = AckMode.ALL
    retries: int = Field(default=3, ge=0)
    retry_backoff_ms: int = Field(default=100, ge=0)
    compression_type: str = "none"


class ConsumerConfig(BaseModel):
    """Configuration for FlyMQ consumer."""
    
    model_config = ConfigDict(validate_assignment=True)
    
    group_id: str | None = None
    auto_offset_reset: SubscribeMode = SubscribeMode.LATEST
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = Field(default=5000, ge=100)
    fetch_min_bytes: int = Field(default=1, ge=1)
    fetch_max_bytes: int = Field(default=52428800, ge=1)
    fetch_max_wait_ms: int = Field(default=500, ge=0)
    max_poll_records: int = Field(default=500, ge=1)
    session_timeout_ms: int = Field(default=30000, ge=1000)
    heartbeat_interval_ms: int = Field(default=3000, ge=100)


# ============================================================================
# Response Models
# ============================================================================


class Response(BaseModel, Generic[T]):
    """Generic response wrapper."""
    
    success: bool
    data: T | None = None
    error: str | None = None


class ProduceResult(BaseModel):
    """Result of a produce operation."""
    
    model_config = ConfigDict(frozen=True)
    
    topic: str
    offset: int
    partition: int = 0
    timestamp: datetime = Field(default_factory=datetime.now)


class ConsumedMessage(BaseModel):
    """A message consumed from a topic."""
    
    model_config = ConfigDict(frozen=True)
    
    topic: str
    partition: int
    offset: int
    data: bytes
    timestamp: datetime | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    
    @property
    def value(self) -> bytes:
        """Alias for data."""
        return self.data
    
    def decode(self, encoding: str = "utf-8") -> str:
        """Decode message data as string."""
        return self.data.decode(encoding)
    
    def json_data(self) -> Any:
        """Parse message data as JSON."""
        import json
        return json.loads(self.data)


class FetchResult(BaseModel):
    """Result of a fetch operation."""
    
    model_config = ConfigDict(frozen=True)
    
    messages: list[ConsumedMessage]
    next_offset: int


class TopicMetadata(BaseModel):
    """Metadata about a topic."""
    
    model_config = ConfigDict(frozen=True)
    
    name: str
    partitions: int
    replication_factor: int = 1
    config: dict[str, Any] = Field(default_factory=dict)


class PartitionInfo(BaseModel):
    """Information about a partition."""
    
    model_config = ConfigDict(frozen=True)
    
    partition: int
    leader: str
    replicas: list[str] = Field(default_factory=list)
    in_sync_replicas: list[str] = Field(default_factory=list)


class ConsumerGroupInfo(BaseModel):
    """Information about a consumer group."""
    
    model_config = ConfigDict(frozen=True)
    
    group_id: str
    state: str
    members: list[str] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list)


class SchemaInfo(BaseModel):
    """Schema metadata."""
    
    model_config = ConfigDict(frozen=True)
    
    id: str
    name: str
    type: SchemaType
    version: int
    definition: str
    compatibility: Compatibility = Compatibility.BACKWARD
    created_at: datetime | None = None


class DLQMessage(BaseModel):
    """A message in the dead letter queue."""
    
    model_config = ConfigDict(frozen=True)
    
    id: str
    topic: str
    data: bytes
    error: str
    retries: int
    timestamp: datetime
    original_offset: int | None = None


class NodeInfo(BaseModel):
    """Information about a cluster node."""
    
    model_config = ConfigDict(frozen=True)
    
    id: str
    address: str
    cluster_addr: str
    state: str
    raft_state: str
    is_leader: bool
    uptime: str
    memory_used_mb: float = 0.0
    goroutines: int = 0


class ClusterInfo(BaseModel):
    """Information about the FlyMQ cluster."""
    
    model_config = ConfigDict(frozen=True)
    
    cluster_id: str
    leader_id: str
    raft_term: int
    raft_commit_index: int
    node_count: int
    topic_count: int
    total_messages: int
    nodes: list[NodeInfo] = Field(default_factory=list)


# ============================================================================
# Transaction Models
# ============================================================================


class TransactionState(str, Enum):
    """Transaction states."""
    ACTIVE = "active"
    PREPARING = "preparing"
    COMMITTED = "committed"
    ABORTED = "aborted"


class TransactionInfo(BaseModel):
    """Information about a transaction."""
    
    model_config = ConfigDict(frozen=True)
    
    txn_id: str
    state: TransactionState
    created_at: datetime
    timeout_ms: int = 30000
