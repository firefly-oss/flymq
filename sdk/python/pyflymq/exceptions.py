# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0

"""Custom exceptions for FlyMQ Python SDK."""

from __future__ import annotations


class FlyMQError(Exception):
    """Base exception for all FlyMQ errors."""


class ConnectionError(FlyMQError):
    """Raised when connection to FlyMQ server fails."""

    def __init__(self, message: str, host: str | None = None, port: int | None = None) -> None:
        super().__init__(message)
        self.host = host
        self.port = port


class ConnectionClosedError(ConnectionError):
    """Raised when connection is unexpectedly closed."""


class ConnectionTimeoutError(ConnectionError):
    """Raised when connection times out."""


class AuthenticationError(FlyMQError):
    """Raised when authentication fails."""


class ServerError(FlyMQError):
    """Raised when server returns an error response."""

    def __init__(self, message: str, op_code: int | None = None) -> None:
        super().__init__(message)
        self.op_code = op_code


class TopicError(FlyMQError):
    """Base exception for topic-related errors."""


class TopicNotFoundError(TopicError):
    """Raised when topic doesn't exist."""

    def __init__(self, topic: str) -> None:
        super().__init__(f"Topic not found: {topic}")
        self.topic = topic


class TopicAlreadyExistsError(TopicError):
    """Raised when trying to create a topic that already exists."""

    def __init__(self, topic: str) -> None:
        super().__init__(f"Topic already exists: {topic}")
        self.topic = topic


class ProducerError(FlyMQError):
    """Base exception for producer errors."""


class MessageTooLargeError(ProducerError):
    """Raised when message exceeds maximum size."""

    def __init__(self, size: int, max_size: int) -> None:
        super().__init__(f"Message size {size} exceeds maximum {max_size}")
        self.size = size
        self.max_size = max_size


class ConsumerError(FlyMQError):
    """Base exception for consumer errors."""


class OffsetOutOfRangeError(ConsumerError):
    """Raised when requested offset is out of range."""

    def __init__(self, topic: str, offset: int) -> None:
        super().__init__(f"Offset {offset} out of range for topic {topic}")
        self.topic = topic
        self.offset = offset


class ConsumerGroupError(ConsumerError):
    """Raised for consumer group related errors."""


class TransactionError(FlyMQError):
    """Base exception for transaction errors."""


class TransactionNotActiveError(TransactionError):
    """Raised when operation is attempted on inactive transaction."""

    def __init__(self) -> None:
        super().__init__("Transaction is not active")


class TransactionTimeoutError(TransactionError):
    """Raised when transaction times out."""

    def __init__(self, txn_id: str) -> None:
        super().__init__(f"Transaction {txn_id} timed out")
        self.txn_id = txn_id


class SchemaError(FlyMQError):
    """Base exception for schema-related errors."""


class SchemaNotFoundError(SchemaError):
    """Raised when schema is not found."""

    def __init__(self, schema_name: str) -> None:
        super().__init__(f"Schema not found: {schema_name}")
        self.schema_name = schema_name


class SchemaValidationError(SchemaError):
    """Raised when message fails schema validation."""

    def __init__(self, schema_name: str, errors: list[str]) -> None:
        super().__init__(f"Schema validation failed for {schema_name}: {errors}")
        self.schema_name = schema_name
        self.errors = errors


class ClusterError(FlyMQError):
    """Base exception for cluster-related errors."""


class NotLeaderError(ClusterError):
    """Raised when operation is sent to non-leader node."""

    def __init__(self, leader_id: str | None = None) -> None:
        msg = "Not the cluster leader"
        if leader_id:
            msg += f", current leader is {leader_id}"
        super().__init__(msg)
        self.leader_id = leader_id


class NoAvailableServerError(ClusterError):
    """Raised when no servers are available."""

    def __init__(self, servers: list[str]) -> None:
        super().__init__(f"No available servers from: {servers}")
        self.servers = servers


class CryptoError(FlyMQError):
    """Base exception for cryptographic errors."""


class EncryptionError(CryptoError):
    """Raised when encryption fails."""


class DecryptionError(CryptoError):
    """Raised when decryption fails (data may be corrupted or tampered)."""


class InvalidKeyError(CryptoError):
    """Raised when encryption key is invalid."""
