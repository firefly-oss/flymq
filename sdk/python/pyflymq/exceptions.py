# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0

"""
Custom exceptions for FlyMQ Python SDK.

All exceptions inherit from FlyMQError, making it easy to catch all FlyMQ-related
errors with a single except clause:

    try:
        client.produce("topic", b"message")
    except FlyMQError as e:
        print(f"FlyMQ error: {e}")

For more granular error handling, catch specific exception types:

    try:
        client.produce("topic", b"message")
    except ConnectionError as e:
        print(f"Connection failed to {e.host}:{e.port}")
    except TopicNotFoundError as e:
        print(f"Topic {e.topic} does not exist")
"""

from __future__ import annotations


class FlyMQError(Exception):
    """
    Base exception for all FlyMQ errors.

    All FlyMQ exceptions inherit from this class, allowing you to catch
    all FlyMQ-related errors with a single except clause.
    """

    def __init__(self, message: str, *, hint: str | None = None) -> None:
        self.hint = hint
        if hint:
            message = f"{message}\n\n  Hint: {hint}"
        super().__init__(message)


class ConnectionError(FlyMQError):
    """
    Raised when connection to FlyMQ server fails.

    Common causes:
    - Server is not running
    - Wrong host or port
    - Firewall blocking connection
    - Network issues
    """

    def __init__(
        self,
        message: str,
        host: str | None = None,
        port: int | None = None,
        *,
        hint: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        if hint is None and host:
            hint = f"Check that FlyMQ server is running on {host}:{port}"
        super().__init__(message, hint=hint)


class ConnectionClosedError(ConnectionError):
    """
    Raised when connection is unexpectedly closed.

    This typically happens when:
    - Server was shut down
    - Network connection was interrupted
    - Server closed the connection due to timeout
    """

    def __init__(self, message: str = "Connection closed by server") -> None:
        super().__init__(message, hint="The server may have been restarted. Try reconnecting.")


class ConnectionTimeoutError(ConnectionError):
    """
    Raised when connection times out.

    This typically happens when:
    - Server is overloaded
    - Network latency is too high
    - Firewall is silently dropping packets
    """

    def __init__(self, message: str, host: str | None = None, port: int | None = None) -> None:
        super().__init__(
            message,
            host,
            port,
            hint="Try increasing connect_timeout_ms or check network connectivity",
        )


class AuthenticationError(FlyMQError):
    """
    Raised when authentication fails.

    Common causes:
    - Invalid username or password
    - User account is disabled
    - Server requires authentication but none provided
    """

    def __init__(self, message: str = "Authentication failed") -> None:
        super().__init__(
            message,
            hint="Check your username and password. Use client.authenticate(user, pass) or pass credentials to connect().",
        )


class ServerError(FlyMQError):
    """
    Raised when server returns an error response.

    This is a general error from the server. Check the message for details.
    """

    def __init__(self, message: str, op_code: int | None = None) -> None:
        self.op_code = op_code
        super().__init__(message)


class TopicError(FlyMQError):
    """Base exception for topic-related errors."""


class TopicNotFoundError(TopicError):
    """
    Raised when topic doesn't exist.

    The topic must be created before producing or consuming messages.
    """

    def __init__(self, topic: str) -> None:
        self.topic = topic
        super().__init__(
            f"Topic not found: {topic}",
            hint=f"Create the topic first: client.create_topic('{topic}')",
        )


class TopicAlreadyExistsError(TopicError):
    """Raised when trying to create a topic that already exists."""

    def __init__(self, topic: str) -> None:
        self.topic = topic
        super().__init__(
            f"Topic already exists: {topic}",
            hint="Use client.list_topics() to see existing topics",
        )


class ProducerError(FlyMQError):
    """Base exception for producer errors."""


class MessageTooLargeError(ProducerError):
    """
    Raised when message exceeds maximum size.

    The default maximum message size is 1MB.
    """

    def __init__(self, size: int, max_size: int) -> None:
        self.size = size
        self.max_size = max_size
        super().__init__(
            f"Message size {size} bytes exceeds maximum {max_size} bytes",
            hint="Split large messages or increase server's max_message_size setting",
        )


class ConsumerError(FlyMQError):
    """Base exception for consumer errors."""


class OffsetOutOfRangeError(ConsumerError):
    """
    Raised when requested offset is out of range.

    This happens when:
    - Offset is negative
    - Offset is beyond the latest message
    - Messages at that offset have been deleted (retention policy)
    """

    def __init__(self, topic: str, offset: int) -> None:
        self.topic = topic
        self.offset = offset
        super().__init__(
            f"Offset {offset} out of range for topic {topic}",
            hint="Use subscribe(mode='earliest') to start from the beginning, or 'latest' for new messages",
        )


class ConsumerGroupError(ConsumerError):
    """Raised for consumer group related errors."""


class TransactionError(FlyMQError):
    """Base exception for transaction errors."""


class TransactionNotActiveError(TransactionError):
    """
    Raised when operation is attempted on inactive transaction.

    A transaction becomes inactive after commit() or rollback().
    """

    def __init__(self) -> None:
        super().__init__(
            "Transaction is not active",
            hint="Start a new transaction with client.transaction()",
        )


class TransactionTimeoutError(TransactionError):
    """
    Raised when transaction times out.

    Transactions have a default timeout of 60 seconds.
    """

    def __init__(self, txn_id: str) -> None:
        self.txn_id = txn_id
        super().__init__(
            f"Transaction {txn_id} timed out",
            hint="Increase transaction timeout or reduce transaction scope",
        )


class SchemaError(FlyMQError):
    """Base exception for schema-related errors."""


class SchemaNotFoundError(SchemaError):
    """Raised when schema is not found."""

    def __init__(self, schema_name: str) -> None:
        self.schema_name = schema_name
        super().__init__(
            f"Schema not found: {schema_name}",
            hint=f"Register the schema first: client.register_schema('{schema_name}', schema_def)",
        )


class SchemaValidationError(SchemaError):
    """
    Raised when message fails schema validation.

    The message does not conform to the registered schema.
    """

    def __init__(self, schema_name: str, errors: list[str]) -> None:
        self.schema_name = schema_name
        self.errors = errors
        super().__init__(
            f"Schema validation failed for {schema_name}: {errors}",
            hint="Check that your message matches the schema definition",
        )


class ClusterError(FlyMQError):
    """Base exception for cluster-related errors."""


class NotLeaderError(ClusterError):
    """
    Raised when operation is sent to non-leader node.

    Write operations must be sent to the cluster leader.
    The client will automatically retry with the correct leader.
    """

    def __init__(self, leader_id: str | None = None) -> None:
        self.leader_id = leader_id
        msg = "Not the cluster leader"
        hint = "The client will automatically redirect to the leader"
        if leader_id:
            msg += f", current leader is {leader_id}"
        super().__init__(msg, hint=hint)


class NoAvailableServerError(ClusterError):
    """
    Raised when no servers are available.

    All configured bootstrap servers are unreachable.
    """

    def __init__(self, servers: list[str]) -> None:
        self.servers = servers
        super().__init__(
            f"No available servers from: {', '.join(servers)}",
            hint="Check that at least one FlyMQ server is running and accessible",
        )


class CryptoError(FlyMQError):
    """Base exception for cryptographic errors."""


class EncryptionError(CryptoError):
    """Raised when encryption fails."""

    def __init__(self, message: str = "Encryption failed") -> None:
        super().__init__(message, hint="Check that the encryption key is valid")


class DecryptionError(CryptoError):
    """
    Raised when decryption fails.

    This typically means:
    - Wrong encryption key
    - Data was corrupted
    - Data was tampered with
    """

    def __init__(self, message: str = "Decryption failed") -> None:
        super().__init__(
            message,
            hint="Ensure you're using the same key that was used for encryption",
        )


class InvalidKeyError(CryptoError):
    """
    Raised when encryption key is invalid.

    Keys must be 32 bytes (256 bits) for AES-256-GCM.
    """

    def __init__(self, message: str = "Invalid encryption key") -> None:
        super().__init__(
            message,
            hint="Use generate_key() to create a valid 256-bit key",
        )
