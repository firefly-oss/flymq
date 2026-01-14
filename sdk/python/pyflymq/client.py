# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0

"""
FlyMQ Python Client.

A high-performance client for FlyMQ message queue with support for:
- Automatic failover and reconnection
- TLS/SSL encryption
- Consumer groups
- Transactions
- Schema validation
- Dead letter queues
- Delayed messages and TTL

Example:
    >>> from pyflymq import FlyMQClient
    >>> client = FlyMQClient("localhost:9092")
    >>> offset = client.produce("my-topic", b"Hello, FlyMQ!")
    >>> message = client.consume("my-topic", offset)
    >>> client.close()
"""

from __future__ import annotations

import socket
import ssl
import threading
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Iterator

from .exceptions import (
    AuthenticationError,
    ConnectionClosedError,
    ConnectionError,
    ConnectionTimeoutError,
    NoAvailableServerError,
    NotLeaderError,
    ServerError,
    TransactionNotActiveError,
)
from .protocol import OpCode, Header, MAGIC_BYTE, PROTOCOL_VERSION, read_message, write_message
from .binary import (
    FLAG_BINARY,
    encode_produce_request,
    decode_produce_response,
    encode_consume_request,
    decode_consume_response,
    encode_create_topic_request,
    decode_create_topic_response,
    encode_delete_topic_request,
    decode_delete_topic_response,
    decode_list_topics_response,
    encode_fetch_request,
    decode_fetch_response,
    encode_subscribe_request,
    decode_subscribe_response,
    encode_commit_request,
    decode_commit_response,
    encode_get_offset_request,
    decode_get_offset_response,
    encode_reset_offset_request,
    decode_simple_bool_response,
    decode_list_groups_response,
    encode_describe_group_request,
    decode_describe_group_response,
    encode_get_lag_request,
    decode_get_lag_response,
    encode_delete_group_request,
    encode_produce_delayed_request,
    encode_produce_with_ttl_request,
    encode_produce_with_schema_request,
    encode_txn_request,
    decode_begin_txn_response,
    encode_txn_produce_request,
    encode_register_schema_request,
    encode_list_schemas_request,
    decode_list_schemas_response,
    encode_get_schema_request,
    encode_validate_schema_request,
    decode_validate_schema_response,
    encode_delete_schema_request,
    encode_fetch_dlq_request,
    decode_fetch_dlq_response,
    encode_replay_dlq_request,
    encode_purge_dlq_request,
    encode_cluster_join_request,
    BinaryProduceRequest,
    BinaryConsumeRequest,
    BinaryCreateTopicRequest,
    BinaryDeleteTopicRequest,
    BinaryFetchRequest,
    BinarySubscribeRequest,
    BinaryCommitRequest,
    BinaryGetOffsetRequest,
    BinaryResetOffsetRequest,
    BinaryDescribeGroupRequest,
    BinaryGetLagRequest,
    BinaryDeleteGroupRequest,
    BinaryProduceDelayedRequest,
    BinaryProduceWithTTLRequest,
    BinaryProduceWithSchemaRequest,
    BinaryTxnRequest,
    BinaryTxnProduceRequest,
    BinaryRegisterSchemaRequest,
    BinaryListSchemasRequest,
    BinaryGetSchemaRequest,
    BinaryValidateSchemaRequest,
    BinaryDeleteSchemaRequest,
    BinaryFetchDLQRequest,
    BinaryReplayDLQRequest,
    BinaryPurgeDLQRequest,
    BinaryClusterJoinRequest,
    BinaryAuthRequest,
    encode_auth_request,
    decode_auth_response,
    decode_whoami_response,
)
from .types import (
    AuthResponse,
    ClientConfig,
    ConsumedMessage,
    DLQMessage,
    FetchResult,
    ProduceResult,
    SchemaInfo,
    WhoAmIResponse,
)

if TYPE_CHECKING:
    from datetime import datetime


class FlyMQClient:
    """
    FlyMQ client with support for HA, TLS, and all FlyMQ operations.

    The client automatically handles:
    - Connection to multiple bootstrap servers
    - Automatic failover on connection errors
    - Leader redirection in cluster mode
    - TLS encryption
    - Thread-safe operations

    Example:
        >>> client = FlyMQClient("localhost:9092")
        >>> client.create_topic("my-topic", partitions=3)
        >>> offset = client.produce("my-topic", b"Hello!")
        >>> msg = client.consume("my-topic", offset)
        >>> client.close()
    """

    def __init__(
        self,
        bootstrap_servers: str | list[str] = "localhost:9092",
        *,
        config: ClientConfig | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize FlyMQ client.

        Args:
            bootstrap_servers: Comma-separated servers or list of servers.
            config: Optional ClientConfig object.
            **kwargs: Override config options (tls_enabled, connect_timeout_ms, etc.)
        """
        if config is None:
            config = ClientConfig(bootstrap_servers=bootstrap_servers, **kwargs)
        else:
            config.bootstrap_servers = bootstrap_servers
            for key, value in kwargs.items():
                if hasattr(config, key):
                    setattr(config, key, value)

        self._config = config
        self._servers = config.get_servers()
        self._current_server_idx = 0
        self._sock: socket.socket | None = None
        self._ssl_context: ssl.SSLContext | None = None
        self._lock = threading.RLock()
        self._closed = False
        self._encryptor = None
        self._authenticated = False
        self._username: str | None = None

        if config.tls_enabled:
            self._setup_tls()

        # Setup encryption if key provided
        if config.encryption_key:
            from .crypto import Encryptor
            self._encryptor = Encryptor.from_hex_key(config.encryption_key)

        self._connect()

        # Auto-authenticate if credentials are provided
        if config.username:
            self.authenticate(config.username, config.password or "")

    def _setup_tls(self) -> None:
        """Configure TLS/SSL context."""
        self._ssl_context = ssl.create_default_context()

        if self._config.tls_insecure_skip_verify:
            self._ssl_context.check_hostname = False
            self._ssl_context.verify_mode = ssl.CERT_NONE
        elif self._config.tls_ca_file:
            self._ssl_context.load_verify_locations(self._config.tls_ca_file)

        if self._config.tls_cert_file and self._config.tls_key_file:
            self._ssl_context.load_cert_chain(
                self._config.tls_cert_file, self._config.tls_key_file
            )

    def _connect(self) -> None:
        """Connect to a FlyMQ server with failover."""
        errors: list[str] = []

        for attempt in range(self._config.max_retries):
            for i in range(len(self._servers)):
                server_idx = (self._current_server_idx + i) % len(self._servers)
                server = self._servers[server_idx]

                try:
                    self._connect_to_server(server)
                    self._current_server_idx = server_idx
                    return
                except Exception as e:
                    errors.append(f"{server}: {e}")

            if attempt < self._config.max_retries - 1:
                time.sleep(self._config.retry_delay_ms / 1000.0)

        raise NoAvailableServerError(self._servers)

    def _connect_to_server(self, server: str) -> None:
        """Connect to a specific server."""
        host, port_str = server.rsplit(":", 1)
        port = int(port_str)

        timeout = self._config.connect_timeout_ms / 1000.0

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)

        try:
            sock.connect((host, port))

            if self._ssl_context:
                sock = self._ssl_context.wrap_socket(sock, server_hostname=host)

            self._sock = sock
        except socket.timeout as e:
            sock.close()
            raise ConnectionTimeoutError(f"Connection to {server} timed out", host, port) from e
        except OSError as e:
            sock.close()
            raise ConnectionError(f"Failed to connect to {server}: {e}", host, port) from e

    def _ensure_connected(self) -> None:
        """Ensure we have an active connection."""
        if self._closed:
            raise ConnectionClosedError("Client is closed")
        if self._sock is None:
            self._connect()

    def _send_binary_request(self, op: OpCode, payload_bytes: bytes) -> bytes:
        """
        Send a binary request and receive binary response.

        Args:
            op: Operation code.
            payload_bytes: Binary-encoded request payload.

        Returns:
            Raw binary response payload.

        Raises:
            ServerError: If server returns an error.
            ConnectionError: If connection fails.
        """
        with self._lock:
            self._ensure_connected()

            # Create a file-like object for the socket
            sock_file = self._sock.makefile("rwb")

            try:
                # Write binary message with FLAG_BINARY set
                header = Header(
                    magic=MAGIC_BYTE,
                    version=PROTOCOL_VERSION,
                    op=op,
                    flags=FLAG_BINARY,
                    length=len(payload_bytes),
                )
                sock_file.write(header.to_bytes())
                if payload_bytes:
                    sock_file.write(payload_bytes)
                sock_file.flush()

                response = read_message(sock_file)

                if response.op == OpCode.ERROR:
                    error_msg = response.payload.decode("utf-8")
                    self._handle_server_error(error_msg)

                return response.payload

            except EOFError as e:
                self._sock = None
                raise ConnectionClosedError(str(e)) from e
            except OSError as e:
                self._sock = None
                raise ConnectionError(str(e)) from e
            finally:
                sock_file.close()

    def _handle_server_error(self, error_msg: str) -> None:
        """Handle server error response."""
        if "not leader" in error_msg.lower():
            # Extract leader info if available
            leader_id = None
            if "current leader is" in error_msg:
                leader_id = error_msg.split("current leader is")[-1].strip()
            raise NotLeaderError(leader_id)
        raise ServerError(error_msg)

    def close(self) -> None:
        """Close the client connection."""
        with self._lock:
            self._closed = True
            if self._sock:
                try:
                    self._sock.close()
                except Exception:
                    pass
                self._sock = None

    # =========================================================================
    # Authentication Methods
    # =========================================================================

    @property
    def is_authenticated(self) -> bool:
        """Check if the client is authenticated."""
        return self._authenticated

    @property
    def username(self) -> str | None:
        """Get the authenticated username."""
        return self._username

    def authenticate(self, username: str, password: str) -> AuthResponse:
        """
        Authenticate with the FlyMQ server.

        Args:
            username: Username for authentication.
            password: Password for authentication.

        Returns:
            AuthResponse with authentication result.

        Raises:
            AuthenticationError: If authentication fails.
        """
        req = BinaryAuthRequest(username=username, password=password)
        response_bytes = self._send_binary_request(OpCode.AUTH, encode_auth_request(req))
        response = decode_auth_response(response_bytes)

        auth_resp = AuthResponse(
            success=response.success,
            error=None,
            username=response.username,
            roles=response.roles,
            permissions=response.permissions,
        )

        if auth_resp.success:
            self._authenticated = True
            self._username = username
        else:
            raise AuthenticationError("Authentication failed")

        return auth_resp

    def whoami(self) -> WhoAmIResponse:
        """
        Get information about the current authentication status.

        Returns:
            WhoAmIResponse with current user information.
        """
        response_bytes = self._send_binary_request(OpCode.WHOAMI, b"")
        response = decode_whoami_response(response_bytes)

        return WhoAmIResponse(
            authenticated=response.authenticated,
            username=response.username,
            roles=response.roles,
            permissions=response.permissions,
        )

    def __enter__(self) -> FlyMQClient:
        """Context manager entry."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        self.close()

    # =========================================================================
    # Core Operations
    # =========================================================================

    def produce(
        self,
        topic: str,
        data: bytes | str,
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
    ) -> int:
        """
        Produce a message to a topic.

        Args:
            topic: Target topic name.
            data: Message data (bytes or string).
            key: Optional message key for key-based partitioning.
                 When provided, messages with the same key go to the same partition.
            partition: Target partition (default: None for automatic selection).
                       If both key and partition are provided, partition takes precedence.

        Returns:
            The offset of the produced message.

        Example:
            >>> # Automatic partition selection
            >>> offset = client.produce("my-topic", b"Hello!")
            >>> # Key-based partitioning (same key = same partition)
            >>> offset = client.produce("orders", b'{"id": 1}', key=b"user-123")
            >>> # Explicit partition selection
            >>> offset = client.produce("events", b"data", partition=2)
        """
        if isinstance(data, str):
            data = data.encode("utf-8")
        if isinstance(key, str):
            key = key.encode("utf-8")

        # Encrypt data if encryptor is configured
        if self._encryptor:
            data = self._encryptor.encrypt(data)

        # Use binary protocol for maximum performance
        req = BinaryProduceRequest(
            topic=topic,
            key=key or b"",
            value=data,
            partition=partition if partition is not None else -1,
        )
        response_bytes = self._send_binary_request(OpCode.PRODUCE, encode_produce_request(req))
        response = decode_produce_response(response_bytes)
        return response.offset

    def produce_with_key(
        self,
        topic: str,
        key: bytes | str,
        data: bytes | str,
    ) -> int:
        """
        Produce a message with a key for key-based partitioning.

        Messages with the same key are guaranteed to go to the same partition,
        ensuring ordering for related messages (like Kafka's key-based partitioning).

        Args:
            topic: Target topic name.
            key: Message key (determines partition).
            data: Message data.

        Returns:
            The offset of the produced message.

        Example:
            >>> # All messages for user-123 go to the same partition
            >>> client.produce_with_key("orders", "user-123", b'{"order": 1}')
            >>> client.produce_with_key("orders", "user-123", b'{"order": 2}')
        """
        return self.produce(topic, data, key=key)

    def produce_to_partition(
        self,
        topic: str,
        partition: int,
        data: bytes | str,
        *,
        key: bytes | str | None = None,
    ) -> int:
        """
        Produce a message to a specific partition.

        Use this when you need explicit control over partition assignment.

        Args:
            topic: Target topic name.
            partition: Target partition number.
            data: Message data.
            key: Optional message key (for tracking, not partition selection).

        Returns:
            The offset of the produced message.

        Example:
            >>> # Send to partition 2 explicitly
            >>> offset = client.produce_to_partition("events", 2, b"data")
        """
        return self.produce(topic, data, key=key, partition=partition)

    def consume(
        self,
        topic: str,
        offset: int,
        partition: int = 0,
    ) -> ConsumedMessage:
        """
        Consume a single message from a topic.

        Args:
            topic: Topic to consume from.
            offset: Offset of the message to consume.
            partition: Partition to consume from (default: 0).

        Returns:
            ConsumedMessage with data and key.

        Example:
            >>> msg = client.consume("my-topic", 0)
            >>> print(msg.data.decode())
            >>> if msg.key:
            ...     print(f"Key: {msg.key.decode()}")
            >>> # Consume from specific partition
            >>> msg = client.consume("my-topic", 0, partition=2)
        """
        # Use binary protocol for maximum performance
        req = BinaryConsumeRequest(topic=topic, partition=partition, offset=offset)
        response_bytes = self._send_binary_request(OpCode.CONSUME, encode_consume_request(req))
        response = decode_consume_response(response_bytes)

        data = response.value
        key = response.key if response.key else None

        # Decrypt data if encryptor is configured
        if self._encryptor and data:
            try:
                data = self._encryptor.decrypt(data)
            except Exception:
                pass  # Data may not be encrypted

        return ConsumedMessage(
            topic=topic,
            partition=partition,
            offset=offset,
            data=data,
            key=key,
        )

    def fetch(
        self,
        topic: str,
        partition: int = 0,
        offset: int = 0,
        max_messages: int = 10,
    ) -> FetchResult:
        """
        Fetch multiple messages from a topic.

        Args:
            topic: Topic to fetch from.
            partition: Partition to fetch from.
            offset: Starting offset.
            max_messages: Maximum messages to fetch.

        Returns:
            FetchResult with messages and next offset.
        """
        # Use binary protocol for maximum performance
        req = BinaryFetchRequest(
            topic=topic,
            partition=partition,
            offset=offset,
            max_messages=max_messages,
        )
        response_bytes = self._send_binary_request(OpCode.FETCH, encode_fetch_request(req))
        response = decode_fetch_response(response_bytes)

        messages = [
            ConsumedMessage(
                topic=topic,
                partition=partition,
                offset=msg.offset,
                data=msg.value,
                key=msg.key if msg.key else None,
            )
            for msg in response.messages
        ]

        return FetchResult(
            messages=messages,
            next_offset=response.next_offset,
        )

    def create_topic(self, topic: str, partitions: int = 1) -> None:
        """
        Create a new topic.

        Args:
            topic: Name of the topic to create.
            partitions: Number of partitions (default: 1).
        """
        req = BinaryCreateTopicRequest(topic=topic, partitions=partitions)
        self._send_binary_request(OpCode.CREATE_TOPIC, encode_create_topic_request(req))

    def delete_topic(self, topic: str) -> None:
        """
        Delete a topic.

        Args:
            topic: Name of the topic to delete.
        """
        req = BinaryDeleteTopicRequest(topic=topic)
        self._send_binary_request(OpCode.DELETE_TOPIC, encode_delete_topic_request(req))

    def list_topics(self) -> list[str]:
        """
        List all topics.

        Returns:
            List of topic names.
        """
        # List topics has no request body, just empty payload
        response_bytes = self._send_binary_request(OpCode.LIST_TOPICS, b"")
        response = decode_list_topics_response(response_bytes)
        return response.topics

    # =========================================================================
    # Consumer Group Operations
    # =========================================================================

    def subscribe(
        self,
        topic: str,
        group_id: str,
        partition: int = 0,
        mode: str = "latest",
    ) -> int:
        """
        Subscribe to a topic with a consumer group.

        Args:
            topic: Topic to subscribe to.
            group_id: Consumer group ID.
            partition: Partition to subscribe to.
            mode: Start position ("earliest", "latest", "commit").

        Returns:
            Starting offset for consumption.
        """
        req = BinarySubscribeRequest(
            topic=topic,
            group_id=group_id,
            partition=partition,
            mode=mode,
        )
        response_bytes = self._send_binary_request(OpCode.SUBSCRIBE, encode_subscribe_request(req))
        response = decode_subscribe_response(response_bytes)
        return response.offset

    def commit_offset(
        self,
        topic: str,
        group_id: str,
        partition: int,
        offset: int,
    ) -> None:
        """
        Commit consumer offset.

        Args:
            topic: Topic name.
            group_id: Consumer group ID.
            partition: Partition number.
            offset: Offset to commit.
        """
        req = BinaryCommitRequest(
            topic=topic,
            group_id=group_id,
            partition=partition,
            offset=offset,
        )
        self._send_binary_request(OpCode.COMMIT, encode_commit_request(req))

    def get_committed_offset(
        self,
        topic: str,
        group_id: str,
        partition: int = 0,
    ) -> int:
        """
        Get the committed offset for a consumer group.

        Args:
            topic: Topic name.
            group_id: Consumer group ID.
            partition: Partition number.

        Returns:
            The committed offset.
        """
        req = BinaryGetOffsetRequest(
            topic=topic,
            group_id=group_id,
            partition=partition,
        )
        response_bytes = self._send_binary_request(
            OpCode.GET_OFFSET, encode_get_offset_request(req)
        )
        response = decode_get_offset_response(response_bytes)
        return response.offset

    def reset_offset(
        self,
        topic: str,
        group_id: str,
        partition: int = 0,
        mode: str = "earliest",
        offset: int | None = None,
    ) -> None:
        """
        Reset consumer group offset to a specific position.

        Args:
            topic: Topic name.
            group_id: Consumer group ID.
            partition: Partition number.
            mode: Reset mode ("earliest", "latest", or "offset").
            offset: Specific offset (only used when mode="offset").
        """
        req = BinaryResetOffsetRequest(
            topic=topic,
            group_id=group_id,
            partition=partition,
            mode=mode,
            offset=offset if offset is not None else 0,
        )
        self._send_binary_request(OpCode.RESET_OFFSET, encode_reset_offset_request(req))

    def reset_offset_to_earliest(
        self,
        topic: str,
        group_id: str,
        partition: int = 0,
    ) -> None:
        """
        Reset consumer group offset to the earliest position.

        Args:
            topic: Topic name.
            group_id: Consumer group ID.
            partition: Partition number.
        """
        self.reset_offset(topic, group_id, partition, mode="earliest")

    def reset_offset_to_latest(
        self,
        topic: str,
        group_id: str,
        partition: int = 0,
    ) -> None:
        """
        Reset consumer group offset to the latest position.

        Args:
            topic: Topic name.
            group_id: Consumer group ID.
            partition: Partition number.
        """
        self.reset_offset(topic, group_id, partition, mode="latest")

    def get_lag(
        self,
        topic: str,
        group_id: str,
        partition: int = 0,
    ) -> dict[str, Any]:
        """
        Get consumer lag for a consumer group.

        Args:
            topic: Topic name.
            group_id: Consumer group ID.
            partition: Partition number.

        Returns:
            Dictionary with lag information including:
            - current_offset: Current consumer position
            - committed_offset: Last committed offset
            - latest_offset: Latest offset in the topic
            - lag: Number of messages behind
        """
        req = BinaryGetLagRequest(
            topic=topic,
            group_id=group_id,
            partition=partition,
        )
        response_bytes = self._send_binary_request(
            OpCode.GET_LAG, encode_get_lag_request(req)
        )
        response = decode_get_lag_response(response_bytes)
        return {
            "current_offset": response.current_offset,
            "committed_offset": response.committed_offset,
            "latest_offset": response.latest_offset,
            "lag": response.lag,
        }

    def list_consumer_groups(self, topic: str = "") -> list[dict[str, Any]]:
        """
        List all consumer groups.

        Args:
            topic: Optional topic filter.

        Returns:
            List of consumer group information dictionaries.
        """
        # List groups requires no payload - send empty binary request
        response_bytes = self._send_binary_request(OpCode.LIST_GROUPS, b"")
        response = decode_list_groups_response(response_bytes)
        return [
            {
                "topic": g.topic,
                "group_id": g.group_id,
                "members": g.members,
                "offsets": [
                    {"partition": o.partition, "offset": o.offset}
                    for o in g.offsets
                ],
            }
            for g in response.groups
        ]

    def describe_consumer_group(self, topic: str, group_id: str) -> dict[str, Any]:
        """
        Get detailed information about a consumer group.

        Args:
            topic: Topic name.
            group_id: Consumer group ID.

        Returns:
            Dictionary with group information including:
            - topic: Topic name
            - group_id: Group identifier
            - members: Number of members
            - offsets: List of partition offsets
        """
        req = BinaryDescribeGroupRequest(topic=topic, group_id=group_id)
        response_bytes = self._send_binary_request(
            OpCode.DESCRIBE_GROUP, encode_describe_group_request(req)
        )
        response = decode_describe_group_response(response_bytes)
        return {
            "topic": response.topic,
            "group_id": response.group_id,
            "members": response.members,
            "offsets": [
                {"partition": o.partition, "offset": o.offset}
                for o in response.offsets
            ],
        }

    def delete_consumer_group(self, topic: str, group_id: str) -> None:
        """
        Delete a consumer group.

        Args:
            topic: Topic name.
            group_id: Consumer group ID to delete.
        """
        req = BinaryDeleteGroupRequest(topic=topic, group_id=group_id)
        self._send_binary_request(OpCode.DELETE_GROUP, encode_delete_group_request(req))

    # =========================================================================
    # Advanced Messaging
    # =========================================================================

    def produce_delayed(self, topic: str, data: bytes | str, delay_ms: int) -> int:
        """
        Produce a message with delayed delivery.

        Args:
            topic: Target topic.
            data: Message data.
            delay_ms: Delay in milliseconds.

        Returns:
            Message offset.
        """
        if isinstance(data, str):
            data = data.encode("utf-8")

        req = BinaryProduceDelayedRequest(topic=topic, data=data, delay_ms=delay_ms)
        response_bytes = self._send_binary_request(
            OpCode.PRODUCE_DELAYED, encode_produce_delayed_request(req)
        )
        return decode_produce_response(response_bytes).offset

    def produce_with_ttl(self, topic: str, data: bytes | str, ttl_ms: int) -> int:
        """
        Produce a message with time-to-live.

        Args:
            topic: Target topic.
            data: Message data.
            ttl_ms: TTL in milliseconds.

        Returns:
            Message offset.
        """
        if isinstance(data, str):
            data = data.encode("utf-8")

        req = BinaryProduceWithTTLRequest(topic=topic, data=data, ttl_ms=ttl_ms)
        response_bytes = self._send_binary_request(
            OpCode.PRODUCE_WITH_TTL, encode_produce_with_ttl_request(req)
        )
        return decode_produce_response(response_bytes).offset

    def produce_with_schema(self, topic: str, data: bytes | str, schema_name: str) -> int:
        """
        Produce a message with schema validation.

        Args:
            topic: Target topic.
            data: Message data.
            schema_name: Schema to validate against.

        Returns:
            Message offset.
        """
        if isinstance(data, str):
            data = data.encode("utf-8")

        req = BinaryProduceWithSchemaRequest(topic=topic, data=data, schema_name=schema_name)
        response_bytes = self._send_binary_request(
            OpCode.PRODUCE_WITH_SCHEMA, encode_produce_with_schema_request(req)
        )
        return decode_produce_response(response_bytes).offset

    # =========================================================================
    # Transaction Operations
    # =========================================================================

    def begin_transaction(self) -> Transaction:
        """
        Begin a new transaction.

        Returns:
            Transaction object for transactional operations.

        Example:
            >>> txn = client.begin_transaction()
            >>> txn.produce("topic1", b"msg1")
            >>> txn.produce("topic2", b"msg2")
            >>> txn.commit()
        """
        response_bytes = self._send_binary_request(OpCode.BEGIN_TX, b"")
        response = decode_begin_txn_response(response_bytes)
        return Transaction(self, response.txn_id)

    @contextmanager
    def transaction(self) -> Iterator[Transaction]:
        """
        Context manager for transactions.

        Example:
            >>> with client.transaction() as txn:
            ...     txn.produce("topic", b"message")
            ...     # Auto-commits on success, rollbacks on exception
        """
        txn = self.begin_transaction()
        try:
            yield txn
            txn.commit()
        except Exception:
            txn.rollback()
            raise

    def _commit_transaction(self, txn_id: str) -> None:
        """Commit a transaction (internal)."""
        req = BinaryTxnRequest(txn_id=txn_id)
        self._send_binary_request(OpCode.COMMIT_TX, encode_txn_request(req))

    def _abort_transaction(self, txn_id: str) -> None:
        """Abort a transaction (internal)."""
        req = BinaryTxnRequest(txn_id=txn_id)
        self._send_binary_request(OpCode.ABORT_TX, encode_txn_request(req))

    def _produce_in_transaction(self, txn_id: str, topic: str, data: bytes) -> int:
        """Produce within a transaction (internal)."""
        req = BinaryTxnProduceRequest(txn_id=txn_id, topic=topic, data=data)
        response_bytes = self._send_binary_request(
            OpCode.PRODUCE_TX, encode_txn_produce_request(req)
        )
        return decode_produce_response(response_bytes).offset

    # =========================================================================
    # Schema Registry Operations
    # =========================================================================

    def register_schema(self, name: str, schema_type: str, schema: str | bytes) -> None:
        """
        Register a schema.

        Args:
            name: Schema name.
            schema_type: Schema type ("json", "avro", "protobuf").
            schema: Schema definition.
        """
        if isinstance(schema, str):
            schema = schema.encode("utf-8")

        req = BinaryRegisterSchemaRequest(name=name, schema_type=schema_type, schema=schema)
        self._send_binary_request(OpCode.REGISTER_SCHEMA, encode_register_schema_request(req))

    def list_schemas(self, topic: str = "") -> list[dict[str, Any]]:
        """
        List registered schemas.

        Args:
            topic: Optional topic filter.

        Returns:
            List of schema metadata.
        """
        req = BinaryListSchemasRequest(topic=topic)
        response_bytes = self._send_binary_request(
            OpCode.LIST_SCHEMAS, encode_list_schemas_request(req)
        )
        response = decode_list_schemas_response(response_bytes)
        return [
            {"name": s.name, "type": s.schema_type, "version": s.version}
            for s in response.schemas
        ]

    def get_schema(self, topic: str, version: int = 0) -> dict[str, Any]:
        """
        Get a schema by topic and version.

        Args:
            topic: Topic name.
            version: Schema version (0 for latest).

        Returns:
            Schema metadata.
        """
        req = BinaryGetSchemaRequest(topic=topic, version=version)
        response_bytes = self._send_binary_request(
            OpCode.GET_SCHEMA, encode_get_schema_request(req)
        )
        response = decode_list_schemas_response(response_bytes)
        if response.schemas:
            s = response.schemas[0]
            return {"name": s.name, "type": s.schema_type, "version": s.version}
        return {}

    def validate_schema(self, name: str, message: bytes | str) -> bool:
        """
        Validate a message against a schema.

        Args:
            name: Schema name.
            message: Message to validate.

        Returns:
            True if valid.
        """
        if isinstance(message, str):
            message = message.encode("utf-8")

        req = BinaryValidateSchemaRequest(name=name, message=message)
        response_bytes = self._send_binary_request(
            OpCode.VALIDATE_SCHEMA, encode_validate_schema_request(req)
        )
        return decode_validate_schema_response(response_bytes).valid

    def delete_schema(self, topic: str, version: int) -> None:
        """
        Delete a schema version.

        Args:
            topic: Topic name.
            version: Schema version.
        """
        req = BinaryDeleteSchemaRequest(topic=topic, version=version)
        self._send_binary_request(OpCode.DELETE_SCHEMA, encode_delete_schema_request(req))

    # =========================================================================
    # Dead Letter Queue Operations
    # =========================================================================

    def fetch_dlq(self, topic: str, max_messages: int = 10) -> list[DLQMessage]:
        """
        Fetch messages from dead letter queue.

        Args:
            topic: Topic name.
            max_messages: Maximum messages to fetch.

        Returns:
            List of DLQ messages.
        """
        from datetime import datetime

        req = BinaryFetchDLQRequest(topic=topic, max_messages=max_messages)
        response_bytes = self._send_binary_request(
            OpCode.FETCH_DLQ, encode_fetch_dlq_request(req)
        )
        response = decode_fetch_dlq_response(response_bytes)

        messages = []
        for msg in response.messages:
            messages.append(
                DLQMessage(
                    id=msg.id,
                    topic=topic,
                    data=msg.data,
                    error=msg.error,
                    retries=msg.retries,
                    timestamp=datetime.now(),
                )
            )
        return messages

    def replay_dlq(self, topic: str, message_id: str) -> None:
        """
        Replay a message from the dead letter queue.

        Args:
            topic: Topic name.
            message_id: Message ID to replay.
        """
        req = BinaryReplayDLQRequest(topic=topic, message_id=message_id)
        self._send_binary_request(OpCode.REPLAY_DLQ, encode_replay_dlq_request(req))

    def purge_dlq(self, topic: str) -> None:
        """
        Purge all messages from dead letter queue.

        Args:
            topic: Topic name.
        """
        req = BinaryPurgeDLQRequest(topic=topic)
        self._send_binary_request(OpCode.PURGE_DLQ, encode_purge_dlq_request(req))

    # =========================================================================
    # Cluster Operations
    # =========================================================================

    def cluster_join(self, peer_addr: str) -> None:
        """
        Join a cluster via peer.

        Args:
            peer_addr: Address of peer to join through.
        """
        req = BinaryClusterJoinRequest(peer=peer_addr)
        self._send_binary_request(OpCode.CLUSTER_JOIN, encode_cluster_join_request(req))

    def cluster_leave(self) -> None:
        """Leave the cluster gracefully."""
        self._send_binary_request(OpCode.CLUSTER_LEAVE, b"")


class Transaction:
    """
    Represents a FlyMQ transaction.

    Use within a context manager for automatic commit/rollback:

        with client.transaction() as txn:
            txn.produce("topic", b"message")
            # Auto-commits on success
    """

    def __init__(self, client: FlyMQClient, txn_id: str) -> None:
        """Initialize transaction."""
        self._client = client
        self._txn_id = txn_id
        self._active = True

    @property
    def txn_id(self) -> str:
        """Get transaction ID."""
        return self._txn_id

    @property
    def active(self) -> bool:
        """Check if transaction is active."""
        return self._active

    def produce(self, topic: str, data: bytes | str) -> int:
        """
        Produce a message within the transaction.

        Args:
            topic: Target topic.
            data: Message data.

        Returns:
            Message offset.
        """
        if not self._active:
            raise TransactionNotActiveError()

        if isinstance(data, str):
            data = data.encode("utf-8")

        return self._client._produce_in_transaction(self._txn_id, topic, data)

    def commit(self) -> None:
        """Commit the transaction."""
        if not self._active:
            raise TransactionNotActiveError()

        self._client._commit_transaction(self._txn_id)
        self._active = False

    def rollback(self) -> None:
        """Rollback the transaction."""
        if not self._active:
            raise TransactionNotActiveError()

        self._client._abort_transaction(self._txn_id)
        self._active = False
