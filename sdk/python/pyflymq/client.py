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

Usage Patterns:

    # Pattern 1: Simple usage (recommended for scripts)
    from pyflymq import connect
    client = connect("localhost:9092")
    offset = client.produce("my-topic", b"Hello!")
    # Connection auto-closes when script ends

    # Pattern 2: Context manager (recommended for applications)
    from pyflymq import FlyMQClient
    with FlyMQClient("localhost:9092") as client:
        offset = client.produce("my-topic", b"Hello!")
    # Connection auto-closes when exiting the block

    # Pattern 3: Explicit lifecycle management
    client = FlyMQClient("localhost:9092")
    try:
        offset = client.produce("my-topic", b"Hello!")
    finally:
        client.close()
"""

from __future__ import annotations

import socket
import ssl
import struct
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterator

from .exceptions import (
    AuthenticationError,
    ConnectionClosedError,
    ConnectionError,
    ConnectionTimeoutError,
    FlyMQError,
    NoAvailableServerError,
    NotLeaderError,
    ServerError,
    TransactionNotActiveError,
)
from .protocol import OpCode, Header, MAGIC_BYTE, PROTOCOL_VERSION, read_message, write_message
from .serde import BytesSerializer, BytesDeserializer, Serializer, Deserializer
from .matcher import is_pattern, match_pattern
from .binary import (
    FLAG_BINARY,
    RecordMetadata,
    encode_produce_request,
    decode_produce_response,
    decode_record_metadata,
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
    decode_error_response,
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
    BinaryClusterMetadataRequest,
    BinaryClusterMetadataResponse,
    PartitionMetadata,
    TopicMetadata,
    encode_cluster_metadata_request,
    decode_cluster_metadata_response,
)
from .types import (
    AuthResponse,
    ClientConfig,
    ClusterMetadata,
    ConsumedMessage,
    DLQMessage,
    FetchResult,
    ProduceResult,
    SchemaInfo,
    WhoAmIResponse,
)
from .tls import TLSConfig

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
        tls: TLSConfig | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize FlyMQ client.

        Args:
            bootstrap_servers: Comma-separated servers or list of servers.
            config: Optional ClientConfig object.
            tls: Optional TLSConfig for secure connections.
            **kwargs: Override config options (tls_enabled, connect_timeout_ms, etc.)
        """
        if config is None:
            config = ClientConfig(bootstrap_servers=bootstrap_servers, **kwargs)
        else:
            config.bootstrap_servers = bootstrap_servers
            for key, value in kwargs.items():
                if hasattr(config, key):
                    setattr(config, key, value)

        # Apply TLS config if provided
        if tls:
            config.tls_enabled = tls.enabled
            config.tls_cert_file = tls.cert_file
            config.tls_key_file = tls.key_file
            config.tls_ca_file = tls.ca_file
            config.tls_server_name = tls.server_name
            config.tls_insecure_skip_verify = tls.insecure_skip_verify

        self._config = config
        self._servers = config.get_servers()
        self._current_server_idx = 0
        self._sock: socket.socket | None = None
        self._ssl_context: ssl.SSLContext | None = None
        self._lock = threading.RLock()
        self._closed = False
        self._authenticated = False
        self._username: str | None = None

        if config.tls_enabled:
            self._setup_tls()

        self._connect()

        # Auto-authenticate if credentials are provided
        if config.username:
            self.authenticate(config.username, config.password or "")

    def _setup_tls(self) -> None:
        """Configure TLS/SSL context."""
        self._ssl_context = ssl.create_default_context()

        if self._config.tls_insecure_skip_verify:
            # Insecure mode: skip all certificate verification
            self._ssl_context.check_hostname = False
            self._ssl_context.verify_mode = ssl.CERT_NONE
        elif self._config.tls_ca_file:
            # Custom CA: load CA certificate for verification
            self._ssl_context.load_verify_locations(self._config.tls_ca_file)
        # else: Use system CA store (default)

        # Load client certificate for mutual TLS
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
        """Connect to a specific server, trying all available addresses."""
        host, port_str = server.rsplit(":", 1)
        port = int(port_str)

        timeout = self._config.connect_timeout_ms / 1000.0

        # Get all addresses for the host, preferring IPv6
        try:
            addrs = socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
        except socket.gaierror as e:
            raise ConnectionError(f"Failed to resolve {host}: {e}", host, port) from e

        # Sort to prefer IPv6 (AF_INET6 = 30 on macOS, 10 on Linux; AF_INET = 2)
        # We want IPv6 first, so sort by family descending
        addrs.sort(key=lambda x: x[0], reverse=True)

        last_error: Exception | None = None
        for family, socktype, proto, canonname, sockaddr in addrs:
            sock = None
            try:
                sock = socket.socket(family, socktype, proto)
                sock.settimeout(timeout)
                sock.connect(sockaddr)

                if self._ssl_context:
                    # Use server_name if provided, otherwise use host
                    server_hostname = self._config.tls_server_name or host
                    sock = self._ssl_context.wrap_socket(sock, server_hostname=server_hostname)

                self._sock = sock
                return  # Success!
            except socket.timeout as e:
                last_error = ConnectionTimeoutError(f"Connection to {server} timed out", host, port)
                if sock:
                    sock.close()
            except OSError as e:
                last_error = ConnectionError(f"Failed to connect to {server}: {e}", host, port)
                if sock:
                    sock.close()

        # All addresses failed
        if last_error:
            raise last_error
        raise ConnectionError(f"No addresses found for {server}", host, port)

    def _ensure_connected(self) -> None:
        """Ensure we have an active connection."""
        if self._closed:
            raise ConnectionClosedError("Client is closed")
        if self._sock is None:
            self._connect()

    def _send_binary_request(self, op: OpCode, payload_bytes: bytes, flags: int = FLAG_BINARY) -> bytes:
        """
        Send a binary request and receive binary response.

        Args:
            op: Operation code.
            payload_bytes: Binary-encoded request payload.
            flags: Message flags (default: FLAG_BINARY).

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
                # Write binary message
                header = Header(
                    magic=MAGIC_BYTE,
                    version=PROTOCOL_VERSION,
                    op=op,
                    flags=flags,
                    length=len(payload_bytes),
                )
                sock_file.write(header.to_bytes())
                if payload_bytes:
                    sock_file.write(payload_bytes)
                sock_file.flush()

                response = read_message(sock_file)

                if response.op == OpCode.ERROR:
                    err_resp = decode_error_response(response.payload)
                    self._handle_server_error(err_resp.message)

                return response.payload

            except EOFError as e:
                self._sock = None
                raise ConnectionClosedError(str(e)) from e
            except OSError as e:
                self._sock = None
                raise ConnectionError(str(e)) from e
            finally:
                sock_file.close()

    def re_inject_dlq(self, topic: str, offset: int) -> bool:
        """
        Re-inject a message from DLQ back into its original topic by offset.

        Args:
            topic: Original topic name.
            offset: Offset of the message in DLQ.

        Returns:
            True if successful.
        """
        from .binary import BinaryReInjectDLQRequest, encode_re_inject_dlq_request, decode_success_response
        
        req = BinaryReInjectDLQRequest(topic=topic, offset=offset)
        payload = encode_re_inject_dlq_request(req)
        
        # OpCode.REPLAY_DLQ is used for both replay by ID and by offset
        resp_payload = self._send_binary_request(OpCode.REPLAY_DLQ, payload)
        resp = decode_success_response(resp_payload)
        return resp.success

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
            error=response.error if response.error else None,
            username=response.username,
            roles=response.roles,
            permissions=[],  # Server doesn't send permissions in auth response
        )

        if auth_resp.success:
            self._authenticated = True
            self._username = username
        else:
            raise AuthenticationError(response.error or "Authentication failed")

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

    def set_serde(self, name: str) -> None:
        """Set default SerDe by name from SerdeRegistry."""
        from .serde import SerdeRegistry
        ser, deser = SerdeRegistry.get(name)
        if ser:
            self._config.value_serializer = ser
        if deser:
            self._config.value_deserializer = deser

    def set_serializer(self, serializer: Serializer) -> None:
        """Set default serializer for values."""
        self._config.value_serializer = serializer

    def set_deserializer(self, deserializer: Deserializer) -> None:
        """Set default deserializer for values."""
        self._config.value_deserializer = deserializer

    def produce_object(
        self,
        topic: str,
        value: Any,
        serializer: Serializer | None = None,
    ) -> RecordMetadata:
        """
        Produce an object using a serializer.
        
        Args:
            topic: Topic name.
            value: Object to produce.
            serializer: Serializer to use. If None, uses default from config.
        """
        ser = serializer or self._config.value_serializer or BytesSerializer()
        data = ser.serialize(topic, value)
        return self.produce(topic, data)

    def consume_object(
        self,
        topic: str,
        offset: int,
        deserializer: Deserializer | None = None,
    ) -> Any:
        """
        Consume a message and decode it using a deserializer.
        
        Args:
            topic: Topic name.
            offset: Message offset.
            deserializer: Deserializer to use. If None, uses default from config.
        """
        data = self.consume(topic, offset)
        deser = deserializer or self._config.value_deserializer or BytesDeserializer()
        return deser.deserialize(topic, data)

    def produce(
        self,
        topic: str,
        data: bytes | str,
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
        compression_type: str = "none",
    ) -> RecordMetadata:
        """
        Produce a message to a topic and return RecordMetadata (Kafka-like).

        Args:
            topic: Target topic name.
            data: Message data (bytes or string).
            key: Optional message key for key-based partitioning.
                 When provided, messages with the same key go to the same partition.
            partition: Target partition (default: None for automatic selection).
                       If both key and partition are provided, partition takes precedence.
            compression_type: Compression algorithm to use ("gzip", "none").

        Returns:
            RecordMetadata with topic, partition, offset, timestamp, key_size, value_size.

        Example:
            >>> # Automatic partition selection
            >>> meta = client.produce("my-topic", b"Hello!")
            >>> print(f"Offset: {meta.offset}, Partition: {meta.partition}")
            >>> # Key-based partitioning (same key = same partition)
            >>> meta = client.produce("orders", b'{"id": 1}', key=b"user-123")
            >>> # Explicit partition selection
            >>> meta = client.produce("events", b"data", partition=2)
        """
        if isinstance(data, str):
            data = data.encode("utf-8")
        if isinstance(key, str):
            key = key.encode("utf-8")

        # Use binary protocol for maximum performance
        req = BinaryProduceRequest(
            topic=topic,
            key=key or b"",
            value=data,
            partition=partition if partition is not None else -1,
        )
        
        payload = encode_produce_request(req)
        flags = FLAG_BINARY

        if compression_type == "gzip":
            payload = gzip.compress(payload)
            flags |= FLAG_COMPRESSION_GZIP

        response_bytes = self._send_binary_request(OpCode.PRODUCE, payload, flags=flags)
        return decode_record_metadata(response_bytes)

    def produce_with_key(
        self,
        topic: str,
        key: bytes | str,
        data: bytes | str,
    ) -> RecordMetadata:
        """
        Produce a message with a key for key-based partitioning.

        Messages with the same key are guaranteed to go to the same partition,
        ensuring ordering for related messages (like Kafka's key-based partitioning).

        Args:
            topic: Target topic name.
            key: Message key (determines partition).
            data: Message data.

        Returns:
            RecordMetadata with topic, partition, offset, timestamp, key_size, value_size.

        Example:
            >>> # All messages for user-123 go to the same partition
            >>> meta = client.produce_with_key("orders", "user-123", b'{"order": 1}')
            >>> print(f"Offset: {meta.offset}")
        """
        return self.produce(topic, data, key=key)

    def produce_to_partition(
        self,
        topic: str,
        partition: int,
        data: bytes | str,
        *,
        key: bytes | str | None = None,
    ) -> RecordMetadata:
        """
        Produce a message to a specific partition.

        Use this when you need explicit control over partition assignment.

        Args:
            topic: Target topic name.
            partition: Target partition number.
            data: Message data.
            key: Optional message key (for tracking, not partition selection).

        Returns:
            RecordMetadata with topic, partition, offset, timestamp, key_size, value_size.

        Example:
            >>> # Send to partition 2 explicitly
            >>> meta = client.produce_to_partition("events", 2, b"data")
            >>> print(f"Offset: {meta.offset}")
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
        filter: str = "",
    ) -> FetchResult:
        """
        Fetch multiple messages from a topic.

        Args:
            topic: Topic to fetch from.
            partition: Partition to fetch from.
            offset: Starting offset.
            max_messages: Maximum messages to fetch.
            filter: Optional regex filter to apply server-side.

        Returns:
            FetchResult with messages and next offset.
        """
        # Use binary protocol for maximum performance
        req = BinaryFetchRequest(
            topic=topic,
            partition=partition,
            offset=offset,
            max_messages=max_messages,
            filter=filter,
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

    def produce_delayed(self, topic: str, data: bytes | str, delay_ms: int) -> RecordMetadata:
        """
        Produce a message with delayed delivery.

        Args:
            topic: Target topic.
            data: Message data.
            delay_ms: Delay in milliseconds.

        Returns:
            RecordMetadata with topic, partition, offset, timestamp, key_size, value_size.
        """
        if isinstance(data, str):
            data = data.encode("utf-8")

        req = BinaryProduceDelayedRequest(topic=topic, data=data, delay_ms=delay_ms)
        response_bytes = self._send_binary_request(
            OpCode.PRODUCE_DELAYED, encode_produce_delayed_request(req)
        )
        return decode_record_metadata(response_bytes)

    def produce_with_ttl(self, topic: str, data: bytes | str, ttl_ms: int) -> RecordMetadata:
        """
        Produce a message with time-to-live.

        Args:
            topic: Target topic.
            data: Message data.
            ttl_ms: TTL in milliseconds.

        Returns:
            RecordMetadata with topic, partition, offset, timestamp, key_size, value_size.
        """
        if isinstance(data, str):
            data = data.encode("utf-8")

        req = BinaryProduceWithTTLRequest(topic=topic, data=data, ttl_ms=ttl_ms)
        response_bytes = self._send_binary_request(
            OpCode.PRODUCE_WITH_TTL, encode_produce_with_ttl_request(req)
        )
        return decode_record_metadata(response_bytes)

    def produce_with_schema(self, topic: str, data: bytes | str, schema_name: str) -> RecordMetadata:
        """
        Produce a message with schema validation.

        Args:
            topic: Target topic.
            data: Message data.
            schema_name: Schema to validate against.

        Returns:
            RecordMetadata with topic, partition, offset, timestamp, key_size, value_size.
        """
        if isinstance(data, str):
            data = data.encode("utf-8")

        req = BinaryProduceWithSchemaRequest(topic=topic, data=data, schema_name=schema_name)
        response_bytes = self._send_binary_request(
            OpCode.PRODUCE_WITH_SCHEMA, encode_produce_with_schema_request(req)
        )
        return decode_record_metadata(response_bytes)

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

    def _produce_in_transaction(self, txn_id: str, topic: str, data: bytes) -> RecordMetadata:
        """Produce within a transaction (internal)."""
        req = BinaryTxnProduceRequest(txn_id=txn_id, topic=topic, data=data)
        response_bytes = self._send_binary_request(
            OpCode.PRODUCE_TX, encode_txn_produce_request(req)
        )
        return decode_record_metadata(response_bytes)

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

    def get_cluster_metadata(self, topic: str = "") -> ClusterMetadata:
        """
        Get cluster metadata with partition-to-node mappings.

        This enables smart routing where clients can route requests directly
        to partition leaders, improving throughput and reducing latency.

        Args:
            topic: Optional topic name. If empty, returns metadata for all topics.

        Returns:
            ClusterMetadata with partition leader information.

        Example:
            >>> metadata = client.get_cluster_metadata("my-topic")
            >>> for topic_info in metadata.topics:
            ...     for partition in topic_info.partitions:
            ...         print(f"Partition {partition.partition} -> {partition.leader_addr}")
        """
        from .types import PartitionInfo, TopicPartitionInfo

        req = BinaryClusterMetadataRequest(topic=topic)
        payload = encode_cluster_metadata_request(req)
        resp_payload = self._send_binary_request(OpCode.CLUSTER_METADATA, payload)
        resp = decode_cluster_metadata_response(resp_payload)

        # Convert to public types
        topics = []
        for t in resp.topics:
            partitions = [
                PartitionInfo(
                    partition=p.partition,
                    leader_id=p.leader_id,
                    leader_addr=p.leader_addr,
                    epoch=p.epoch,
                    state=p.state,
                    replicas=tuple(p.replicas),
                    isr=tuple(p.isr),
                )
                for p in t.partitions
            ]
            topics.append(TopicPartitionInfo(topic=t.topic, partitions=partitions))

        return ClusterMetadata(cluster_id=resp.cluster_id, topics=topics)

    # =========================================================================
    # High-Level Consumer API (Kafka-like)
    # =========================================================================

    def consumer(
        self,
        topics: str | list[str],
        group_id: str,
        *,
        auto_offset_reset: str = "latest",
        enable_auto_commit: bool = True,
        auto_commit_interval_ms: int = 5000,
        max_poll_records: int = 500,
    ) -> "HighLevelConsumer":
        """
        Create a high-level consumer similar to Kafka's KafkaConsumer.

        This consumer automatically handles:
        - Partition assignment (consumes from all partitions)
        - Offset tracking and commits
        - Automatic rebalancing

        Args:
            topics: Topic or list of topics to subscribe to.
            group_id: Consumer group ID for offset tracking.
            auto_offset_reset: Where to start if no committed offset exists.
                - "earliest": Start from the beginning
                - "latest": Start from the end (new messages only)
            enable_auto_commit: Automatically commit offsets periodically.
            auto_commit_interval_ms: Auto-commit interval in milliseconds.
            max_poll_records: Maximum records to return per poll.

        Returns:
            HighLevelConsumer instance.

        Example:
            >>> consumer = client.consumer("my-topic", "my-group")
            >>> for msg in consumer:
            ...     print(msg.value.decode())
            ...     # Offsets are auto-committed
            >>> consumer.close()

            # Or with context manager:
            >>> with client.consumer("my-topic", "my-group") as consumer:
            ...     for msg in consumer:
            ...         print(msg.value.decode())
        """
        from .consumer import ConsumerConfig

        topic_list = [topics] if isinstance(topics, str) else list(topics)
        config = ConsumerConfig(
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            auto_commit_interval_ms=auto_commit_interval_ms,
            max_poll_records=max_poll_records,
        )
        return HighLevelConsumer(self, topic_list, group_id, config)

    # =========================================================================
    # Audit Trail Operations
    # =========================================================================

    def query_audit_events(
        self,
        *,
        start_time: "datetime | None" = None,
        end_time: "datetime | None" = None,
        event_types: list[str] | None = None,
        user: str = "",
        resource: str = "",
        result: str = "",
        search: str = "",
        limit: int = 100,
        offset: int = 0,
    ) -> "AuditQueryResult":
        """
        Query audit events with optional filters.

        Args:
            start_time: Start of time range (inclusive).
            end_time: End of time range (inclusive).
            event_types: List of event types to filter by.
            user: Filter by username.
            resource: Filter by resource (topic, user, etc.).
            result: Filter by result (success, failure, denied).
            search: Full-text search query.
            limit: Maximum number of events to return.
            offset: Offset for pagination.

        Returns:
            AuditQueryResult with events, total count, and has_more flag.

        Raises:
            FlyMQError: If the query fails.
        """
        from .models import AuditEvent, AuditQueryResult

        # Build request payload
        payload = bytearray()

        # Start time (int64)
        start_ts = int(start_time.timestamp()) if start_time else 0
        payload.extend(struct.pack(">q", start_ts))

        # End time (int64)
        end_ts = int(end_time.timestamp()) if end_time else 0
        payload.extend(struct.pack(">q", end_ts))

        # Event types count and strings
        types = event_types or []
        payload.extend(struct.pack(">H", len(types)))
        for et in types:
            et_bytes = et.encode("utf-8")
            payload.extend(struct.pack(">H", len(et_bytes)))
            payload.extend(et_bytes)

        # User filter
        user_bytes = user.encode("utf-8")
        payload.extend(struct.pack(">H", len(user_bytes)))
        payload.extend(user_bytes)

        # Resource filter
        resource_bytes = resource.encode("utf-8")
        payload.extend(struct.pack(">H", len(resource_bytes)))
        payload.extend(resource_bytes)

        # Result filter
        result_bytes = result.encode("utf-8")
        payload.extend(struct.pack(">H", len(result_bytes)))
        payload.extend(result_bytes)

        # Search query
        search_bytes = search.encode("utf-8")
        payload.extend(struct.pack(">H", len(search_bytes)))
        payload.extend(search_bytes)

        # Limit and offset
        payload.extend(struct.pack(">I", limit))
        payload.extend(struct.pack(">I", offset))

        # Send request
        self._send_message(OpCode.AUDIT_QUERY, bytes(payload))

        # Read response
        op, resp_payload = self._read_message()
        if op == OpCode.ERROR:
            raise FlyMQError(resp_payload.decode("utf-8"))

        # Parse response
        pos = 0

        # Total count (int32)
        total_count = struct.unpack(">I", resp_payload[pos : pos + 4])[0]
        pos += 4

        # Has more (bool)
        has_more = resp_payload[pos] != 0
        pos += 1

        # Event count
        event_count = struct.unpack(">I", resp_payload[pos : pos + 4])[0]
        pos += 4

        events = []
        for _ in range(event_count):
            # ID
            id_len = struct.unpack(">H", resp_payload[pos : pos + 2])[0]
            pos += 2
            event_id = resp_payload[pos : pos + id_len].decode("utf-8")
            pos += id_len

            # Timestamp (int64 milliseconds)
            timestamp_ms = struct.unpack(">q", resp_payload[pos : pos + 8])[0]
            pos += 8
            timestamp = datetime.fromtimestamp(timestamp_ms / 1000.0)

            # Type
            type_len = struct.unpack(">H", resp_payload[pos : pos + 2])[0]
            pos += 2
            event_type = resp_payload[pos : pos + type_len].decode("utf-8")
            pos += type_len

            # User
            user_len = struct.unpack(">H", resp_payload[pos : pos + 2])[0]
            pos += 2
            event_user = resp_payload[pos : pos + user_len].decode("utf-8")
            pos += user_len

            # Client IP
            ip_len = struct.unpack(">H", resp_payload[pos : pos + 2])[0]
            pos += 2
            client_ip = resp_payload[pos : pos + ip_len].decode("utf-8")
            pos += ip_len

            # Resource
            res_len = struct.unpack(">H", resp_payload[pos : pos + 2])[0]
            pos += 2
            event_resource = resp_payload[pos : pos + res_len].decode("utf-8")
            pos += res_len

            # Action
            action_len = struct.unpack(">H", resp_payload[pos : pos + 2])[0]
            pos += 2
            action = resp_payload[pos : pos + action_len].decode("utf-8")
            pos += action_len

            # Result
            result_len = struct.unpack(">H", resp_payload[pos : pos + 2])[0]
            pos += 2
            event_result = resp_payload[pos : pos + result_len].decode("utf-8")
            pos += result_len

            # Details count
            details_count = struct.unpack(">H", resp_payload[pos : pos + 2])[0]
            pos += 2
            details = {}
            for _ in range(details_count):
                key_len = struct.unpack(">H", resp_payload[pos : pos + 2])[0]
                pos += 2
                key = resp_payload[pos : pos + key_len].decode("utf-8")
                pos += key_len
                val_len = struct.unpack(">H", resp_payload[pos : pos + 2])[0]
                pos += 2
                val = resp_payload[pos : pos + val_len].decode("utf-8")
                pos += val_len
                details[key] = val

            # Node ID
            node_len = struct.unpack(">H", resp_payload[pos : pos + 2])[0]
            pos += 2
            node_id = resp_payload[pos : pos + node_len].decode("utf-8")
            pos += node_len

            events.append(
                AuditEvent(
                    id=event_id,
                    timestamp=timestamp,
                    type=event_type,
                    user=event_user,
                    client_ip=client_ip,
                    resource=event_resource,
                    action=action,
                    result=event_result,
                    details=details,
                    node_id=node_id,
                )
            )

        return AuditQueryResult(
            events=events, total_count=total_count, has_more=has_more
        )

    def export_audit_events(
        self,
        format: str = "json",
        *,
        start_time: "datetime | None" = None,
        end_time: "datetime | None" = None,
        event_types: list[str] | None = None,
        user: str = "",
        resource: str = "",
        result: str = "",
        search: str = "",
    ) -> bytes:
        """
        Export audit events in the specified format.

        Args:
            format: Export format ("json" or "csv").
            start_time: Start of time range (inclusive).
            end_time: End of time range (inclusive).
            event_types: List of event types to filter by.
            user: Filter by username.
            resource: Filter by resource.
            result: Filter by result.
            search: Full-text search query.

        Returns:
            Exported data as bytes.

        Raises:
            FlyMQError: If the export fails.
        """
        # Build request payload
        payload = bytearray()

        # Format
        format_bytes = format.encode("utf-8")
        payload.extend(struct.pack(">H", len(format_bytes)))
        payload.extend(format_bytes)

        # Start time (int64)
        start_ts = int(start_time.timestamp()) if start_time else 0
        payload.extend(struct.pack(">q", start_ts))

        # End time (int64)
        end_ts = int(end_time.timestamp()) if end_time else 0
        payload.extend(struct.pack(">q", end_ts))

        # Event types count and strings
        types = event_types or []
        payload.extend(struct.pack(">H", len(types)))
        for et in types:
            et_bytes = et.encode("utf-8")
            payload.extend(struct.pack(">H", len(et_bytes)))
            payload.extend(et_bytes)

        # User filter
        user_bytes = user.encode("utf-8")
        payload.extend(struct.pack(">H", len(user_bytes)))
        payload.extend(user_bytes)

        # Resource filter
        resource_bytes = resource.encode("utf-8")
        payload.extend(struct.pack(">H", len(resource_bytes)))
        payload.extend(resource_bytes)

        # Result filter
        result_bytes = result.encode("utf-8")
        payload.extend(struct.pack(">H", len(result_bytes)))
        payload.extend(result_bytes)

        # Search query
        search_bytes = search.encode("utf-8")
        payload.extend(struct.pack(">H", len(search_bytes)))
        payload.extend(search_bytes)

        # Send request
        self._send_message(OpCode.AUDIT_EXPORT, bytes(payload))

        # Read response
        op, resp_payload = self._read_message()
        if op == OpCode.ERROR:
            raise FlyMQError(resp_payload.decode("utf-8"))

        # Response is just the raw data
        return resp_payload

    # =========================================================================
    # High-Level Producer API (Kafka-like)
    # =========================================================================

    def producer(
        self,
        *,
        batch_size: int = 16384,
        linger_ms: int = 0,
        max_batch_messages: int = 1000,
        acks: str = "leader",
        retries: int = 3,
        retry_backoff_ms: int = 100,
        compression_type: str = "none",
    ) -> "HighLevelProducer":
        """
        Create a high-level producer similar to Kafka's KafkaProducer.

        This producer provides:
        - Automatic batching for improved throughput
        - Configurable acknowledgment levels
        - Automatic retries with backoff
        - Async send with callbacks
        - Thread-safe operations

        Args:
            batch_size: Maximum batch size in bytes before sending.
            linger_ms: Time to wait for more messages before sending a batch.
                       Set to 0 for immediate send (no batching).
            max_batch_messages: Maximum number of messages per batch.
            acks: Acknowledgment level ("leader" or "all").
            retries: Number of retries on failure.
            retry_backoff_ms: Backoff time between retries.
            compression_type: Compression algorithm to use ("gzip", "none").

        Returns:
            HighLevelProducer instance.
        """
        return HighLevelProducer(
            self,
            batch_size=batch_size,
            linger_ms=linger_ms,
            max_batch_messages=max_batch_messages,
            acks=acks,
            retries=retries,
            retry_backoff_ms=retry_backoff_ms,
            compression_type=compression_type,
        )


class HighLevelProducer:
    """
    High-level producer with batching, callbacks, and retries.

    Similar to Kafka's KafkaProducer, this producer:
    - Batches messages for improved throughput
    - Supports async send with callbacks
    - Automatically retries on transient failures
    - Thread-safe for concurrent use
    """

    def __init__(
        self,
        client: FlyMQClient,
        *,
        batch_size: int = 16384,
        linger_ms: int = 0,
        max_batch_messages: int = 1000,
        acks: str = "leader",
        retries: int = 3,
        retry_backoff_ms: int = 100,
        compression_type: str = "none",
    ) -> None:
        """Initialize high-level producer."""
        self._client = client
        self._batch_size = batch_size
        self._linger_ms = linger_ms
        self._max_batch_messages = max_batch_messages
        self._acks = acks
        self._retries = retries
        self._retry_backoff_ms = retry_backoff_ms
        self._compression_type = compression_type
        self._closed = False
        self._lock = threading.Lock()

        # Batch state
        self._batch: list[dict[str, Any]] = []
        self._batch_bytes = 0
        self._last_flush = time.time()

        # Background flusher thread (if linger_ms > 0)
        self._flusher_thread: threading.Thread | None = None
        self._stop_flusher = threading.Event()

        if linger_ms > 0:
            self._start_flusher()

    def _start_flusher(self) -> None:
        """Start background flusher thread."""
        self._flusher_thread = threading.Thread(target=self._flusher_loop, daemon=True)
        self._flusher_thread.start()

    def _flusher_loop(self) -> None:
        """Background loop to flush batches based on linger_ms."""
        while not self._stop_flusher.is_set():
            self._stop_flusher.wait(timeout=self._linger_ms / 1000.0)
            if not self._stop_flusher.is_set():
                with self._lock:
                    if self._batch and (time.time() - self._last_flush) * 1000 >= self._linger_ms:
                        self._flush_batch()

    def send(
        self,
        topic: str,
        value: bytes | str,
        *,
        key: bytes | str | None = None,
        partition: int | None = None,
        on_success: Any = None,
        on_error: Any = None,
    ) -> "ProduceFuture":
        """
        Send a message to a topic.

        If linger_ms > 0, the message may be batched with others.
        Use flush() to ensure all messages are sent.

        Args:
            topic: Target topic name.
            value: Message value (bytes or string).
            key: Optional message key for partitioning.
            partition: Optional explicit partition.
            on_success: Callback on successful send: fn(RecordMetadata).
            on_error: Callback on error: fn(Exception).

        Returns:
            ProduceFuture that resolves when the message is sent.
        """
        if self._closed:
            raise RuntimeError("Producer is closed")

        if isinstance(value, str):
            value = value.encode("utf-8")
        if isinstance(key, str):
            key = key.encode("utf-8")

        future = ProduceFuture()
        record = {
            "topic": topic,
            "value": value,
            "key": key,
            "partition": partition,
            "future": future,
            "on_success": on_success,
            "on_error": on_error,
        }

        with self._lock:
            # If no batching, send immediately
            if self._linger_ms == 0:
                self._send_record(record)
                return future

            # Add to batch
            self._batch.append(record)
            self._batch_bytes += len(value)

            # Check if batch should be flushed
            if self._batch_bytes >= self._batch_size or len(self._batch) >= self._max_batch_messages:
                self._flush_batch()

        return future

    def _send_record(self, record: dict[str, Any]) -> None:
        """Send a single record with retries."""
        last_error = None

        for attempt in range(self._retries + 1):
            try:
                metadata = self._client.produce(
                    record["topic"],
                    record["value"],
                    key=record["key"],
                    partition=record["partition"],
                    compression_type=self._compression_type,
                )
                record["future"]._set_result(metadata)
                if record["on_success"]:
                    record["on_success"](metadata)
                return
            except Exception as e:
                last_error = e
                if attempt < self._retries:
                    time.sleep(self._retry_backoff_ms / 1000.0)

        # All retries failed
        record["future"]._set_error(last_error)
        if record["on_error"]:
            record["on_error"](last_error)

    def _flush_batch(self) -> None:
        """Flush the current batch."""
        if not self._batch:
            return

        for record in self._batch:
            self._send_record(record)

        self._batch.clear()
        self._batch_bytes = 0
        self._last_flush = time.time()

    def flush(self, timeout_ms: int = 30000) -> None:
        """
        Flush all pending messages.

        Blocks until all messages in the batch are sent.

        Args:
            timeout_ms: Maximum time to wait (currently unused).
        """
        with self._lock:
            self._flush_batch()

    def close(self, timeout_ms: int = 30000) -> None:
        """
        Close the producer.

        Flushes any pending messages before closing.

        Args:
            timeout_ms: Maximum time to wait for pending messages.
        """
        if self._closed:
            return

        self._stop_flusher.set()
        if self._flusher_thread:
            self._flusher_thread.join(timeout=timeout_ms / 1000.0)

        self.flush(timeout_ms)
        self._closed = True

    def __enter__(self) -> "HighLevelProducer":
        """Context manager entry."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        self.close()


class ProduceFuture:
    """
    Future representing a pending produce operation.

    Similar to Kafka's RecordMetadata future.
    """

    def __init__(self) -> None:
        """Initialize future."""
        self._event = threading.Event()
        self._result: "RecordMetadata | None" = None
        self._error: Exception | None = None

    def _set_result(self, result: "RecordMetadata") -> None:
        """Set successful result."""
        self._result = result
        self._event.set()

    def _set_error(self, error: Exception) -> None:
        """Set error result."""
        self._error = error
        self._event.set()

    def get(self, timeout_ms: int = 30000) -> "RecordMetadata":
        """
        Wait for the result.

        Args:
            timeout_ms: Maximum time to wait.

        Returns:
            RecordMetadata with topic, partition, and offset.

        Raises:
            Exception: If the send failed.
            TimeoutError: If timeout expires.
        """
        if not self._event.wait(timeout=timeout_ms / 1000.0):
            raise TimeoutError("Produce operation timed out")

        if self._error:
            raise self._error

        return self._result  # type: ignore

    def done(self) -> bool:
        """Check if the operation is complete."""
        return self._event.is_set()

    def succeeded(self) -> bool:
        """Check if the operation succeeded."""
        return self._event.is_set() and self._error is None


class HighLevelConsumer:
    """
    High-level consumer that abstracts away partitions and offsets.

    Similar to Kafka's KafkaConsumer, this consumer:
    - Automatically subscribes to all partitions of the specified topics
    - Tracks offsets per partition
    - Supports auto-commit or manual commit
    - Provides a simple iterator interface

    Example:
        >>> consumer = client.consumer("orders", "order-processor")
        >>> for message in consumer:
        ...     process(message.value)
        ...     consumer.commit()  # Manual commit (if auto-commit disabled)
    """

    def __init__(
        self,
        client: FlyMQClient,
        topics: list[str],
        group_id: str,
        config: Any,
    ) -> None:
        """Initialize high-level consumer."""
        self._client = client
        self._topics = topics
        self._group_id = group_id
        self._config = config
        self._closed = False
        self._lock = threading.Lock()

        # Track offsets per (topic, partition)
        self._offsets: dict[tuple[str, int], int] = {}
        self._partition_counts: dict[str, int] = {}

        # Initialize: get partition counts and subscribe
        self._initialize()

    def _initialize(self) -> None:
        """Initialize subscriptions for all topics and partitions."""
        from .protocol import SubscribeMode

        mode = (
            SubscribeMode.EARLIEST
            if self._config.auto_offset_reset == "earliest"
            else SubscribeMode.LATEST
        )

        for topic in self._topics:
            # Try to discover partition count by subscribing to partition 0 first
            # Then try higher partitions until we get an error
            num_partitions = 1

            # Subscribe to partition 0
            try:
                offset = self._client.subscribe(topic, self._group_id, 0, mode.value)
                self._offsets[(topic, 0)] = offset
            except Exception:
                continue  # Skip topic if can't subscribe

            # Try to discover more partitions (up to 64)
            for partition in range(1, 64):
                try:
                    offset = self._client.subscribe(topic, self._group_id, partition, mode.value)
                    self._offsets[(topic, partition)] = offset
                    num_partitions = partition + 1
                except Exception:
                    break  # No more partitions

            self._partition_counts[topic] = num_partitions

    def poll(self, timeout_ms: int = 1000, max_records: int | None = None) -> list[ConsumedMessage]:
        """
        Poll for messages from all subscribed topics and partitions.

        Args:
            timeout_ms: Maximum time to wait for messages (currently unused).
            max_records: Maximum records to return (default: from config).

        Returns:
            List of consumed messages from all partitions.
        """
        if self._closed:
            from .exceptions import ConsumerError
            raise ConsumerError("Consumer is closed")

        max_records = max_records or self._config.max_poll_records
        all_messages: list[ConsumedMessage] = []

        with self._lock:
            # Round-robin across all topic-partitions
            for topic in self._topics:
                num_partitions = self._partition_counts.get(topic, 1)
                per_partition = max(1, max_records // (len(self._topics) * num_partitions))

                for partition in range(num_partitions):
                    key = (topic, partition)
                    offset = self._offsets.get(key, 0)

                    try:
                        result = self._client.fetch(topic, partition, offset, per_partition)
                        if result.messages:
                            all_messages.extend(result.messages)
                            self._offsets[key] = result.next_offset
                    except Exception:
                        pass  # Skip partition on error

        return all_messages

    def commit(self) -> None:
        """
        Commit current offsets for all partitions.

        Call this after processing messages to save progress.
        """
        with self._lock:
            for (topic, partition), offset in self._offsets.items():
                try:
                    self._client.commit_offset(topic, self._group_id, partition, offset)
                except Exception:
                    pass  # Best effort commit

    def commit_async(self, callback: Any = None) -> None:
        """
        Commit offsets asynchronously.

        Args:
            callback: Optional callback function (currently synchronous).
        """
        # For now, just do synchronous commit
        self.commit()
        if callback:
            callback(None)

    def close(self) -> None:
        """Close the consumer and commit final offsets."""
        if not self._closed:
            self.commit()
            self._closed = True

    @property
    def topics(self) -> list[str]:
        """Get subscribed topics."""
        return self._topics

    @property
    def group_id(self) -> str:
        """Get consumer group ID."""
        return self._group_id

    def __iter__(self) -> Iterator[ConsumedMessage]:
        """Iterate over messages from all partitions."""
        import time
        last_commit = time.time()
        commit_interval = self._config.auto_commit_interval_ms / 1000.0

        while not self._closed:
            messages = self.poll()

            for msg in messages:
                yield msg

            # Auto-commit if enabled
            if self._config.enable_auto_commit:
                now = time.time()
                if now - last_commit >= commit_interval:
                    self.commit()
                    last_commit = now

            if not messages:
                time.sleep(0.1)  # Brief pause if no messages

    def __enter__(self) -> "HighLevelConsumer":
        """Context manager entry."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        self.close()


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

    def produce(self, topic: str, data: bytes | str) -> RecordMetadata:
        """
        Produce a message within the transaction.

        Args:
            topic: Target topic.
            data: Message data.

        Returns:
            RecordMetadata with topic, partition, offset, timestamp, key_size, value_size.
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


# =============================================================================
# Module-level convenience functions
# =============================================================================

def connect(
    servers: str | list[str] = "localhost:9092",
    *,
    username: str | None = None,
    password: str | None = None,
    tls: TLSConfig | None = None,
    tls_enabled: bool = False,
    tls_ca_file: str | None = None,
    tls_cert_file: str | None = None,
    tls_key_file: str | None = None,
    tls_server_name: str | None = None,
    tls_insecure_skip_verify: bool = False,
    **kwargs: Any,
) -> FlyMQClient:
    """
    Create and connect a FlyMQ client.

    This is the simplest way to connect to FlyMQ. The client will automatically
    connect on creation and can be used immediately.

    Args:
        servers: Server address(es). Can be:
            - Single server: "localhost:9092"
            - Multiple servers: "server1:9092,server2:9092"
            - List: ["server1:9092", "server2:9092"]
        username: Optional username for authentication.
        password: Optional password for authentication.
        tls: TLSConfig object for secure connections (recommended).
        tls_enabled: Enable TLS encryption (alternative to tls parameter).
        tls_ca_file: Path to CA certificate file for TLS verification.
        tls_cert_file: Path to client certificate file (for mutual TLS).
        tls_key_file: Path to client private key file (for mutual TLS).
        tls_server_name: Expected server name in certificate (for SNI).
        tls_insecure_skip_verify: Skip TLS certificate verification (not for production).
        **kwargs: Additional configuration options.

    Returns:
        Connected FlyMQClient instance.

    Raises:
        ConnectionError: If connection fails.
        AuthenticationError: If authentication fails.

    Examples:
        # Simple connection
        >>> client = connect()
        >>> client.produce("my-topic", b"Hello!")

        # With authentication
        >>> client = connect("localhost:9092", username="admin", password="secret")

        # With TLS (recommended approach)
        >>> from pyflymq import TLSConfig
        >>> tls = TLSConfig(enabled=True, ca_file="ca.crt")
        >>> client = connect("localhost:9093", tls=tls)

        # With TLS (alternative approach)
        >>> client = connect("localhost:9093", tls_enabled=True, tls_ca_file="ca.crt")

        # Insecure TLS (testing only)
        >>> client = connect("localhost:9093", tls_enabled=True, tls_insecure_skip_verify=True)

        # Mutual TLS
        >>> tls = TLSConfig(
        ...     enabled=True,
        ...     cert_file="client.crt",
        ...     key_file="client.key",
        ...     ca_file="ca.crt"
        ... )
        >>> client = connect("localhost:9093", tls=tls)

        # Multiple servers for high availability
        >>> client = connect(["server1:9092", "server2:9092", "server3:9092"])

    Note:
        Remember to call client.close() when done, or use the context manager:
        >>> with connect() as client:
        ...     client.produce("topic", b"message")
    """
    # Build TLS config if not provided
    if tls is None and (tls_enabled or tls_ca_file or tls_cert_file):
        tls = TLSConfig(
            enabled=tls_enabled,
            ca_file=tls_ca_file,
            cert_file=tls_cert_file,
            key_file=tls_key_file,
            server_name=tls_server_name,
            insecure_skip_verify=tls_insecure_skip_verify,
        )
    
    return FlyMQClient(
        servers,
        tls=tls,
        username=username,
        password=password,
        **kwargs,
    )
