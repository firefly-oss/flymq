# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0

"""
PyFlyMQ - Python Client SDK for FlyMQ Message Queue.

A high-performance client library for FlyMQ with support for:
- Automatic failover and reconnection
- TLS/SSL encryption
- Consumer groups
- Transactions
- Schema validation
- Dead letter queues
- Delayed messages and TTL

Quick Start (Simplest):
    >>> from pyflymq import connect
    >>>
    >>> # Connect and use immediately
    >>> client = connect("localhost:9092")
    >>> offset = client.produce("my-topic", b"Hello, FlyMQ!")
    >>> data = client.consume("my-topic", offset)
    >>> print(data.decode())
    Hello, FlyMQ!

Context Manager (Recommended for applications):
    >>> from pyflymq import connect
    >>>
    >>> with connect("localhost:9092") as client:
    ...     client.create_topic("my-topic")
    ...     offset = client.produce("my-topic", b"Hello!")
    ...     print(f"Produced at offset {offset}")
    # Connection auto-closes when exiting the block

Consumer Groups (Kafka-like, recommended):
    >>> from pyflymq import connect
    >>>
    >>> client = connect("localhost:9092")
    >>> # High-level consumer - no need to manage partitions or offsets!
    >>> consumer = client.consumer("my-topic", "my-group")
    >>>
    >>> for msg in consumer:
    ...     print(msg.value.decode())
    ...     # Offsets are auto-committed by default
    >>> consumer.close()

Consumer Groups (Low-level control):
    >>> from pyflymq import connect, Consumer
    >>>
    >>> client = connect("localhost:9092")
    >>> consumer = Consumer(client, "my-topic", partition=0, group_id="my-group")
    >>>
    >>> for msg in consumer:
    ...     print(msg.data.decode())
    ...     consumer.commit()  # Manual commit

Transactions:
    >>> with client.transaction() as txn:
    ...     txn.produce("topic1", b"message1")
    ...     txn.produce("topic2", b"message2")
    ...     # Auto-commits on success, auto-rollbacks on exception

TLS Connection:
    >>> client = connect(
    ...     "localhost:9093",
    ...     tls_enabled=True,
    ...     tls_ca_file="/path/to/ca.crt"
    ... )

Authentication:
    >>> client = connect(
    ...     "localhost:9092",
    ...     username="admin",
    ...     password="secret"
    ... )

High Availability (Multiple Servers):
    >>> client = connect(["server1:9092", "server2:9092", "server3:9092"])
    >>> # Client automatically fails over if a server goes down
"""

from .client import (
    FlyMQClient,
    HighLevelConsumer,
    HighLevelProducer,
    ProduceFuture,
    Transaction,
    connect,
)
from .tls import TLSConfig
from .binary import RecordMetadata
from .consumer import Consumer, ConsumerGroup
from .crypto import Encryptor, generate_key, validate_key
from .exceptions import (
    AuthenticationError,
    ClusterError,
    ConnectionClosedError,
    ConnectionError,
    ConnectionTimeoutError,
    ConsumerError,
    ConsumerGroupError,
    CryptoError,
    DecryptionError,
    EncryptionError,
    FlyMQError,
    InvalidKeyError,
    MessageTooLargeError,
    NoAvailableServerError,
    NotLeaderError,
    OffsetOutOfRangeError,
    ProducerError,
    SchemaError,
    SchemaNotFoundError,
    SchemaValidationError,
    ServerError,
    TopicAlreadyExistsError,
    TopicError,
    TopicNotFoundError,
    TransactionError,
    TransactionNotActiveError,
    TransactionTimeoutError,
)
from .models import (
    AckMode,
    AuditEvent,
    AuditEventType,
    AuditQueryFilter,
    AuditQueryResult,
    ClientConfig,
    ClusterInfo,
    Compatibility,
    ConsumedMessage,
    ConsumerConfig,
    ConsumerGroupInfo,
    DLQMessage,
    FetchResult,
    NodeInfo,
    PartitionInfo,
    ProducerConfig,
    ProduceResult,
    Response,
    SchemaInfo,
    SchemaType,
    SubscribeMode,
    TopicMetadata,
    TransactionInfo,
    TransactionState,
)
from .producer import AsyncProducer, Producer
from .protocol import OpCode
from .reactive import (
    AsyncReactiveConsumer,
    ReactiveConsumer,
    ReactiveProducer,
    from_consumer,
    to_producer,
)

__version__ = "1.26.11"
__author__ = "Firefly Software Solutions Inc."
__license__ = "Apache-2.0"

__all__ = [
    # Client
    "FlyMQClient",
    "HighLevelConsumer",
    "HighLevelProducer",
    "ProduceFuture",
    "RecordMetadata",  # Kafka-like metadata for produced records
    "Transaction",
    "connect",
    # TLS
    "TLSConfig",
    # Producer
    "Producer",
    "AsyncProducer",
    # Consumer
    "Consumer",
    "ConsumerGroup",
    # Reactive
    "ReactiveConsumer",
    "ReactiveProducer",
    "AsyncReactiveConsumer",
    "from_consumer",
    "to_producer",
    # Crypto
    "Encryptor",
    "generate_key",
    "validate_key",
    # Protocol
    "OpCode",
    "SubscribeMode",
    "AckMode",
    # Configuration
    "ClientConfig",
    "ProducerConfig",
    "ConsumerConfig",
    # Types (Pydantic models)
    "Response",
    "ProduceResult",
    "ConsumedMessage",
    "FetchResult",
    "TopicMetadata",
    "PartitionInfo",
    "ConsumerGroupInfo",
    "SchemaInfo",
    "SchemaType",
    "Compatibility",
    "DLQMessage",
    "ClusterInfo",
    "NodeInfo",
    "TransactionInfo",
    "TransactionState",
    # Audit Trail
    "AuditEvent",
    "AuditEventType",
    "AuditQueryFilter",
    "AuditQueryResult",
    # Exceptions
    "FlyMQError",
    "ConnectionError",
    "ConnectionClosedError",
    "ConnectionTimeoutError",
    "AuthenticationError",
    "ServerError",
    "TopicError",
    "TopicNotFoundError",
    "TopicAlreadyExistsError",
    "ProducerError",
    "MessageTooLargeError",
    "ConsumerError",
    "OffsetOutOfRangeError",
    "ConsumerGroupError",
    "TransactionError",
    "TransactionNotActiveError",
    "TransactionTimeoutError",
    "SchemaError",
    "SchemaNotFoundError",
    "SchemaValidationError",
    "ClusterError",
    "NotLeaderError",
    "NoAvailableServerError",
    "CryptoError",
    "EncryptionError",
    "DecryptionError",
    "InvalidKeyError",
]
