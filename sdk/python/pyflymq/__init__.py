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

Quick Start:
    >>> from pyflymq import FlyMQClient
    >>>
    >>> # Connect to FlyMQ
    >>> client = FlyMQClient("localhost:9092")
    >>>
    >>> # Produce a message
    >>> offset = client.produce("my-topic", b"Hello, FlyMQ!")
    >>>
    >>> # Consume the message
    >>> data = client.consume("my-topic", offset)
    >>> print(data.decode())
    Hello, FlyMQ!
    >>>
    >>> client.close()

Consumer Groups:
    >>> from pyflymq import FlyMQClient, Consumer
    >>>
    >>> client = FlyMQClient("localhost:9092")
    >>> consumer = Consumer(client, "my-topic", group_id="my-group")
    >>>
    >>> for msg in consumer:
    ...     print(msg.decode())
    ...     consumer.commit()

Transactions:
    >>> with client.transaction() as txn:
    ...     txn.produce("topic1", b"message1")
    ...     txn.produce("topic2", b"message2")
    ...     # Auto-commits on success

TLS Connection:
    >>> client = FlyMQClient(
    ...     "localhost:9093",
    ...     tls_enabled=True,
    ...     tls_ca_file="/path/to/ca.crt"
    ... )
"""

from .client import FlyMQClient, Transaction
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

__version__ = "1.26.7"
__author__ = "Firefly Software Solutions Inc."
__license__ = "Apache-2.0"

__all__ = [
    # Client
    "FlyMQClient",
    "Transaction",
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
