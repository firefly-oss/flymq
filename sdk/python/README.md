# PyFlyMQ - Python Client SDK for FlyMQ

A high-performance Python client library for FlyMQ message queue.

**Website:** [https://getfirefly.io](https://getfirefly.io)

## Features

- **Full Protocol Support** - Implements the complete FlyMQ binary protocol
- **AES-256-GCM Encryption** - Data-in-motion and data-at-rest encryption
- **Pydantic Models** - Validated, serializable data models
- **Reactive Streams (RxPY)** - Reactive programming patterns with backpressure
- **Automatic Failover** - Connects to multiple bootstrap servers with automatic failover
- **TLS/SSL Support** - Secure connections with certificate verification
- **Consumer Groups** - Coordinated consumption with offset management
- **Transactions** - Exactly-once semantics with atomic operations
- **Schema Validation** - JSON, Avro, and Protobuf schema support
- **Dead Letter Queues** - Failed message handling
- **Delayed Messages** - Scheduled message delivery
- **Message TTL** - Time-based message expiration
- **Async/Await Support** - Full asyncio integration
- **Thread-Safe** - Safe for concurrent use

## Installation

```bash
pip install pyflymq
```

Or install from source:

```bash
cd sdk/python
pip install -e .
```

## Quick Start

### Basic Usage

```python
from pyflymq import FlyMQClient

# Connect to FlyMQ
client = FlyMQClient("localhost:9092")

# Create a topic
client.create_topic("my-topic", partitions=3)

# Produce a message
offset = client.produce("my-topic", b"Hello, FlyMQ!")
print(f"Message written at offset {offset}")

# Consume the message
msg = client.consume("my-topic", offset)
print(f"Received: {msg.data.decode()}")

# Close the client
client.close()
```

### Key-Based Messaging (Kafka-style)

Messages with the same key are guaranteed to go to the same partition,
ensuring ordering for related messages.

```python
from pyflymq import FlyMQClient

with FlyMQClient("localhost:9092") as client:
    # Produce messages with keys
    client.produce("orders", b'{"id": 1}', key="user-123")
    client.produce("orders", b'{"id": 2}', key="user-123")  # Same partition
    client.produce("orders", b'{"id": 3}', key="user-456")  # May differ

    # Consume and access key
    msg = client.consume("orders", 0)
    print(f"Key: {msg.decode_key()}")  # "user-123"
    print(f"Value: {msg.decode()}")    # '{"id": 1}'

    # Fetch multiple messages with keys
    result = client.fetch("orders", partition=0, offset=0, max_messages=10)
    for m in result.messages:
        print(f"Offset {m.offset}: key={m.decode_key()} value={m.decode()}")
```

### Using Context Manager

```python
from pyflymq import FlyMQClient

with FlyMQClient("localhost:9092") as client:
    client.produce("my-topic", b"Hello!")
    topics = client.list_topics()
    print(f"Topics: {topics}")
```

### Cluster Connection (HA)

```python
from pyflymq import FlyMQClient

# Connect to multiple servers for high availability
client = FlyMQClient("node1:9092,node2:9092,node3:9092")

# Automatic failover if a server becomes unavailable
offset = client.produce("my-topic", b"Hello!")
```

## Consumer Groups

```python
from pyflymq import FlyMQClient, Consumer

client = FlyMQClient("localhost:9092")

# Create a consumer with a group ID
consumer = Consumer(client, "my-topic", group_id="my-group")

# Poll for messages
for message in consumer:
    print(f"Received: {message.decode()}")
    consumer.commit()  # Commit offset
```

### Consumer Group with Handler

```python
from pyflymq import FlyMQClient, ConsumerGroup, ConsumedMessage

def process_message(msg: ConsumedMessage) -> None:
    print(f"Processing: {msg.decode()}")

client = FlyMQClient("localhost:9092")

# Consumer group with automatic offset commits
group = ConsumerGroup(
    client,
    topics=["topic1", "topic2"],
    group_id="my-group",
    handler=process_message
)

group.start()
# ... run until shutdown
group.stop()
```

## Transactions

```python
from pyflymq import FlyMQClient

client = FlyMQClient("localhost:9092")

# Using context manager (auto-commit/rollback)
with client.transaction() as txn:
    txn.produce("topic1", b"Message 1")
    txn.produce("topic2", b"Message 2")
    # Automatically commits on success, rolls back on exception

# Manual transaction control
txn = client.begin_transaction()
try:
    txn.produce("topic1", b"Message 1")
    txn.produce("topic2", b"Message 2")
    txn.commit()
except Exception:
    txn.rollback()
    raise
```

## Producer with Batching

```python
from pyflymq import FlyMQClient, Producer, ProducerConfig

client = FlyMQClient("localhost:9092")

# Configure batching
config = ProducerConfig(
    batch_size=16384,      # 16KB batch
    linger_ms=100,         # Wait up to 100ms for more messages
    max_batch_messages=100
)

producer = Producer(client, config=config)

# Send messages (may be batched)
producer.send("my-topic", b"Message 1")
producer.send("my-topic", b"Message 2")

# Ensure all messages are sent
producer.flush()
producer.close()
```

## Schema Validation

```python
from pyflymq import FlyMQClient

client = FlyMQClient("localhost:9092")

# Register a JSON schema
schema = '''
{
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "age": {"type": "integer"}
    },
    "required": ["name"]
}
'''
client.register_schema("user-schema", "json", schema)

# Produce with schema validation
import json
data = json.dumps({"name": "Alice", "age": 30}).encode()
offset = client.produce_with_schema("users", data, "user-schema")
```

## Dead Letter Queue

```python
from pyflymq import FlyMQClient

client = FlyMQClient("localhost:9092")

# Fetch messages from DLQ
dlq_messages = client.fetch_dlq("my-topic", max_messages=10)
for msg in dlq_messages:
    print(f"Failed message: {msg.id}, error: {msg.error}")
    
    # Replay the message
    client.replay_dlq("my-topic", msg.id)

# Purge all DLQ messages
client.purge_dlq("my-topic")
```

## Delayed Messages and TTL

```python
from pyflymq import FlyMQClient

client = FlyMQClient("localhost:9092")

# Delayed delivery (5 second delay)
client.produce_delayed("my-topic", b"Delayed message", delay_ms=5000)

# Message with TTL (expires in 60 seconds)
client.produce_with_ttl("my-topic", b"Expiring message", ttl_ms=60000)
```

## Encryption (AES-256-GCM)

The SDK supports AES-256-GCM encryption for both data-in-motion and data-at-rest.

```python
from pyflymq import Encryptor, generate_key

# Generate a new encryption key
key = generate_key()  # 64-char hex string

# Create encryptor
encryptor = Encryptor.from_hex_key(key)

# Encrypt/decrypt data
encrypted = encryptor.encrypt(b"Hello, FlyMQ!")
decrypted = encryptor.decrypt(encrypted)

# Use with client config
from pyflymq import ClientConfig

config = ClientConfig(
    bootstrap_servers="localhost:9092",
    encryption_enabled=True,
    encryption_key=key
)
```

## Reactive Streams (RxPY)

The SDK provides reactive programming patterns using RxPY.

```python
from pyflymq import FlyMQClient, ReactiveConsumer, ReactiveProducer
from reactivex import operators as ops

client = FlyMQClient("localhost:9092")

# Reactive consumer
consumer = ReactiveConsumer(client, "my-topic", "my-group")
consumer.messages().pipe(
    ops.filter(lambda m: b"important" in m.data),
    ops.map(lambda m: m.decode()),
    ops.buffer_with_count(10),
).subscribe(on_next=process_batch)

consumer.start()

# Reactive producer
producer = ReactiveProducer(client, "my-topic")
import reactivex as rx

source = rx.of(b"msg1", b"msg2", b"msg3")
source.pipe(
    producer.publish()
).subscribe(on_next=lambda r: print(f"Published at {r.offset}"))
```

### Async Consumer

```python
import asyncio
from pyflymq import FlyMQClient, AsyncReactiveConsumer

client = FlyMQClient("localhost:9092")

async def consume():
    async with AsyncReactiveConsumer(client, "topic", "group") as consumer:
        async for message in consumer:
            print(message.decode())

asyncio.run(consume())
```

## TLS Configuration

### Basic TLS (Server Verification)

```python
from pyflymq import FlyMQClient

# Connect with TLS and verify server certificate
client = FlyMQClient(
    "localhost:9093",
    tls_enabled=True,
    tls_ca_file="/path/to/ca.crt"  # CA certificate for server verification
)

# Use context manager for automatic cleanup
with FlyMQClient("localhost:9093", tls_enabled=True, tls_ca_file="/path/to/ca.crt") as client:
    client.produce("my-topic", b"Hello, TLS!")
```

### Mutual TLS (mTLS - Client Certificate Authentication)

```python
from pyflymq import FlyMQClient, ClientConfig

# mTLS with client certificate authentication
config = ClientConfig(
    bootstrap_servers="localhost:9093",
    tls_enabled=True,
    tls_ca_file="/path/to/ca.crt",       # CA certificate
    tls_cert_file="/path/to/client.crt", # Client certificate
    tls_key_file="/path/to/client.key"   # Client private key
)
client = FlyMQClient(config=config)
```

### Skip Certificate Verification (Testing Only)

```python
# WARNING: Only use for testing - disables all certificate verification
client = FlyMQClient(
    "localhost:9093",
    tls_enabled=True,
    tls_insecure_skip_verify=True  # Insecure - testing only!
)
```

### TLS Security Notes

- **Minimum TLS Version**: The client uses Python's `ssl.create_default_context()` which enforces TLS 1.2+
- **Certificate Verification**: Enabled by default when `tls_ca_file` is provided
- **Cipher Suites**: Uses Python's default secure cipher suite selection

## Configuration Options

### ClientConfig

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bootstrap_servers` | str/list | localhost:9092 | Server addresses |
| `connect_timeout_ms` | int | 10000 | Connection timeout |
| `request_timeout_ms` | int | 30000 | Request timeout |
| `max_retries` | int | 3 | Max retry attempts |
| `retry_delay_ms` | int | 1000 | Delay between retries |
| `tls_enabled` | bool | False | Enable TLS |
| `tls_ca_file` | str | None | CA certificate path |
| `tls_cert_file` | str | None | Client certificate path (mTLS) |
| `tls_key_file` | str | None | Client key path (mTLS) |
| `tls_insecure_skip_verify` | bool | False | Skip cert verification (testing only) |

### ProducerConfig

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `batch_size` | int | 16384 | Batch size in bytes |
| `linger_ms` | int | 0 | Time to wait for batching |
| `max_batch_messages` | int | 1000 | Max messages per batch |
| `acks` | str | "all" | Acknowledgment level |
| `retries` | int | 3 | Max retries |

### ConsumerConfig

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `group_id` | str | None | Consumer group ID |
| `auto_offset_reset` | str | "latest" | Where to start consuming |
| `enable_auto_commit` | bool | True | Auto-commit offsets |
| `auto_commit_interval_ms` | int | 5000 | Auto-commit interval |
| `max_poll_records` | int | 500 | Max records per poll |

## Development

### Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run unit tests
pytest tests/test_protocol.py -v

# Run integration tests (requires running FlyMQ server)
pytest tests/test_integration.py -v

# Run all tests with coverage
pytest --cov=pyflymq
```

### Type Checking

```bash
mypy pyflymq
```

### Linting

```bash
ruff check pyflymq
```

## License

Copyright (c) 2026 Firefly Software Solutions Inc.

Licensed under the Apache License, Version 2.0.
