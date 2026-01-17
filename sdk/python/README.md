# PyFlyMQ - Python Client SDK for FlyMQ

A high-performance Python client library for FlyMQ message queue with Kafka-like APIs.

**Website:** [https://flymq.com](https://flymq.com)

---

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start) - Get running in 30 seconds
3. [Key Concepts](#key-concepts) - Understanding the fundamentals
4. [High-Level APIs](#high-level-apis) - Recommended for most applications
   - [Producer](#high-level-producer)
   - [Consumer](#high-level-consumer)
5. [Low-Level APIs](#low-level-apis) - For advanced control
   - [Direct Produce/Consume](#direct-produceconsume)
   - [Manual Offset Management](#manual-offset-management)
6. [Advanced Features](#advanced-features)
   - [Transactions](#transactions)
   - [Schema Validation](#schema-validation)
   - [Dead Letter Queues](#dead-letter-queues)
   - [Delayed Messages](#delayed-messages)
   - [Message TTL](#message-ttl)
7. [Configuration Reference](#configuration-reference)
8. [Error Handling](#error-handling)
9. [Development](#development)

---

## Features

| Category | Features |
|----------|----------|
| **Core** | Kafka-like APIs, High-level Producer/Consumer, Full protocol support |
| **Reliability** | Automatic retries, Failover, Consumer groups with offset tracking |
| **Security** | TLS/mTLS, AES-256-GCM encryption, Authentication |
| **Advanced** | Transactions, Schema validation, Dead letter queues, Delayed messages, TTL |
| **Developer Experience** | Async/await, Reactive streams (RxPY), Pydantic models, Helpful error hints |

---

## Installation

```bash
pip install pyflymq
```

Or install from source:

```bash
cd sdk/python
pip install -e .
```

**Requirements:** Python 3.10+

---

## Quick Start

Get up and running in 30 seconds:

```python
from pyflymq import connect

# Connect to FlyMQ
client = connect("localhost:9092")

# Produce a message
client.produce("my-topic", b"Hello, FlyMQ!")

# Consume messages with a consumer group (tracks your position automatically)
with client.consumer("my-topic", group_id="my-app") as consumer:
    for message in consumer:
        print(f"Received: {message.decode()}")
        break  # Exit after first message for demo

client.close()
```

That's it! The consumer group `"my-app"` tracks your position. If you restart, you'll continue from where you left off.

---

## Key Concepts

Before diving into the APIs, understand these core concepts:

### Topics and Partitions

A **topic** is a named stream of messages. Topics are divided into **partitions** for parallelism.

```
Topic: "orders"
├── Partition 0: [msg0] [msg1] [msg2] ...
├── Partition 1: [msg0] [msg1] ...
└── Partition 2: [msg0] [msg1] [msg2] [msg3] ...
```

### Message Keys

A **key** determines which partition a message goes to. Same key = same partition = guaranteed order.

```python
# All orders for user-123 go to the same partition (ordered)
client.produce("orders", b'{"id": 1}', key="user-123")
client.produce("orders", b'{"id": 2}', key="user-123")  # Same partition!
```

### Consumer Groups

A **consumer group** tracks your position in the topic. Key benefits:

- **Resume on restart**: If your app crashes, it continues from where it left off
- **Independent groups**: Multiple apps can each receive ALL messages
- **Load balancing**: Multiple consumers in the same group share partitions

```
Group "analytics" ──► Receives ALL messages (offset: 100)
Group "billing"   ──► Receives ALL messages (offset: 50, processing slower)
```

### High-Level vs Low-Level APIs

| API Level | When to Use | Features |
|-----------|-------------|----------|
| **High-Level** | Most applications | Auto-commit, batching, simple iteration |
| **Low-Level** | Advanced control | Manual offsets, direct partition access |

**Recommendation:** Start with High-Level APIs. They handle the complexity for you.

---

## High-Level APIs

These APIs are recommended for most applications. They handle offset tracking, batching, and error recovery automatically.

### High-Level Producer

The high-level producer provides batching, callbacks, and automatic retries.

#### Basic Usage

```python
from pyflymq import connect

client = connect("localhost:9092")

# Create a producer with batching
with client.producer(batch_size=100, linger_ms=10) as producer:
    # Send messages (batched automatically)
    producer.send("events", b'{"type": "click"}')
    producer.send("events", b'{"type": "view"}')

    # With a key (ensures ordering for same key)
    producer.send("orders", b'{"id": 1}', key="user-123")

# Producer auto-flushes on exit
```

#### With Callbacks

```python
def on_success(metadata):
    print(f"Sent to {metadata.topic}[{metadata.partition}] @ offset {metadata.offset}")

def on_error(error):
    print(f"Failed: {error}")

with client.producer() as producer:
    future = producer.send(
        topic="events",
        value=b'{"event": "purchase"}',
        key="user-123",
        on_success=on_success,
        on_error=on_error
    )

    # Or wait for result synchronously
    metadata = future.get(timeout_ms=5000)
```

### High-Level Consumer

The high-level consumer provides automatic offset tracking and simple iteration.

#### Basic Usage

```python
from pyflymq import connect

client = connect("localhost:9092")

# Create consumer with a group ID (tracks your position automatically)
with client.consumer("my-topic", group_id="my-app") as consumer:
    for message in consumer:
        print(f"Received: {message.decode()}")
        print(f"Key: {message.key}, Offset: {message.offset}")
        # Offset is committed automatically every 5 seconds
```

#### Starting Position

When a consumer group first connects (no committed offset):

```python
# Start from the beginning (process all historical messages)
consumer = client.consumer("my-topic", group_id="my-app", auto_offset_reset="earliest")

# Start from the end (only new messages) - this is the default
consumer = client.consumer("my-topic", group_id="my-app", auto_offset_reset="latest")
```

After the first run, the consumer always resumes from its committed offset.

#### Manual Commit (Exactly-Once Processing)

For critical workloads, disable auto-commit and commit after successful processing:

```python
consumer = client.consumer(
    topics=["orders"],
    group_id="payment-processor",
    auto_commit=False  # Disable auto-commit
)

for message in consumer:
    try:
        process_payment(message)
        consumer.commit()  # Only commit after successful processing
    except Exception as e:
        # Message will be reprocessed on next run
        log_error(e)
```

#### Poll-Based Consumption

```python
consumer = client.consumer(
    topics=["topic1", "topic2"],
    group_id="my-group",
    auto_commit=True,
    auto_commit_interval_ms=5000
)

# Poll for messages with timeout
while True:
    messages = consumer.poll(timeout_ms=1000, max_records=100)
    for msg in messages:
        process(msg)
```

#### Seek Operations

```python
consumer = client.consumer("my-topic", "my-group")

# Seek to specific offset
consumer.seek(partition=0, offset=100)

# Seek to beginning/end
consumer.seek_to_beginning()
consumer.seek_to_end()

# Get current position
position = consumer.position(partition=0)
```

---

## Low-Level APIs

Use low-level APIs when you need fine-grained control over partitions and offsets.

### Direct Produce/Consume

The low-level API gives you direct access to partitions and offsets:

```python
from pyflymq import FlyMQClient

client = FlyMQClient("localhost:9092")

# Create a topic with specific partition count
client.create_topic("my-topic", partitions=3)

# Produce to a specific partition
offset = client.produce("my-topic", b"Hello!", partition=1)
print(f"Produced at offset {offset}")

# Consume from a specific partition and offset
message = client.consume("my-topic", partition=1, offset=0)
print(f"Message: {message.decode()}")

# Fetch multiple messages
result = client.fetch("my-topic", partition=0, offset=0, max_messages=100)
for msg in result.messages:
    print(f"Offset {msg.offset}: {msg.decode()}")

client.close()
```

### Manual Offset Management

For complete control over offset tracking:

```python
from pyflymq import FlyMQClient

client = FlyMQClient("localhost:9092")

# Subscribe to a topic with a consumer group
# Returns the starting offset (committed or based on reset policy)
offset = client.subscribe("my-topic", group_id="my-group", partition=0)

# Fetch messages starting from the offset
result = client.fetch("my-topic", partition=0, offset=offset, max_messages=10)

for msg in result.messages:
    # Process the message
    process(msg)

    # Commit the offset after processing
    # The offset to commit is the NEXT message to read
    client.commit_offset("my-topic", "my-group", partition=0, offset=msg.offset + 1)

client.close()
```

### Cluster Connection (High Availability)

```python
from pyflymq import FlyMQClient

# Connect to multiple servers for automatic failover
client = FlyMQClient("node1:9092,node2:9092,node3:9092")

# If node1 fails, automatically connects to node2 or node3
offset = client.produce("my-topic", b"Hello!")
```

---

## Advanced Features

### Transactions

Transactions ensure atomic writes across multiple topics:

```python
from pyflymq import FlyMQClient

client = FlyMQClient("localhost:9092")

# Using context manager (auto-commit/rollback)
with client.transaction() as txn:
    txn.produce("orders", b'{"id": 1}')
    txn.produce("inventory", b'{"sku": "ABC", "delta": -1}')
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

### Schema Validation

Validate messages against JSON, Avro, or Protobuf schemas:

```python
from pyflymq import FlyMQClient
import json

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
data = json.dumps({"name": "Alice", "age": 30}).encode()
offset = client.produce_with_schema("users", data, "user-schema")

# Invalid data will raise SchemaValidationError
```

### Dead Letter Queues

Handle failed messages with dead letter queues:

```python
from pyflymq import FlyMQClient

client = FlyMQClient("localhost:9092")

# Fetch messages from DLQ
dlq_messages = client.fetch_dlq("my-topic", max_messages=10)
for msg in dlq_messages:
    print(f"Failed message: {msg.id}, error: {msg.error}")

    # Replay the message back to the original topic
    client.replay_dlq("my-topic", msg.id)

# Purge all DLQ messages
client.purge_dlq("my-topic")
```

### Delayed Messages

Schedule messages for future delivery:

```python
from pyflymq import FlyMQClient

client = FlyMQClient("localhost:9092")

# Delayed delivery (5 second delay)
client.produce_delayed("my-topic", b"Delayed message", delay_ms=5000)
```

### Message TTL

Set expiration time for messages:

```python
from pyflymq import FlyMQClient

client = FlyMQClient("localhost:9092")

# Message expires in 60 seconds
client.produce_with_ttl("my-topic", b"Expiring message", ttl_ms=60000)
```

### Topic Filtering (MQTT-style)

FlyMQ supports powerful MQTT-style wildcards for topic subscriptions:

- `+`: Matches exactly one topic level.
- `#`: Matches zero or more topic levels at the end.

```python
# Subscribe to multiple topics using patterns
with client.consumer(["sensors/+/temp", "logs/#"], group_id="monitor") as consumer:
    for msg in consumer:
        print(f"Topic: {msg.topic}, Data: {msg.decode()}")
```

### Server-Side Message Filtering

Reduce bandwidth by filtering messages on the server before they are sent to the client:

```python
# Only receive messages containing "ERROR" (regex supported)
with client.consumer("app-logs", group_id="error-mon", filter="ERROR") as consumer:
    for msg in consumer:
        process_error(msg)
```

### Plug-and-Play SerDe System

Seamlessly handle structured data with built-in or custom Serializers/Deserializers:

```python
from pyflymq.serde import JSONSerializer, JSONDeserializer

# Set global SerDe for the client
client.set_serde("json")

# Produce dictionary directly
client.produce("users", {"id": 1, "name": "Alice"})

# Consumer will automatically decode based on the set SerDe
with client.consumer("users", group_id="user-processor") as consumer:
    for msg in consumer:
        user = msg.decode() # returns dict
        print(f"User: {user['name']}")
```

### Encryption (AES-256-GCM)

The SDK supports AES-256-GCM encryption:

```python
from pyflymq import Encryptor, generate_key, ClientConfig

# Generate a new encryption key
key = generate_key()  # 64-char hex string

# Create encryptor for manual encryption
encryptor = Encryptor.from_hex_key(key)
encrypted = encryptor.encrypt(b"Hello, FlyMQ!")
decrypted = encryptor.decrypt(encrypted)

# Or configure client-level encryption
config = ClientConfig(
    bootstrap_servers="localhost:9092",
    encryption_enabled=True,
    encryption_key=key
)
```

### TLS Configuration

```python
from pyflymq import FlyMQClient, ClientConfig

# Basic TLS (server verification)
client = FlyMQClient(
    "localhost:9093",
    tls_enabled=True,
    tls_ca_file="/path/to/ca.crt"
)

# Mutual TLS (client certificate authentication)
config = ClientConfig(
    bootstrap_servers="localhost:9093",
    tls_enabled=True,
    tls_ca_file="/path/to/ca.crt",
    tls_cert_file="/path/to/client.crt",
    tls_key_file="/path/to/client.key"
)
client = FlyMQClient(config=config)
```

### Reactive Streams (RxPY)

For reactive programming patterns:

```python
from pyflymq import FlyMQClient, ReactiveConsumer
from reactivex import operators as ops

client = FlyMQClient("localhost:9092")

consumer = ReactiveConsumer(client, "my-topic", "my-group")
consumer.messages().pipe(
    ops.filter(lambda m: b"important" in m.data),
    ops.map(lambda m: m.decode()),
    ops.buffer_with_count(10),
).subscribe(on_next=process_batch)

consumer.start()
```

### Async/Await Support

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

---

## Configuration Reference

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

---

## Error Handling

PyFlyMQ provides clear error messages with helpful hints.

### Exception Types

```python
from pyflymq.exceptions import (
    FlyMQError,              # Base exception
    ConnectionError,         # Connection failures
    TopicNotFoundError,      # Topic doesn't exist
    AuthenticationError,     # Auth failures
    TimeoutError,            # Operation timeout
    OffsetOutOfRangeError,   # Invalid offset
    ServerError,             # Server-side errors
)
```

### Errors with Hints

All exceptions include helpful hints:

```python
from pyflymq import connect
from pyflymq.exceptions import TopicNotFoundError

try:
    client = connect("localhost:9092")
    client.produce("nonexistent-topic", b"data")
except TopicNotFoundError as e:
    print(f"Error: {e}")
    print(f"Hint: {e.hint}")
    # Hint: Create the topic first with client.create_topic("nonexistent-topic", partitions)
```

### Retry Pattern

```python
from pyflymq import connect
from pyflymq.exceptions import FlyMQError
import time

def produce_with_retry(client, topic, data, max_retries=3):
    """Produce with exponential backoff retry."""
    backoff = 0.1
    for attempt in range(max_retries):
        try:
            return client.produce(topic, data)
        except FlyMQError as e:
            if attempt < max_retries - 1:
                print(f"Retry {attempt + 1}: {e}")
                time.sleep(backoff)
                backoff *= 2
            else:
                raise

client = connect("localhost:9092")
offset = produce_with_retry(client, "my-topic", b"important data")
```

---

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

---

## License

Copyright (c) 2026 Firefly Software Solutions Inc.

Licensed under the Apache License, Version 2.0.
