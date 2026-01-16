# Producer Patterns - PyFlyMQ

Best practices and patterns for producing messages with PyFlyMQ.

## Table of Contents

- [Quick Start](#quick-start)
- [High-Level Producer](#high-level-producer)
- [Basic Production](#basic-production)
- [Key-Based Routing](#key-based-routing)
- [Batch Production](#batch-production)
- [Transactions](#transactions)
- [Advanced Patterns](#advanced-patterns)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)

## Quick Start

The fastest way to produce messages:

```python
from pyflymq import connect

client = connect("localhost:9092")
offset = client.produce("my-topic", b"Hello, FlyMQ!")
print(f"Produced at offset: {offset}")
client.close()
```

## High-Level Producer

For production workloads, use the `HighLevelProducer` with batching and callbacks:

```python
from pyflymq import connect

client = connect("localhost:9092")

# Create producer with batching
with client.producer(batch_size=100, linger_ms=10) as producer:
    # Send with callbacks
    def on_success(metadata):
        print(f"✓ Sent to {metadata.topic}[{metadata.partition}] @ {metadata.offset}")

    def on_error(error):
        print(f"✗ Failed: {error}")

    # Async send
    future = producer.send(
        topic="events",
        value=b'{"event": "click", "user": "123"}',
        key="user-123",
        on_success=on_success,
        on_error=on_error
    )

    # Wait for result if needed
    metadata = future.get(timeout_ms=5000)
    print(f"Offset: {metadata.offset}")
```

### Producer Configuration

```python
producer = client.producer(
    batch_size=1000,        # Max messages per batch
    linger_ms=50,           # Wait time for batching (ms)
    max_retries=3,          # Retry attempts on failure
    retry_backoff_ms=100    # Backoff between retries
)
```

### High-Throughput Pattern

```python
from pyflymq import connect

client = connect("localhost:9092")

with client.producer(batch_size=1000, linger_ms=50) as producer:
    # Send 100k messages efficiently
    for i in range(100000):
        producer.send("events", f"event-{i}".encode())

    # Flush ensures all messages are sent
    producer.flush()
    print("All messages sent!")
```

### Fire-and-Forget vs Wait

```python
# Fire-and-forget (fastest)
producer.send("topic", b"data")

# Wait for acknowledgment
future = producer.send("topic", b"data")
metadata = future.get(timeout_ms=5000)  # Blocks until sent

# Check if sent without blocking
if future.done():
    if future.succeeded():
        print(f"Sent at offset {future.get().offset}")
    else:
        print(f"Failed: {future.error}")
```

## Basic Production

### Simple Produce (Low-Level)

```python
from pyflymq import FlyMQClient

client = FlyMQClient("localhost:9092")
offset = client.produce("my-topic", b"Hello, FlyMQ!")
print(f"Produced at offset: {offset}")
client.close()
```

### Using Context Manager

```python
from pyflymq import FlyMQClient

with FlyMQClient("localhost:9092") as client:
    offset = client.produce("my-topic", b"Hello!")
```

## Key-Based Routing

Messages with the same key go to the same partition:

```python
from pyflymq import connect

client = connect("localhost:9092")

# All orders for user-123 go to same partition (ordering guaranteed)
with client.producer() as producer:
    producer.send("orders", b"Order #1", key="user-123")
    producer.send("orders", b"Order #2", key="user-123")  # Same partition
    producer.send("orders", b"Order #3", key="user-456")  # Different partition
```

## Batch Production

### Using Transactions for Atomicity

```python
with client.transaction() as txn:
    for item in items:
        txn.produce("batch-topic", item.encode())
    # All messages committed atomically
```

### High-Throughput with HighLevelProducer

```python
from pyflymq import connect

client = connect("localhost:9092")

with client.producer(batch_size=1000, linger_ms=100) as producer:
    for i in range(10000):
        producer.send("high-volume-topic", f"Message {i}".encode())

    producer.flush()  # Ensure all messages sent
```

## Transactions

### Multi-Topic Atomic Writes

```python
with client.transaction() as txn:
    txn.produce("orders", order_data)
    txn.produce("payments", payment_data)
    txn.produce("notifications", notification_data)
    # All or nothing - atomic across topics
```

### Manual Transaction Control

```python
txn = client.begin_transaction()
try:
    txn.produce("topic1", b"message1")
    txn.produce("topic2", b"message2")
    txn.commit()
except Exception as e:
    txn.rollback()
    raise
```

## Advanced Patterns

### TTL Messages

```python
# Message expires after 30 seconds
client.produce_with_ttl("expiring-topic", b"Temporary data", ttl_ms=30000)
```

### Delayed Messages

```python
# Message visible after 5 seconds
client.produce_delayed("scheduled-topic", b"Delayed message", delay_ms=5000)
```

### Schema Validation

```python
# Register schema first
client.register_schema("user-schema", "json", user_schema)

# Produce with validation
client.produce_with_schema("users", user_json, schema_name="user-schema")
```

## Error Handling

### Retry with Exponential Backoff

```python
import time

def produce_with_retry(client, topic, data, max_retries=3):
    backoff = 0.1
    for attempt in range(max_retries):
        try:
            return client.produce(topic, data)
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(backoff)
                backoff *= 2
            else:
                raise
```

### Handling Specific Errors

```python
from pyflymq import (
    TopicNotFoundError,
    MessageTooLargeError,
    ConnectionError
)

try:
    client.produce("topic", data)
except TopicNotFoundError:
    client.create_topic("topic", partitions=1)
    client.produce("topic", data)
except MessageTooLargeError:
    # Split message or compress
    pass
except ConnectionError:
    # Reconnect logic
    pass
```

## Best Practices

### ✅ DO

- Reuse client instances across operations
- Use transactions for multi-topic atomicity
- Implement retry logic with backoff
- Close clients properly (use context managers)
- Use keys for ordering guarantees
- Monitor producer metrics

### ❌ DON'T

- Create new client for each message
- Ignore production errors
- Send very large messages (>32MB)
- Block indefinitely without timeouts
- Hardcode connection strings

### Performance Tips

1. **Batch messages** when possible
2. **Use async producer** for high throughput
3. **Reuse connections** across operations
4. **Compress large payloads** before sending
5. **Monitor latency** and adjust timeouts

