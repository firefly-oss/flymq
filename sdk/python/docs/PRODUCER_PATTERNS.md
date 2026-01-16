# Producer Patterns - PyFlyMQ

Best practices and patterns for producing messages with PyFlyMQ.

## Table of Contents

- [Basic Production](#basic-production)
- [Key-Based Routing](#key-based-routing)
- [Batch Production](#batch-production)
- [Transactions](#transactions)
- [Advanced Patterns](#advanced-patterns)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)

## Basic Production

### Simple Produce

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
# All orders for user-123 go to same partition (ordering guaranteed)
client.produce_with_key("orders", "user-123", b"Order #1")
client.produce_with_key("orders", "user-123", b"Order #2")
client.produce_with_key("orders", "user-123", b"Order #3")
```

## Batch Production

### Using Transactions for Atomicity

```python
with client.transaction() as txn:
    for item in items:
        txn.produce("batch-topic", item.encode())
    # All messages committed atomically
```

### High-Throughput Producer

```python
from pyflymq import Producer

producer = Producer(client, "high-volume-topic")

for i in range(10000):
    producer.send(f"Message {i}".encode())

producer.flush()  # Ensure all messages sent
producer.close()
```

### Async Producer

```python
from pyflymq import AsyncProducer
import asyncio

async def produce_async():
    producer = AsyncProducer(client, "async-topic")
    
    tasks = [
        producer.send(f"Message {i}".encode())
        for i in range(1000)
    ]
    
    await asyncio.gather(*tasks)
    await producer.close()
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

