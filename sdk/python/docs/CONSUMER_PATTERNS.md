# Consumer Patterns - PyFlyMQ

Best practices and patterns for consuming messages with PyFlyMQ.

## Table of Contents

- [Quick Start](#quick-start)
- [High-Level Consumer](#high-level-consumer)
- [Basic Consumption](#basic-consumption)
- [Consumer Groups](#consumer-groups)
- [Offset Management](#offset-management)
- [Batch Processing](#batch-processing)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)

## Quick Start

The fastest way to consume messages:

```python
from pyflymq import connect

client = connect("localhost:9092")

# High-level consumer with Kafka-like API
with client.consumer("my-topic", "my-group") as consumer:
    for message in consumer:
        print(f"Received: {message.decode()}")
        # Auto-commit enabled by default
```

## High-Level Consumer

The `HighLevelConsumer` provides a Kafka-like API with auto-commit and poll-based consumption.

### Basic Usage

```python
from pyflymq import connect

client = connect("localhost:9092")

with client.consumer("my-topic", "my-group") as consumer:
    for message in consumer:
        print(f"Key: {message.key}")
        print(f"Value: {message.decode()}")
        print(f"Offset: {message.offset}")
```

### Poll-Based Consumption

```python
from pyflymq import connect

client = connect("localhost:9092")

consumer = client.consumer(
    topics=["topic1", "topic2"],
    group_id="my-group",
    auto_commit=False  # Manual commit
)

while True:
    messages = consumer.poll(timeout_ms=1000, max_records=100)

    for msg in messages:
        process(msg)

    consumer.commit()  # Commit after processing batch

consumer.close()
```

### Consumer Configuration

```python
consumer = client.consumer(
    topics="my-topic",
    group_id="my-group",
    auto_commit=True,              # Enable auto-commit
    auto_commit_interval_ms=5000,  # Commit every 5 seconds
    max_poll_records=500,          # Max records per poll
    session_timeout_ms=30000       # Session timeout
)
```

### Seek Operations

```python
consumer = client.consumer("my-topic", "my-group")

# Seek to specific offset
consumer.seek(partition=0, offset=100)

# Seek to beginning of partition
consumer.seek_to_beginning(partition=0)

# Seek to end (only new messages)
consumer.seek_to_end(partition=0)

# Get current position
position = consumer.position(partition=0)
print(f"Current position: {position}")
```

### Async Commit

```python
def on_commit_complete(offsets, error):
    if error:
        print(f"Commit failed: {error}")
    else:
        print(f"Committed: {offsets}")

consumer.commit_async(callback=on_commit_complete)
```

## Basic Consumption (Low-Level)

### Simple Consume

```python
from pyflymq import FlyMQClient

client = FlyMQClient("localhost:9092")
message = client.consume("my-topic", offset=0)
print(message.decode())
client.close()
```

### Consume with Key

```python
# Consume message and get its key
msg = client.consume("orders", offset=0)
print(f"Key: {msg.key}, Data: {msg.decode()}")
```

## Consumer Groups (Low-Level)

### Basic Consumer Group

```python
from pyflymq import Consumer

consumer = Consumer(client, "my-topic", group_id="my-group")

for message in consumer:
    process(message.decode())
    consumer.commit()  # Save position
```

### Multiple Consumers (Load Balancing)

```python
# Consumer 1 (process A)
consumer1 = Consumer(client, "tasks", group_id="workers")
for msg in consumer1:
    process(msg)
    consumer1.commit()

# Consumer 2 (process B) - same group, shares work
consumer2 = Consumer(client, "tasks", group_id="workers")
for msg in consumer2:
    process(msg)
    consumer2.commit()
```

### Subscribe Modes

```python
from pyflymq import SubscribeMode

# Start from committed offset (default)
consumer.subscribe(mode=SubscribeMode.COMMIT)

# Start from beginning
consumer.subscribe(mode=SubscribeMode.EARLIEST)

# Start from end (new messages only)
consumer.subscribe(mode=SubscribeMode.LATEST)
```

## Offset Management

### Manual Commit

```python
consumer = client.consumer("topic", "group", auto_commit=False)

for message in consumer:
    try:
        process(message)
        consumer.commit()  # Commit after successful processing
    except Exception as e:
        # Don't commit - message will be reprocessed
        log_error(e)
```

### Auto-Commit

```python
# Auto-commit is enabled by default
consumer = client.consumer(
    "topic",
    "group",
    auto_commit=True,
    auto_commit_interval_ms=5000
)

for message in consumer:
    process(message)
    # Offsets committed automatically every 5 seconds
```

### Seeking

```python
consumer = client.consumer("topic", "group")

# Seek to specific offset
consumer.seek(partition=0, offset=100)

# Seek to beginning
consumer.seek_to_beginning()

# Seek to end
consumer.seek_to_end()

# Get current position
position = consumer.position(partition=0)
```

## Batch Processing

### Fetch Multiple Messages

```python
result = client.fetch("topic", partition=0, offset=0, max_messages=100)

for msg in result.messages:
    process(msg)

next_offset = result.next_offset
```

### Batch with Transactions

```python
batch = []
for message in consumer:
    batch.append(message)
    
    if len(batch) >= 100:
        with client.transaction() as txn:
            for msg in batch:
                result = process(msg)
                txn.produce("results", result)
        consumer.commit()
        batch.clear()
```

## Error Handling

### Graceful Error Recovery

```python
from pyflymq import ConsumerError, OffsetOutOfRangeError

consumer = Consumer(client, "topic", group_id="group")

while True:
    try:
        for message in consumer:
            try:
                process(message)
                consumer.commit()
            except ProcessingError as e:
                # Log and continue
                log_error(e)
                consumer.commit()  # Skip bad message
    except OffsetOutOfRangeError:
        # Reset to beginning
        consumer.seek_to_beginning()
    except ConsumerError as e:
        # Reconnect
        time.sleep(1)
        consumer = Consumer(client, "topic", group_id="group")
```

### Dead Letter Queue

```python
for message in consumer:
    try:
        process(message)
        consumer.commit()
    except Exception as e:
        # Send to DLQ after max retries
        if message.retry_count >= 3:
            client.produce("topic-dlq", message.data)
        consumer.commit()
```

## Best Practices

### ✅ DO

- Use consumer groups for parallel processing
- Commit offsets after successful processing
- Handle errors gracefully
- Monitor consumer lag
- Close consumers properly
- Use appropriate subscribe mode

### ❌ DON'T

- Commit before processing completes
- Create new consumer for each message
- Ignore processing errors
- Leave consumers without health checks
- Process without timeout handling

### Performance Tips

1. **Batch fetch** for high throughput
2. **Use multiple consumers** in same group
3. **Monitor lag** to detect slow consumers
4. **Tune poll timeout** for responsiveness
5. **Process async** when possible

