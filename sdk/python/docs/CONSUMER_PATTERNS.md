# Consumer Patterns - PyFlyMQ

Best practices and patterns for consuming messages with PyFlyMQ.

## Table of Contents

- [Basic Consumption](#basic-consumption)
- [Consumer Groups](#consumer-groups)
- [Offset Management](#offset-management)
- [Batch Processing](#batch-processing)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)

## Basic Consumption

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
msg = client.consume_with_key("orders", offset=0)
print(f"Key: {msg.key}, Data: {msg.data.decode()}")
```

## Consumer Groups

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
consumer = Consumer(client, "topic", group_id="group")

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
from pyflymq import ConsumerConfig

config = ConsumerConfig(
    auto_commit=True,
    auto_commit_interval_ms=5000
)

consumer = Consumer(client, "topic", group_id="group", config=config)
```

### Seeking

```python
# Seek to specific offset
consumer.seek(100)

# Seek to beginning
consumer.seek_to_beginning()

# Seek to end
consumer.seek_to_end()

# Get current position
position = consumer.position()
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

