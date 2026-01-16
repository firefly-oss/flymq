# Getting Started with PyFlyMQ

This guide will help you get started with the FlyMQ Python SDK in 10 minutes.

## Installation

Install PyFlyMQ using pip:

```bash
pip install pyflymq
```

Or install from source:

```bash
cd sdk/python
pip install -e .
```

## Prerequisites

- FlyMQ server running (default: `localhost:9092`)
- Python 3.7+

## Your First Program

Create a file named `hello_flymq.py`:

```python
from pyflymq import FlyMQClient

# Connect to FlyMQ
client = FlyMQClient("localhost:9092")

try:
    # Create a topic
    client.create_topic("hello", partitions=1)
    
    # Produce a message
    offset = client.produce("hello", b"Hello, FlyMQ!")
    print(f"Message produced at offset {offset}")
    
    # Consume the message
    msg = client.consume("hello", offset)
    print(f"Received: {msg.decode()}")
finally:
    client.close()
```

Run it:

```bash
python hello_flymq.py
```

Output:

```
Message produced at offset 0
Received: Hello, FlyMQ!
```

## Key Concepts

### Topics

A topic is a named channel where messages are produced and consumed. Create topics with:

```python
client.create_topic("my-topic", partitions=3)
```

- **Partitions**: Divide a topic for parallel processing
- Higher partitions = more parallelism but more resource usage

### Producing Messages

Send messages to a topic:

```python
# Simple produce
offset = client.produce("my-topic", b"Message data")

# Produce with key (Kafka-style)
offset = client.produce("my-topic", b"Data", key="user-123")
```

Messages with the same key always go to the same partition (ordering guarantee).

### Consuming Messages

Read messages from a topic:

```python
# Consume by offset
msg = client.consume("my-topic", offset=0)

# Fetch multiple messages
result = client.fetch("my-topic", partition=0, offset=0, max_messages=100)
for msg in result.messages:
    print(msg.decode())
```

### Consumer Groups

Process messages as a group with offset tracking:

```python
from pyflymq import Consumer

consumer = Consumer(client, "my-topic", group_id="my-group")

for message in consumer:
    print(message.decode())
    consumer.commit()  # Save position
```

## Common Patterns

### Pattern 1: Request-Reply

```python
client = FlyMQClient("localhost:9092")
client.create_topic("requests", partitions=1)
client.create_topic("responses", partitions=1)

# Producer sends request
request_id = "req-123"
client.produce("requests", b"Process this", key=request_id)

# Consumer processes and responds
consumer = Consumer(client, "requests", group_id="processor")
for msg in consumer:
    result = process(msg.decode())
    client.produce("responses", result.encode(), key=msg.decode_key())
    consumer.commit()
```

### Pattern 2: Fan-Out

```python
# One producer, multiple consumers
def producer(client):
    for i in range(100):
        client.produce("events", f"Event {i}".encode())

# Multiple consumers, each gets all messages
for consumer_id in range(3):
    consumer = Consumer(client, "events", group_id=f"consumer-{consumer_id}")
    for msg in consumer:
        print(f"Consumer {consumer_id}: {msg.decode()}")
```

### Pattern 3: Load Balancing

```python
# One topic, multiple consumers in same group
# Partitions distributed among consumers

# Consumer 1
consumer = Consumer(client, "tasks", group_id="workers")
for msg in consumer:
    process_task(msg.decode())
    consumer.commit()

# Consumer 2 (separate process)
consumer = Consumer(client, "tasks", group_id="workers")
for msg in consumer:
    process_task(msg.decode())
    consumer.commit()

# Each consumer gets some partitions automatically
```

## Error Handling

Always handle errors gracefully:

```python
from pyflymq import FlyMQClient
from pyflymq.exceptions import (
    ConnectionError, TopicNotFoundError, ServerError
)

try:
    client = FlyMQClient("localhost:9092")
    client.produce("my-topic", b"Data")
except TopicNotFoundError:
    print("Topic does not exist")
except ServerError:
    print("Server returned an error")
except ConnectionError:
    print("Failed to connect")
finally:
    client.close()
```

## Authentication

If your FlyMQ server requires authentication:

```python
client = FlyMQClient(
    "localhost:9092",
    username="alice",
    password="secret123"
)
```

Or load from environment:

```python
import os

client = FlyMQClient(
    "localhost:9092",
    username=os.getenv("FLYMQ_USERNAME"),
    password=os.getenv("FLYMQ_PASSWORD")
)
```

Set environment variables:

```bash
export FLYMQ_USERNAME=alice
export FLYMQ_PASSWORD=secret123
python script.py
```

## Advanced Features

### Encryption

Encrypt messages with AES-256-GCM:

```python
from pyflymq import generate_key

key = generate_key()
client = FlyMQClient("localhost:9092", encryption_key=key)

# Messages are automatically encrypted/decrypted
offset = client.produce("secure-topic", b"Secret data")
msg = client.consume("secure-topic", offset)
print(msg.decode())  # Automatically decrypted
```

### Transactions

Atomic operations across topics:

```python
with client.transaction() as txn:
    txn.produce("orders", b"Order 1")
    txn.produce("payments", b"Payment 1")
    # Auto-commits on success, rolls back on error
```

### Message with TTL

Messages that expire automatically:

```python
# Expires in 60 seconds
client.produce_with_ttl("events", b"Temporary event", ttl_ms=60000)
```

### Delayed Messages

Schedule messages for later delivery:

```python
# Deliver after 5 seconds
client.produce_delayed("scheduled", b"Delayed message", delay_ms=5000)
```

## Next Steps

1. **Run examples**: Check `examples/` directory for complete examples
2. **Read tutorials**: Look for specific patterns in the `docs/` folder
   - `PRODUCER_PATTERNS.md` - Producing best practices
   - `CONSUMER_PATTERNS.md` - Consuming best practices
   - `SECURITY.md` - Encryption and authentication
3. **Check API reference**: See README.md for full API documentation

## Troubleshooting

### Cannot connect to FlyMQ

```python
# Make sure server is running
# Default: localhost:9092

# Try connecting with explicit host/port
client = FlyMQClient("localhost", 9092)
```

### Authentication failed

```python
# Check credentials
# Enable auth on server if needed
client = FlyMQClient(
    "localhost:9092",
    username="admin",
    password="password"
)
```

### Topic not found

```python
# Create the topic first
client.create_topic("my-topic", partitions=3)

# Or auto-create may be enabled on server
```

## Getting Help

- Check examples in `sdk/python/examples/`
- Read the main README.md for API reference
- See `docs/` folder for detailed guides
- Check protocol documentation: `docs/sdk_development.md`
