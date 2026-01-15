# Topics, Keys, and Partitions Guide

This guide covers everything you need to know about working with topics, message keys, and partitions in FlyMQ. Whether you're coming from Kafka or starting fresh, this guide will help you understand and implement common messaging patterns.

## Table of Contents

1. [Core Concepts](#core-concepts)
2. [Partitioning Strategies](#partitioning-strategies)
3. [Producer Patterns](#producer-patterns)
4. [Consumer Patterns](#consumer-patterns)
5. [CLI Examples](#cli-examples)
6. [Best Practices](#best-practices)
7. [Migration from Kafka](#migration-from-kafka)

---

## Core Concepts

### Topics

A **topic** is a named stream of messages. Topics are the primary way to organize and categorize messages in FlyMQ.

```
Topic: "orders"
├── Partition 0: [msg0, msg1, msg2, ...]
├── Partition 1: [msg0, msg1, msg2, ...]
└── Partition 2: [msg0, msg1, msg2, ...]
```

**Key characteristics:**
- Topics are created explicitly or auto-created on first produce
- Each topic can have multiple partitions for parallelism
- Messages within a partition are strictly ordered
- Topics persist messages until retention policy expires

### Partitions

A **partition** is an ordered, immutable sequence of messages within a topic. Partitions enable:

- **Parallelism**: Multiple consumers can read from different partitions simultaneously
- **Ordering**: Messages within a partition maintain strict order
- **Scalability**: Distribute load across multiple brokers

**Partition numbering** starts at 0. A topic with 4 partitions has partitions 0, 1, 2, and 3.

### Message Keys

A **key** is optional metadata attached to a message that determines partition assignment:

- Messages with the **same key** always go to the **same partition**
- This guarantees ordering for related messages (e.g., all events for user-123)
- Keys can be any byte sequence (strings, UUIDs, composite keys)

```
Key: "user-123" → hash("user-123") % num_partitions → Partition 2
Key: "user-456" → hash("user-456") % num_partitions → Partition 0
Key: "user-123" → hash("user-123") % num_partitions → Partition 2 (same!)
```

### Offsets

An **offset** is a unique, sequential identifier for each message within a partition:

- Offsets start at 0 and increment for each new message
- Consumers track their position using offsets
- Offsets are per-partition (partition 0 offset 5 ≠ partition 1 offset 5)

---

## Partitioning Strategies

### 1. Key-Based Partitioning (Recommended)

Use message keys when you need ordering guarantees for related messages.

**Use cases:**
- All orders for a customer must be processed in order
- All events for a session must be consumed sequentially
- Aggregating data by entity (user, device, account)

**How it works:**
```
partition = hash(key) % number_of_partitions
```

### 2. Explicit Partition Selection

Specify the exact partition when you need precise control.

**Use cases:**
- Geo-based routing (partition 0 = US, partition 1 = EU)
- Priority queues (partition 0 = high priority)
- Testing and debugging

### 3. Round-Robin (No Key)

When no key is provided, messages are distributed across partitions.

**Use cases:**
- Maximum throughput with no ordering requirements
- Load balancing across consumers
- Fire-and-forget messaging

---

## Producer Patterns

### Go Examples

```go
package main

import "github.com/firefly-oss/flymq/pkg/client"

func main() {
    c, _ := client.NewClient("localhost:9092")
    defer c.Close()

    // 1. Simple produce (auto partition selection)
    offset, _ := c.Produce("events", []byte("Hello World"))

    // 2. Key-based partitioning (same key = same partition)
    c.ProduceWithKey("orders", []byte("user-123"), []byte(`{"order": 1}`))
    c.ProduceWithKey("orders", []byte("user-123"), []byte(`{"order": 2}`))
    // Both messages go to the same partition, maintaining order

    // 3. Explicit partition selection
    c.ProduceToPartition("events", 2, []byte("To partition 2"))

    // 4. Key + explicit partition (partition overrides key routing)
    c.ProduceWithKeyToPartition("events", 1, []byte("key"), []byte("data"))
}
```

### Python Examples

```python
from pyflymq import FlyMQClient

client = FlyMQClient("localhost:9092")

# 1. Simple produce
offset = client.produce("events", b"Hello World")

# 2. Key-based partitioning
client.produce("orders", b'{"order": 1}', key=b"user-123")
client.produce("orders", b'{"order": 2}', key=b"user-123")
# Both go to same partition

# 3. Convenience method for key-based
client.produce_with_key("orders", "user-123", b'{"order": 3}')

# 4. Explicit partition
client.produce("events", b"data", partition=2)
# Or use the explicit method
client.produce_to_partition("events", 2, b"data")

client.close()
```

### Java Examples

```java
import com.firefly.flymq.FlyMQClient;

try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
    // 1. Simple produce
    long offset = client.produce("events", "Hello World".getBytes());

    // 2. Key-based partitioning
    client.produceWithKey("orders", "user-123", "{\"order\": 1}");
    client.produceWithKey("orders", "user-123", "{\"order\": 2}");

    // 3. Explicit partition
    client.produceToPartition("events", 2, "data".getBytes());

    // 4. Key + explicit partition
    client.produceWithKeyToPartition("events", 1,
        "key".getBytes(), "data".getBytes());
}
```

---

## Consumer Patterns

### Single Consumer (Go)

```go
// Consume from a specific partition
msg, _ := c.ConsumeFromPartition("orders", 0, offset)
fmt.Printf("Key: %s, Value: %s\n", msg.Key, msg.Value)

// Fetch multiple messages
messages, nextOffset, _ := c.FetchWithKeys("orders", 0, 0, 100)
for _, msg := range messages {
    fmt.Printf("[%d] key=%s: %s\n", msg.Offset, msg.Key, msg.Value)
}
```

### Consumer Groups (Go)

```go
// Subscribe to a partition with a consumer group
offset, _ := c.Subscribe("orders", "order-processors", 0, "earliest")

// Consume and commit
for {
    messages, next, _ := c.FetchWithKeys("orders", 0, offset, 10)
    for _, msg := range messages {
        processOrder(msg)
    }
    c.CommitOffset("orders", "order-processors", 0, next)
    offset = next
}
```

### Single Consumer (Python)

```python
from pyflymq import FlyMQClient
from pyflymq.consumer import Consumer

client = FlyMQClient("localhost:9092")

# Low-level: consume from specific partition
msg = client.consume("orders", offset=0, partition=2)
print(f"Key: {msg.key}, Data: {msg.data}")

# High-level Consumer class
consumer = Consumer(client, "orders", group_id="my-group")
for message in consumer:
    print(f"Received: {message.data.decode()}")
    if message.key:
        print(f"Key: {message.key.decode()}")
    consumer.commit()
```

### Consumer Groups (Python)

```python
from pyflymq import FlyMQClient
from pyflymq.consumer import ConsumerGroup

client = FlyMQClient("localhost:9092")

def handle_message(msg):
    print(f"Processing: {msg.data.decode()}")

# Automatic offset management
with ConsumerGroup(client, "orders", "order-processors",
                   handler=handle_message) as group:
    # Runs until interrupted
    pass

# Check lag
lag = group.get_lag()
print(f"Current lag: {lag}")
```

### Single Consumer (Java)

```java
// Low-level consume from partition
ConsumedMessage msg = client.consumeFromPartition("orders", 2, 0);
System.out.println("Key: " + new String(msg.key()));
System.out.println("Data: " + new String(msg.data()));

// Fetch multiple messages
FetchResult result = client.fetch("orders", 0, 0, 100);
for (ConsumedMessage m : result.messages()) {
    System.out.printf("[%d] %s%n", m.offset(), new String(m.data()));
}
```

### Consumer Groups (Java)

```java
import com.firefly.flymq.consumer.Consumer;
import com.firefly.flymq.consumer.ConsumerConfig;

ConsumerConfig config = ConsumerConfig.builder()
    .enableAutoCommit(true)
    .autoCommitIntervalMs(5000)
    .maxPollRecords(100)
    .build();

try (Consumer consumer = new Consumer(client, "orders", "my-group", 0, config)) {
    consumer.subscribe(SubscribeMode.EARLIEST);

    while (true) {
        List<ConsumedMessage> messages = consumer.poll(Duration.ofSeconds(1));
        for (ConsumedMessage msg : messages) {
            process(msg);
        }
        // Auto-commit handles offset management
    }
}
```

---

## CLI Examples

### Producing Messages

```bash
# Simple produce
flymq-cli produce my-topic "Hello World"

# Produce with key (for ordering)
flymq-cli produce orders '{"id": 1}' --key user-123
flymq-cli produce orders '{"id": 2}' --key user-123

# Produce to specific partition
flymq-cli produce events "data" --partition 2

# Produce with key to specific partition
flymq-cli produce events "data" --key mykey --partition 1
```

### Consuming Messages

```bash
# Consume from partition 0 (default)
flymq-cli consume my-topic --offset 0 --count 10

# Consume from specific partition
flymq-cli consume my-topic --partition 2 --offset 0

# Show message keys
flymq-cli consume orders --show-key

# Raw output (just message content)
flymq-cli consume my-topic --raw
```

### Subscribing (Streaming)

```bash
# Subscribe from latest (new messages only)
flymq-cli subscribe my-topic

# Subscribe from beginning (all messages)
flymq-cli subscribe my-topic --from-beginning

# Subscribe with consumer group
flymq-cli subscribe orders --group order-processors --from-beginning

# Show keys and lag
flymq-cli subscribe orders --group my-group --show-key --show-lag
```

### Consumer Group Management

```bash
# List all consumer groups
flymq-cli groups list

# Describe a group
flymq-cli groups describe order-processors

# Check consumer lag
flymq-cli groups lag order-processors --topic orders

# Reset offsets to beginning
flymq-cli groups reset-offsets order-processors --topic orders --to-earliest

# Reset to latest
flymq-cli groups reset-offsets order-processors --topic orders --to-latest

# Reset to specific offset
flymq-cli groups reset-offsets order-processors --topic orders --to-offset 1000
```

### Topic Management

```bash
# Create topic with partitions
flymq-cli create my-topic --partitions 4

# List topics
flymq-cli topics

# Get topic info
flymq-cli info my-topic

# Delete topic
flymq-cli delete my-topic
```

---

## Best Practices

### Choosing Partition Count

| Scenario | Recommended Partitions |
|----------|----------------------|
| Development/Testing | 1-2 |
| Low throughput (<1K msg/s) | 2-4 |
| Medium throughput (1K-10K msg/s) | 4-8 |
| High throughput (>10K msg/s) | 8-16+ |

**Guidelines:**
- More partitions = more parallelism but more overhead
- Partitions can be increased but not decreased
- Consider your consumer count (partitions ≥ consumers)

### Key Design

**Good keys:**
- User ID: `user-123`
- Session ID: `session-abc`
- Entity ID: `order-456`
- Composite: `tenant:user:session`

**Avoid:**
- Timestamps (creates hot partitions)
- Random values (defeats ordering purpose)
- Very high cardinality with few partitions

### Ordering Guarantees

| Scenario | Ordering Guarantee |
|----------|-------------------|
| Same key, same partition | ✅ Strict ordering |
| Same key, different partitions | ❌ No ordering |
| No key | ❌ No ordering |
| Explicit partition | ✅ Ordering within partition |

### Consumer Group Best Practices

1. **One consumer per partition** for maximum parallelism
2. **Use meaningful group IDs** (e.g., `order-service-prod`)
3. **Commit offsets after processing** to avoid reprocessing
4. **Monitor lag** to detect slow consumers
5. **Handle rebalancing** gracefully in distributed systems

---

## Migration from Kafka

FlyMQ is designed to be familiar to Kafka users. Here's a quick comparison:

### Concept Mapping

| Kafka | FlyMQ | Notes |
|-------|-------|-------|
| Topic | Topic | Same concept |
| Partition | Partition | Same concept |
| Key | Key | Same concept |
| Offset | Offset | Same concept |
| Consumer Group | Consumer Group | Same concept |
| Broker | Server | Single server (no cluster yet) |
| Producer | Client | Unified client API |
| Consumer | Client/Consumer | Low-level or high-level API |

### API Comparison

**Kafka Java:**
```java
ProducerRecord<String, String> record =
    new ProducerRecord<>("topic", "key", "value");
producer.send(record);
```

**FlyMQ Java:**
```java
client.produceWithKey("topic", "key", "value");
```

**Kafka Python:**
```python
producer.send('topic', key=b'key', value=b'value')
```

**FlyMQ Python:**
```python
client.produce('topic', b'value', key=b'key')
```

### Key Differences

1. **Simpler API**: FlyMQ has a unified client for produce/consume
2. **No ZooKeeper**: FlyMQ is self-contained
3. **Single binary**: No cluster setup required
4. **Wire protocol**: Custom binary protocol (not Kafka-compatible)
5. **Replication**: Not yet supported (single node)

### Migration Steps

1. **Update dependencies**: Replace Kafka client with FlyMQ SDK
2. **Update connection**: Change bootstrap servers to FlyMQ address
3. **Update API calls**: Adapt to FlyMQ method names
4. **Test thoroughly**: Verify ordering and delivery guarantees
5. **Migrate data**: Use export/import or dual-write strategy

---

## Troubleshooting

### Common Issues

**"Partition not found"**
- Ensure the partition exists (check with `flymq-cli info <topic>`)
- Partition numbers are 0-indexed

**"Messages out of order"**
- Verify you're using the same key for related messages
- Check that you're consuming from a single partition

**"Consumer lag increasing"**
- Add more consumers (up to partition count)
- Increase batch size for processing
- Check for slow message handlers

**"Key not appearing in consumed message"**
- Ensure you're using `consumeWithKey` or `FetchWithKeys`
- Check that the producer actually sent a key

### Debug Commands

```bash
# Check topic partitions
flymq-cli info my-topic

# Verify message keys
flymq-cli consume my-topic --show-key --count 5

# Check consumer group state
flymq-cli groups describe my-group

# Monitor lag in real-time
flymq-cli subscribe my-topic --group my-group --show-lag
```

---

## Summary

| Operation | Go | Python | Java | CLI |
|-----------|-----|--------|------|-----|
| Produce | `Produce()` | `produce()` | `produce()` | `produce` |
| Produce with key | `ProduceWithKey()` | `produce(key=)` | `produceWithKey()` | `produce --key` |
| Produce to partition | `ProduceToPartition()` | `produce(partition=)` | `produceToPartition()` | `produce --partition` |
| Consume | `Consume()` | `consume()` | `consume()` | `consume` |
| Consume from partition | `ConsumeFromPartition()` | `consume(partition=)` | `consumeFromPartition()` | `consume --partition` |
| Subscribe | `Subscribe()` | `Consumer()` | `Consumer` | `subscribe` |

For more details, see:
- [API Reference](api-reference.md)
- [Getting Started](getting-started.md)
- [Architecture](architecture.md)
