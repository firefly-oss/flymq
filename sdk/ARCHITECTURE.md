# FlyMQ SDK Architecture Guide

## Overview

This document provides a comprehensive understanding of FlyMQ's architecture and how the SDKs interact with the FlyMQ message broker. Understanding this architecture is crucial for building robust, scalable applications.

FlyMQ SDKs provide **Kafka-like APIs** that feel familiar to developers coming from the Kafka ecosystem, while offering improved performance and simpler deployment.

## Table of Contents

- [System Architecture](#system-architecture)
- [High-Level APIs](#high-level-apis)
- [Protocol Design](#protocol-design)
- [Client Architecture](#client-architecture)
- [Message Flow](#message-flow)
- [Partitioning Strategy](#partitioning-strategy)
- [Consumer Groups](#consumer-groups)
- [Transaction Model](#transaction-model)
- [Security Architecture](#security-architecture)
- [Error Handling](#error-handling)
- [Performance Characteristics](#performance-characteristics)

## System Architecture

FlyMQ follows a distributed broker architecture similar to Apache Kafka but optimized for simplicity and performance:

```
┌───────────────────────────────────────────────────────────┐
│                     Client Applications                   │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│   │ Producer │  │ Consumer │  │ Producer │  │ Consumer │  │
│   └─────┬────┘  └─────┬────┘  └─────┬────┘  └─────┬────┘  │
│         │             │             │             │       │
└─────────┼─────────────┼─────────────┼─────────────┼───────┘
          │             │             │             │
    ┌─────▼─────────────▼─────────────▼─────────────▼──────┐
    │                   FlyMQ SDKs (Python/Java)           │
    │  ┌─────────────────────────────────────────────────┐ │
    │  │  Connection Pool | Retry Logic | Serialization  │ │
    │  │  Encryption | Authentication | Failover         │ │
    │  └─────────────────────────────────────────────────┘ │
    └────────────────────┬─────────────────────────────────┘
                         │ Binary Protocol (TCP/TLS)
    ┌────────────────────▼─────────────────────────────────┐
    │               FlyMQ Broker Cluster                   │
    │      ┌──────────┐  ┌──────────┐  ┌──────────┐        │
    │      │ Broker 1 │  │ Broker 2 │  │ Broker 3 │        │
    │      │ (Leader) │  │(Follower)│  │(Follower)│        │
    │      └──────────┘  └──────────┘  └──────────┘        │
    │            │             │             │             │
    │  ┌─────────▼─────────────▼─────────────▼───────────┐ │
    │  │           Distributed Storage Layer             │ │
    │  │  Topics | Partitions | Messages | Offsets       │ │
    │  └─────────────────────────────────────────────────┘ │
    └──────────────────────────────────────────────────────┘
```

### Key Components

1. **Client Applications**: Your applications using FlyMQ SDKs
2. **FlyMQ SDKs**: Language-specific client libraries (Python, Java)
3. **Broker Cluster**: FlyMQ server instances managing messages
4. **Storage Layer**: Persistent message storage with replication

## High-Level APIs

FlyMQ SDKs provide Kafka-like high-level APIs for production workloads. These APIs abstract away low-level protocol details and provide familiar patterns.

### API Hierarchy

```
┌─────────────────────────────────────────────────────────────────┐
│                     High-Level APIs (Recommended)               │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  connect()          - One-liner connection                  ││
│  │  client.producer()  - HighLevelProducer with batching       ││
│  │  client.consumer()  - HighLevelConsumer with auto-commit    ││
│  └─────────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────────┤
│                     Low-Level APIs (Advanced)                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  FlyMQClient()      - Direct client instantiation           ││
│  │  client.produce()   - Single message production             ││
│  │  client.consume()   - Single message consumption            ││
│  │  client.fetch()     - Batch message retrieval               ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### Quick Comparison

| Feature | High-Level API | Low-Level API |
|---------|----------------|---------------|
| Connection | `connect("host:port")` | `FlyMQClient("host:port")` |
| Producing | `producer.send()` with batching | `client.produce()` per message |
| Consuming | `consumer.poll()` with auto-commit | `client.consume()` per offset |
| Callbacks | ✅ `on_success`, `on_error` | ❌ Manual handling |
| Batching | ✅ Automatic | ❌ Manual |
| Retries | ✅ Automatic | ❌ Manual |

### Python High-Level API

```python
from pyflymq import connect

# One-liner connection
client = connect("localhost:9092")

# High-level producer with batching and callbacks
with client.producer(batch_size=100, linger_ms=10) as producer:
    def on_success(metadata):
        print(f"Sent to {metadata.topic} @ offset {metadata.offset}")

    producer.send("events", b'{"event": "click"}', key="user-123",
                  on_success=on_success)
    producer.flush()

# High-level consumer with auto-commit
with client.consumer("events", "my-group") as consumer:
    for message in consumer:
        print(f"Key: {message.key}, Value: {message.decode()}")
        # Auto-commit enabled by default

client.close()
```

### Java High-Level API

```java
import com.firefly.flymq.FlyMQClient;

// One-liner connection
try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {

    // High-level producer with batching
    try (var producer = client.producer()) {
        producer.send("events", "{\"event\": \"click\"}".getBytes(), "user-123".getBytes())
            .whenComplete((metadata, error) -> {
                if (error == null) {
                    System.out.println("Sent @ offset " + metadata.offset());
                }
            });
        producer.flush();
    }

    // High-level consumer with auto-commit
    try (var consumer = client.consumer("events", "my-group")) {
        consumer.subscribe();
        for (var msg : consumer.poll(Duration.ofSeconds(1))) {
            System.out.println("Key: " + msg.keyAsString() + ", Value: " + msg.dataAsString());
        }
    }
}
```

### HighLevelProducer Features

The `HighLevelProducer` provides production-ready message sending:

| Feature | Description |
|---------|-------------|
| **Batching** | Groups messages for efficient network usage |
| **Linger** | Waits for batch to fill before sending |
| **Callbacks** | `on_success` and `on_error` handlers |
| **Futures** | Async results with `get()` for blocking |
| **Retries** | Automatic retry with exponential backoff |
| **Flush** | Ensures all pending messages are sent |

### RecordMetadata

All produce operations return `RecordMetadata`, similar to Kafka's RecordMetadata.
This provides complete information about where and when the message was stored:

| Field | Type | Description |
|-------|------|-------------|
| `topic` | string | Topic name the message was produced to |
| `partition` | int32 | Partition the message was assigned to |
| `offset` | uint64 | Offset of the message in the partition |
| `timestamp` | int64 | Server timestamp when message was stored (ms) |
| `key_size` | int32 | Size of key in bytes (-1 if no key) |
| `value_size` | int32 | Size of value in bytes |

**Python:**
```python
meta = client.produce_with_metadata("events", b"data", key="user-123")
print(f"Topic: {meta.topic}")
print(f"Partition: {meta.partition}")
print(f"Offset: {meta.offset}")
print(f"Timestamp: {meta.timestamp_datetime}")  # datetime object
print(f"Key size: {meta.key_size}")
print(f"Value size: {meta.value_size}")
```

**Java:**
```java
RecordMetadata meta = client.produceWithMetadata("events", data);
System.out.println("Topic: " + meta.topic());
System.out.println("Partition: " + meta.partition());
System.out.println("Offset: " + meta.offset());
System.out.println("Timestamp: " + meta.timestampAsInstant());
System.out.println("Has key: " + meta.hasKey());
```

### HighLevelConsumer Features

The `HighLevelConsumer` provides Kafka-like consumption:

| Feature | Description |
|---------|-------------|
| **Auto-commit** | Automatic offset commits at intervals |
| **Poll-based** | `poll(timeout)` returns batch of messages |
| **Seek** | Navigate to specific offsets |
| **Iterator** | Python `for msg in consumer` pattern |
| **Manual commit** | `commit()` for at-least-once semantics |
| **Async commit** | `commit_async()` for performance |

## Protocol Design

FlyMQ uses a custom binary protocol over TCP for efficient communication:

### Message Format

```
┌─────────────┬──────────┬──────────┬─────────┬──────────┐
│ Magic Byte  │ Version  │  OpCode  │ Length  │ Payload  │
│   (1 byte)  │ (1 byte) │ (1 byte) │(4 bytes)│(variable)│
└─────────────┴──────────┴──────────┴─────────┴──────────┘

Magic Byte: 0xFB (identifies FlyMQ protocol)
Version:    0x01 (protocol version)
OpCode:     Operation identifier (PRODUCE, CONSUME, etc.)
Length:     Payload size in bytes (big-endian)
Payload:    Binary data (MessagePack encoded)
```

### Operation Codes (OpCodes)

| OpCode | Operation | Description |
|--------|-----------|-------------|
| 0x01 | PRODUCE | Send message to topic |
| 0x02 | CONSUME | Retrieve message by offset |
| 0x03 | CREATE_TOPIC | Create new topic |
| 0x04 | DELETE_TOPIC | Remove topic |
| 0x05 | LIST_TOPICS | Get all topics |
| 0x06 | FETCH | Batch retrieve messages |
| 0x10 | SUBSCRIBE | Join consumer group |
| 0x11 | COMMIT | Save consumer offset |
| 0x20 | BEGIN_TXN | Start transaction |
| 0x21 | COMMIT_TXN | Commit transaction |
| 0x22 | ABORT_TXN | Rollback transaction |

## Client Architecture

### Python SDK Architecture

```
pyflymq Module
    │
    ├── connect()                    # Factory function for easy connection
    │   └── Returns FlyMQClient
    │
    ├── FlyMQClient                  # Core client class
    │   ├── Connection Management
    │   │   ├── TCP Socket
    │   │   ├── TLS/SSL Context
    │   │   └── Automatic Reconnection
    │   │
    │   ├── High-Level Factory Methods
    │   │   ├── producer()           # Returns HighLevelProducer
    │   │   └── consumer()           # Returns HighLevelConsumer
    │   │
    │   ├── Low-Level Operations
    │   │   ├── produce()            # Single message
    │   │   ├── consume()            # By offset
    │   │   ├── fetch()              # Batch retrieval
    │   │   └── subscribe()          # Consumer group
    │   │
    │   └── Utilities
    │       ├── Encryption (AES-256-GCM)
    │       └── Authentication
    │
    ├── HighLevelProducer            # Kafka-like producer
    │   ├── send()                   # Async with callbacks
    │   ├── flush()                  # Wait for pending
    │   └── Batching & Retries
    │
    ├── HighLevelConsumer            # Kafka-like consumer
    │   ├── poll()                   # Batch consumption
    │   ├── commit() / commit_async()
    │   ├── seek() / seek_to_beginning() / seek_to_end()
    │   └── Auto-commit support
    │
    └── Exceptions
        ├── FlyMQError               # Base exception with hints
        ├── ConnectionError
        ├── TopicNotFoundError
        └── AuthenticationError
```

### Java SDK Architecture

```
com.firefly.flymq Package
    │
    ├── FlyMQClient                  # Core client class
    │   ├── connect()                # Static factory method
    │   │
    │   ├── High-Level Factory Methods
    │   │   ├── producer()           # Returns HighLevelProducer
    │   │   └── consumer()           # Returns Consumer
    │   │
    │   ├── Low-Level Operations
    │   │   ├── produce()            # Single message
    │   │   ├── consume()            # By offset
    │   │   ├── fetch()              # Batch retrieval
    │   │   └── subscribe()          # Consumer group
    │   │
    │   └── Connection Management
    │       ├── Socket/SSLSocket
    │       ├── Connection Pool
    │       └── Automatic Failover
    │
    ├── producer Package
    │   ├── HighLevelProducer        # Kafka-like producer
    │   │   ├── send()               # Returns CompletableFuture
    │   │   └── flush()
    │   └── ProducerConfig           # Builder pattern config
    │
    ├── consumer Package
    │   ├── Consumer                 # Kafka-like consumer
    │   │   ├── poll()               # Returns List<ConsumedMessage>
    │   │   ├── commitSync() / commitAsync()
    │   │   └── seek() / seekToBeginning() / seekToEnd()
    │   └── ConsumerConfig           # Builder pattern config
    │
    ├── exception Package
    │   ├── FlyMQException           # Base with getHint()
    │   ├── ProtocolException
    │   └── Factory methods          # connectionFailed(), topicNotFound()
    │
    └── Spring Integration
        ├── AutoConfiguration
        ├── Properties Binding
        └── Bean Factory
```

## Message Flow

### Producer Flow

1. **Application** calls `client.produce(topic, data)`
2. **SDK** performs:
   - Optional encryption (if configured)
   - Serialization to binary format
   - Partition selection (if key provided)
   - Connection pooling/selection
3. **Protocol** creates binary message with header
4. **Network** sends over TCP/TLS to broker
5. **Broker** stores message and returns offset
6. **SDK** returns offset to application

### Consumer Flow

1. **Application** calls `client.consume(topic, offset)`
2. **SDK** creates CONSUME request
3. **Network** sends request to broker
4. **Broker** retrieves message from storage
5. **Protocol** deserializes response
6. **SDK** performs:
   - Optional decryption
   - Deserialization
   - Error handling
7. **Application** receives message data

## Partitioning Strategy

FlyMQ uses partitioning for scalability and ordering:

### Partition Selection

```
if message has key:
    partition = hash(key) % num_partitions
else:
    partition = round_robin_counter % num_partitions
```

### Ordering Guarantees

- **Per-partition ordering**: Messages in same partition maintain order
- **Key-based ordering**: Same key always goes to same partition
- **Global ordering**: Only with single partition (not scalable)

### Example Scenarios

```python
# Scenario 1: User activity stream (ordering per user)
client.produce("user-events", event_data, key=f"user-{user_id}")

# Scenario 2: Log aggregation (no ordering needed)
client.produce("logs", log_data)  # Round-robin distribution

# Scenario 3: Financial transactions (strict ordering)
client.create_topic("transactions", partitions=1)  # Single partition
client.produce("transactions", txn_data)
```

## Consumer Groups

Consumer groups enable parallel processing with automatic coordination:

### Group Coordination

```
Consumer Group "analytics"
    ├── Consumer A (handles partitions 0, 1)
    ├── Consumer B (handles partitions 2, 3)
    └── Consumer C (handles partitions 4, 5)

Rebalancing occurs when:
- Consumer joins group
- Consumer leaves group
- Topic partitions change
```

### Offset Management

```
Topic: events
Partition 0: [msg0, msg1, msg2, msg3, msg4, msg5]
                          ^
                    committed offset: 2
                                       ^
                                 current position: 5

On commit(): committed offset = current position
On restart: resume from committed offset + 1
```

## Transaction Model

FlyMQ supports ACID transactions across multiple topics:

### Transaction Lifecycle

```
BEGIN_TXN
    ├── PRODUCE to topic A
    ├── PRODUCE to topic B
    ├── PRODUCE to topic C
    └── COMMIT_TXN or ABORT_TXN

All or nothing: either all messages are visible or none
```

### Isolation Levels

- **Read Committed**: Only see committed messages (default)
- **Read Uncommitted**: Can see in-progress transaction messages

## Security Architecture

### Authentication Flow

```
Client                          Broker
  │                               │
  ├──────── Connect ──────────────►
  │                               │
  ◄──────── Challenge ─────────────
  │                               │
  ├──────── AUTH Request ─────────►
  │   (username + password)       │
  │                               │
  ◄──────── AUTH Response ─────────
  │   (success/failure)           │
  │                               │
  ├──────── Authenticated Ops ────►
```

### Encryption Layers

1. **Transport Encryption (TLS)**
   - Encrypts all network traffic
   - Server certificate validation
   - Optional client certificates (mTLS)

2. **Message Encryption (AES-256-GCM)**
   - End-to-end encryption
   - Client manages keys
   - Broker stores encrypted data

```python
# Both layers active
client = connect(
    "broker:9092",
    tls_enabled=True,           # Transport encryption
    encryption_key=secret_key    # Message encryption
)
```

## Error Handling

FlyMQ SDKs provide clear error messages with actionable hints to speed up debugging.

### Exception Hierarchy

```
FlyMQError (Python) / FlyMQException (Java)
    │
    ├── ConnectionError          # Cannot connect to broker
    │   └── hint: "Check that the FlyMQ server is running..."
    │
    ├── TopicNotFoundError       # Topic doesn't exist
    │   └── hint: "Create the topic first with client.create_topic()..."
    │
    ├── AuthenticationError      # Auth failed
    │   └── hint: "Check username and password..."
    │
    ├── TimeoutError             # Operation timed out
    │   └── hint: "The server may be overloaded..."
    │
    └── ProtocolError            # Protocol-level error
        └── hint: "Check SDK and server versions..."
```

### Error Hints

All exceptions include a `hint` property with actionable suggestions:

**Python:**
```python
from pyflymq import connect
from pyflymq.exceptions import TopicNotFoundError

try:
    client = connect("localhost:9092")
    client.produce("nonexistent-topic", b"data")
except TopicNotFoundError as e:
    print(f"Error: {e}")
    print(f"Hint: {e.hint}")
    # Output: Create the topic first with client.create_topic("nonexistent-topic", partitions)
    #         or use the CLI: flymq-cli topic create nonexistent-topic
```

**Java:**
```java
import com.firefly.flymq.exception.FlyMQException;

try {
    client.produce("nonexistent-topic", data);
} catch (FlyMQException e) {
    System.err.println("Error: " + e.getMessage());
    System.err.println("Hint: " + e.getHint());
    // Or get full message with hint included
    System.err.println(e.getFullMessage());
}
```

### Factory Methods for Common Errors

Both SDKs provide factory methods for creating exceptions with appropriate hints:

```java
// Java
throw FlyMQException.connectionFailed("localhost:9092", cause);
throw FlyMQException.topicNotFound("my-topic");
throw FlyMQException.authenticationFailed("alice");
throw FlyMQException.timeout("produce");
```

```python
# Python
raise FlyMQError.connection_failed("localhost:9092")
raise FlyMQError.topic_not_found("my-topic")
raise FlyMQError.authentication_failed("alice")
```

## Performance Characteristics

### Throughput Considerations

| Factor | Impact | Recommendation |
|--------|--------|----------------|
| Batch Size | Higher batch = better throughput | Use fetch() for bulk reads |
| Compression | Reduces network I/O | Enable for large messages |
| Partitions | More partitions = more parallelism | Scale with consumers |
| Replication | Higher factor = lower write speed | Balance durability vs speed |

### Latency Factors

1. **Network Round Trip**: ~1-5ms (LAN), ~20-100ms (WAN)
2. **Serialization**: ~0.1-1ms per message
3. **Encryption**: ~0.5-2ms per message
4. **Disk I/O**: ~1-10ms (SSD), ~5-20ms (HDD)

### Server-Side Zero-Copy I/O

FlyMQ uses platform-specific zero-copy optimizations for large message transfers (≥64KB):

| Platform | Optimization | Benefit |
|----------|--------------|---------|
| **Linux** | `sendfile()` + `splice()` | Full zero-copy, ~30% less CPU |
| **macOS** | `sendfile()` | Zero-copy reads, ~20% less CPU |
| **Windows** | Buffered I/O | Graceful fallback |

These optimizations are automatic and transparent to SDK clients. Large message consumers benefit from reduced server CPU usage and lower latency.

### Optimization Strategies

**Python - Using HighLevelProducer:**
```python
from pyflymq import connect

client = connect("localhost:9092")

# Strategy 1: Batching with HighLevelProducer
with client.producer(batch_size=1000, linger_ms=50) as producer:
    for i in range(100000):
        producer.send("topic", f"data-{i}".encode(), key=f"key-{i}")
    producer.flush()  # Ensure all sent

# Strategy 2: Fire-and-forget for maximum throughput
with client.producer(batch_size=5000, linger_ms=100) as producer:
    for data in large_dataset:
        producer.send("topic", data)  # Don't wait for result
    producer.flush()

# Strategy 3: Parallel consumers
with client.consumer("topic", "group", max_poll_records=500) as consumer:
    for msg in consumer:
        process(msg)  # Auto-commit handles offsets
```

**Java - Using HighLevelProducer:**
```java
try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {

    // Strategy 1: Batching with HighLevelProducer
    ProducerConfig config = ProducerConfig.builder()
        .batchSize(1000)
        .lingerMs(50)
        .build();

    try (HighLevelProducer producer = client.producer(config)) {
        for (int i = 0; i < 100000; i++) {
            producer.send("topic", ("data-" + i).getBytes());
        }
        producer.flush();
    }

    // Strategy 2: Parallel consumers with poll
    try (Consumer consumer = client.consumer("topic", "group")) {
        consumer.subscribe();
        while (running) {
            var messages = consumer.poll(Duration.ofSeconds(1));
            messages.parallelStream().forEach(this::process);
        }
    }
}
```

## Design Decisions

### Why Binary Protocol?

- **Efficiency**: 3-5x faster than JSON/HTTP
- **Compact**: Minimal overhead per message
- **Type Safety**: Clear field definitions
- **Performance**: Direct memory operations

### Why MessagePack?

- **Speed**: Faster than JSON, Protocol Buffers
- **Size**: More compact than JSON
- **Simplicity**: No schema compilation needed
- **Language Support**: Available in all major languages

### Why Not Kafka Protocol?

- **Simplicity**: Kafka protocol is complex
- **Flexibility**: Custom features easier to add
- **Learning**: Educational value in custom protocol
- **Control**: No dependency on Kafka ecosystem

## Production Deployment Considerations

### Capacity Planning

```
Messages/sec = Producers × Messages per Producer / sec
Storage = Messages/sec × Message Size × Retention Period
Memory = Active Partitions × Buffer Size × Consumer Groups
Network = (Messages/sec × Message Size × Replication Factor) × 2
```

### High Availability Setup

```
Minimum Production Setup:
- 3 brokers (tolerate 1 failure)
- Replication factor: 2
- Min in-sync replicas: 2
- Consumer groups for failover
- Health checks and monitoring
```

### Monitoring Metrics

1. **Client Metrics**
   - Connection count
   - Request latency
   - Error rates
   - Retry counts

2. **Broker Metrics**
   - Message throughput
   - Storage usage
   - Consumer lag
   - Replication lag

## Conclusion

Understanding FlyMQ's architecture enables you to:
- Design efficient messaging patterns
- Optimize for your use case
- Troubleshoot issues effectively
- Scale applications properly

For implementation details, refer to:
- [Python SDK Documentation](python/docs/GETTING_STARTED.md)
- [Java SDK Documentation](java/docs/GETTING_STARTED.md)
- [SDK Examples](START_HERE.md)