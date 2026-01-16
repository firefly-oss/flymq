# FlyMQ SDK Architecture Guide

## Overview

This document provides a comprehensive understanding of FlyMQ's architecture and how the SDKs interact with the FlyMQ message broker. Understanding this architecture is crucial for building robust, scalable applications.

## Table of Contents

- [System Architecture](#system-architecture)
- [Protocol Design](#protocol-design)
- [Client Architecture](#client-architecture)
- [Message Flow](#message-flow)
- [Partitioning Strategy](#partitioning-strategy)
- [Consumer Groups](#consumer-groups)
- [Transaction Model](#transaction-model)
- [Security Architecture](#security-architecture)
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

```python
FlyMQClient
    ├── Connection Management
    │   ├── TCP Socket
    │   ├── TLS/SSL Context
    │   └── Connection Pool
    ├── Protocol Layer
    │   ├── Binary Encoder/Decoder
    │   ├── MessagePack Serialization
    │   └── Header Management
    ├── High-Level APIs
    │   ├── Producer API
    │   ├── Consumer API
    │   ├── Consumer Groups
    │   └── Transactions
    └── Utilities
        ├── Retry Logic
        ├── Failover Handler
        ├── Encryption (AES-256-GCM)
        └── Authentication
```

### Java SDK Architecture

```java
FlyMQClient
    ├── Connection Management
    │   ├── Socket/SSLSocket
    │   ├── Connection Pool
    │   └── Reconnection Logic
    ├── Protocol Layer
    │   ├── BinaryProtocol
    │   ├── MessagePack Codec
    │   └── Records (POJOs)
    ├── High-Level APIs
    │   ├── Producer API
    │   ├── Consumer API
    │   ├── Consumer Groups
    │   └── Transactions
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
client = FlyMQClient(
    "broker:9092",
    tls_enabled=True,           # Transport encryption
    encryption_key=secret_key    # Message encryption
)
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

### Optimization Strategies

```python
# Strategy 1: Batch operations
messages = []
for i in range(1000):
    messages.append((f"key-{i}", f"data-{i}".encode()))
client.produce_batch("topic", messages)

# Strategy 2: Connection pooling
pool = FlyMQClientPool(size=10)
with pool.get_client() as client:
    client.produce("topic", data)

# Strategy 3: Async processing
import asyncio
async def process():
    async with AsyncFlyMQClient() as client:
        await client.produce("topic", data)
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