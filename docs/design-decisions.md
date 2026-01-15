# FlyMQ Design Decisions

This document explains the key design decisions made in FlyMQ and the trade-offs involved.

## 1. Embedded-First Architecture

### Decision
FlyMQ is designed as an embeddable library first, with standalone server mode as a secondary deployment option.

### Rationale
- **Simplicity**: No external dependencies for simple use cases
- **Performance**: Zero network overhead when embedded
- **Testing**: Easy to test applications without external services
- **Flexibility**: Same codebase supports both embedded and distributed modes

### Trade-offs
- **Scalability**: Embedded mode is limited to single-process
- **Isolation**: Shared memory space with application
- **Operations**: No separate monitoring/management for embedded mode

### Alternatives Considered
- Server-only architecture (like Kafka)
- Sidecar pattern (like Dapr)

---

## 2. FlyDB as Storage Backend

### Decision
Use FlyDB as the underlying storage engine instead of building custom storage or using other databases.

### Rationale
- **Optimized for append-only workloads**: FlyDB's LSM-tree structure is ideal for message logs
- **Embedded**: No external database dependency
- **Go-native**: Same language, easy integration
- **Proven**: FlyDB is battle-tested in production

### Trade-offs
- **Coupling**: Tied to FlyDB's capabilities and limitations
- **Features**: Some advanced storage features require custom implementation

### Alternatives Considered
- Custom append-only log (like Kafka)
- BadgerDB
- BoltDB
- SQLite

---

## 3. Partition-Based Parallelism

### Decision
Topics are divided into partitions, with each partition being the unit of parallelism and ordering.

### Rationale
- **Scalability**: Partitions can be distributed across consumers
- **Ordering**: Messages within a partition maintain strict order
- **Familiarity**: Follows Kafka's proven model

### Trade-offs
- **Complexity**: Users must understand partitioning
- **Rebalancing**: Partition reassignment during scaling
- **Hot partitions**: Uneven key distribution can cause imbalance

### Alternatives Considered
- Single queue per topic (simpler but less scalable)
- Virtual partitions (more flexible but complex)

---

## 4. Pull-Based Consumption

### Decision
Consumers pull messages from the server rather than having messages pushed to them.

### Rationale
- **Backpressure**: Consumers control their own rate
- **Batching**: Consumers can batch based on their capacity
- **Simplicity**: No need for consumer-side flow control
- **Replayability**: Easy to re-read messages from any offset

### Trade-offs
- **Latency**: Slight delay compared to push (mitigated by long polling)
- **Polling overhead**: Consumers must continuously poll

### Alternatives Considered
- Push-based delivery (like RabbitMQ)
- Hybrid push/pull

---

## 5. Consumer Groups for Scaling

### Decision
Implement consumer groups where partitions are distributed among group members.

### Rationale
- **Horizontal scaling**: Add consumers to increase throughput
- **Fault tolerance**: Partitions reassigned on consumer failure
- **Exactly-once semantics**: Partition ownership enables deduplication

### Trade-offs
- **Rebalancing storms**: Many consumers joining/leaving causes disruption
- **Complexity**: Group coordination adds implementation complexity

---

## 6. At-Least-Once Delivery Default

### Decision
Default to at-least-once delivery semantics with optional exactly-once support.

### Rationale
- **Simplicity**: Easier to implement and reason about
- **Performance**: No coordination overhead for most use cases
- **Flexibility**: Applications can implement idempotency if needed

### Trade-offs
- **Duplicates**: Applications must handle duplicate messages
- **Complexity for exactly-once**: Requires additional configuration

### Delivery Guarantees

| Mode | Guarantee | Use Case |
|------|-----------|----------|
| `AcksNone` | At-most-once | Metrics, logs |
| `AcksLeader` | At-least-once | General messaging |
| `AcksAll` + idempotent | Exactly-once | Financial transactions |

---

## 7. Binary-Only Protocol

### Decision
Use a custom **binary-only** protocol instead of HTTP/REST, gRPC, or JSON-over-TCP.

### Rationale
- **Performance**: ~30% less overhead than JSON encoding
- **Efficiency**: Compact wire format with no parsing overhead
- **Simplicity**: Single encoding format, no format negotiation
- **Batching**: Native support for message batches
- **Compatibility**: Similar to Kafka protocol for familiarity

### Why Binary-Only (No JSON Fallback)
- **Consistency**: One encoding format eliminates edge cases
- **Performance**: JSON parsing adds 10%+ latency
- **Memory**: No string allocation for field names
- **Simplicity**: SDK implementation is straightforward

### Binary Encoding Decisions
- **Big-endian**: Network byte order for cross-platform compatibility
- **Length-prefixed strings**: uint16 length prefix (max 64KB per string)
- **Length-prefixed bytes**: uint32 length prefix (max 4GB per value)
- **Flags byte**: Reserved for future compression support (0x02)

### Trade-offs
- **Tooling**: Requires custom clients and debugging tools
- **Learning curve**: Not as universally understood as HTTP
- **Debugging**: Hex dumps needed instead of readable JSON

### Alternatives Considered
- HTTP/REST (simpler but 3-5x slower)
- gRPC (good performance but adds protobuf dependency)
- JSON-over-TCP (readable but 30% more overhead)
- AMQP (standard but complex)

---

## 8. Offset-Based Message Tracking

### Decision
Use sequential offsets to track message position rather than message IDs.

### Rationale
- **Efficiency**: Single integer vs. UUID storage
- **Ordering**: Natural ordering for sequential reads
- **Compaction**: Easy to identify and remove old messages

### Trade-offs
- **Partition-specific**: Offsets only meaningful within a partition
- **No global ordering**: Cannot order across partitions

---

## 9. Configurable Retention

### Decision
Support both time-based and size-based message retention.

### Rationale
- **Flexibility**: Different use cases need different retention
- **Cost control**: Limit storage growth
- **Compliance**: Meet data retention requirements

### Configuration Options

```yaml
retention:
  # Keep messages for 7 days
  time_ms: 604800000
  
  # Or keep up to 10GB per partition
  bytes: 10737418240
  
  # Check retention every hour
  check_interval_ms: 3600000
```

---

## 10. Synchronous Replication (Future)

### Decision
Plan for synchronous replication with configurable consistency levels.

### Rationale
- **Durability**: Survive node failures
- **Consistency**: Configurable based on use case
- **Availability**: Continue operating during partial failures

### Planned Consistency Levels

| Level | Behavior | Latency | Durability |
|-------|----------|---------|------------|
| `async` | Fire and forget | Lowest | Lowest |
| `leader` | Wait for leader | Medium | Medium |
| `quorum` | Wait for majority | Higher | High |
| `all` | Wait for all replicas | Highest | Highest |

