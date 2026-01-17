# FlyMQ Architecture

FlyMQ is a high-performance distributed message queue written in Go. It provides persistent storage, partitioned topics, consumer groups, and enterprise-grade features including clustering, schema validation, and observability.

## Design Philosophy

FlyMQ was designed with the following principles in mind:

1. **Kafka-Compatible Concepts**: If you know Apache Kafka, you already know FlyMQ. We use the same fundamental concepts (topics, partitions, consumer groups, offsets) so teams can adopt FlyMQ without learning new paradigms.

2. **Performance by Default**: The default configuration (`acks=leader`) provides excellent throughput (~7,000+ msg/s) while maintaining reasonable durability—the same trade-off Kafka makes with `acks=1`. You can tune for more safety (`acks=all`) or more speed (`acks=none`) as needed.

3. **Binary Protocol for Efficiency**: Unlike HTTP/REST-based message queues, FlyMQ uses a compact binary protocol that eliminates JSON parsing overhead, resulting in ~30% lower latency and bandwidth usage.

4. **Single Binary Simplicity**: FlyMQ deploys as a single Go binary with no external dependencies (no ZooKeeper, no JVM). This makes deployment, operation, and debugging dramatically simpler.

5. **Enterprise Features Built-In**: Schema validation, dead letter queues, delayed delivery, transactions, and RBAC authentication are all included—not add-ons requiring separate services.

## High-Level Architecture

```
┌──────────────────────────────────────────────────────────┐
│                       FlyMQ Server                       │
├──────────────────────────────────────────────────────────┤
│    ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│    │   Topic A   │  │   Topic B   │  │   Topic C   │     │
│    ├─────────────┤  ├─────────────┤  ├─────────────┤     │
│    │ Partition 0 │  │ Partition 0 │  │ Partition 0 │     │
│    │ Partition 1 │  │ Partition 1 │  │ Partition 1 │     │
│    │ Partition 2 │  │ Partition 2 │  │ Partition 2 │     │
│    └─────────────┘  └─────────────┘  └─────────────┘     │
├──────────────────────────────────────────────────────────┤
│               Storage Layer (Segmented Log)              │
└──────────────────────────────────────────────────────────┘
         ▲                    ▲                    ▲
         │                    │                    │
    ┌────┴────┐          ┌────┴────┐          ┌────┴────┐
    │Producer │          │Consumer │          │Consumer │
    │         │          │Group A  │          │Group B  │
    └─────────┘          └─────────┘          └─────────┘
```

## Core Components

### 1. Server (`internal/server/`)

The TCP server handles client connections and routes protocol messages to the broker.

**Key responsibilities:**
- Accept TCP/TLS client connections
- Parse FlyMQ binary protocol messages
- Route requests to appropriate handlers
- Manage connection lifecycle and graceful shutdown

**Files:**
- `server.go` - Main server with connection handling and message routing

### 2. Broker (`internal/broker/`)

The broker is the heart of FlyMQ, managing topics, partitions, and message flow.

**Key features:**
- Topic and partition management
- Message production and consumption
- Consumer group coordination with offset tracking
- Auto-creation of topics on first produce

**Files:**
- `broker.go` - Core broker with topic registry and message operations
- `consumer.go` - Consumer group manager with persistent offset storage
- `admin.go` - Admin handler for REST API operations

### 3. Protocol (`internal/protocol/`)

Defines the FlyMQ **binary-only** wire protocol for efficient client-server communication.

> **IMPORTANT**: All payloads use binary encoding. There is NO JSON support.

**Protocol format:**
```
[Magic(1)][Version(1)][OpCode(1)][Flags(1)][Length(4)][Binary Payload(...)]
```

**Binary encoding rules:**
- Strings: `[uint16 length][UTF-8 bytes]`
- Byte slices: `[uint32 length][raw bytes]`
- Integers: Big-endian encoding
- See `docs/protocol.md` for complete specification

**Operation categories:**
- Core (0x01-0x0F): Produce, Consume, Topics, Subscribe, Commit, Fetch
- Schema (0x10-0x1F): Register, Get, List, Validate schemas
- DLQ (0x20-0x2F): Get, Replay, Purge dead letter messages
- Delayed/TTL (0x30-0x3F): Delayed delivery, message expiration
- Transactions (0x40-0x4F): Begin, Commit, Abort, Transactional produce
- Cluster (0x50-0x5F): Join, Leave, Status
- Consumer Groups (0x60-0x6F): Offset management, lag monitoring
- Authentication (0x70-0x7F): Auth, user management, ACLs

**Files:**
- `protocol.go` - Message types, opcodes, and header read/write
- `binary.go` - Binary encoders/decoders for all request/response types

### 4. Storage (`internal/storage/`)

Append-only log storage inspired by Apache Kafka's log-structured design.

**Key features:**
- Length-prefixed records with buffered writes
- Memory-mapped index files for O(1) offset lookups
- Segment-based logs for efficient retention
- Sync-on-write for durability

**Files:**
- `store.go` - Low-level file wrapper with append/read operations
- `index.go` - Memory-mapped index for offset-to-position mapping
- `segment.go` - Combines store and index for a log segment
- `log.go` - Manages multiple segments for a complete partition log

### 5. Cluster (`internal/cluster/`)

Distributed coordination using Raft consensus and SWIM-based gossip.

**Key features:**
- Raft consensus for leader election and log replication
- SWIM gossip protocol for membership management
- **Partition-level leadership** for horizontal scaling (see below)
- Partition assignment and rebalancing
- Automatic failover on node failure
- Stats exchange via Raft heartbeats for cluster-wide monitoring
- Per-request TCP connections for reliable RPC communication

**Files:**
- `raft.go` - Raft consensus implementation with leader tracking and stats collection
- `membership.go` - SWIM-based cluster membership
- `partition.go` - Partition assignment management with leader addresses
- `distributor.go` - Partition distribution strategies (round-robin, least-loaded, rack-aware)
- `replication.go` - Log replication between nodes
- `transport.go` - TCP transport for Raft RPC with per-request connections
- `discovery.go` - mDNS/DNS-SD service discovery for automatic node detection
- `cluster.go` - Main cluster coordinator

**Service Discovery (mDNS):**

FlyMQ supports automatic node discovery using mDNS (Bonjour/Avahi). When enabled:
- Nodes advertise themselves as `_flymq._tcp.local.` with metadata (node ID, cluster address, version)
- New nodes can discover existing cluster members without manual configuration
- The `flymq-discover` CLI tool can scan for nodes on the local network
- Cluster ID filtering prevents cross-cluster discovery when multiple clusters exist

**Raft Protocol Extensions:**
- `AppendEntriesResponse` includes `NodeStats` for distributed monitoring
- Leader tracks peer stats from heartbeat responses
- `StatsCollector` callback provides local node metrics
- Partition metadata commands for leader updates and ISR management

#### Partition-Level Leadership (Horizontal Scaling)

Unlike simple leader-follower architectures where one node handles all writes, FlyMQ supports **partition-level leadership** similar to Apache Kafka. This enables true horizontal scaling:

```
┌─────────────────────────────────────────────────────────────────┐
│                    FlyMQ Cluster (3 Nodes)                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Node 1                Node 2                Node 3             │
│  ┌─────────────┐       ┌─────────────┐       ┌─────────────┐    │
│  │ Topic A     │       │ Topic A     │       │ Topic A     │    │
│  │ P0: LEADER  │◄─────►│ P0: Replica │       │ P0: Replica │    │
│  │ P1: Replica │       │ P1: LEADER  │◄─────►│ P1: Replica │    │
│  │ P2: Replica │       │ P2: Replica │       │ P2: LEADER  │    │
│  └─────────────┘       └─────────────┘       └─────────────┘    │
│                                                                 │
│  Write to P0 ──────────► Node 1 (direct)                        │
│  Write to P1 ──────────► Node 2 (direct)                        │
│  Write to P2 ──────────► Node 3 (direct)                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Key concepts:**

1. **Each partition has its own leader**: Different partitions of the same topic can have different leaders, spreading write load across the cluster.

2. **Smart client routing**: Clients can query cluster metadata to discover partition leaders and route requests directly, avoiding proxy overhead.

3. **Message partitioning strategies**:
   - **Key-based (FNV-1a hash)**: Messages with the same key always go to the same partition, ensuring ordering for related messages
   - **Round-robin**: When no key is provided, messages are distributed evenly across partitions

4. **Leader distribution strategies** (configurable):
   - **round-robin**: Distribute partition leaders evenly in order (default)
   - **least-loaded**: Assign new partitions to the node with fewest leaders
   - **rack-aware**: Consider rack placement for fault tolerance

5. **Automatic rebalancing**: When nodes join or leave, partition leaders can be redistributed to maintain balance.

**Configuration:**
```json
{
  "partition": {
    "distribution_strategy": "round-robin",
    "default_replication_factor": 3,
    "default_partitions": 6,
    "auto_rebalance_enabled": true,
    "auto_rebalance_interval": 300,
    "rebalance_threshold": 0.2
  }
}
```

**Admin API endpoints:**
- `GET /api/v1/cluster/metadata` - Get partition-to-node mappings
- `GET /api/v1/cluster/leaders` - View leader distribution
- `POST /api/v1/cluster/rebalance` - Trigger manual rebalance
- `POST /api/v1/cluster/partitions/{topic}/{partition}` - Reassign partition

**CLI commands:**
```bash
# View partition leader distribution
flymq-cli cluster leaders

# Get partition-to-node mappings for a topic
flymq-cli cluster metadata --topic my-topic

# Trigger rebalance
flymq-cli cluster rebalance

# Reassign a partition to a new leader
flymq-cli cluster reassign my-topic 0 --leader node-2
```

### 6. Config (`internal/config/`)

Comprehensive configuration management with multiple sources.

**Configuration precedence (highest to lowest):**
1. Command-line flags
2. Environment variables (FLYMQ_* prefix)
3. Configuration file (JSON format)
4. Default values

**Files:**
- `config.go` - All configuration structures and environment loading

## Data Flow

### Publishing Messages

```
Producer                    Server                     Topic/Partition
   │                          │                              │
   │── ProduceRequest ───────>│                              │
   │                          │── route by partition key ───>│
   │                          │                              │── append to log
   │                          │<── offset assigned ──────────│
   │<── ProduceResponse ──────│                              │
   │   (with offset)          │                              │
```

### Consuming Messages

```
Consumer                    Server                     Topic/Partition
   │                          │                              │
   │── FetchRequest ─────────>│                              │
   │   (topic, partition,     │── fetch from offset ────────>│
   │    offset)               │                              │
   │                          │<── messages ─────────────────│
   │<── FetchResponse ────────│                              │
   │   (messages batch)       │                              │
   │                          │                              │
   │── OffsetCommit ─────────>│                              │
   │   (mark processed)       │── store offset ─────────────>│
```

## Consumer Groups

Consumer groups enable horizontal scaling by distributing partitions among group members.

```
                    Topic (6 partitions)
            ┌───┬───┬───┬───┬───┬───┐
            │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │
            └─┬─┴─┬─┴─┬─┴─┬─┴─┬─┴─┬─┘
              │   │   │   │   │   │
    ┌─────────┼───┼───┘   │   │   │
    │         │   │       │   │   │
    ▼         ▼   ▼       ▼   ▼   ▼
    ┌────────┐ ┌────────┐ ┌────────┐
    │Consumer│ │Consumer│ │Consumer│
    │   1    │ │   2    │ │   3    │
    │ [0,1]  │ │ [2,3]  │ │ [4,5]  │
    └────────┘ └────────┘ └────────┘
       Consumer Group "analytics"
```

**Partition Assignment Strategies:**
- Range: Assigns contiguous partitions to each consumer
- RoundRobin: Distributes partitions evenly across consumers

## Message Structure

```go
type Message struct {
    Topic     string            // Target topic name
    Partition int32             // Partition number (-1 for auto-assign)
    Key       []byte            // Optional key for partitioning
    Value     []byte            // Message payload
    Headers   map[string]string // Optional metadata headers
    Timestamp time.Time         // Message timestamp
    Offset    int64             // Assigned after publishing
}
```

## Configuration

### Server Options

| Option | Default | Description |
|--------|---------|-------------|
| `ListenAddr` | `:9092` | TCP listen address |
| `DataDir` | `./data` | Storage directory |
| `DefaultPartitions` | `3` | Default partitions for new topics |
| `RetentionBytes` | `1GB` | Max bytes per partition |
| `RetentionMs` | `7 days` | Message retention time |

### Consumer Options

| Option | Default | Description |
|--------|---------|-------------|
| `GroupID` | `""` | Consumer group identifier |
| `AutoCommit` | `true` | Automatic offset commits |
| `AutoCommitInterval` | `5s` | Commit frequency |
| `MaxPollRecords` | `500` | Max messages per poll |

## Additional Components

### 7. Schema Registry (`internal/schema/`)

Centralized schema management with validation support.

**Supported formats:** JSON Schema, Avro, Protobuf

**Files:**
- `registry.go` - Schema storage and versioning
- `validator.go` - Message validation engine

### 8. Dead Letter Queue (`internal/dlq/`)

Automatic routing of failed messages for later analysis.

**Files:**
- `dlq.go` - DLQ manager with retry policies

### 9. TTL & Delayed Delivery (`internal/ttl/`, `internal/delay/`)

Time-based message features.

**Files:**
- `ttl.go` - Message expiration management
- `delay.go` - Scheduled message delivery

### 10. Transactions (`internal/transaction/`)

Exactly-once semantics for atomic message batches.

**Files:**
- `coordinator.go` - Transaction coordinator

### 11. Observability (`internal/metrics/`, `internal/tracing/`, `internal/health/`)

Comprehensive monitoring and diagnostics.

**Files:**
- `metrics.go` - Prometheus metrics endpoint
- `tracing.go` - OpenTelemetry distributed tracing
- `health.go` - Kubernetes-compatible health probes

### 12. Admin API (`internal/admin/`)

REST API for cluster management.

**Files:**
- `admin.go` - HTTP API server

### 13. Performance (`internal/performance/`)

Low-level optimizations for high throughput.

**Files:**
- `zerocopy.go` - Zero-copy I/O core types and interfaces
- `zerocopy_linux.go` - Linux sendfile/splice implementation
- `zerocopy_darwin.go` - macOS sendfile implementation
- `zerocopy_others.go` - Fallback for Windows/BSD
- `compression.go` - LZ4, Snappy, Zstd compression
- `multiplexing.go` - Connection multiplexing
- `asyncio.go` - Async disk I/O with worker pools

**Platform-specific zero-copy:**
- Linux: Full zero-copy with `sendfile()` and `splice()`
- macOS: Zero-copy reads with Darwin `sendfile()`
- Others: Graceful fallback to buffered I/O

## Thread Safety

FlyMQ is designed for concurrent access:

- **Topics**: Thread-safe for concurrent produce/consume operations
- **Partitions**: Each partition has its own lock for append operations
- **Consumer Groups**: Coordinated access through group coordinator
- **Storage**: Segmented log provides concurrent read/write safety
- **Raft**: Careful lock ordering to prevent deadlocks (lock released before stats collection)

## Partitioning Strategies

FlyMQ has two distinct partitioning concepts:

### 1. Message Partitioning (Client-Side)

Determines which partition a message is written to:

**Key-Based Partitioning (Recommended)**
```
Message Key → FNV-1a Hash → Partition = hash % numPartitions
```

**Why FNV-1a?**
- Fast computation with minimal CPU overhead
- Good distribution properties for typical key patterns
- Deterministic: same key always maps to same partition
- Consistent across all SDKs (Go, Python, Java)

**Round-Robin Partitioning**
When no key is provided, messages are distributed evenly across partitions using round-robin.

**Ordering Guarantees:**
- Messages with the same key are always sent to the same partition
- Within a partition, messages maintain strict ordering
- This enables ordered processing of related events (e.g., all events for user-123)

### 2. Leader Distribution (Cluster-Side)

Determines which node is the leader for each partition (see [Partition-Level Leadership](#partition-level-leadership-horizontal-scaling) above):

| Strategy | Description | Use Case |
|----------|-------------|----------|
| `round-robin` | Distribute leaders evenly in order | Default, simple clusters |
| `least-loaded` | Assign to node with fewest leaders | Dynamic workloads |
| `rack-aware` | Consider rack placement | Multi-rack deployments |

## Network Ports

| Port | Purpose | Configuration |
|------|---------|---------------|
| 9092 | Client connections (TCP/TLS) | `bind_addr` |
| 9093 | Cluster communication (Raft) | `cluster_addr` |
| 9094 | Prometheus metrics | `observability.metrics.addr` |
| 9095 | Health check endpoints | `observability.health.addr` |
| 9096 | Admin REST API | `observability.admin.addr` |

## Security Architecture

### TLS/SSL
- Client-to-server encryption with optional mutual TLS (mTLS)
- Cluster-to-cluster encryption for Raft communication
- Certificate-based authentication

### Encryption at Rest
- AES-256-GCM encryption for stored messages
- Key management via configuration or environment variables
- Compatible encryption across all SDKs

