# Changelog

All notable changes to FlyMQ will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to Month.Year.Patch versioning (e.g., v1.26.1 = January 2026, Patch 1).

## [1.26.9] - 2026-01-17

### Added

#### Encryption Key Security Enhancements
- **Environment Variable Only** - Encryption key must be set via `FLYMQ_ENCRYPTION_KEY` environment variable
  - Removed support for encryption key in config files (security best practice)
  - Key never written to disk or logged
  - Clear error messages guide users to set the environment variable
- **Startup Verification** - Server verifies encryption key on startup
  - Validates key is present when encryption is enabled
  - Validates key format and length requirements
  - Fails fast with clear error if key is invalid
- **Separate Secrets File** - Installer stores encryption key in dedicated secrets file
  - `/etc/flymq/secrets.env` with restricted permissions (600)
  - Sourced by systemd service before starting FlyMQ
  - Keeps secrets separate from main configuration

#### Cluster Encryption Key Validation
- **Cross-Node Validation** - All cluster nodes must use the same encryption key
  - Validates encryption enabled/disabled matches across nodes
  - Validates encryption key fingerprint matches (SHA-256 hash of key)
  - Prevents data corruption from mismatched encryption settings
- **Raft-Level Validation** - Encryption validated on every heartbeat
  - `AppendEntriesRequest/Response` includes encryption metadata
  - Continuous validation, not just at node join time
  - Clear error logs when encryption mismatch detected
- **Membership Validation** - New nodes validated before joining cluster
  - `ErrEncryptionMismatch` error for mismatched encryption settings
  - Prevents nodes with wrong key from joining cluster

#### Partition-Level Leadership for Horizontal Scaling
- **Partition Leader Distribution** - Each partition can have a different leader node
  - Enables horizontal write scaling across cluster nodes (like Kafka)
  - `PartitionLeaderInfo` struct with routing information (leader address, epoch)
  - `PartitionAssignment` extended with `LeaderAddr`, `ReplicaAddrs`, and `Epoch` fields
- **Cluster Metadata API** - New endpoint for smart client routing
  - `GET /cluster/metadata` - Returns partition-to-node mappings
  - `GET /cluster/partitions` - Detailed partition assignments with ISR status
  - Enables clients to route requests directly to partition leaders

#### mDNS Service Discovery
- **`flymq-discover` Tool** - New CLI tool for automatic node discovery
  - Discovers FlyMQ nodes on local network using mDNS (Bonjour/Avahi)
  - Supports `--json`, `--quiet`, and `--timeout` flags
  - Used by installer for zero-configuration cluster joining
- **Discovery Service** - mDNS advertisement and discovery (`internal/cluster/discovery.go`)
  - Service type: `_flymq._tcp.local`
  - Publishes node_id, cluster_id, client_port, version in TXT records

#### CLI Partition Management Commands
- **`cluster metadata`** - Get partition-to-node mappings for smart routing
- **`cluster partitions`** - List all partition assignments with replicas and ISR
- **`cluster leaders`** - Show leader distribution across nodes
- **`cluster rebalance`** - Trigger partition rebalance for even distribution
- **`cluster reassign`** - Reassign a partition to a new leader

#### SDK Cluster Metadata Support
- **Python SDK** - `client.get_cluster_metadata(topic)` method
  - Returns `ClusterMetadata` with `TopicPartitionInfo` and `PartitionInfo`
- **Java SDK** - `ClusterMetadata` class with partition routing helpers
  - `getPartitionLeader(topic, partition)` for direct leader lookup
  - `getTopicPartitions(topic)` for all partition info

#### Platform-Specific Zero-Copy I/O
- **Linux Zero-Copy** - Full zero-copy support using kernel syscalls
  - `sendfile()` for file-to-socket transfers (consuming messages)
  - `splice()` for socket-to-file transfers (receiving replicated data)
  - Uses `golang.org/x/sys/unix` for cross-compilation support
- **macOS/Darwin Zero-Copy** - Native sendfile support
  - Darwin-specific `sendfile()` syscall (different signature than Linux)
  - Falls back to buffered I/O for socket-to-file (no splice on Darwin)
- **Windows/Others Fallback** - Graceful degradation
  - Automatic fallback to buffered I/O on unsupported platforms
  - `IsZeroCopySupported()` API to check platform capabilities
- **Build Tags** - Platform-specific files with Go build constraints
  - `zerocopy_linux.go` - Linux implementation
  - `zerocopy_darwin.go` - macOS implementation
  - `zerocopy_others.go` - Fallback for Windows, BSD, etc.

### Changed
- **Broker Interface** - Extended `Cluster` interface with partition-level leadership methods
  - `IsPartitionLeader()`, `GetPartitionLeaderInfo()`, `GetAllPartitionLeaders()`
  - `ProposeAssignPartition()`, `TriggerRebalance()`, `ReassignPartition()`
- **Swagger/OpenAPI** - Added cluster metadata and partition endpoints to API spec
- **Installer** - Enhanced with mDNS discovery integration for cluster setup
  - Displays platform-specific zero-copy I/O status during installation
  - Improved git clone error handling and cleanup
  - Simplified source detection and dependency checks
  - Clones repository automatically when running via curl
- **Zero-Copy Architecture** - Refactored to platform-specific implementations
  - Moved from single `zerocopy.go` to platform-specific files
  - Improved cross-compilation support for all target platforms
- **Encryption Configuration** - Encryption key now required via environment variable only
  - Removed `encryption.key` from config file support
  - Added `encryption.key_fingerprint` for cluster validation

---

## [1.26.8] - 2026-01-16

### Added

#### Kafka-Style RecordMetadata API
- **RecordMetadata on Produce** - All `produce()` methods now return complete `RecordMetadata` (like Kafka)
  - `topic` - Topic name where message was written
  - `partition` - Partition number assigned
  - `offset` - Offset within the partition
  - `timestamp` - Server-side timestamp (milliseconds since epoch)
  - `key_size` - Size of the key (-1 if no key)
  - `value_size` - Size of the value
- **Simplified API** - Removed duplicate methods (`produceWithMetadata`, `produce_with_metadata`)
  - Go: `meta, err := client.Produce(topic, data)` returns `*RecordMetadata`
  - Python: `meta = client.produce(topic, data)` returns `RecordMetadata`
  - Java: `RecordMetadata meta = client.produce(topic, data)` returns `RecordMetadata`

#### High-Level APIs (Kafka-like)
- **Python SDK**
  - `connect()` - One-liner connection function
  - `HighLevelProducer` - Batching, callbacks, automatic retries
  - `HighLevelConsumer` - Auto-commit, poll-based consumption, seek operations
  - `client.producer()` / `client.consumer()` - Factory methods
- **Java SDK**
  - `FlyMQClient.connect()` - Static factory method
  - `HighLevelProducer` - Batching with `CompletableFuture`, callbacks, retries
  - `ProducerConfig` - Builder pattern for configuration
  - `ProduceMetadata` - Extended with timestamp, keySize, valueSize
  - `client.producer()` / `client.consumer()` - Factory methods

#### Enhanced Error Handling
- **Python SDK** - Exceptions with `hint` property providing actionable suggestions
  - `TopicNotFoundError`, `AuthenticationError`, `ConnectionError`, `TimeoutError`
- **Java SDK** - `FlyMQException` with `getHint()` and `getFullMessage()` methods
  - Factory methods: `FlyMQException.topicNotFound()`, `FlyMQException.authenticationFailed()`

#### CLI Improvements
- **RecordMetadata Display** - Produce command shows full metadata (topic, partition, offset, timestamp)
- **Improved Help** - Commands organized by category with clear descriptions
- **Better Error Messages** - Errors include hints and suggestions

### Changed
- **Breaking Change**: `produce()` return type changed from `long`/`int`/`uint64` to `RecordMetadata`
  - Migration: Replace `offset = client.produce(...)` with `meta = client.produce(...); offset = meta.offset`
- **Protocol Enhancement**: Binary produce response now includes timestamp (int64)

### Fixed
- **Java Spring WebFlux** - Updated `ReactiveFlyMQClient` and `ReactiveTransaction` for new API
- **Java Transaction** - `Transaction.produce()` now returns `RecordMetadata`

---

## [1.26.7] - 2026-01-15

### Added

#### Admin API Security & Authentication
- **Endpoint Security Tiers** - Fine-grained access control for Admin API
  - Public endpoints: `/api/v1/health`, `/swagger/*` (no auth required)
  - Read permission: `/api/v1/cluster`, `/api/v1/cluster/nodes`, topic/group GET operations
  - Admin permission: `/api/v1/metrics`, `/api/v1/stats`, user/ACL management
- **Auto-enable Admin Auth** - Admin API authentication automatically enabled when `auth.enabled: true`
- **Rich Stats Endpoint** - New `/api/v1/stats` with comprehensive JSON metrics
  - Cluster overview (node count, topic count, total messages)
  - Per-node statistics (memory, goroutines, message counts)
  - Topic statistics (partitions, message counts)
  - Consumer group statistics (members, lag)
  - System metrics (uptime, Go version, GC stats)

#### Documentation & Deployment
- **Swagger UI** - Interactive API documentation at `/swagger/`
- **Cloud Deployment Guides** - Comprehensive guides for AWS, Azure, GCP
- **TLS Configuration Docs** - Detailed TLS/SSL setup documentation

### Fixed
- **CLI Admin Options** - Fixed `--admin-user`, `--admin-pass` argument parsing
- **Swagger UI Layout** - Fixed "No layout defined for StandaloneLayout" error

---

## [1.26.6] - 2026-01-14

### Added

#### Authentication & Role-Based Access Control (RBAC)
- **Username/password authentication** - Secure authentication with bcrypt password hashing
  - `auth` command to authenticate with the server
  - `whoami` command to check current authentication status
  - Auto-authentication via `FLYMQ_USERNAME` and `FLYMQ_PASSWORD` environment variables
- **Role-Based Access Control (RBAC)** - Fine-grained permission system
  - Built-in roles: `admin` (full access), `producer` (write), `consumer` (read), `guest` (public only)
  - Role-based permission checking for all operations
- **User Management** - Complete user lifecycle management
  - `users list`, `users create`, `users update`, `users delete`, `users passwd`
- **Topic-Level ACLs** - Per-topic access control
  - `acl list`, `acl get`, `acl set`, `acl delete`
- **Cluster Replication** - User and ACL changes replicated via Raft consensus

#### Official Client SDKs
- **Python SDK** (`sdk/python/pyflymq`)
  - Full FlyMQ protocol implementation
  - AES-256-GCM encryption support
  - Authentication and consumer group management
  - Reactive streams support using RxPY
- **Java SDK** (`sdk/java/`) - Multi-module Maven project
  - `flymq-client-core` - Core client library
  - `flymq-client-spring` - Spring Boot auto-configuration
  - `flymq-client-spring-webflux` - Spring WebFlux reactive client
- **Go Client** (`pkg/client`) - Authentication, user/ACL management

#### Key-Based Message Partitioning
- **Kafka-style key support** - Messages with same key routed to same partition
- **CLI**: `--key` flag for produce, `--show-key` for consume
- **All SDKs**: Key support in produce, consume, and fetch operations

#### Consumer Group Management
- **`groups` command** - Comprehensive consumer group management
  - `groups list`, `groups describe`, `groups lag`, `groups reset-offsets`, `groups delete`

---

## [1.26.5] - 2026-01-13

### Added

#### Performance Optimizations
- **Binary-Only Protocol** - Complete migration to binary protocol (JSON removed)
  - ~30% less protocol overhead compared to JSON
  - Server handlers accept and respond with binary format only
- **Zero-Copy Infrastructure** - Server-side zero-copy for large messages
  - `handleConsumeZeroCopy()` - Uses sendfile() for large message transfer
  - `handleFetchBinary()` - Binary-encoded batch fetch responses
  - Threshold-based: Binary response for messages ≥64KB
- **Large Message Handling** - Optimized path for 100KB+ messages
  - FlyMQ 10% faster than Kafka for 100KB messages
  - p50 latency 7% lower (0.74ms vs 0.80ms)

#### Protocol Operations
- `OpConsumeZeroCopy` (0x0A) - Zero-copy consume using sendfile()
- `OpFetchBinary` (0x0B) - Binary-encoded batch fetch

### Changed
- **Overall Performance** - 1.60x faster throughput than Kafka on average
- **Latency** - 1.22x lower latency (p50: 0.31ms vs 0.38ms)

---

## [1.26.4] - 2026-01-12

### Added

#### Clustering and Replication
- **Raft Consensus** - Leader election using Raft consensus algorithm
  - Persistent state management (term, votedFor, log)
  - Election timeout with randomization
  - Heartbeat mechanism for leader liveness
  - Log entry replication to followers
- **Log Replication** - Replicate data across cluster nodes
  - Replica fetcher for pulling data from leader
  - High watermark tracking for replication progress
- **Automatic Failover** - Automatic leader election on node failure
  - Member health checking with suspect/dead states
  - Automatic partition leader election from ISR
- **Cluster Membership Management** - Dynamic cluster membership
  - Member join/leave events
  - Gossip-based health checking
  - Persistent membership state
- **Partition Reassignment** - Load balancing across nodes
  - In-sync replica (ISR) tracking
  - Leader election from ISR

#### Advertise Address Auto-Detection
- **Automatic IP detection** for cluster communication
  - Docker containers, Kubernetes, AWS EC2, GCP, Azure

#### Rancher-Style Join Bundle
- **Join bundle system** - Single-file cluster join workflow
  - Server creates `flymq-join-bundle.tar.gz` with certs and config
  - Agents extract bundle with `--bundle` flag

### Fixed
- **Cluster replication timeout** - Fixed ReplicationTimeout defaulting to 0
- **OOM in cluster benchmarks** - Switched to leader-local writes (like Kafka's ISR model)
- **Memory leak in Raft** - Fixed heartbeat and replication log copy patterns
- **Unbounded Raft log growth** - Added automatic log compaction

---

## [1.26.3] - 2026-01-12

### Added

#### Observability
- **Prometheus Metrics Endpoint** - HTTP metrics server
  - Message throughput metrics
  - Latency histograms
  - Error counters
  - Topic-level metrics
  - Consumer group lag metrics
- **OpenTelemetry Tracing** - Distributed tracing support
  - Span creation and propagation
  - W3C Trace Context support
  - Configurable sampling
- **Health Check Endpoints** - HTTP health endpoints
  - Liveness probe (/health/live)
  - Readiness probe (/health/ready)
  - Kubernetes-compatible responses

#### Admin API
- **REST API** - HTTP API for cluster management
  - Cluster information endpoint
  - Node management
  - Topic operations (list, create, delete)
  - Consumer group management
  - Schema registry operations

---

## [1.26.2] - 2026-01-11

### Added

#### Message Schemas with Validation
- **Schema Registry** - Centralized schema management
  - JSON Schema validation support
  - Avro schema support
  - Protobuf schema support
  - Schema versioning and evolution

#### Dead Letter Queues (DLQ)
- **Failed Message Routing** - Automatic routing of failed messages
  - Configurable retry policies
  - Exponential backoff support
- **DLQ Management** - List, replay, purge operations

#### Message TTL and Expiration
- **Time-Based Expiration** - Messages expire after TTL
  - Per-message and topic-level TTL
  - Automatic cleanup of expired messages

#### Delayed Message Delivery
- **Scheduled Delivery** - Messages delivered after delay
  - Per-message delay configuration
  - Priority queue implementation

#### Transaction Support
- **Exactly-Once Semantics** - Transactional message production
  - Begin/Commit/Rollback operations
  - Transaction coordinator
  - Multi-message transactions

### Protocol Operations
- Schema: `OpRegisterSchema` (0x10), `OpGetSchema` (0x11), `OpListSchemas` (0x12)
- DLQ: `OpGetDLQMessages` (0x20), `OpReplayDLQ` (0x21), `OpPurgeDLQ` (0x22)
- Delayed: `OpProduceDelayed` (0x30), `OpCancelDelayed` (0x31)
- TTL: `OpProduceWithTTL` (0x35)
- Transaction: `OpBeginTx` (0x40), `OpCommitTx` (0x41), `OpAbortTx` (0x42), `OpProduceTx` (0x43)

---

## [1.26.1] - 2026-01-10

### Added

#### Core Messaging Engine
- **Segmented Log Storage** - Write-Ahead Log (WAL) style append-only storage
  - Segment-based log files with configurable size limits
  - Memory-mapped index files for fast offset lookups
- **Topics and Partitions** - Message organization
  - Multiple partitions per topic
  - Partition-based parallelism
- **Consumer Groups** - Offset tracking and persistence
  - Group-based consumption
  - Offset commit and reset

#### Protocol
- **Custom Binary Protocol** - Magic byte 0xAF for efficient communication
- `OpProduce` (0x01) - Produce messages to topics
- `OpConsume` (0x02) - Consume single message by offset
- `OpCreateTopic` (0x03) - Create new topics
- `OpMetadata` (0x04) - Retrieve topic metadata
- `OpSubscribe` (0x05) - Subscribe with consumer groups
- `OpCommit` (0x06) - Commit consumer offsets
- `OpFetch` (0x07) - Batch fetch messages
- `OpListTopics` (0x08) - List all topics
- `OpDeleteTopic` (0x09) - Delete topics

#### CLI Tool
- `produce` - Send messages to topics
- `consume` - Read messages by offset
- `subscribe` - Subscribe to topics (earliest/latest/commit modes)
- `topics` - List and manage topics
- `fetch` - Batch fetch messages

#### Go Client SDK
- Full protocol implementation
- TLS support
- Connection pooling
- Automatic reconnection

#### Security
- **TLS 1.2+** - Modern cipher suites
- **AES-256-GCM Encryption** - Data-at-rest encryption
- **mTLS Support** - Client certificate authentication

#### Configuration
- Environment variables
- JSON/YAML config files
- Structured logging with JSON output

---

[1.26.9]: https://github.com/firefly-oss/flymq/compare/v1.26.8...v1.26.9
[1.26.8]: https://github.com/firefly-oss/flymq/compare/v1.26.7...v1.26.8
[1.26.7]: https://github.com/firefly-oss/flymq/compare/v1.26.6...v1.26.7
[1.26.6]: https://github.com/firefly-oss/flymq/compare/v1.26.5...v1.26.6
[1.26.5]: https://github.com/firefly-oss/flymq/compare/v1.26.4...v1.26.5
[1.26.4]: https://github.com/firefly-oss/flymq/compare/v1.26.3...v1.26.4
[1.26.3]: https://github.com/firefly-oss/flymq/compare/v1.26.2...v1.26.3
[1.26.2]: https://github.com/firefly-oss/flymq/compare/v1.26.1...v1.26.2
[1.26.1]: https://github.com/firefly-oss/flymq/releases/tag/v1.26.1
