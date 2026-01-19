# FlyMQ Roadmap

This document outlines the planned features and improvements for FlyMQ.

## Current Status (v1.26.11)

The following features are implemented and available:

### Core Features
- ✅ Segmented log storage engine with memory-mapped indexes
- ✅ Topics and partitions for message organization
- ✅ Custom binary protocol (magic byte 0xAF) for efficient communication
- ✅ Consumer groups with offset tracking and persistence
- ✅ CLI tool for producing, consuming, and managing topics
- ✅ Go client SDK with TLS support
- ✅ Configuration via environment variables and config files
- ✅ Structured logging with JSON output option
- ✅ TLS 1.2+ support for secure client-server communication
- ✅ AES-256-GCM encryption for data-at-rest
- ✅ Subscribe functionality (earliest/latest/commit modes)
- ✅ Batch fetch for efficient message consumption
- ✅ Key-based message partitioning (Kafka-style)
- ✅ **Kafka-style RecordMetadata** - All produce operations return complete metadata
- ✅ **Best-in-Class Topic Filtering** - Pattern-based subscriptions (MQTT-style: `+`, `#`)
- ✅ **Server-Side Filtering** - Content-based message filtering on the broker
- ✅ **Interactive Topic Explorer** - Dashboard for cluster/topic inspection

### Authentication & Security
- ✅ Username/password authentication with bcrypt hashing
- ✅ Role-Based Access Control (RBAC) with built-in roles (admin, producer, consumer, guest)
- ✅ Topic-level ACLs for fine-grained access control
- ✅ User management via CLI and API (create, update, delete, list)
- ✅ Auth replication across cluster nodes via Raft consensus
- ✅ Mutual TLS (mTLS) support for client certificate authentication
- ✅ **Encryption Key Security** - Key via environment variable only (never in config files)
- ✅ **Cluster Encryption Validation** - All nodes must use same encryption key

### Clustering & High Availability
- ✅ Raft consensus for leader election
- ✅ Log replication across nodes
- ✅ Automatic failover
- ✅ Cluster membership management
- ✅ Partition reassignment
- ✅ Rancher-style join bundle for easy cluster deployment
- ✅ Advertise address auto-detection (Docker, Kubernetes, AWS, GCP, Azure)
- ✅ Schema replication across cluster nodes
- ✅ **Partition-Level Leadership** - Horizontal write scaling with per-partition leaders
- ✅ **mDNS Service Discovery** - Zero-configuration cluster node discovery
- ✅ **Cluster Metadata API** - Partition-to-node mappings for smart client routing
- ✅ **gRPC Support** - High-performance gRPC API for production and consumption
  - gRPC health check protocol for load balancer integration
  - Connection keepalive for long-lived connections
  - Graceful shutdown with health status signaling
- ✅ **WebSocket Gateway** - Direct browser-based messaging support
  - Ping/pong heartbeat for connection health monitoring
  - Configurable origin checking for CORS security
  - Write timeouts and exponential backoff
- ✅ **MQTT Bridge** - Basic MQTT v3.1.1 protocol bridging support
  - Read/write timeouts for connection management
  - Exponential backoff for subscription errors

### Performance Improvements
- ✅ Zero-copy message delivery
- ✅ Batch compression (LZ4, Snappy, Zstd, Gzip)
- ✅ Connection multiplexing
- ✅ Async disk I/O
- ✅ **Automatic SDK Batching** - Optimized client-side message grouping

### Advanced Messaging Features
- ✅ Message schemas with validation (JSON Schema, Avro, Protobuf)
  - Real Avro validation using `hamba/avro` library
  - Protobuf wire format validation
  - Schema registry REST API integration
- ✅ Dead letter queues with retry policies
- ✅ **DLQ Re-injection** - Manual re-injection of failed messages by offset
- ✅ Message TTL and expiration
- ✅ Delayed message delivery
- ✅ Transaction support (exactly-once semantics)
- ✅ **Typed SerDe System** - Pluggable encoders/decoders for multiple payload formats
- ✅ **Consumer Group Member Tracking** - Active member tracking with automatic cleanup

### Observability
- ✅ Prometheus metrics endpoint
- ✅ OpenTelemetry tracing
- ✅ Health check endpoints (liveness/readiness probes)
- ✅ Admin API for cluster management
- ✅ Consumer group lag monitoring

### Client SDKs
- ✅ Go client with full feature support
- ✅ Python SDK (`pyflymq`) with authentication, consumer groups, encryption, and compression
- ✅ Java SDK with Spring Boot, WebFlux integration, and compression
- ✅ **High-Level APIs** - Kafka-like `producer()` and `consumer()` patterns
- ✅ **Multi-Partition Aware** - High-level consumers automatically discover and consume from all partitions
- ✅ **RecordMetadata** - Complete metadata returned from all produce operations
- ✅ **connect() Factory** - One-liner connection in Python and Java
- ✅ **Enhanced Exceptions** - Errors with hints and actionable suggestions
- ✅ **Cluster Metadata API** - `get_cluster_metadata()` for smart partition routing
- ✅ **Server-Side Filtering Support** - Content filters in all SDK consumers
- ✅ **Advanced SerDe Support** - Typed objects with JSON/String/Binary encoders
- ✅ **Java ConsumerGroup** - High-level pattern subscription support with multi-partition awareness
- ✅ **Automatic Compression** - Transparent Gzip compression in Python and Java SDKs

### Deployment & Operations
- ✅ Interactive installer for Linux, macOS, Windows
  - Installation verification with version checking
  - Backup before removal option in uninstaller
- ✅ Docker deployment with enterprise setup script
- ✅ Cloud deployment guides (AWS, Azure, GCP)
- ✅ On-premise deployment documentation
- ✅ Systemd service integration

---

## Future Considerations

These features are under consideration for future versions:

### Protocol Enhancements
- Binary payload encoding (Protocol Buffers, MessagePack)
- Tiered storage (hot/warm/cold)
- S3-compatible object storage backend
- Compaction strategies
- Log segment archival to object storage

### Ecosystem Integration
- Kafka protocol compatibility layer
- Connect framework for data pipelines
- Stream processing primitives
- Schema Registry UI

### Additional SDKs
- Node.js/TypeScript SDK
- Rust SDK
- C# / .NET SDK

### Enterprise Features
- Multi-tenancy support
- Quotas and rate limiting
- ~~Audit logging~~ ✅ Implemented in v1.26.9
- LDAP/SAML/OIDC authentication providers

---

## Contributing

We welcome contributions! If you're interested in working on any of these features,
please open an issue to discuss the implementation approach before submitting a PR.

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Feedback

Have ideas for features not listed here? Open a GitHub issue with the `enhancement` label.

