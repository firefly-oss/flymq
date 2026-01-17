# FlyMQ Roadmap

This document outlines the planned features and improvements for FlyMQ.

## Current Status (v1.26.9)

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

### Performance Improvements
- ✅ Zero-copy message delivery
- ✅ Batch compression (LZ4, Snappy, Zstd)
- ✅ Connection multiplexing
- ✅ Async disk I/O

### Advanced Messaging Features
- ✅ Message schemas with validation (JSON Schema, Avro, Protobuf)
- ✅ Dead letter queues with retry policies
- ✅ Message TTL and expiration
- ✅ Delayed message delivery
- ✅ Transaction support (exactly-once semantics)

### Observability
- ✅ Prometheus metrics endpoint
- ✅ OpenTelemetry tracing
- ✅ Health check endpoints (liveness/readiness probes)
- ✅ Admin API for cluster management
- ✅ Consumer group lag monitoring

### Client SDKs
- ✅ Go client with full feature support
- ✅ Python SDK (`pyflymq`) with authentication, consumer groups, and encryption
- ✅ Java SDK with Spring Boot and WebFlux integration
- ✅ **High-Level APIs** - Kafka-like `producer()` and `consumer()` patterns
- ✅ **RecordMetadata** - Complete metadata returned from all produce operations
- ✅ **connect() Factory** - One-liner connection in Python and Java
- ✅ **Enhanced Exceptions** - Errors with hints and actionable suggestions
- ✅ **Cluster Metadata API** - `get_cluster_metadata()` for smart partition routing

### Deployment & Operations
- ✅ Interactive installer for Linux, macOS, Windows
- ✅ Docker deployment with enterprise setup script
- ✅ Cloud deployment guides (AWS, Azure, GCP)
- ✅ On-premise deployment documentation
- ✅ Systemd service integration

---

## Future Considerations

These features are under consideration for future versions:

### Protocol Enhancements
- Binary payload encoding (Protocol Buffers, MessagePack)
- HTTP/2 and gRPC support
- WebSocket support for browser clients

### Storage Optimizations
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

