# FlyMQ

```
  _____.__           _____   ________   
_/ ____\  | ___.__. /     \  \_____  \  
\   __\|  |<   |  |/  \ /  \  /  / \  \ 
 |  |  |  |_\___  /    Y    \/   \_/.  \
 |__|  |____/ ____\____|__  /\_____\ \_/
            \/            \/        \__>
```

**High-Performance Distributed Message Queue**

FlyMQ is a distributed message queue system written in Go, designed for high throughput
and low latency. It provides persistent storage, partitioned topics, consumer groups,
and enterprise-grade security features.

---

## Table of Contents

- [Features](#features)
- [Performance](#performance)
- [Quick Start](#quick-start)
- [Key Concepts](#key-concepts) - Topics, partitions, consumer groups, offsets
- [Installation](#installation)
- [Cluster Deployment](#cluster-deployment)
- [Configuration](#configuration)
- [Security](#security)
- [Architecture](#architecture)
- [CLI Reference](#cli-reference)
- [Client SDKs](#client-sdks) - Python and Java
- [Benchmarks](#benchmarks)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

**Additional Documentation:**
- [Consumer Groups Guide](docs/consumer-groups.md) - Detailed guide on offset tracking and consumption patterns

---

## Features

### Core Capabilities
- **Segmented Log Storage** - High-throughput append-only log with memory-mapped indexes
- **Topics and Partitions** - Scalable message organization with configurable partitioning
- **Key-Based Partitioning** - Kafka-style message keys for ordered delivery within partitions
- **Consumer Groups** - Coordinated consumption with offset tracking
- **Custom Binary Protocol** - Efficient wire protocol for minimal overhead

### Clustering & High Availability
- **Raft Consensus** - Leader election and log replication
- **Automatic Failover** - Seamless leader election on node failure
- **Partition Reassignment** - Dynamic load balancing across nodes

### Advanced Messaging
- **Best-in-Class Topic Filtering** - Pattern-based subscriptions using Trie-based matching (MQTT-style: `+`, `#`)
- **Typed SerDe System** - Efficiently handle multiple payload formats (JSON, String, Binary) with built-in Encoders/Decoders
- **Interactive Topic Explorer** - Powerful CLI tool to browse topics, navigate messages, and manage offsets interactively
- **Message Schemas** - JSON Schema, Avro, and Protobuf validation
- **Dead Letter Queues** - Failed message routing with retry policies
- **Message TTL** - Time-based message expiration
- **Delayed Delivery** - Scheduled message delivery
- **Transactions** - Exactly-once semantics with atomic operations

### Observability
- **Prometheus Metrics** - HTTP metrics endpoint for monitoring
- **OpenTelemetry Tracing** - Distributed tracing with W3C Trace Context
- **Health Checks** - Kubernetes-compatible liveness/readiness probes
- **Admin API** - REST API for cluster management

### Security
- **Authentication** - Username/password authentication with bcrypt hashing
- **Role-Based Access Control (RBAC)** - Fine-grained permissions (read, write, admin)
- **Topic-Level ACLs** - Per-topic access control with user and role-based rules
- **User Management** - Create, update, delete users via CLI and API
- **TLS Encryption** - Secure client-server communication with TLS 1.2+
- **Data-at-Rest Encryption** - AES-256-GCM encryption for stored messages
- **Mutual TLS** - Client certificate authentication support
- **Audit Trail** - Comprehensive logging of security-relevant operations for compliance

### Operations
- **Configurable Retention** - Time and size-based message retention policies
- **Structured Logging** - JSON logging with configurable levels
- **Interactive Installer** - Guided setup with configuration generation

---

## Performance

FlyMQ is designed for high performance. Our benchmarks show significant advantages over Apache Kafka:

### FlyMQ vs Kafka Comparison

All benchmarks run in Docker containers with identical resource limits for fair comparison.

#### Standalone Mode (Single Node)

| Metric | FlyMQ | Kafka | FlyMQ Advantage |
|--------|-------|-------|-----------------|
| **Average Throughput** | **15,413 msgs/s** | **10,433 msgs/s** | **1.48x faster** |
| **Peak Throughput** | **30,689 msgs/s** | **22,104 msgs/s** | **1.39x faster** |
| **Latency (p50)** | **0.30ms** | **0.38ms** | **1.29x lower** |

#### 2-Node Cluster (with Replication)

| Metric | FlyMQ | Kafka | FlyMQ Advantage |
|--------|-------|-------|-----------------|
| **Average Throughput** | **14,082 msgs/s** | **8,161 msgs/s** | **1.73x faster** |
| **Peak Throughput** | **28,027 msgs/s** | **18,218 msgs/s** | **1.54x faster** |
| **Latency (p50)** | **0.31ms** | **0.47ms** | **1.53x lower** |

#### 3-Node Cluster (with Replication)

| Metric | FlyMQ | Kafka | FlyMQ Advantage |
|--------|-------|-------|-----------------|
| **Average Throughput** | **11,762 msgs/s** | **6,000 msgs/s** | **1.96x faster** |
| **Peak Throughput** | **22,713 msgs/s** | **14,952 msgs/s** | **1.52x faster** |
| **Latency (p50)** | **0.35ms** | **0.64ms** | **1.86x lower** |

#### Key Performance Highlights

- **Consistent Low Latency**: p50 latency remains under 0.35ms regardless of cluster size
- **Better Cluster Scaling**: Performance advantage increases with cluster size (1.48x → 1.96x)
- **Binary Protocol**: Go client uses efficient binary encoding (~30% less overhead than JSON)
- **Memory Efficiency**: Buffer pooling and reduced allocations under load
- **Zero-Copy I/O**: Platform-specific optimizations (Linux: `sendfile`+`splice`, macOS: `sendfile`)

> **Test Environment**: Apple M3 Pro, 12 cores, 36GB RAM, NVMe SSD (January 16, 2026)
> Results may vary based on hardware. See [benchmarks/](benchmarks/) for full benchmark suite and reproduction instructions.

---

## Quick Start

### Basic Usage

```bash
# Clone the repository
git clone https://github.com/firefly-oss/flymq
cd flymq

# Install
./install.sh

# Start the server (JSON logs by default)
./bin/flymq

# Or with human-readable logs for development
./bin/flymq -human-readable

# In another terminal, produce a message
./bin/flymq-cli produce my-topic "Hello, FlyMQ!"

# Consume messages
./bin/flymq-cli subscribe my-topic --from earliest
```

### Key-Based Messaging (Kafka-style)

```bash
# Produce messages with keys - same key goes to same partition
./bin/flymq-cli produce orders '{"id": 1}' --key user-123
./bin/flymq-cli produce orders '{"id": 2}' --key user-123  # Same partition
./bin/flymq-cli produce orders '{"id": 3}' --key user-456  # May differ

# Consume messages and display keys
./bin/flymq-cli consume orders --show-key
# Output: [0] key=user-123: {"id": 1}

# Stream messages with keys
./bin/flymq-cli subscribe orders --show-key --from earliest
```

### Advanced Feature Examples

```bash
# Delayed message delivery (5 second delay)
./bin/flymq-cli produce-delayed my-topic "Delayed message" 5000

# Message with TTL (expires in 60 seconds)
./bin/flymq-cli produce-ttl my-topic "Expiring message" 60000

# Schema validation
./bin/flymq-cli schema register user-schema json '{"type":"object","properties":{"name":{"type":"string"}}}'
./bin/flymq-cli schema list

# Dead letter queue management
./bin/flymq-cli dlq list my-topic
./bin/flymq-cli dlq stats my-topic

# Transaction support
./bin/flymq-cli txn begin
./bin/flymq-cli txn produce my-topic "Transactional message"
./bin/flymq-cli txn commit

# Health checks
./bin/flymq-cli health live
./bin/flymq-cli health ready

# Cluster management
./bin/flymq-cli cluster status
./bin/flymq-cli cluster members
```

### Authentication & User Management

```bash
# Authenticate with the server
./bin/flymq-cli auth --username admin --password secret

# Check current authentication status
./bin/flymq-cli whoami

# User management (requires admin role)
./bin/flymq-cli users list
./bin/flymq-cli users create alice mypassword --roles producer,consumer
./bin/flymq-cli users update alice --roles admin
./bin/flymq-cli users delete alice

# ACL management
./bin/flymq-cli acl list
./bin/flymq-cli acl set private-topic --users alice,bob --roles admin
./bin/flymq-cli acl set public-topic --public
./bin/flymq-cli acl delete private-topic

# Role management (view available roles)
./bin/flymq-cli roles list
./bin/flymq-cli roles get admin

# Use credentials with other commands
./bin/flymq-cli produce my-topic "Hello" --username admin --password secret
```

---

## Key Concepts

Understanding these core concepts will help you use FlyMQ effectively.

### Topics and Partitions

A **topic** is a named stream of messages. Topics are divided into **partitions** for parallelism and scalability.

```
Topic: "orders"
┌─────────────────────────────────────────────────────────────┐
│  Partition 0: [msg0] [msg1] [msg2] [msg3] ...               │
│  Partition 1: [msg0] [msg1] [msg2] ...                      │
│  Partition 2: [msg0] [msg1] [msg2] [msg3] [msg4] ...        │
└─────────────────────────────────────────────────────────────┘
```

- **Messages within a partition are ordered** - Message 0 always comes before Message 1
- **Messages across partitions have no ordering guarantee** - Partition 0's Message 1 may arrive before or after Partition 1's Message 0
- **Partitions enable parallelism** - Multiple consumers can read different partitions simultaneously

### Message Keys

A **message key** determines which partition a message goes to. Messages with the same key always go to the same partition, guaranteeing order for related messages.

```
Producer sends:
  key="user-123" → hash("user-123") % 3 = Partition 1
  key="user-123" → hash("user-123") % 3 = Partition 1  (same partition!)
  key="user-456" → hash("user-456") % 3 = Partition 0  (different partition)
```

**Use case**: All orders for a user go to the same partition, so they're processed in order.

### Offsets

An **offset** is a message's position within a partition. Offsets start at 0 and increment by 1 for each message.

```
Partition 0:
┌─────┬─────┬─────┬─────┬─────┬─────┐
│  0  │  1  │  2  │  3  │  4  │  5  │  ← Offsets
└─────┴─────┴─────┴─────┴─────┴─────┘
                           ↑
                    Current position
```

- **Offsets are per-partition** - Each partition has its own offset sequence
- **Offsets are immutable** - Once assigned, an offset never changes
- **Consumers track their position** using offsets

### Consumer Groups

A **consumer group** is a named group of consumers that share message processing. FlyMQ tracks each group's position (offset) independently.

```
                        ┌─────────────────────────────────┐
                        │  Topic: "orders"                │
                        │  [0] [1] [2] [3] [4] [5] [6]    │
                        └─────────────────────────────────┘
                                        │
              ┌─────────────────────────┼─────────────────────────┐
              │                         │                         │
              ▼                         ▼                         ▼
       ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
       │ Group: analytics │    │ Group: billing   │    │ Group: shipping  │
       │ Offset: 7        │    │ Offset: 4        │    │ Offset: 2        │
       │ (caught up)      │    │ (processing)     │    │ (behind)         │
       └──────────────────┘    └──────────────────┘    └──────────────────┘
```

**Key behaviors:**
- **Each group receives ALL messages** - Groups are independent
- **Position is tracked per group** - Each group has its own offset
- **Resume on restart** - If your app crashes, it resumes from the last committed offset
- **Multiple consumers in a group share work** - Partitions are distributed among them

### Offset Commits

**Committing an offset** saves your position so you don't reprocess messages after a restart.

```
1. Consumer reads message at offset 5
2. Consumer processes the message successfully
3. Consumer commits offset 6 (next message to read)
4. If consumer crashes and restarts, it resumes from offset 6
```

**Auto-commit vs Manual commit:**
- **Auto-commit** (default): Offsets are committed automatically every 5 seconds
- **Manual commit**: You control exactly when offsets are committed (for exactly-once processing)

### High-Level vs Low-Level APIs

FlyMQ SDKs provide two API levels:

| API Level | Use Case | Features |
|-----------|----------|----------|
| **High-Level** | Most applications | Auto-commit, batching, retries, simple iteration |
| **Low-Level** | Advanced control | Manual offset management, direct partition access |

**Recommendation**: Start with High-Level APIs. Use Low-Level APIs only when you need fine-grained control.

For detailed documentation, see:
- [Consumer Groups Guide](docs/consumer-groups.md)
- [Python SDK](sdk/python/README.md)
- [Java SDK](sdk/java/README.md)

---

## Installation

### Prerequisites
- Go 1.21 or later

### From Source

```bash
git clone https://github.com/firefly-oss/flymq
cd flymq
./install.sh
```

The interactive installer will guide you through configuration options including:
- **Deployment mode** - Standalone or cluster deployment
- **Network settings** - Bind address, cluster address
- **Storage settings** - Data directory, segment size
- **Security settings** - TLS, encryption, authentication, RBAC
- **Authentication** - Enable auth with default admin user creation
- **Advanced Features** - Schema validation, DLQ, delayed delivery, transactions
- **Observability** - Prometheus metrics, OpenTelemetry tracing, health checks
- **Systemd integration** - Automatic service installation (Linux)

### Manual Build

```bash
go build -o bin/flymq cmd/flymq/main.go
go build -o bin/flymq-cli cmd/flymq-cli/main.go
```

---

## Cluster Deployment

FlyMQ provides multiple deployment options for running production clusters. Choose the approach that best fits your infrastructure.

### Quick Start with Docker (Recommended)

The fastest way to deploy a FlyMQ cluster is using the enterprise deployment script with **Rancher-style join bundles**:

**Step 1: Start the bootstrap server (first host)**

```bash
# Start a bootstrap server with TLS, auth, and encryption
./deploy/docker/scripts/setup-enterprise.sh server

# The output shows:
# - Admin credentials (auto-generated)
# - Join bundle location (flymq-join-bundle.tar.gz)
# - Simple 2-step instructions for adding agents
```

**Step 2: Copy the join bundle and start agents**

```bash
# Copy the join bundle to each agent host
scp deploy/docker/flymq-join-bundle.tar.gz agent1:/path/to/flymq/deploy/docker/
scp deploy/docker/flymq-join-bundle.tar.gz agent2:/path/to/flymq/deploy/docker/

# On each agent host, run:
./deploy/docker/scripts/setup-enterprise.sh agent --bundle flymq-join-bundle.tar.gz
```

That's it! The join bundle contains everything agents need: TLS certificates, encryption key, server address, and join token.

See [On-Premise Deployment Guide](docs/deployment-onprem.md) for advanced configuration and troubleshooting.

### Advertise Address Auto-Detection

FlyMQ automatically detects the correct IP address to advertise to other nodes. This works in:

- **Docker containers** - Detects container IP via default route
- **Kubernetes** - Uses `POD_IP` environment variable
- **AWS EC2** - Uses instance metadata service
- **GCP** - Uses metadata service
- **Azure** - Uses IMDS

For manual override (e.g., behind NAT):
```bash
export FLYMQ_ADVERTISE_CLUSTER=<external-ip>:9093
./deploy/docker/scripts/setup-enterprise.sh server --admin-password MySecurePass123
```

### Docker Cluster Management

The enterprise deployment script provides comprehensive cluster management:

```bash
# View cluster status
./deploy/docker/scripts/setup-enterprise.sh status

# View logs
./deploy/docker/scripts/setup-enterprise.sh logs

# View join bundle info and instructions
./deploy/docker/scripts/setup-enterprise.sh bundle

# Get join token (for manual setup)
./deploy/docker/scripts/setup-enterprise.sh token

# Export certificates (for manual setup)
./deploy/docker/scripts/setup-enterprise.sh export-certs ./certs-export

# Stop the node
./deploy/docker/scripts/setup-enterprise.sh stop

# Stop and remove all data
./deploy/docker/scripts/setup-enterprise.sh stop --volumes
```

### Deployment Patterns

| Pattern | Use Case | Documentation |
|---------|----------|---------------|
| **Docker Bootstrap/Agent** | Multi-host clusters with simple management | [On-Premise Guide](docs/deployment-onprem.md) |
| **Interactive Installer** | Bare metal or VM deployments | [INSTALL.md](INSTALL.md) |
| **AWS** | EC2, ECS, EKS deployments | [AWS Guide](docs/deployment-aws.md) |
| **Azure** | VMs, ACI, AKS deployments | [Azure Guide](docs/deployment-azure.md) |
| **Google Cloud** | GCE, Cloud Run, GKE deployments | [GCP Guide](docs/deployment-gcp.md) |

### Network Ports

| Port | Purpose | Required |
|------|---------|----------|
| 9092 | Client connections | Yes |
| 9093 | Cluster communication (Raft) | Cluster only |
| 9094 | Prometheus metrics | Optional |
| 9095 | Health checks | Recommended |
| 9096 | Admin API | Recommended |

### Cluster Behavior

- **Leader Election**: Uses Raft consensus for automatic leader election
- **Metadata Replication**: Topic creation/deletion is replicated to all nodes via Raft log
- **Message Replication**: Messages are replicated through Raft consensus for durability
- **Failover**: If the leader fails, a new leader is automatically elected (typically within 2-3 seconds)
- **Catch-up**: Nodes that rejoin the cluster automatically sync missed operations

---

## Configuration

FlyMQ can be configured via configuration file (JSON), environment variables, or command-line flags.

### Configuration File

```json
{
  "bind_addr": ":9092",
  "cluster_addr": ":9093",
  "node_id": "node-1",
  "peers": [],

  "data_dir": "/var/lib/flymq",
  "segment_bytes": 67108864,
  "retention_bytes": 0,

  "log_level": "info",

  "security": {
    "tls_enabled": true,
    "tls_cert_file": "/etc/flymq/server.crt",
    "tls_key_file": "/etc/flymq/server.key",
    "encryption_enabled": true
    // Note: encryption_key is set via FLYMQ_ENCRYPTION_KEY env var (never in config)
  },

  "auth": {
    "enabled": true,
    "allow_anonymous": false,
    "admin_username": "admin",
    "admin_password": "your-secure-password"
  },

  "performance": {
    "acks": "leader",
    "sync_interval_ms": 5
  },

  "schema": {
    "enabled": true,
    "validation": "strict"
  },

  "dlq": {
    "enabled": true,
    "max_retries": 3,
    "retry_delay": 1000,
    "topic_suffix": ".dlq"
  },

  "ttl": {
    "default_ttl": 86400,
    "cleanup_interval": 60
  },

  "delayed": {
    "enabled": true,
    "max_delay": 604800
  },

  "transaction": {
    "enabled": true,
    "timeout": 60
  },

  "observability": {
    "metrics": { "enabled": true, "addr": ":9094" },
    "tracing": { "enabled": false, "endpoint": "localhost:4317", "sample_rate": 0.1 },
    "health": {
      "enabled": true,
      "addr": ":9095",
      "tls_enabled": false
    },
    "admin": {
      "enabled": true,
      "addr": ":9096",
      "tls_enabled": false
    }
  }
}
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FLYMQ_BIND_ADDR` | Client listen address | `:9092` |
| `FLYMQ_CLUSTER_ADDR` | Cluster listen address | `:9093` |
| `FLYMQ_ADVERTISE_ADDR` | Advertised client address (for NAT/Docker) | auto-detected |
| `FLYMQ_ADVERTISE_CLUSTER` | Advertised cluster address (for NAT/Docker) | auto-detected |
| `FLYMQ_DATA_DIR` | Data storage directory | `~/.local/share/flymq` |
| `FLYMQ_NODE_ID` | Unique node identifier | hostname |
| `FLYMQ_LOG_LEVEL` | Log level (debug, info, warn, error) | `info` |
| `FLYMQ_LOG_JSON` | Output logs in JSON format | `true` |
| `FLYMQ_TLS_ENABLED` | Enable TLS | `false` |
| `FLYMQ_ENCRYPTION_ENABLED` | Enable data encryption | `false` |
| `FLYMQ_ENCRYPTION_KEY` | AES-256 encryption key (64 hex chars) | - |
| `FLYMQ_AUTH_ENABLED` | Enable authentication | `false` |
| `FLYMQ_USERNAME` | Default username for CLI | - |
| `FLYMQ_PASSWORD` | Default password for CLI | - |

**Command-Line Flags:**

| Flag | Description |
|------|-------------|
| `-config` | Path to configuration file (JSON format) |
| `-human-readable` | Use human-readable log format instead of JSON |
| `-quiet` | Skip banner and config display, output logs only |
| `-version` | Show version information |

**Docker/Kubernetes Environment Variables:**

| Variable | Description | Default |
|----------|-------------|---------|
| `FLYMQ_ROLE` | Node role: `bootstrap`, `agent`, or `standalone` | `standalone` |
| `FLYMQ_SERVER` | Bootstrap server address (agent mode only) | - |
| `FLYMQ_JOIN_TOKEN` | Cluster join token (agent mode only) | - |
| `FLYMQ_CLUSTER_TOKEN` | Cluster secret (bootstrap mode only) | auto-generated |

---

## Security

### Authentication & RBAC

FlyMQ provides enterprise-grade authentication and role-based access control:

**Built-in Roles:**
| Role | Permissions | Description |
|------|-------------|-------------|
| `admin` | read, write, admin | Full access to all operations |
| `producer` | write | Write-only access (produce messages) |
| `consumer` | read | Read-only access (consume messages) |
| `guest` | - | Access to public topics only |

**Configuration:**
```json
{
  "auth": {
    "enabled": true,
    "allow_anonymous": false,
    "admin_username": "admin",
    "admin_password": "secure-password"
  }
}
```

**User Management via CLI:**
```bash
# Create users with roles
flymq-cli users create alice password123 --roles producer,consumer
flymq-cli users create bob password456 --roles admin

# Update user roles
flymq-cli users update alice --roles admin

# Disable a user
flymq-cli users update alice --enabled false
```

**Topic ACLs:**
```bash
# Make a topic public (no auth required)
flymq-cli acl set public-events --public

# Restrict topic to specific users/roles
flymq-cli acl set orders --users alice,bob --roles admin
```

### TLS Configuration

Generate certificates for TLS:

```bash
# Generate CA
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 365 -key ca.key -out ca.crt

# Generate server certificate
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr
openssl x509 -req -days 365 -in server.csr -CA ca.crt -CAkey ca.key -out server.crt
```

### Data-at-Rest Encryption

FlyMQ encrypts stored messages using AES-256-GCM. For security, the encryption key is **never stored in configuration files** and must be provided via environment variable.

**Step 1: Generate an encryption key**

```bash
openssl rand -hex 32 > /etc/flymq/encryption.key
chmod 600 /etc/flymq/encryption.key
```

**Step 2: Enable encryption in configuration**

```json
{
  "security": {
    "encryption_enabled": true
  }
}
```

**Step 3: Set the encryption key environment variable**

```bash
export FLYMQ_ENCRYPTION_KEY=$(cat /etc/flymq/encryption.key)
flymq --config /etc/flymq/flymq.json
```

> **⚠️ Security Best Practices:**
> - Store the key in a secrets manager (HashiCorp Vault, AWS Secrets Manager, etc.)
> - Use restricted file permissions (600) for any file containing the key
> - Never commit encryption keys to version control
> - Back up the key securely - **data cannot be recovered without it**
>
> **🔗 Cluster Requirement:** All nodes in a cluster **must use the same encryption key**. FlyMQ validates key consistency when nodes join the cluster and rejects nodes with mismatched keys.
>
> **Key Verification:** FlyMQ verifies the encryption key on startup. If the wrong key is provided, the server will refuse to start with a clear error message.

### Audit Trail

FlyMQ includes a comprehensive audit trail system for tracking security-relevant operations. Audit logging is **enabled by default** for compliance and security monitoring.

**Tracked Events:**

| Category       | Events                                         | Description              |
|----------------|------------------------------------------------|--------------------------|
| Authentication | `auth.success`, `auth.failure`, `auth.logout`  | User login/logout events |
| Authorization  | `access.granted`, `access.denied`              | Resource access decisions|
| Topics         | `topic.create`, `topic.delete`, `topic.modify` | Topic management         |
| Users          | `user.create`, `user.delete`, `user.modify`    | User management          |
| ACLs           | `acl.change`                                   | Access control changes   |
| Cluster        | `cluster.join`, `cluster.leave`                | Node membership changes  |

**Query Audit Events via CLI:**
```bash
# List recent events
flymq-cli audit list

# Query with filters
flymq-cli audit query --user admin --type auth.success,auth.failure --start 2026-01-01T00:00:00Z

# Export for compliance reporting
flymq-cli audit export --format csv --output audit-report.csv
```

**Query via Admin API:**
```bash
# Query events
curl -u admin:password "http://localhost:8080/api/v1/audit/events?user=admin&limit=50"

# Export as CSV
curl -u admin:password "http://localhost:8080/api/v1/audit/export?format=csv" > audit.csv
```

**Configuration:**
```json
{
  "audit": {
    "enabled": true,
    "log_dir": "/var/lib/flymq/audit",
    "max_file_size": 104857600,
    "retention_days": 90
  }
}
```

---

## Observability

### Prometheus Metrics

FlyMQ exposes Prometheus-compatible metrics at the configured metrics endpoint:

```bash
curl http://localhost:9094/metrics
```

Available metrics include:
- `flymq_messages_produced_total` - Total messages produced
- `flymq_messages_consumed_total` - Total messages consumed
- `flymq_produce_latency_seconds` - Message produce latency histogram
- `flymq_consume_latency_seconds` - Message consume latency histogram
- `flymq_topic_messages_total` - Messages per topic
- `flymq_consumer_group_lag` - Consumer group lag

### Health Checks

Kubernetes-compatible health endpoints:

```bash
# Liveness probe - is the process running?
curl http://localhost:9095/health/live

# Readiness probe - is the service ready to accept traffic?
curl http://localhost:9095/health/ready

# Detailed health status
curl http://localhost:9095/health
```

### Admin API

REST API for cluster management:

```bash
# Get cluster info
curl http://localhost:9096/api/v1/cluster

# List topics
curl http://localhost:9096/api/v1/topics

# Create topic
curl -X POST http://localhost:9096/api/v1/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "my-topic", "options": {"partitions": 3}}'

# Delete topic
curl -X DELETE http://localhost:9096/api/v1/topics/my-topic

# List consumer groups
curl http://localhost:9096/api/v1/consumer-groups

# List schemas
curl http://localhost:9096/api/v1/schemas
```

---

## Architecture

### Storage Engine

FlyMQ uses a Write-Ahead Log (WAL) style append-only storage system:

- **Segments** - Logs are split into configurable-size segments for efficient retention
- **Indexes** - Memory-mapped index files provide O(1) offset lookups
- **Partitions** - Topics are divided into partitions for parallel processing

### Protocol

FlyMQ uses a custom binary protocol for efficiency:

```
[Magic(1)][Version(1)][OpCode(1)][Flags(1)][Length(4)][Payload(...)]
```

- Magic byte: `0xAF`
- Supported operations: Produce, Consume, Subscribe, Commit, Fetch

---

## CLI Reference

```bash
# Topic management
flymq-cli create <topic> [--partitions N]
flymq-cli delete <topic>
flymq-cli topics                    # List all topics
flymq-cli info <topic>              # Topic details
flymq-cli explore                   # Interactive topic explorer (Metadata, Messages, Filter, Commit)

# Producing messages
flymq-cli produce <topic> <message> [--key KEY] [--encoder json|string|binary]

# Consuming messages
flymq-cli consume <topic> [--offset N] [--count N] [--show-key] [--decoder json|string|binary] [--filter PATTERN]
flymq-cli subscribe <topic> [--group GROUP] [--from earliest|latest] [--show-key] [--filter PATTERN]

# Consumer groups
flymq-cli groups list               # List all groups
flymq-cli groups describe <group>   # Group details
flymq-cli groups lag <group>        # Consumer lag

# Advanced Commands
flymq-cli produce-delayed <topic> <message> <delay-ms>
flymq-cli produce-ttl <topic> <message> <ttl-ms>
flymq-cli txn <topic> <message1> [message2] ...

# Dead letter queue
flymq-cli dlq list <topic> [--count N]
flymq-cli dlq replay <topic> <message-id>
flymq-cli dlq purge <topic>

# Schema registry
flymq-cli schema register <name> <type> <schema>
flymq-cli schema produce <topic> <schema-name> <message>
flymq-cli schema list

# Cluster & Admin
flymq-cli cluster                   # Cluster status
flymq-cli health                    # Health checks
flymq-cli admin                     # Admin API

# Security
flymq-cli auth                      # Authenticate
flymq-cli whoami                    # Current user
flymq-cli users/roles/acl           # Access control
```

---

## Client SDKs

FlyMQ provides official client SDKs for Go, Python, and Java. All SDKs support the full feature set including encryption, transactions, schemas, and reactive streams.

### Feature Support Matrix

| Feature | Go | Python | Java |
|---------|----|---------|---------|
| Basic Produce/Consume | ✅ | ✅ | ✅ |
| Key-Based Partitioning | ✅ | ✅ | ✅ |
| Consumer Groups | ✅ | ✅ | ✅ |
| Topic Wildcards (+, #) | ✅ | ✅ | ✅ |
| Server-Side Filtering | ✅ | ✅ | ✅ |
| Plug-and-Play SerDes | ✅ | ✅ | ✅ |
| Transactions | ✅ | ✅ | ✅ |
| Encryption (AES-256-GCM) | ✅ | ✅ | ✅ |
| TLS/mTLS | ✅ | ✅ | ✅ |
| Authentication | ✅ | ✅ | ✅ |
| Schema Validation | ✅ | ✅ | ✅ |
| Dead Letter Queues | ✅ | ✅ | ✅ |
| Delayed Messages | ✅ | ✅ | ✅ |
| Message TTL | ✅ | ✅ | ✅ |
| Reactive Streams | ✅ | ✅ | ✅ |
| Spring Boot Integration | - | - | ✅ |

### Getting Started

- **Python**: [PyFlyMQ Quick Start](sdk/python/docs/GETTING_STARTED.md) | [10 Examples](sdk/python/examples/)
- **Java**: [Java SDK Quick Start](sdk/java/docs/GETTING_STARTED.md) | [Examples](sdk/java/examples/)
- **Go**: Reference implementation in `pkg/client/`

For SDK development guidelines, see [docs/sdk_development.md](docs/sdk_development.md).

---

### Go Client

```go path=null start=null
import "github.com/firefly-oss/flymq/pkg/client"

// Create client
c, err := client.NewClient("localhost:9092")
if err != nil {
    log.Fatal(err)
}
defer c.Close()

// Authenticate (if auth is enabled)
err = c.Authenticate("admin", "password")

// Produce message - returns RecordMetadata (Kafka-like)
meta, err := c.Produce("my-topic", []byte("Hello, World!"))
fmt.Printf("Produced to %s partition %d at offset %d\n", meta.Topic, meta.Partition, meta.Offset)

// Produce with key (Kafka-style partitioning)
meta, err = c.ProduceWithKey("orders", []byte("user-123"), []byte(`{"id": 1}`))

// Consume message (returns value only)
data, err := c.Consume("my-topic", meta.Offset)

// Consume with key
msg, err := c.ConsumeWithKey("orders", meta.Offset)
fmt.Printf("Key: %s, Value: %s\n", msg.Key, msg.Value)

// Fetch multiple messages with keys
messages, nextOffset, err := c.FetchWithKeys("orders", 0, 0, 100)
for _, m := range messages {
    fmt.Printf("Offset %d: key=%s value=%s\n", m.Offset, m.Key, m.Value)
}

// Best-in-Class Topic Filtering (MQTT-style)
// Subscribe to a pattern: 'sensors/+/temp' or 'logs/#'
consumer := c.Consumer([]string{"sensors/+/temp"}, client.DefaultConsumerConfig("temp-mon"))

// Server-Side Content Filtering
// Only fetch messages containing "ERROR" (regex supported)
config := client.DefaultConsumerConfig("error-mon")
config.Filter = "ERROR"
consumerWithFilter := c.Consumer([]string{"app-logs"}, config)

// Typed SerDe System
type User struct {
    ID   int    `json:"id"`
    Name string `json:"name"`
}
c.SetSerde("json")
meta, err = c.ProduceObject("users", User{ID: 1, Name: "Alice"})
// On consumer side:
// var user User
// err = consumer.Decode(msg.Value, &user)

// With authentication and encryption
c, err = client.NewClientWithOptions("localhost:9092", client.ClientOptions{
    Username:      "admin",
    Password:      "password",
    EncryptionKey: "your-64-char-hex-key",
})

// User management (admin only)
users, err := c.ListUsers()
err = c.CreateUser("alice", "password123", []string{"producer", "consumer"})
err = c.DeleteUser("alice")
```

### Python Client

Install from the SDK directory:

```bash path=null start=null
pip install ./sdk/python
```

**Quick Start (Kafka-like API):**

```python path=null start=null
from pyflymq import connect

# One-liner connection
client = connect("localhost:9092")

# High-level producer with batching
with client.producer(batch_size=100) as producer:
    producer.send("events", b'{"event": "click"}', key="user-123")
    producer.flush()

# High-level consumer with auto-commit
with client.consumer("events", "my-group") as consumer:
    for msg in consumer:
        print(f"Key: {msg.key}, Value: {msg.decode()}")

# Best-in-Class Topic Filtering
# Subscribe to patterns: 'sensors/+/temp' or 'logs/#'
with client.consumer(["sensors/+/temp"], "temp-mon") as consumer:
    for msg in consumer:
        print(f"Topic: {msg.topic}, Temp: {msg.decode()}")

# Server-Side Content Filtering
with client.consumer("app-logs", "error-mon", filter="ERROR") as consumer:
    for msg in consumer:
        print(f"Error Log: {msg.decode()}")

# Typed SerDe System
from pyflymq.serde import JSONSerializer, JSONDeserializer
client.set_serde("json")
client.produce("users", {"id": 1, "name": "Alice"})
# On consumer side:
# user = msg.decode() # returns dict automatically if serde is json

client.close()
```

**Basic Usage:**

```python path=null start=null
from pyflymq import connect

client = connect("localhost:9092")

# Produce message
offset = client.produce("my-topic", b"Hello, World!")

# Produce with key (Kafka-style partitioning)
offset = client.produce("orders", b'{"id": 1}', key="user-123")

# Consume message (returns ConsumedMessage with key)
msg = client.consume("my-topic", offset)
print(f"Key: {msg.decode_key()}, Value: {msg.decode()}")

client.close()
```

**With Authentication and Encryption:**

```python path=null start=null
from pyflymq import connect

client = connect(
    "localhost:9092",
    username="admin",
    password="password",
    encryption_key="your-64-char-hex-key"  # AES-256-GCM
)
```

**Reactive Streams (RxPY):**

```python path=null start=null
from pyflymq.reactive import ReactiveConsumer, ReactiveProducer

# Reactive consumer with backpressure
consumer = ReactiveConsumer(client, "my-topic")
consumer.subscribe(
    on_next=lambda msg: print(f"Received: {msg.value}"),
    on_error=lambda e: print(f"Error: {e}"),
    on_completed=lambda: print("Done")
)

# Reactive producer
producer = ReactiveProducer(client, "my-topic")
producer.send_many([b"msg1", b"msg2", b"msg3"])
```

**Pydantic Models:**

```python path=null start=null
from pyflymq import RecordMetadata, TopicMetadata, ConsumedMessage

# produce() returns RecordMetadata (Kafka-like)
metadata: RecordMetadata = client.produce("my-topic", b"data")
print(f"Offset: {metadata.offset}, Partition: {metadata.partition}")
```

### Java Client (Spring Boot)

Add to your `pom.xml`:

```xml path=null start=null
<!-- Core client -->
<dependency>
    <groupId>com.firefly.flymq</groupId>
    <artifactId>flymq-client-core</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- Spring Boot MVC integration -->
<dependency>
    <groupId>com.firefly.flymq</groupId>
    <artifactId>flymq-client-spring</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- Spring WebFlux (reactive) integration -->
<dependency>
    <groupId>com.firefly.flymq</groupId>
    <artifactId>flymq-client-spring-webflux</artifactId>
    <version>1.0.0</version>
</dependency>
```

**Quick Start (Kafka-like API):**

```java path=null start=null
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

    // Best-in-Class Topic Filtering
    // Subscribe to patterns: 'sensors/+/temp' or 'logs/#'
    try (var cg = client.consumerGroup(List.of("sensors/+/temp"), "temp-mon")) {
        for (var msg : cg.poll(Duration.ofSeconds(1))) {
            System.out.println("Topic: " + msg.topic() + ", Temp: " + msg.dataAsString());
        }
    }

    // Server-Side Content Filtering
    var config = new ConsumerConfig().filter("ERROR");
    try (var consumer = client.consumer(List.of("app-logs"), "error-mon", config)) {
        for (var msg : consumer.poll(Duration.ofSeconds(1))) {
            System.out.println("Error: " + msg.dataAsString());
        }
    }

    // Typed SerDe System
    client.setSerde(Serdes.JSON);
    client.produce("users", new User(1, "Alice"));
    // On consumer side:
    // User user = msg.decode(User.class);
}
```

**Basic Usage:**

```java path=null start=null
import com.firefly.flymq.FlyMQClient;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    // Produce message - returns RecordMetadata (Kafka-like)
    var meta = client.produce("my-topic", "Hello, World!".getBytes());
    System.out.println("Produced to " + meta.topic() + " at offset " + meta.offset());

    // Produce with key (Kafka-style partitioning)
    meta = client.produceWithKey("orders", "user-123", "{\"id\": 1}");

    // Consume with key
    var msg = client.consumeWithKey("orders", meta.offset());
    System.out.println("Key: " + msg.keyAsString() + ", Value: " + msg.dataAsString());
}
```

**Spring Boot Configuration:**

```yaml path=null start=null
# application.yml
flymq:
  host: localhost
  port: 9092
  username: admin              # Optional authentication
  password: password
  encryption-key: your-64-char-hex-key  # Optional AES-256-GCM encryption
```

**Spring Boot Integration:**

```java path=null start=null
import com.firefly.flymq.FlyMQClient;
import org.springframework.stereotype.Service;

@Service
public class MyService {
    private final FlyMQClient client;
    
    public MyService(FlyMQClient client) {
        this.client = client;
    }
    
    public void sendMessage() {
        client.produceWithKey("orders", "user-123", "{\"id\": 1}");
    }
}
```

**Reactive (WebFlux):**

```java path=null start=null
import com.firefly.flymq.spring.webflux.ReactiveFlyMQClient;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

@Service
public class ReactiveService {
    @Autowired
    private ReactiveFlyMQClient flyMQ;
    
    public Mono<Long> sendMessage(byte[] data) {
        return flyMQ.produce("my-topic", data);
    }
    
    public Flux<Message> streamMessages() {
        return flyMQ.subscribe("my-topic", "my-group");
    }
}
```

**Transactions:**

```java path=null start=null
import com.firefly.flymq.spring.webflux.ReactiveTransaction;

Mono<Void> transactionalSend() {
    return flyMQ.beginTransaction()
        .flatMap(txn -> txn.produce("topic", "msg1".getBytes())
            .then(txn.produce("topic", "msg2".getBytes()))
            .then(txn.commit()));
}
```

### Advanced Features (All SDKs)

All SDKs support:

- **Key-Based Partitioning** - Kafka-style message keys for ordered delivery
- **AES-256-GCM Encryption** - Data-in-motion encryption compatible with server-side encryption
- **Transactions** - Exactly-once semantics with atomic commits/rollbacks
- **Schema Validation** - JSON Schema, Avro, and Protobuf support
- **Delayed Delivery** - Schedule messages for future delivery
- **TTL** - Time-based message expiration
- **Dead Letter Queues** - Failed message handling with retry policies
- **Consumer Groups** - Coordinated consumption with offset tracking

---

## Benchmarks

FlyMQ includes a comprehensive benchmark suite for comparing performance against Apache Kafka.

### Running Benchmarks

```bash
# Build the benchmark tool
cd benchmarks
go build -o benchmark benchmark.go

# Start Kafka for comparison
docker compose --profile standalone up -d

# Start FlyMQ (in another terminal)
./bin/flymq

# Run quick benchmark
./benchmark -quick

# Run full benchmark suite
./benchmark -mode=standalone
```

### Benchmark Features

- **Message Verification**: Checksums ensure data integrity
- **Warmup Phases**: Eliminates cold-start effects
- **Resource Monitoring**: Tracks memory usage during tests
- **Multiple Modes**: Standalone and cluster benchmarks
- **Detailed Metrics**: Throughput, latency percentiles, error rates

See [benchmarks/README.md](benchmarks/README.md) for complete documentation.

---

## Project Structure

```
flymq/
├── cmd/
│   ├── flymq/              # Server binary
│   └── flymq-cli/          # CLI binary
├── internal/
│   ├── admin/              # REST Admin API
│   ├── auth/               # Authentication and RBAC
│   ├── banner/             # Server startup banner
│   ├── broker/             # Topic, partition, and consumer group management
│   ├── cluster/            # Raft consensus, membership, replication
│   ├── config/             # Configuration handling
│   ├── crypto/             # Encryption and TLS utilities
│   ├── delayed/            # Delayed message delivery
│   ├── dlq/                # Dead letter queues
│   ├── health/             # Health check endpoints
│   ├── logging/            # Structured logging
│   ├── metrics/            # Prometheus metrics
│   ├── performance/        # Zero-copy (Linux/macOS), compression, async I/O
│   ├── protocol/           # Wire protocol (binary)
│   ├── schema/             # Schema registry and validation
│   ├── server/             # TCP server
│   ├── storage/            # Segmented log storage engine
│   ├── tracing/            # OpenTelemetry tracing
│   ├── transaction/        # Transaction coordinator
│   └── ttl/                # Message TTL and expiration
├── pkg/
│   ├── cli/                # CLI utilities (colors, formatting)
│   └── client/             # Go client SDK
├── sdk/
│   ├── python/             # Python SDK (pyflymq)
│   │   ├── pyflymq/        # Client, models, crypto, reactive
│   │   ├── examples/       # Usage examples
│   │   └── tests/          # Unit tests
│   └── java/               # Java SDK (Maven multi-module)
│       ├── flymq-client-core/        # Core client library
│       ├── flymq-client-spring/      # Spring Boot MVC integration
│       └── flymq-client-spring-webflux/  # Spring WebFlux (reactive)
├── benchmarks/             # Performance benchmark suite
├── tests/                  # Integration and E2E tests
├── docs/                   # Documentation
├── deploy/                 # Deployment configurations
│   ├── docker/             # Docker files
│   └── systemd/            # Systemd service files
├── install.sh              # Interactive installer (macOS/Linux)
├── install.ps1             # PowerShell installer (Windows)
├── uninstall.sh            # Uninstaller
├── CHANGELOG.md            # Version history
├── CONTRIBUTING.md         # Contribution guidelines
└── ROADMAP.md              # Planned features
```

---

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## License

Copyright (c) 2026 Firefly Software Solutions Inc.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
