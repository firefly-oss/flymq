# FlyMQ Configuration Guide

This document provides a comprehensive reference for all FlyMQ configuration options.

## Configuration Sources

FlyMQ loads configuration from multiple sources with the following precedence (highest to lowest):

1. **Command-line flags** - Override all other sources
2. **Environment variables** - Prefixed with `FLYMQ_`
3. **Configuration file** - JSON format
4. **Default values** - Built-in defaults

## Server Configuration

### Core Settings

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `bind_addr` | `FLYMQ_BIND_ADDR` | `:9092` | TCP listen address for client connections |
| `data_dir` | `FLYMQ_DATA_DIR` | `./data` | Directory for persistent storage |
| `log_level` | `FLYMQ_LOG_LEVEL` | `info` | Logging level (debug, info, warn, error) |
| `log_json` | `FLYMQ_LOG_JSON` | `true` | Output logs in JSON format (default for production) |
| `default_partitions` | `FLYMQ_DEFAULT_PARTITIONS` | `3` | Default partitions for new topics |
| `retention_bytes` | `FLYMQ_RETENTION_BYTES` | `1073741824` | Max bytes per partition (1GB) |
| `retention_ms` | `FLYMQ_RETENTION_MS` | `604800000` | Message retention time (7 days) |
| `segment_bytes` | `FLYMQ_SEGMENT_BYTES` | `1073741824` | Max segment file size (1GB) |

### Command-Line Flags

| Flag | Description |
|------|-------------|
| `-config` | Path to configuration file (JSON format) |
| `-human-readable` | Use human-readable log format instead of JSON |
| `-quiet` | Skip banner and config display, output logs only |
| `-version` | Show version information |
| `-help`, `-h` | Show help message |

### Logging

FlyMQ outputs logs in **JSON format by default**, which is ideal for production environments and log aggregators (ELK, Loki, Datadog, etc.).

**JSON log format:**
```json
{"ts":"2026-01-16T10:30:00.123456789Z","level":"INFO","logger":"server","msg":"Server started","addr":":9092"}
```

**Human-readable format** (use `-human-readable` flag):
```
2026-01-16T10:30:00.123Z INFO  [server] Server started addr=:9092
```

**Examples:**
```bash
# Default: JSON logs with banner
flymq

# Human-readable logs for development
flymq -human-readable

# Quiet mode: JSON logs only, no banner (ideal for containers)
flymq -quiet

# Combine: human-readable without banner
flymq -quiet -human-readable
```

### TLS Configuration

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `tls.enabled` | `FLYMQ_TLS_ENABLED` | `false` | Enable TLS for client connections |
| `tls.cert_file` | `FLYMQ_TLS_CERT_FILE` | `""` | Path to server certificate |
| `tls.key_file` | `FLYMQ_TLS_KEY_FILE` | `""` | Path to server private key |
| `tls.ca_file` | `FLYMQ_TLS_CA_FILE` | `""` | Path to CA certificate (for mTLS) |
| `tls.client_auth` | `FLYMQ_TLS_CLIENT_AUTH` | `false` | Require client certificates |

### Encryption at Rest

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `encryption.enabled` | `FLYMQ_ENCRYPTION_ENABLED` | `false` | Enable message encryption |
| - | `FLYMQ_ENCRYPTION_KEY` | - | 32-byte AES-256 encryption key (64 hex chars) |

> **⚠️ Security Notice: Encryption Key Handling**
>
> The encryption key is **never stored in configuration files** for security reasons.
> It must be provided via the `FLYMQ_ENCRYPTION_KEY` environment variable.
>
> **Best Practices:**
> - Generate a secure key: `openssl rand -hex 32`
> - Store in a secrets manager (HashiCorp Vault, AWS Secrets Manager, etc.)
> - Use restricted file permissions (600) for any file containing the key
> - Never commit encryption keys to version control
> - Back up the key securely - data cannot be recovered without it
>
> **🔗 Cluster Requirement:**
> **All nodes in a cluster MUST use the same encryption key.** When a node attempts
> to join a cluster, FlyMQ validates that its encryption key fingerprint matches the
> existing cluster members. Nodes with mismatched keys will be rejected with a clear
> error message. This ensures data consistency and prevents accidental misconfiguration.
>
> **Key Verification:**
> On first startup with encryption enabled, FlyMQ creates a `.encryption_marker` file
> in the data directory. On subsequent startups, this marker is used to verify the
> correct key is provided. If the wrong key is used, FlyMQ will refuse to start with
> a clear error message.
>
> **Example Usage:**
> ```bash
> # Set the encryption key
> export FLYMQ_ENCRYPTION_KEY=$(cat /path/to/secrets/flymq.key)
>
> # Or source from a secrets file
> source /etc/flymq/flymq.secrets
>
> # Start FlyMQ
> flymq --config /etc/flymq/flymq.json
> ```
>
> **Cluster Deployment:**
> ```bash
> # Generate key once on the bootstrap node
> openssl rand -hex 32 > /etc/flymq/encryption.key
>
> # Copy to all cluster nodes
> scp /etc/flymq/encryption.key node2:/etc/flymq/
> scp /etc/flymq/encryption.key node3:/etc/flymq/
>
> # Each node loads the same key
> export FLYMQ_ENCRYPTION_KEY=$(cat /etc/flymq/encryption.key)
> ```

### Authentication & RBAC

FlyMQ provides enterprise-grade authentication and authorization through a Role-Based Access Control (RBAC) system. When enabled, all client connections must authenticate using username/password credentials, and access to topics is controlled by roles and ACLs.

**How it works:**
1. **Authentication**: Clients provide credentials via the binary protocol's `OpAuth` operation
2. **Authorization**: Each request is checked against the user's roles and topic ACLs
3. **Password storage**: Passwords are hashed using bcrypt (secure, not stored in plain text)
4. **User database**: Stored in a JSON file, hot-reloaded when modified

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `auth.enabled` | `FLYMQ_AUTH_ENABLED` | `false` | Enable authentication |
| `auth.rbac_enabled` | - | `true` | Enable role-based access control (when auth enabled) |
| `auth.allow_anonymous` | `FLYMQ_AUTH_ALLOW_ANONYMOUS` | `false` | Allow unauthenticated connections |
| `auth.user_file` | `FLYMQ_AUTH_USER_FILE` | `""` | Path to users database file (JSON format) |
| `auth.acl_file` | `FLYMQ_AUTH_ACL_FILE` | `""` | Path to ACL database file (JSON format) |
| `auth.default_public` | `FLYMQ_AUTH_DEFAULT_PUBLIC` | `true` | Topics are public by default (backward compatible) |
| `auth.admin_username` | `FLYMQ_AUTH_ADMIN_USERNAME` | `admin` | Default admin username |
| `auth.admin_password` | `FLYMQ_AUTH_ADMIN_PASSWORD` | `""` | Default admin password (created on first startup) |

**Built-in Roles:**

| Role | Permissions | Description |
|------|-------------|-------------|
| `admin` | read, write, admin | Full access to all operations |
| `producer` | write | Write-only access (produce messages) |
| `consumer` | read | Read-only access (consume messages) |
| `guest` | - | Access to public topics only |

**Topic ACLs:**

Topics can have explicit ACLs that override the default public setting:
- `public: true` - No authentication required
- `allowed_users` - List of usernames with access
- `allowed_roles` - List of roles with access

## Cluster Configuration

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `cluster.enabled` | `FLYMQ_CLUSTER_ENABLED` | `false` | Enable cluster mode |
| `cluster.node_id` | `FLYMQ_CLUSTER_NODE_ID` | `""` | Unique node identifier |
| `cluster.addr` | `FLYMQ_CLUSTER_ADDR` | `:9093` | Raft communication address |
| `cluster.advertise_cluster` | `FLYMQ_ADVERTISE_CLUSTER` | auto-detected | Address advertised to cluster peers |
| `cluster.peers` | `FLYMQ_CLUSTER_PEERS` | `[]` | Comma-separated list of peer addresses |
| `cluster.election_timeout_ms` | `FLYMQ_CLUSTER_ELECTION_TIMEOUT_MS` | `1000` | Raft election timeout |
| `cluster.heartbeat_interval_ms` | `FLYMQ_CLUSTER_HEARTBEAT_INTERVAL_MS` | `100` | Raft heartbeat interval |

### Advertise Address Auto-Detection

FlyMQ automatically detects the correct IP address to advertise to other cluster nodes. The detection order is:

1. Explicit `FLYMQ_ADVERTISE_CLUSTER` environment variable
2. Kubernetes `POD_IP` environment variable
3. AWS EC2 instance metadata
4. GCP instance metadata
5. Azure IMDS
6. Docker container IP (via default route)
7. First non-loopback interface IP

For manual override (e.g., behind NAT):
```bash
export FLYMQ_ADVERTISE_CLUSTER=<external-ip>:9093
```

### Partition Management (Horizontal Scaling)

These settings control how partition leaders are distributed across cluster nodes for horizontal scaling. This is separate from message partitioning (key-based or round-robin), which determines which partition a message is written to.

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `partition.distribution_strategy` | `FLYMQ_PARTITION_DISTRIBUTION_STRATEGY` | `round-robin` | How partition leaders are distributed across nodes |
| `partition.default_replication_factor` | `FLYMQ_PARTITION_DEFAULT_REPLICATION_FACTOR` | `3` | Default replicas for new topics |
| `partition.default_partitions` | `FLYMQ_PARTITION_DEFAULT_PARTITIONS` | `6` | Default partitions for new topics |
| `partition.auto_rebalance_enabled` | `FLYMQ_PARTITION_AUTO_REBALANCE_ENABLED` | `true` | Auto-rebalance when nodes join/leave |
| `partition.auto_rebalance_interval` | `FLYMQ_PARTITION_AUTO_REBALANCE_INTERVAL` | `300` | Seconds between rebalance checks |
| `partition.rebalance_threshold` | `FLYMQ_PARTITION_REBALANCE_THRESHOLD` | `0.2` | Max imbalance ratio before rebalance |

**Distribution Strategies:**

| Strategy | Description | Best For |
|----------|-------------|----------|
| `round-robin` | Distribute leaders evenly in order | Simple clusters, predictable distribution |
| `least-loaded` | Assign to node with fewest leaders | Dynamic workloads, uneven topic creation |
| `rack-aware` | Consider rack placement for fault tolerance | Multi-rack/AZ deployments |

**Example Configuration:**
```json
{
  "partition": {
    "distribution_strategy": "least-loaded",
    "default_replication_factor": 3,
    "default_partitions": 12,
    "auto_rebalance_enabled": true,
    "auto_rebalance_interval": 300,
    "rebalance_threshold": 0.15
  }
}
```

**Message Partitioning vs Leader Distribution:**

| Concept | What It Does | Configured By |
|---------|--------------|---------------|
| **Message Partitioning** | Determines which partition a message goes to | Message key (FNV-1a hash) or round-robin |
| **Leader Distribution** | Determines which node is leader for each partition | `partition.distribution_strategy` |

### Service Discovery (mDNS)

FlyMQ supports automatic node discovery using mDNS (Bonjour/Avahi). When enabled, nodes advertise themselves on the local network and can automatically discover other FlyMQ nodes.

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `discovery.enabled` | `FLYMQ_DISCOVERY_ENABLED` | `false` | Enable mDNS service discovery |
| `discovery.cluster_id` | `FLYMQ_DISCOVERY_CLUSTER_ID` | `""` | Cluster identifier for filtering (optional) |

**How It Works:**

1. **Service Advertisement**: Each node advertises itself as `_flymq._tcp.local.` with metadata including node ID, cluster address, and version.
2. **Node Discovery**: Nodes can discover other FlyMQ instances on the same network segment.
3. **Cluster Filtering**: Use `cluster_id` to filter discovery to specific clusters when multiple FlyMQ clusters exist on the same network.

**Example Configuration:**
```json
{
  "discovery": {
    "enabled": true,
    "cluster_id": "production-cluster"
  }
}
```

**Discovery Tool:**

FlyMQ includes a standalone discovery tool for finding nodes:

```bash
# Discover FlyMQ nodes on the network
flymq-discover

# With custom timeout
flymq-discover --timeout 10

# JSON output for scripting
flymq-discover --json

# Just addresses (for scripting)
flymq-discover --quiet
```

**Network Requirements:**
- mDNS uses UDP port 5353 (multicast)
- Nodes must be on the same network segment or have multicast routing enabled
- Firewalls must allow mDNS traffic

## Schema Registry Configuration

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `schema.enabled` | `FLYMQ_SCHEMA_ENABLED` | `false` | Enable schema validation |
| `schema.compatibility` | `FLYMQ_SCHEMA_COMPATIBILITY` | `backward` | Compatibility mode |
| `schema.storage_path` | `FLYMQ_SCHEMA_STORAGE_PATH` | `./schemas` | Schema storage directory |

**Compatibility Modes:**
- `none` - No compatibility checking
- `backward` - New schema can read old data
- `forward` - Old schema can read new data
- `full` - Both backward and forward compatible

## Dead Letter Queue Configuration

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `dlq.enabled` | `FLYMQ_DLQ_ENABLED` | `true` | Enable dead letter queue |
| `dlq.max_retries` | `FLYMQ_DLQ_MAX_RETRIES` | `3` | Max delivery attempts before DLQ |
| `dlq.retention_ms` | `FLYMQ_DLQ_RETENTION_MS` | `604800000` | DLQ message retention (7 days) |

## TTL and Delayed Delivery Configuration

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `ttl.enabled` | `FLYMQ_TTL_ENABLED` | `true` | Enable message TTL |
| `ttl.check_interval_ms` | `FLYMQ_TTL_CHECK_INTERVAL_MS` | `60000` | TTL check interval (1 min) |
| `delayed.enabled` | `FLYMQ_DELAYED_ENABLED` | `true` | Enable delayed delivery |
| `delayed.max_delay_ms` | `FLYMQ_DELAYED_MAX_DELAY_MS` | `86400000` | Max delay (24 hours) |

## Transaction Configuration

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `transaction.enabled` | `FLYMQ_TRANSACTION_ENABLED` | `true` | Enable transactions |
| `transaction.timeout_ms` | `FLYMQ_TRANSACTION_TIMEOUT_MS` | `60000` | Transaction timeout (1 min) |
| `transaction.max_size` | `FLYMQ_TRANSACTION_MAX_SIZE` | `1000` | Max messages per transaction |

## Observability Configuration

### Prometheus Metrics

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `observability.metrics.enabled` | `FLYMQ_METRICS_ENABLED` | `true` | Enable Prometheus metrics |
| `observability.metrics.addr` | `FLYMQ_METRICS_ADDR` | `:9094` | Metrics endpoint address |
| `observability.metrics.path` | `FLYMQ_METRICS_PATH` | `/metrics` | Metrics endpoint path |

### OpenTelemetry Tracing

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `observability.tracing.enabled` | `FLYMQ_TRACING_ENABLED` | `false` | Enable distributed tracing |
| `observability.tracing.endpoint` | `FLYMQ_TRACING_ENDPOINT` | `""` | OTLP collector endpoint |
| `observability.tracing.sample_rate` | `FLYMQ_TRACING_SAMPLE_RATE` | `0.1` | Trace sampling rate (0.0-1.0) |

### Health Checks

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `observability.health.enabled` | `FLYMQ_HEALTH_ENABLED` | `true` | Enable health endpoints |
| `observability.health.addr` | `FLYMQ_HEALTH_ADDR` | `:9095` | Health check address |

#### Health Endpoint TLS Configuration

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `observability.health.tls_enabled` | `FLYMQ_HEALTH_TLS_ENABLED` | `false` | Enable HTTPS for health endpoints |
| `observability.health.tls_cert_file` | `FLYMQ_HEALTH_TLS_CERT_FILE` | `""` | Path to TLS certificate file |
| `observability.health.tls_key_file` | `FLYMQ_HEALTH_TLS_KEY_FILE` | `""` | Path to TLS private key file |
| `observability.health.tls_auto_generate` | `FLYMQ_HEALTH_TLS_AUTO_GENERATE` | `false` | Auto-generate self-signed certificate |
| `observability.health.tls_use_admin_cert` | `FLYMQ_HEALTH_TLS_USE_ADMIN_CERT` | `false` | Share TLS certificate with Admin API |

### Admin API

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `observability.admin.enabled` | `FLYMQ_ADMIN_ENABLED` | `true` | Enable admin REST API |
| `observability.admin.addr` | `FLYMQ_ADMIN_ADDR` | `:9096` | Admin API address |

#### Admin API TLS Configuration

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `observability.admin.tls_enabled` | `FLYMQ_ADMIN_TLS_ENABLED` | `false` | Enable HTTPS for Admin API |
| `observability.admin.tls_cert_file` | `FLYMQ_ADMIN_TLS_CERT_FILE` | `""` | Path to TLS certificate file |
| `observability.admin.tls_key_file` | `FLYMQ_ADMIN_TLS_KEY_FILE` | `""` | Path to TLS private key file |
| `observability.admin.tls_auto_generate` | `FLYMQ_ADMIN_TLS_AUTO_GENERATE` | `false` | Auto-generate self-signed certificate |

> **Note:** The Admin API uses HTTP Basic Authentication when `auth.enabled` is `true`. Credentials are the same as those used for the binary protocol (`auth.admin_username` and `auth.admin_password`).

## Performance Configuration

### Understanding Durability vs Performance

FlyMQ provides configurable durability settings that let you choose the right trade-off between data safety and throughput for your use case. This is similar to Apache Kafka's `acks` setting, which controls how many replicas must acknowledge a write before it's considered successful.

The key insight is that **disk fsync is expensive** (typically ~3ms latency). By controlling when fsync happens, you can dramatically improve throughput at the cost of potential data loss during crashes.

### Storage Performance Settings

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `performance.acks` | `FLYMQ_ACKS` | `leader` | Durability mode: `all`, `leader`, `none` |
| `performance.sync_interval_ms` | `FLYMQ_SYNC_INTERVAL_MS` | `5` | Batch sync interval in ms (leader mode) |
| `performance.sync_batch_size` | `FLYMQ_SYNC_BATCH_SIZE` | auto-tuned | Max writes before forced sync (scales with CPU) |
| `performance.write_buffer_size` | `FLYMQ_WRITE_BUFFER_SIZE` | auto-tuned | Write buffer size (2-64MB based on CPU) |

### Acks Modes: FlyMQ vs Kafka Comparison

| FlyMQ Mode | Kafka Equivalent | Behavior | Throughput | Durability | Use Case |
|------------|------------------|----------|------------|------------|----------|
| `all` | `acks=all` | fsync after every write | ~300 msg/s | Highest | Financial transactions |
| `leader` | `acks=1` | Batch fsync on interval | ~7,000+ msg/s | Balanced | **General production (default)** |
| `none` | `acks=0` | Async writes, fsync on close | ~15,000+ msg/s | Lowest | Metrics, logs |

### Detailed Mode Explanations

**leader** (DEFAULT - recommended for most workloads):
This mode provides the best balance between performance and durability, similar to Kafka's default `acks=1` setting. Messages are written to an in-memory buffer and flushed to the OS page cache immediately. A background goroutine periodically calls fsync (every `sync_interval_ms`) to persist data to disk.

- **What you gain**: 20x+ throughput compared to `all` mode
- **What you risk**: Up to `sync_interval_ms` worth of messages (default: 5ms) could be lost on sudden power failure
- **Why it's the default**: This matches industry best practices (Kafka defaults to `acks=1`) and is suitable for 95% of use cases

**all** (maximum durability):
Every message is synced to disk before acknowledgment. Use this only when data loss is absolutely unacceptable, such as financial transactions or audit logs.

- **What you gain**: Zero data loss guarantee (assuming no disk failure)
- **What you pay**: ~300 msg/s throughput due to disk latency
- **When to use**: Financial systems, compliance-critical data, audit trails

**none** (maximum performance):
Messages are written to an in-memory buffer but only synced to disk on segment rotation or graceful shutdown. Use for high-volume, ephemeral data where some loss is acceptable.

- **What you gain**: Maximum throughput (15,000+ msg/s)
- **What you risk**: All buffered messages since last segment rotation on crash
- **When to use**: Application metrics, debug logs, real-time analytics

### Binary Protocol Flags

| Flag | Value | Description |
|------|-------|-------------|
| `FLAG_BINARY` | `0x01` | Binary payload (always set) |
| `FLAG_COMPRESSED` | `0x02` | Compressed payload (reserved) |

## Example Configuration File

```json
{
  "bind_addr": ":9092",
  "data_dir": "/var/lib/flymq",
  "log_level": "info",
  "default_partitions": 6,
  "retention_bytes": 10737418240,
  "retention_ms": 604800000,

  "tls": {
    "enabled": true,
    "cert_file": "/etc/flymq/server.crt",
    "key_file": "/etc/flymq/server.key",
    "ca_file": "/etc/flymq/ca.crt",
    "client_auth": true
  },

  "auth": {
    "enabled": true,
    "rbac_enabled": true,
    "allow_anonymous": false,
    "user_file": "/var/lib/flymq/users.json",
    "acl_file": "/var/lib/flymq/acls.json",
    "default_public": false,
    "admin_username": "admin",
    "admin_password": "your-secure-password"
  },

  "cluster": {
    "enabled": true,
    "node_id": "node-1",
    "addr": ":9093",
    "peers": ["node-2:9093", "node-3:9093"]
  },

  "observability": {
    "metrics": {"enabled": true, "addr": ":9094"},
    "tracing": {"enabled": true, "endpoint": "localhost:4317"},
    "health": {
      "enabled": true,
      "addr": ":9095",
      "tls_enabled": true,
      "tls_use_admin_cert": true
    },
    "admin": {
      "enabled": true,
      "addr": ":9096",
      "tls_enabled": true,
      "tls_cert_file": "/etc/flymq/admin.crt",
      "tls_key_file": "/etc/flymq/admin.key"
    }
  }
}
```

