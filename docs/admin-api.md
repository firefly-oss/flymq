# FlyMQ Admin REST API

The Admin API provides HTTP endpoints for cluster management, monitoring, and operations.

## Base URL

```
http://localhost:9096    # HTTP (default)
https://localhost:9096   # HTTPS (when TLS enabled)
```

Configure via `observability.admin.addr` or `FLYMQ_ADMIN_ADDR` environment variable.

---

## HTTPS/TLS Configuration

The Admin API supports TLS encryption for secure communication.

### Configuration Options

| Option | Env Variable | Default | Description |
|--------|--------------|---------|-------------|
| `observability.admin.tls_enabled` | `FLYMQ_ADMIN_TLS_ENABLED` | `false` | Enable HTTPS for Admin API |
| `observability.admin.tls_cert_file` | `FLYMQ_ADMIN_TLS_CERT_FILE` | `""` | Path to TLS certificate file |
| `observability.admin.tls_key_file` | `FLYMQ_ADMIN_TLS_KEY_FILE` | `""` | Path to TLS private key file |
| `observability.admin.tls_auto_generate` | `FLYMQ_ADMIN_TLS_AUTO_GENERATE` | `false` | Auto-generate self-signed certificate |

### Example Configuration

```json
{
  "observability": {
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

### Auto-Generated Self-Signed Certificate

For development or testing, FlyMQ can auto-generate a self-signed certificate:

```json
{
  "observability": {
    "admin": {
      "enabled": true,
      "addr": ":9096",
      "tls_enabled": true,
      "tls_auto_generate": true
    }
  }
}
```

> **Note:** Self-signed certificates should only be used for development. Use proper CA-signed certificates in production.

---

## Authentication

When authentication is enabled (`auth.enabled: true`), the Admin API requires HTTP Basic Authentication.

### Request Authentication

Include the `Authorization` header with Base64-encoded credentials:

```
Authorization: Basic <base64(username:password)>
```

### Example with curl

```bash
# HTTP
curl -u admin:secret http://localhost:9096/topics

# HTTPS with CA certificate
curl -u admin:secret --cacert /etc/flymq/ca.crt https://localhost:9096/topics

# HTTPS with self-signed certificate (skip verification)
curl -u admin:secret -k https://localhost:9096/topics
```

### CLI Authentication

The `flymq-cli` supports Admin API authentication via flags or environment variables:

```bash
# Using flags
flymq-cli cluster status --admin-user admin --admin-pass secret

# Using environment variables
export FLYMQ_ADMIN_USER=admin
export FLYMQ_ADMIN_PASS=secret
flymq-cli cluster status

# With HTTPS
flymq-cli cluster status --admin-tls --admin-ca-cert /etc/flymq/ca.crt \
  --admin-user admin --admin-pass secret

# With self-signed certificate (skip verification)
flymq-cli cluster status --admin-tls --admin-insecure \
  --admin-user admin --admin-pass secret
```

### CLI Admin API Options

| Option | Env Variable | Description |
|--------|--------------|-------------|
| `--admin-user` | `FLYMQ_ADMIN_USER` | Username for Admin API authentication |
| `--admin-pass` | `FLYMQ_ADMIN_PASS` | Password for Admin API authentication |
| `--admin-tls` | `FLYMQ_ADMIN_TLS` | Enable HTTPS for Admin API calls |
| `--admin-ca-cert` | `FLYMQ_ADMIN_CA_FILE` | CA certificate for Admin API TLS |
| `--admin-insecure` | `FLYMQ_ADMIN_TLS_INSECURE` | Skip TLS certificate verification |

### HTTP Response Codes

| Code | Description |
|------|-------------|
| `401 Unauthorized` | Missing or invalid credentials |
| `403 Forbidden` | User lacks required permissions |

---

## Endpoint Security Tiers

When authentication is enabled, endpoints are protected according to permission levels:

### Public Endpoints (No Auth Required)
- `GET /api/v1/health` - Basic health check
- `GET /swagger/` - Swagger UI documentation
- `GET /swagger.json` - OpenAPI specification

### Read Permission Required
- `GET /api/v1/cluster` - Cluster information
- `GET /api/v1/cluster/nodes` - Node list
- `GET /api/v1/topics` - List topics
- `GET /api/v1/topics/{name}` - Topic details
- `GET /api/v1/consumer-groups` - List consumer groups
- `GET /api/v1/consumer-groups/{name}` - Consumer group details
- `GET /api/v1/schemas` - List schemas
- `GET /api/v1/dlq/{topic}` - List DLQ messages

### Admin Permission Required
- `POST /api/v1/topics` - Create topic
- `DELETE /api/v1/topics/{name}` - Delete topic
- `DELETE /api/v1/consumer-groups/{name}` - Delete consumer group
- `GET /api/v1/metrics` - Prometheus metrics
- `GET /api/v1/stats` - Rich JSON statistics
- `GET /api/v1/users` - List users
- `POST /api/v1/users` - Create user
- `DELETE /api/v1/users/{username}` - Delete user
- `GET /api/v1/acls` - List ACLs
- `POST /api/v1/acls` - Set ACL
- `DELETE /api/v1/acls/{topic}` - Delete ACL
- `DELETE /api/v1/dlq/{topic}` - Purge DLQ

---

## Endpoints

### Health & Status

#### GET /api/v1/health
Basic health check (public, no auth required).

**Response:**
```json
{"status": "healthy"}
```

---

### Statistics & Metrics

#### GET /api/v1/stats
Rich JSON statistics (requires admin permission).

**Response:**
```json
{
  "cluster": {
    "node_count": 3,
    "healthy_nodes": 3,
    "topic_count": 5,
    "total_partitions": 15,
    "total_messages": 1500000,
    "consumer_group_count": 3
  },
  "nodes": [
    {
      "node_id": "node-1",
      "address": "localhost:9092",
      "state": "active",
      "is_leader": true,
      "memory_used_mb": 256.5,
      "goroutines": 150,
      "messages_received": 500000,
      "messages_sent": 480000
    }
  ],
  "topics": [
    {
      "name": "events",
      "partitions": 6,
      "message_count": 500000
    }
  ],
  "consumer_groups": [
    {
      "group_id": "analytics",
      "topic": "events",
      "members": 3,
      "state": "active",
      "total_lag": 150
    }
  ],
  "system": {
    "uptime_seconds": 86400,
    "start_time": "2026-01-14T10:30:00Z",
    "go_version": "go1.21",
    "num_cpu": 8,
    "memory_alloc_mb": 256.5,
    "goroutines": 150,
    "num_gc": 42
  }
}
```

#### GET /api/v1/metrics
Prometheus format metrics (requires admin permission).

**Response:** Prometheus text format metrics.

---

### Topic Management

#### GET /topics
List all topics with metadata.

**Response:**
```json
{
  "topics": [
    {
      "name": "events",
      "partitions": 6,
      "replication_factor": 3,
      "message_count": 1500000,
      "size_bytes": 524288000,
      "created_at": "2024-01-15T10:30:00Z"
    }
  ]
}
```

#### POST /topics
Create a new topic.

**Request:**
```json
{
  "name": "orders",
  "partitions": 12,
  "replication_factor": 3,
  "config": {
    "retention_ms": 604800000,
    "retention_bytes": 10737418240
  }
}
```

**Response:** `201 Created`

#### GET /topics/{name}
Get topic details.

**Response:**
```json
{
  "name": "events",
  "partitions": [
    {"id": 0, "leader": "node-1", "replicas": ["node-1", "node-2", "node-3"], "isr": ["node-1", "node-2"]},
    {"id": 1, "leader": "node-2", "replicas": ["node-2", "node-3", "node-1"], "isr": ["node-2", "node-3", "node-1"]}
  ],
  "config": {
    "retention_ms": 604800000,
    "retention_bytes": 10737418240
  }
}
```

#### DELETE /topics/{name}
Delete a topic.

**Response:** `204 No Content`

---

### Consumer Groups

#### GET /groups
List all consumer groups.

**Response:**
```json
{
  "groups": [
    {
      "group_id": "analytics",
      "state": "Stable",
      "members": 3,
      "topics": ["events", "orders"]
    }
  ]
}
```

#### GET /groups/{group_id}
Get consumer group details.

**Response:**
```json
{
  "group_id": "analytics",
  "state": "Stable",
  "protocol": "range",
  "members": [
    {
      "member_id": "consumer-1-abc123",
      "client_id": "analytics-consumer",
      "host": "10.0.1.5",
      "assignments": [
        {"topic": "events", "partitions": [0, 1, 2]}
      ]
    }
  ],
  "offsets": [
    {"topic": "events", "partition": 0, "offset": 15000, "lag": 50}
  ]
}
```

#### DELETE /groups/{group_id}
Delete a consumer group (must be empty).

**Response:** `204 No Content`

---

### Cluster Management

#### GET /cluster
Get cluster status.

**Response:**
```json
{
  "cluster_id": "flymq-prod-1",
  "leader": "node-1",
  "nodes": [
    {
      "node_id": "node-1",
      "address": "10.0.1.1:9093",
      "state": "Leader",
      "stats": {
        "memory_used_mb": 512.5,
        "goroutines": 150,
        "topic_count": 25,
        "messages_received": 1500000,
        "messages_sent": 1450000,
        "uptime": "7d12h30m"
      }
    },
    {
      "node_id": "node-2",
      "address": "10.0.1.2:9093",
      "state": "Follower",
      "stats": {...}
    }
  ]
}
```

#### GET /cluster/metadata
Get partition-to-node mappings for smart client routing. Optionally filter by topic.

**Query Parameters:**
- `topic` (optional): Filter to a specific topic

**Response:**
```json
{
  "cluster_id": "flymq-prod-1",
  "topics": [
    {
      "topic": "orders",
      "partitions": [
        {
          "partition": 0,
          "leader_id": "node-1",
          "leader_addr": "10.0.1.1:9092",
          "epoch": 5,
          "state": "online"
        },
        {
          "partition": 1,
          "leader_id": "node-2",
          "leader_addr": "10.0.1.2:9092",
          "epoch": 3,
          "state": "online"
        },
        {
          "partition": 2,
          "leader_id": "node-3",
          "leader_addr": "10.0.1.3:9092",
          "epoch": 4,
          "state": "online"
        }
      ]
    }
  ]
}
```

#### GET /cluster/leaders
Get the distribution of partition leaders across nodes.

**Response:**
```json
{
  "distribution": {
    "node-1": 12,
    "node-2": 11,
    "node-3": 13
  },
  "total_partitions": 36,
  "imbalance_ratio": 0.08
}
```

#### GET /cluster/partitions
Get detailed partition assignment information.

**Query Parameters:**
- `topic` (optional): Filter to a specific topic

**Response:**
```json
{
  "assignments": [
    {
      "topic": "orders",
      "partition": 0,
      "leader": "node-1",
      "leader_addr": "10.0.1.1:9092",
      "replicas": ["node-1", "node-2", "node-3"],
      "isr": ["node-1", "node-2", "node-3"],
      "epoch": 5,
      "state": "online"
    }
  ]
}
```

#### POST /cluster/rebalance
Trigger partition leader rebalancing across the cluster.

**Response:**
```json
{
  "success": true,
  "message": "Rebalance completed successfully",
  "moves": [
    {
      "topic": "orders",
      "partition": 2,
      "from_node": "node-1",
      "to_node": "node-3"
    }
  ],
  "old_leaders": {
    "node-1": 15,
    "node-2": 11,
    "node-3": 10
  },
  "new_leaders": {
    "node-1": 12,
    "node-2": 12,
    "node-3": 12
  }
}
```

#### POST /cluster/partitions/{topic}/{partition}
Reassign a partition to a new leader and/or replicas.

**Request Body:**
```json
{
  "new_leader": "node-2",
  "new_replicas": ["node-2", "node-1", "node-3"]
}
```

**Response:** `202 Accepted`

---

### Metrics

#### GET /metrics
Prometheus metrics endpoint (also available on metrics port).

**Response:** Prometheus text format metrics.

---

## Audit Trail

### GET /audit/events

Query audit events with optional filters.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `start` | string | Start time (RFC3339 format) |
| `end` | string | End time (RFC3339 format) |
| `type` | string | Comma-separated event types (e.g., `auth.success,auth.failure`) |
| `user` | string | Filter by username |
| `resource` | string | Filter by resource (topic, user, etc.) |
| `result` | string | Filter by result (`success`, `failure`, `denied`) |
| `search` | string | Full-text search query |
| `limit` | int | Maximum events to return (default: 100, max: 10000) |
| `offset` | int | Offset for pagination |

**Response:**
```json
{
  "events": [
    {
      "id": "evt_abc123",
      "timestamp": "2026-01-17T10:30:00Z",
      "type": "auth.success",
      "user": "admin",
      "client_ip": "192.168.1.100",
      "resource": "",
      "action": "authenticate",
      "result": "success",
      "details": {
        "method": "password"
      },
      "node_id": "node-1"
    }
  ],
  "total_count": 150,
  "has_more": true
}
```

**Event Types:**
- `auth.success` - Successful authentication
- `auth.failure` - Failed authentication attempt
- `auth.logout` - User logout
- `access.granted` - Access granted to resource
- `access.denied` - Access denied to resource
- `topic.create` - Topic created
- `topic.delete` - Topic deleted
- `topic.modify` - Topic modified
- `user.create` - User created
- `user.delete` - User deleted
- `user.modify` - User modified
- `acl.change` - ACL rule changed
- `config.change` - Configuration changed
- `cluster.join` - Node joined cluster
- `cluster.leave` - Node left cluster

### GET /audit/export

Export audit events in JSON or CSV format.

**Query Parameters:**
Same as `/audit/events` plus:
| Parameter | Type | Description |
|-----------|------|-------------|
| `format` | string | Export format: `json` (default) or `csv` |

**Response:**
- `format=json`: JSON array of events
- `format=csv`: CSV file with headers

---

## Swagger UI

When the Admin API is enabled, an interactive Swagger UI is available at:

```
http://localhost:9096/swagger/    # HTTP
https://localhost:9096/swagger/   # HTTPS (when TLS enabled)
```

Alternative paths also work:
- `/api/v1/swagger/` - Same UI under the API path
- `/swagger.json` or `/api/v1/swagger.json` - Raw OpenAPI specification

The Swagger UI provides interactive documentation for all Admin API endpoints.

