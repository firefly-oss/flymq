# FlyMQ Observability Guide

This document covers monitoring, metrics, tracing, and health checks for FlyMQ.

## Prometheus Metrics

FlyMQ exposes Prometheus metrics at `http://localhost:9094/metrics` by default.

### Available Metrics

#### Server Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `flymq_server_uptime_seconds` | Gauge | Server uptime in seconds |
| `flymq_server_connections_active` | Gauge | Current active client connections |
| `flymq_server_connections_total` | Counter | Total connections since startup |
| `flymq_server_goroutines` | Gauge | Number of active goroutines |
| `flymq_server_memory_bytes` | Gauge | Memory usage in bytes |

#### Message Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `flymq_messages_produced_total` | Counter | `topic` | Total messages produced |
| `flymq_messages_consumed_total` | Counter | `topic`, `group` | Total messages consumed |
| `flymq_messages_bytes_produced_total` | Counter | `topic` | Total bytes produced |
| `flymq_messages_bytes_consumed_total` | Counter | `topic`, `group` | Total bytes consumed |
| `flymq_produce_latency_seconds` | Histogram | `topic` | Produce latency distribution |
| `flymq_consume_latency_seconds` | Histogram | `topic`, `group` | Consume latency distribution |

#### Topic Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `flymq_topic_partitions` | Gauge | `topic` | Number of partitions |
| `flymq_topic_messages_total` | Gauge | `topic`, `partition` | Messages in partition |
| `flymq_topic_bytes_total` | Gauge | `topic`, `partition` | Bytes in partition |
| `flymq_topic_oldest_offset` | Gauge | `topic`, `partition` | Oldest available offset |
| `flymq_topic_newest_offset` | Gauge | `topic`, `partition` | Newest offset |

#### Consumer Group Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `flymq_consumer_group_members` | Gauge | `group` | Active group members |
| `flymq_consumer_group_lag` | Gauge | `group`, `topic`, `partition` | Consumer lag |
| `flymq_consumer_group_rebalances_total` | Counter | `group` | Total rebalances |
| `flymq_consumer_group_commits_total` | Counter | `group`, `topic` | Offset commits |

#### Cluster Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `flymq_cluster_nodes` | Gauge | | Total cluster nodes |
| `flymq_cluster_leader` | Gauge | `node_id` | 1 if node is leader |
| `flymq_raft_term` | Gauge | | Current Raft term |
| `flymq_raft_commit_index` | Gauge | | Raft commit index |
| `flymq_raft_elections_total` | Counter | | Total leader elections |

#### Storage Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `flymq_storage_segments` | Gauge | `topic`, `partition` | Active segments |
| `flymq_storage_bytes_written_total` | Counter | `topic`, `partition` | Bytes written |
| `flymq_storage_bytes_read_total` | Counter | `topic`, `partition` | Bytes read |
| `flymq_storage_sync_latency_seconds` | Histogram | | Disk sync latency |

---

## OpenTelemetry Tracing

FlyMQ supports distributed tracing via OpenTelemetry.

### Configuration

```json
{
  "observability": {
    "tracing": {
      "enabled": true,
      "endpoint": "localhost:4317",
      "sample_rate": 0.1,
      "service_name": "flymq"
    }
  }
}
```

### Traced Operations

| Span Name | Description |
|-----------|-------------|
| `flymq.produce` | Message production |
| `flymq.consume` | Message consumption |
| `flymq.fetch` | Batch fetch operation |
| `flymq.commit` | Offset commit |
| `flymq.storage.append` | Storage append |
| `flymq.storage.read` | Storage read |
| `flymq.raft.append_entries` | Raft replication |

### Span Attributes

| Attribute | Description |
|-----------|-------------|
| `flymq.topic` | Topic name |
| `flymq.partition` | Partition number |
| `flymq.offset` | Message offset |
| `flymq.group_id` | Consumer group ID |
| `flymq.message_size` | Message size in bytes |

---

## Health Checks

FlyMQ provides Kubernetes-compatible health endpoints.

### Endpoints

| Endpoint | Purpose | Success | Failure |
|----------|---------|---------|---------|
| `GET /live` | Liveness probe | `200 OK` | `503` |
| `GET /ready` | Readiness probe | `200 OK` | `503` |
| `GET /health` | Detailed health | JSON status | JSON with errors |

### Health Response

```json
{
  "status": "healthy",
  "checks": {
    "storage": {"status": "healthy", "latency_ms": 2},
    "cluster": {"status": "healthy", "leader": "node-1"},
    "memory": {"status": "healthy", "used_mb": 512, "limit_mb": 2048}
  },
  "uptime": "7d12h30m",
  "version": "1.0.0"
}
```

### Kubernetes Configuration

```yaml
livenessProbe:
  httpGet:
    path: /live
    port: 9095
  initialDelaySeconds: 10
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /ready
    port: 9095
  initialDelaySeconds: 5
  periodSeconds: 5
```

---

## Audit Trail

FlyMQ includes a comprehensive audit trail system for security monitoring and compliance. Audit logging is **enabled by default**.

### Tracked Events

| Category | Events | Description |
|----------|--------|-------------|
| Authentication | `auth.success`, `auth.failure`, `auth.logout` | User login/logout events |
| Authorization | `access.granted`, `access.denied` | Resource access decisions |
| Topics | `topic.create`, `topic.delete`, `topic.modify` | Topic management |
| Users | `user.create`, `user.delete`, `user.modify` | User management |
| ACLs | `acl.change` | Access control changes |
| Cluster | `cluster.join`, `cluster.leave` | Node membership |

### Querying Audit Events

**Via CLI:**
```bash
# List recent events
flymq-cli audit list

# Query with filters
flymq-cli audit query --user admin --type auth.failure --start 2026-01-01T00:00:00Z

# Export for SIEM integration
flymq-cli audit export --format json --output /var/log/flymq-audit.json
```

**Via Admin API:**
```bash
# Query events
curl -u admin:password "http://localhost:8080/api/v1/audit/events?type=auth.failure&limit=100"

# Export as CSV for compliance reporting
curl -u admin:password "http://localhost:8080/api/v1/audit/export?format=csv" > audit.csv
```

### SIEM Integration

Audit logs are stored in JSON Lines format, making them easy to ingest into SIEM systems:

```bash
# Stream audit logs to Splunk
tail -f /var/lib/flymq/audit/audit.log | curl -X POST -d @- https://splunk:8088/services/collector

# Ship to Elasticsearch
filebeat -c filebeat-flymq.yml
```

---

## Grafana Dashboard

A sample Grafana dashboard is available at `examples/grafana/flymq-dashboard.json`.

Key panels include:
- Message throughput (produce/consume rates)
- Consumer lag by group
- Latency percentiles (p50, p95, p99)
- Cluster health and leader status
- Storage utilization

