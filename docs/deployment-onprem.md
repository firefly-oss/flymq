# FlyMQ On-Premise Deployment Guide

This guide covers deploying FlyMQ on bare metal servers, virtual machines, and on-premise infrastructure.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Deployment Options](#deployment-options)
- [Bare Metal Installation](#bare-metal-installation)
- [Docker Deployment](#docker-deployment)
- [Manual Configuration](#manual-configuration)
- [Network Configuration](#network-configuration)
- [Storage Configuration](#storage-configuration)
- [High Availability](#high-availability)
- [Monitoring](#monitoring)
- [Security](#security)
- [Troubleshooting](#troubleshooting)

---

## Overview

On-premise deployment gives you full control over your FlyMQ infrastructure. Choose from:

| Method | Best For | Complexity |
|--------|----------|------------|
| Interactive Installer | Quick setup, single nodes | Low |
| Docker Bootstrap/Agent | Multi-host clusters | Low |
| Manual Configuration | Custom environments | Medium |
| Kubernetes (self-hosted) | Container orchestration | High |

---

## Prerequisites

### Hardware Requirements

| Component | Minimum | Recommended | Production |
|-----------|---------|-------------|------------|
| CPU | 2 cores | 4 cores | 8+ cores |
| Memory | 2 GB | 8 GB | 16+ GB |
| Storage | 20 GB SSD | 100 GB SSD | 500+ GB NVMe |
| Network | 1 Gbps | 10 Gbps | 10+ Gbps |

### Software Requirements

- **Operating System**: Linux (Ubuntu 20.04+, RHEL 8+, Debian 11+), macOS 12+
- **Go 1.21+** (for source installation)
- **Docker 24+** (for container deployment)
- **systemd** (for service management on Linux)

### Network Requirements

| Port | Protocol | Purpose |
|------|----------|---------|
| 9092 | TCP | Client connections |
| 9093 | TCP | Cluster communication (Raft) |
| 9094 | TCP | Prometheus metrics |
| 9095 | TCP | Health checks |
| 9096 | TCP | Admin API |

---

## Bare Metal Installation

### Interactive Installer (Recommended)

The interactive installer guides you through configuration:

```bash
# Clone the repository
git clone https://github.com/firefly-oss/flymq
cd flymq

# Run the installer
./install.sh
```

**Installation Flow:**

1. **Deployment Mode Selection** - Choose standalone or cluster mode first
2. **Cluster Configuration** (if cluster mode):
   - Bootstrap new cluster or join existing
   - Automatic node discovery via mDNS (Bonjour/Avahi)
   - Manual peer entry if discovery unavailable
   - Connectivity validation to peer nodes
3. **Configuration Summary** - Review all settings with iterative modification
4. **Section Customization** - Modify any section (Deployment, Security, Advanced, Observability, Performance)
5. **Installation** - Build, install, and configure systemd/launchd service

**Configuration Sections:**

| Section | Settings |
|---------|----------|
| **Deployment** | Mode, bind address, cluster address, peers, replication factor |
| **Security** | TLS, encryption at rest, authentication, admin credentials |
| **Advanced** | Schema validation, DLQ, TTL, delayed delivery, transactions |
| **Observability** | Prometheus metrics, OpenTelemetry, health checks, Admin API |
| **Performance** | Acks mode, segment size, log level |

### Cluster Setup with Installer

**On the first node (bootstrap):**

```bash
./install.sh
# 1. Select: Cluster mode
# 2. Select: Bootstrap new cluster (first node)
# 3. Configure cluster address (e.g., 192.168.1.100:9093)
# 4. Enable service discovery (recommended)
# 5. Review and confirm configuration
```

**On additional nodes:**

```bash
./install.sh
# 1. Select: Cluster mode
# 2. Select: Join existing cluster
# 3. Automatic discovery will find existing nodes (if mDNS enabled)
#    OR manually enter peer address: 192.168.1.100:9093
# 4. Connectivity validation runs automatically
# 5. Review and confirm configuration
```

### Automatic Node Discovery

FlyMQ includes mDNS-based service discovery for automatic cluster node detection:

```bash
# Discover FlyMQ nodes on the network
flymq-discover

# Example output:
# Found 2 FlyMQ node(s):
#
#   [1] node-1
#       Cluster Address: 192.168.1.100:9093
#       Client Address:  192.168.1.100:9092
#       Cluster ID:      production
#       Version:         1.0.0
#
#   [2] node-2
#       Cluster Address: 192.168.1.101:9093
#       Client Address:  192.168.1.101:9092
```

The installer uses this tool automatically when joining a cluster. If no nodes are found:
- Ensure existing nodes have discovery enabled (`discovery.enabled: true`)
- Check that mDNS/Bonjour is not blocked by firewall (UDP port 5353)
- Fall back to manual peer entry

### Quick Install (Non-Interactive)

```bash
# Install with defaults
./install.sh --yes

# Install with custom prefix
./install.sh --prefix /opt/flymq --yes

# Install with existing config
./install.sh --config-file /path/to/flymq.conf --yes
```

---

## Docker Deployment

### Enterprise Cluster Deployment (Recommended)

FlyMQ provides a Rancher-style enterprise deployment script that handles TLS certificates, encryption, authentication, and cluster formation automatically.

**Prerequisites (all hosts):**
- Docker 24+ with Docker Compose
- OpenSSL (for certificate generation on bootstrap server)
- FlyMQ codebase cloned to the same path on all hosts
- Network connectivity between hosts on ports 9092, 9093, 9095, 9096

---

### Quick Start: Join Bundle Workflow (Recommended)

The simplest way to deploy a cluster is using **join bundles** - a single file containing everything agents need:

**Step 1: Start the Bootstrap Server**

```bash
cd /path/to/flymq
./deploy/docker/scripts/setup-enterprise.sh server
```

The server automatically creates `deploy/docker/flymq-join-bundle.tar.gz` containing:
- All TLS certificates (CA, server, client)
- Join configuration (server address, token, encryption key)

**Step 2: Copy Bundle and Join Agents**

```bash
# Copy the bundle to each agent host
scp deploy/docker/flymq-join-bundle.tar.gz agent1:/path/to/flymq/deploy/docker/
scp deploy/docker/flymq-join-bundle.tar.gz agent2:/path/to/flymq/deploy/docker/

# On each agent host, run:
./deploy/docker/scripts/setup-enterprise.sh agent --bundle flymq-join-bundle.tar.gz
```

That's it! The agent extracts the bundle and joins the cluster automatically.

**Step 3: Verify Cluster Formation**

```bash
./deploy/docker/scripts/setup-enterprise.sh status
```

---

### Alternative: Manual Certificate Distribution

For environments requiring more control over certificate distribution, you can manually export and copy certificates.

**Step 1: Start the Bootstrap Server**

```bash
./deploy/docker/scripts/setup-enterprise.sh server --admin-password MySecurePass123
```

**Step 2: Export and Distribute Certificates**

```bash
# Export certificates
./deploy/docker/scripts/setup-enterprise.sh export-certs ./certs-for-agents

# Get the join token
./deploy/docker/scripts/setup-enterprise.sh token

# Copy to each agent host
scp -r ./certs-for-agents/* agent1:/path/to/flymq/deploy/docker/certs/
```

**Step 3: Join Agent Nodes**

```bash
# On each agent host
./deploy/docker/scripts/setup-enterprise.sh agent \
  --server 192.168.1.100:9093 \
  --token BCxst47nnZqyd0PmG2YcPj4qP936J
```

---

### Cluster Management Commands

```bash
# View cluster status
./deploy/docker/scripts/setup-enterprise.sh status

# View server logs
./deploy/docker/scripts/setup-enterprise.sh logs

# View join bundle info and agent instructions
./deploy/docker/scripts/setup-enterprise.sh bundle

# Get join token (for manual setup)
./deploy/docker/scripts/setup-enterprise.sh token

# Export certificates (for manual setup)
./deploy/docker/scripts/setup-enterprise.sh export-certs ./certs-export

# Stop the node
./deploy/docker/scripts/setup-enterprise.sh stop

# Stop and remove all data (DESTRUCTIVE)
./deploy/docker/scripts/setup-enterprise.sh stop --volumes
```

---

### Network and Firewall Requirements

Ensure the following ports are open between all cluster nodes:

| Port | Protocol | Direction | Purpose |
|------|----------|-----------|---------|
| 9092 | TCP | Inbound | Client connections (TLS) |
| 9093 | TCP | Bidirectional | Cluster/Raft communication (TLS) |
| 9094 | TCP | Inbound | Prometheus metrics (optional) |
| 9095 | TCP | Inbound | Health checks |
| 9096 | TCP | Inbound | Admin API (TLS) |

**Firewall examples:**

```bash
# iptables (Linux)
iptables -A INPUT -p tcp --dport 9092 -j ACCEPT
iptables -A INPUT -p tcp --dport 9093 -s 192.168.1.0/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 9095 -j ACCEPT
iptables -A INPUT -p tcp --dport 9096 -s 10.0.0.0/8 -j ACCEPT

# AWS Security Group
aws ec2 authorize-security-group-ingress \
  --group-id sg-xxx \
  --protocol tcp --port 9092-9096 \
  --source-group sg-xxx  # Same security group for cluster nodes
```

---

### Advertise Address Configuration

FlyMQ automatically detects the correct IP address to advertise to other nodes. For manual override (e.g., behind NAT), set environment variables before starting:

```bash
export FLYMQ_ADVERTISE_ADDR=<external-ip>:9092
export FLYMQ_ADVERTISE_CLUSTER=<external-ip>:9093
./deploy/docker/scripts/setup-enterprise.sh server --admin-password MySecurePass123
```

---

### Client Connection with TLS

After deployment, connect using the CLI with TLS:

```bash
./bin/flymq-cli --tls --ca-cert deploy/docker/certs/ca.crt \
  -u admin -P 'YourPassword' \
  topics
```

---

### Join Bundle Troubleshooting

**Problem: Bundle file not found**
```bash
# Check if bundle exists on server
ls -la deploy/docker/flymq-join-bundle.tar.gz

# If missing, regenerate by restarting server
./deploy/docker/scripts/setup-enterprise.sh stop
./deploy/docker/scripts/setup-enterprise.sh server
```

**Problem: Bundle extraction fails**
```bash
# Verify bundle is a valid tarball
tar -tzf flymq-join-bundle.tar.gz

# Check bundle permissions
ls -la flymq-join-bundle.tar.gz
# Should be readable (at least 600)
```

**Problem: Agent fails after bundle extraction**
```bash
# Verify certificates were extracted
ls -la deploy/docker/certs/

# Check join config was created
cat deploy/docker/.join-config

# Try manual join with extracted config
source deploy/docker/.join-config
./deploy/docker/scripts/setup-enterprise.sh agent \
  --server "$FLYMQ_SERVER" --token "$FLYMQ_TOKEN"
```

---

### Certificate Distribution Troubleshooting

**Problem: Agent fails with "certificate signed by unknown authority"**
```bash
# Verify CA certificate matches on both hosts
openssl x509 -in deploy/docker/certs/ca.crt -noout -fingerprint -sha256
# Compare fingerprints - they must match

# Re-copy certificates from bootstrap server (or use bundle)
scp bootstrap-server:/path/to/flymq/deploy/docker/flymq-join-bundle.tar.gz .
./deploy/docker/scripts/setup-enterprise.sh agent --bundle flymq-join-bundle.tar.gz
```

**Problem: "permission denied" when reading certificates**
```bash
# Fix permissions
chmod 600 deploy/docker/certs/*.key
chmod 644 deploy/docker/certs/*.crt
chown $(whoami):$(whoami) deploy/docker/certs/*
```

**Problem: Agent cannot connect to bootstrap server**
```bash
# Test network connectivity
nc -zv 192.168.1.100 9093
telnet 192.168.1.100 9093

# Test TLS handshake
openssl s_client -connect 192.168.1.100:9093 \
  -CAfile deploy/docker/certs/ca.crt

# Check firewall on bootstrap server
sudo iptables -L -n | grep 9093
```

**Problem: "invalid token" error**
```bash
# Get fresh token from bootstrap server
./deploy/docker/scripts/setup-enterprise.sh token

# Or get fresh bundle (contains new token)
./deploy/docker/scripts/setup-enterprise.sh bundle
```

**Problem: Certificates expired**
```bash
# Check certificate expiration
openssl x509 -in deploy/docker/certs/server.crt -noout -dates

# Regenerate certificates and bundle (on bootstrap server)
./deploy/docker/scripts/setup-enterprise.sh stop --volumes
rm -rf deploy/docker/certs deploy/docker/flymq-join-bundle.tar.gz
./deploy/docker/scripts/setup-enterprise.sh server

# Re-distribute bundle to all agents
```

---

## Manual Configuration

### Configuration File

Create `/etc/flymq/flymq.conf`:

```toml
# Network
bind_addr = "0.0.0.0:9092"
cluster_addr = "0.0.0.0:9093"
node_id = "node-1"

# Storage
data_dir = "/var/lib/flymq"
segment_bytes = 1073741824
retention_bytes = 10737418240

# Cluster (for multi-node)
[cluster]
enabled = true
peers = ["192.168.1.101:9093", "192.168.1.102:9093"]

# Security
[security]
tls_enabled = true
tls_cert_file = "/etc/flymq/server.crt"
tls_key_file = "/etc/flymq/server.key"

# Authentication
[auth]
enabled = true
admin_username = "admin"
admin_password = "your-secure-password"

# Observability
[observability.health]
enabled = true
addr = "0.0.0.0:9095"

[observability.admin]
enabled = true
addr = "0.0.0.0:9096"

[observability.metrics]
enabled = true
addr = "0.0.0.0:9094"
```

### Systemd Service

Create `/etc/systemd/system/flymq.service`:

```ini
[Unit]
Description=FlyMQ Message Queue
After=network.target

[Service]
Type=simple
User=flymq
Group=flymq
ExecStart=/usr/local/bin/flymq --config /etc/flymq/flymq.conf
Restart=always
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable flymq
sudo systemctl start flymq
sudo systemctl status flymq
```

---

## Network Configuration

### Firewall Rules (iptables)

```bash
# Client connections
sudo iptables -A INPUT -p tcp --dport 9092 -j ACCEPT

# Cluster communication (internal only)
sudo iptables -A INPUT -p tcp --dport 9093 -s 192.168.1.0/24 -j ACCEPT

# Health checks
sudo iptables -A INPUT -p tcp --dport 9095 -j ACCEPT

# Admin API (restrict to management network)
sudo iptables -A INPUT -p tcp --dport 9096 -s 10.0.0.0/8 -j ACCEPT

# Metrics (Prometheus scraping)
sudo iptables -A INPUT -p tcp --dport 9094 -s 10.0.0.0/8 -j ACCEPT
```

### Firewall Rules (firewalld)

```bash
# Create FlyMQ zone
sudo firewall-cmd --permanent --new-zone=flymq

# Add ports
sudo firewall-cmd --permanent --zone=flymq --add-port=9092/tcp
sudo firewall-cmd --permanent --zone=flymq --add-port=9093/tcp
sudo firewall-cmd --permanent --zone=flymq --add-port=9095/tcp
sudo firewall-cmd --permanent --zone=flymq --add-port=9096/tcp

# Reload
sudo firewall-cmd --reload
```

### Load Balancer Configuration (HAProxy)

```haproxy
frontend flymq_clients
    bind *:9092
    mode tcp
    default_backend flymq_nodes

backend flymq_nodes
    mode tcp
    balance leastconn
    option tcp-check
    server node1 192.168.1.100:9092 check
    server node2 192.168.1.101:9092 check
    server node3 192.168.1.102:9092 check

frontend flymq_health
    bind *:9095
    mode http
    default_backend flymq_health_nodes

backend flymq_health_nodes
    mode http
    balance roundrobin
    option httpchk GET /health
    server node1 192.168.1.100:9095 check
    server node2 192.168.1.101:9095 check
    server node3 192.168.1.102:9095 check
```

---

## Storage Configuration

### Filesystem Recommendations

| Filesystem | Recommendation |
|------------|----------------|
| **ext4** | Good for most workloads |
| **XFS** | Better for large files, high throughput |
| **ZFS** | Best for data integrity, compression |

### Mount Options

```bash
# /etc/fstab entry for data volume
/dev/sdb1 /var/lib/flymq xfs defaults,noatime,nodiratime 0 2
```

### RAID Configuration

For production, use RAID 10 for best performance and redundancy:

```bash
# Create RAID 10 array
mdadm --create /dev/md0 --level=10 --raid-devices=4 /dev/sd[bcde]

# Format and mount
mkfs.xfs /dev/md0
mount /dev/md0 /var/lib/flymq
```

---

## High Availability

### Recommended Cluster Topology

```
┌─────────────────────────────────────────────────────────────┐
│                      Load Balancer                          │
│                    (HAProxy/nginx)                          │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│   FlyMQ Node  │     │   FlyMQ Node  │     │   FlyMQ Node  │
│   (Leader)    │◄───►│  (Follower)   │◄───►│  (Follower)   │
│ 192.168.1.100 │     │ 192.168.1.101 │     │ 192.168.1.102 │
└───────────────┘     └───────────────┘     └───────────────┘
        │                     │                     │
        ▼                     ▼                     ▼
┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│  Local SSD    │     │  Local SSD    │     │  Local SSD    │
│   Storage     │     │   Storage     │     │   Storage     │
└───────────────┘     └───────────────┘     └───────────────┘
```

### Failure Scenarios

| Scenario | Impact | Recovery |
|----------|--------|----------|
| Single node failure | Automatic failover (2-3s) | Replace node, auto-sync |
| Network partition | Minority partition read-only | Restore connectivity |
| Storage failure | Node becomes unavailable | Replace storage, restore from peers |

---

## Monitoring

### Prometheus Configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'flymq'
    static_configs:
      - targets:
        - '192.168.1.100:9094'
        - '192.168.1.101:9094'
        - '192.168.1.102:9094'
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
        regex: '([^:]+):\d+'
        replacement: '${1}'
```

### Key Metrics to Monitor

| Metric | Alert Threshold | Description |
|--------|-----------------|-------------|
| `flymq_cluster_leader` | No leader for >30s | Cluster has no leader |
| `flymq_consumer_group_lag` | >10000 messages | Consumer falling behind |
| `flymq_storage_bytes_total` | >80% capacity | Storage filling up |
| `flymq_server_connections_active` | >1000 | High connection count |

---

## Security

### TLS Configuration

Generate certificates:

```bash
# Generate CA
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt \
  -subj "/CN=FlyMQ CA"

# Generate server certificate
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
  -subj "/CN=flymq.example.com"
openssl x509 -req -days 365 -in server.csr \
  -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt
```

### Authentication Setup

```bash
# Create admin user during installation
./install.sh
# Enable authentication when prompted
# Set admin username and password

# Or configure manually
cat > /etc/flymq/users.json << 'EOF'
{
  "users": [
    {
      "username": "admin",
      "password_hash": "$2a$10$...",
      "roles": ["admin"],
      "enabled": true
    }
  ]
}
EOF
```

---

## Troubleshooting

### Common Issues

**Node won't join cluster:**
```bash
# Check connectivity
telnet bootstrap-node 9093

# Check logs (Docker deployment)
./deploy/docker/scripts/setup-enterprise.sh logs

# Check logs (systemd)
journalctl -u flymq -f

# Verify cluster status
./deploy/docker/scripts/setup-enterprise.sh status

# Check token
./deploy/docker/scripts/setup-enterprise.sh token
```

**TLS certificate issues:**
```bash
# Verify certificates exist
ls -la deploy/docker/certs/

# Test TLS connection
openssl s_client -connect localhost:9092 -CAfile deploy/docker/certs/ca.crt

# Regenerate certificates (stop server first)
./deploy/docker/scripts/setup-enterprise.sh stop --volumes
rm -rf deploy/docker/certs
./deploy/docker/scripts/setup-enterprise.sh server --admin-password NewPass123
```

**Authentication failures:**
```bash
# Test with CLI
./bin/flymq-cli --tls --ca-cert deploy/docker/certs/ca.crt \
  -u admin -P 'YourPassword' topics

# Check admin password in .env.enterprise
grep ADMIN_PASSWORD deploy/docker/.env.enterprise
```

**High latency:**
```bash
# Check disk I/O
iostat -x 1

# Check network
ping -c 10 peer-node

# Check memory
free -h
```

**Out of disk space:**
```bash
# Check retention settings
grep retention /etc/flymq/flymq.conf

# Manually clean old segments
ls -la /var/lib/flymq/topics/*/partitions/*/

# Check Docker volumes
docker system df
```

---

**Copyright © 2026 Firefly Software Solutions Inc.**
