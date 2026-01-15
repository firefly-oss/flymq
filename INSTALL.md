# FlyMQ Installation Guide

FlyMQ supports installation on Linux, macOS, and Windows. Choose the appropriate installation method for your platform.

## Table of Contents

- [Linux & macOS](#linux--macos)
- [Windows](#windows)
- [Windows (Git Bash/MSYS2/WSL)](#windows-git-bashmsys2wsl)
- [Prerequisites](#prerequisites)
- [Installation Options](#installation-options)
- [Docker Deployment](#docker-deployment)
- [Cloud Deployment](#cloud-deployment)
- [Post-Installation](#post-installation)

---

## Prerequisites

All platforms require:
- **Go 1.21+** - [Download Go](https://go.dev/dl/)

---

## Linux & macOS

### Interactive Installation

```bash
./install.sh
```

This will guide you through the installation process with prompts for:
- Deployment mode (standalone or cluster)
- Network settings
- Storage configuration
- Security options (TLS, encryption, authentication)
- Advanced features (schema validation, DLQ, transactions, etc.)
- Observability settings (metrics, health checks, tracing, Admin API TLS)
- System integration (systemd on Linux, launchd on macOS)

### Quick Install (Defaults)

```bash
./install.sh --yes
```

### Custom Installation

```bash
# Custom installation prefix
./install.sh --prefix /opt/flymq

# Use existing configuration
./install.sh --config-file my-config.conf
```

### Uninstall

```bash
./install.sh --uninstall
```

---

## Windows

### PowerShell Installation

For native Windows (PowerShell), use the PowerShell installer:

```powershell
.\install.ps1
```

### Quick Install

```powershell
.\install.ps1 -Yes
```

### Custom Installation

```powershell
# Custom installation prefix
.\install.ps1 -Prefix "C:\Program Files\FlyMQ"
```

### Uninstall

```powershell
.\install.ps1 -Uninstall
```

---

## Windows (Git Bash/MSYS2/WSL)

If you're using Git Bash, MSYS2, or WSL on Windows, you can use the bash installer:

```bash
./install.sh
```

The script automatically detects your environment:
- **WSL**: Treated as Linux
- **Git Bash/MSYS2**: Treated as Windows with appropriate paths

---

## Installation Options

### Deployment Modes

#### Standalone Mode
Single-node deployment suitable for development and testing:
```bash
./install.sh
# Select option 1 when prompted for deployment mode
```

#### Cluster Mode
Multi-node deployment for production use with high availability:
```bash
./install.sh
# Select option 2 when prompted for deployment mode
```

When choosing cluster mode, you'll configure:
- **Bootstrap node**: First node in a new cluster
- **Join node**: Node joining an existing cluster
- Replication factor (recommended: 3)
- Minimum in-sync replicas

### Default Installation Paths

#### Linux (root user)
- Binaries: `/usr/local/bin`
- Configuration: `/etc/flymq`
- Data: `/var/lib/flymq`

#### Linux (non-root user)
- Binaries: `~/.local/bin`
- Configuration: `~/.config/flymq`
- Data: `~/.local/share/flymq`

#### macOS (root user)
- Binaries: `/usr/local/bin`
- Configuration: `/etc/flymq`
- Data: `/var/lib/flymq`

#### macOS (non-root user)
- Binaries: `~/.local/bin`
- Configuration: `~/.config/flymq`
- Data: `~/.local/share/flymq`

#### Windows
- Binaries: `%LOCALAPPDATA%\FlyMQ\bin`
- Configuration: `%LOCALAPPDATA%\FlyMQ\config`
- Data: `%LOCALAPPDATA%\FlyMQ\data`

---

## Post-Installation

### Add to PATH

#### Linux/macOS
If you installed to `~/.local`, add to your shell profile:

```bash
# For bash (~/.bashrc or ~/.bash_profile)
export PATH="$HOME/.local/bin:$PATH"

# For zsh (~/.zshrc)
export PATH="$HOME/.local/bin:$PATH"
```

#### Windows (PowerShell)
```powershell
# Temporary (current session)
$env:PATH += ";$env:LOCALAPPDATA\FlyMQ\bin"

# Permanent
[Environment]::SetEnvironmentVariable('Path', $env:PATH + ";$env:LOCALAPPDATA\FlyMQ\bin", 'User')
```

### Verify Installation

```bash
flymq --version
flymq-cli --version
```

### Start FlyMQ

#### Standalone

```bash
# Linux/macOS
flymq --config ~/.config/flymq/flymq.conf

# Windows
flymq --config %LOCALAPPDATA%\FlyMQ\config\flymq.conf
```

#### With System Service

##### Linux (systemd)
```bash
sudo systemctl enable flymq
sudo systemctl start flymq
sudo systemctl status flymq
```

##### macOS (launchd)
```bash
launchctl load ~/Library/LaunchAgents/com.firefly.flymq.plist
launchctl list | grep flymq
```

##### Windows (NSSM)
```powershell
# Download NSSM from https://nssm.cc/
nssm install flymq "C:\Users\...\FlyMQ\bin\flymq.exe" --config "C:\Users\...\FlyMQ\config\flymq.conf"
nssm start flymq
```

---

## Quick Start Examples

### Produce a Message
```bash
flymq-cli produce my-topic "Hello, FlyMQ!"
```

### Subscribe to Messages
```bash
flymq-cli subscribe my-topic --from earliest
```

### List Topics
```bash
flymq-cli topics
```

### Check Cluster Status (Cluster Mode)
```bash
flymq-cli cluster status
```

---

## Troubleshooting

### Go Not Found
Ensure Go is installed and in your PATH:
```bash
go version
```

If not installed, download from https://go.dev/dl/

### Permission Denied (Linux/macOS)
Make the install script executable:
```bash
chmod +x install.sh
./install.sh
```

### Build Errors
Ensure you're in the correct directory and have all source files:
```bash
ls cmd/flymq/main.go
ls cmd/flymq-cli/main.go
```

### Port Already in Use
Default ports:
- Client: 9092
- Cluster: 9093
- Metrics: 9094
- Health: 9095
- Admin API: 9096

Change ports in `flymq.conf` or during interactive installation.

---

## Configuration

After installation, edit your configuration file:

```bash
# Linux/macOS
nano ~/.config/flymq/flymq.conf

# Windows
notepad %LOCALAPPDATA%\FlyMQ\config\flymq.conf
```

See the [Configuration Guide](docs/configuration.md) for detailed options.

---

## Advanced Installation

### Cross-Compilation

Build for different platforms:

```bash
# Linux AMD64
GOOS=linux GOARCH=amd64 go build -o flymq-linux-amd64 cmd/flymq/main.go

# Windows AMD64
GOOS=windows GOARCH=amd64 go build -o flymq-windows-amd64.exe cmd/flymq/main.go

# macOS ARM64 (Apple Silicon)
GOOS=darwin GOARCH=arm64 go build -o flymq-darwin-arm64 cmd/flymq/main.go
```

### Docker Installation

```bash
# Build Docker image
docker build -t flymq:latest -f deploy/docker/Dockerfile .

# Run standalone container
docker run -d \
  -p 9092:9092 \
  -p 9095:9095 \
  -p 9096:9096 \
  -v $(pwd)/data:/data \
  flymq:latest
```

---

## Docker Deployment

FlyMQ provides an enterprise deployment script for easy, secure cluster deployment with TLS, authentication, and encryption enabled by default.

### Quick Start (Rancher-Style)

The fastest way to deploy a FlyMQ cluster uses **join bundles** - a single file containing everything agents need:

```bash
# Step 1: Start the server (creates join bundle automatically)
./deploy/docker/scripts/setup-enterprise.sh server

# Step 2: Copy the bundle to agent hosts
scp deploy/docker/flymq-join-bundle.tar.gz agent1:/path/to/flymq/deploy/docker/

# Step 3: On each agent, run:
./deploy/docker/scripts/setup-enterprise.sh agent --bundle flymq-join-bundle.tar.gz
```

That's it! The join bundle contains TLS certificates, encryption key, server address, and join token.

### Enterprise Deployment Script

The `setup-enterprise.sh` script provides comprehensive cluster management:

```bash
# Start bootstrap server (generates TLS certs, encryption key, admin user, join bundle)
./deploy/docker/scripts/setup-enterprise.sh server

# View cluster status
./deploy/docker/scripts/setup-enterprise.sh status

# View logs
./deploy/docker/scripts/setup-enterprise.sh logs

# View join bundle info and agent instructions
./deploy/docker/scripts/setup-enterprise.sh bundle

# Get join token (for manual setup without bundle)
./deploy/docker/scripts/setup-enterprise.sh token

# Export certificates (for manual setup without bundle)
./deploy/docker/scripts/setup-enterprise.sh export-certs ./certs-export

# Stop the node
./deploy/docker/scripts/setup-enterprise.sh stop

# Stop and remove all data
./deploy/docker/scripts/setup-enterprise.sh stop --volumes
```

### Adding Remote Agents

#### Method 1: Join Bundle (Recommended)

The server automatically creates a join bundle at `deploy/docker/flymq-join-bundle.tar.gz`. This is the simplest approach:

```bash
# On the server: bundle is created automatically when you run 'server'
# View bundle info:
./deploy/docker/scripts/setup-enterprise.sh bundle

# Copy bundle to agent host
scp deploy/docker/flymq-join-bundle.tar.gz agent1:/path/to/flymq/deploy/docker/

# On the agent host: join with single command
./deploy/docker/scripts/setup-enterprise.sh agent --bundle flymq-join-bundle.tar.gz
```

#### Method 2: Manual Setup (Advanced)

For environments where you need more control, you can manually distribute certificates:

**1. On the bootstrap server - Export certificates:**

```bash
./deploy/docker/scripts/setup-enterprise.sh export-certs ./certs-for-agents
./deploy/docker/scripts/setup-enterprise.sh token
# Output: Join Token: BCxst47nnZqyd0PmG2YcPj4qP936J
```

**2. Transfer certificates to agent hosts:**

```bash
scp -r ./certs-for-agents/* agent1:/path/to/flymq/deploy/docker/certs/
```

**3. On each agent host - Join the cluster:**

```bash
./deploy/docker/scripts/setup-enterprise.sh agent \
  --server 192.168.1.100:9093 \
  --token BCxst47nnZqyd0PmG2YcPj4qP936J
```

> **Troubleshooting**: See the [On-Premise Deployment Guide](docs/deployment-onprem.md#certificate-distribution-troubleshooting) for common issues.

### Docker Compose Files

| File | Purpose |
|------|---------|
| `deploy/docker/docker-compose.enterprise.yml` | Bootstrap server with TLS/auth |
| `deploy/docker/docker-compose.enterprise-agent.yml` | Agent nodes with TLS/auth |

### Client Connection with TLS

After deployment, connect using the CLI with TLS:

```bash
./bin/flymq-cli --tls --ca-cert deploy/docker/certs/ca.crt \
  -u admin -P 'YourPassword' \
  topics
```

### Advertise Address Configuration

FlyMQ automatically detects the correct IP address to advertise to other cluster nodes. This works in:

- **Docker containers** - Detects container IP via default route
- **Kubernetes** - Uses `POD_IP` environment variable
- **AWS EC2** - Uses instance metadata service
- **GCP** - Uses metadata service
- **Azure** - Uses IMDS

For manual override (e.g., behind NAT or when auto-detection fails):

```bash
# Set explicit advertise addresses before starting
export FLYMQ_ADVERTISE_ADDR=<external-ip>:9092
export FLYMQ_ADVERTISE_CLUSTER=<external-ip>:9093
./deploy/docker/scripts/setup-enterprise.sh server --admin-password MySecurePass123
```

| Variable | Description | Default |
|----------|-------------|---------|
| `FLYMQ_ADVERTISE_ADDR` | Address advertised to clients | auto-detected |
| `FLYMQ_ADVERTISE_CLUSTER` | Address advertised to cluster peers | auto-detected |

---

## Cloud Deployment

FlyMQ can be deployed on all major cloud platforms. See the platform-specific guides for detailed instructions.

### Platform Guides

| Platform | Guide | Key Services |
|----------|-------|--------------|
| **On-Premise** | [docs/deployment-onprem.md](docs/deployment-onprem.md) | Bare metal, Docker, VMs |
| **AWS** | [docs/deployment-aws.md](docs/deployment-aws.md) | EC2, ECS, EKS |
| **Azure** | [docs/deployment-azure.md](docs/deployment-azure.md) | VMs, ACI, AKS |
| **Google Cloud** | [docs/deployment-gcp.md](docs/deployment-gcp.md) | GCE, Cloud Run, GKE |

### Cloud Considerations

#### Network Configuration

All cloud deployments require:
- **Inbound rules** for ports 9092 (clients), 9093 (cluster), 9095 (health), 9096 (admin)
- **Internal communication** between cluster nodes on port 9093
- **Load balancer** for client connections (optional but recommended)

#### Storage

- Use **SSD/NVMe storage** for best performance
- Minimum **50GB** for production workloads
- Enable **persistent volumes** for data durability

#### Instance Sizing

| Workload | vCPUs | Memory | Storage |
|----------|-------|--------|---------|
| Development | 2 | 4 GB | 20 GB SSD |
| Production (small) | 4 | 8 GB | 100 GB SSD |
| Production (medium) | 8 | 16 GB | 500 GB SSD |
| Production (large) | 16 | 32 GB | 1 TB NVMe |

---

## Support

For issues or questions:
- GitHub Issues: https://github.com/firefly-oss/flymq/issues
- Documentation: https://github.com/firefly-oss/flymq/docs

---

**Copyright © 2026 Firefly Software Solutions Inc.**
