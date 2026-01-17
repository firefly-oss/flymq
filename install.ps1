#
# FlyMQ Installation Script for Windows
# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0
#
# Usage:
#   .\install.ps1                    Interactive installation
#   .\install.ps1 -Yes               Non-interactive with defaults
#   .\install.ps1 -Uninstall         Uninstall FlyMQ
#

param(
    [string]$Prefix = "",
    [switch]$Yes = $false,
    [string]$ConfigFile = "",
    [switch]$Uninstall = $false,
    [switch]$Help = $false
)

$ErrorActionPreference = "Stop"

# Script version
$SCRIPT_VERSION = "1.26.9"
$FLYMQ_VERSION = if ($env:FLYMQ_VERSION) { $env:FLYMQ_VERSION } else { "1.26.9" }

# Configuration variables
$script:CFG_DEPLOYMENT_MODE = "standalone"
$script:CFG_CLUSTER_ROLE = "bootstrap"
$script:CFG_CLUSTER_PEERS = ""
$script:CFG_CLUSTER_ADDR = ""
$script:CFG_NODE_ID = $env:COMPUTERNAME
$script:CFG_BIND_ADDR = ":9092"
$script:CFG_REPLICATION_FACTOR = 3
$script:CFG_MIN_ISR = 2
$script:CFG_DATA_DIR = ""
$script:CFG_TLS_ENABLED = $false
$script:CFG_ENCRYPTION_ENABLED = $false
$script:CFG_METRICS_ENABLED = $true
$script:CFG_HEALTH_ENABLED = $true
$script:CFG_ADMIN_ENABLED = $false

# Authentication
$script:CFG_AUTH_ENABLED = $false
$script:CFG_AUTH_ADMIN_USER = "admin"
$script:CFG_AUTH_ADMIN_PASS = ""
$script:CFG_AUTH_ALLOW_ANONYMOUS = $true

# Schema validation
$script:CFG_SCHEMA_ENABLED = $false
$script:CFG_SCHEMA_VALIDATION = "strict"
$script:CFG_SCHEMA_REGISTRY_DIR = ""

# Dead Letter Queue
$script:CFG_DLQ_ENABLED = $false
$script:CFG_DLQ_MAX_RETRIES = 3
$script:CFG_DLQ_RETRY_DELAY = 1000
$script:CFG_DLQ_TOPIC_SUFFIX = "-dlq"

# Message TTL
$script:CFG_TTL_DEFAULT = 0
$script:CFG_TTL_CLEANUP_INTERVAL = 60

# Delayed Message Delivery
$script:CFG_DELAYED_ENABLED = $false
$script:CFG_DELAYED_MAX_DELAY = 604800

# Transaction Support
$script:CFG_TXN_ENABLED = $false
$script:CFG_TXN_TIMEOUT = 60

# Observability - Tracing
$script:CFG_TRACING_ENABLED = $false
$script:CFG_TRACING_ENDPOINT = "localhost:4317"
$script:CFG_TRACING_SAMPLE_RATE = 0.1

# Helper Functions
function Write-Success { param([string]$msg) Write-Host "✓ $msg" -ForegroundColor Green }
function Write-ErrorMsg { param([string]$msg) Write-Host "✗ $msg" -ForegroundColor Red }
function Write-WarningMsg { param([string]$msg) Write-Host "⚠ $msg" -ForegroundColor Yellow }
function Write-Info { param([string]$msg) Write-Host "ℹ $msg" -ForegroundColor Cyan }
function Write-Step { param([string]$msg) Write-Host "`n==> $msg" -ForegroundColor Cyan -NoNewline:$false }

function Show-Banner {
    Write-Host ""
    Write-Host "  FlyMQ Windows Installer" -ForegroundColor Cyan
    Write-Host "  Version $FLYMQ_VERSION | Copyright (c) 2026 Firefly Software Solutions Inc." -ForegroundColor DarkGray
    Write-Host ""
}

function Test-GoInstalled {
    try {
        $goVersion = go version 2>$null
        if ($goVersion) {
            return $true
        }
    } catch {
        return $false
    }
    return $false
}

function Get-DefaultPrefix {
    return "$env:LOCALAPPDATA\FlyMQ"
}

function Get-DefaultDataDir {
    return "$env:LOCALAPPDATA\FlyMQ\data"
}

function Get-DefaultConfigDir {
    return "$env:LOCALAPPDATA\FlyMQ\config"
}

# Generate unique node ID for cluster
function Get-ClusterNodeId {
    $hostname = $env:COMPUTERNAME.ToLower() -replace '[^a-z0-9-]', ''
    $suffix = -join ((48..57) + (97..102) | Get-Random -Count 4 | ForEach-Object { [char]$_ })
    return "$hostname-$suffix"
}

# Get local IP address
function Get-LocalIP {
    try {
        $ip = (Get-NetIPAddress -AddressFamily IPv4 |
               Where-Object { $_.IPAddress -notlike "127.*" -and $_.PrefixOrigin -ne "WellKnown" } |
               Select-Object -First 1).IPAddress
        if ($ip) { return $ip }
    } catch {}
    return "localhost"
}

# Validate peer address format
function Test-PeerAddress {
    param([string]$peer)
    if ($peer -match '^.+:\d+$') {
        $parts = $peer -split ':'
        $port = [int]$parts[-1]
        if ($port -ge 1 -and $port -le 65535) {
            return $true
        }
    }
    return $false
}

# Format peers array for TOML
function Format-PeersArray {
    if ([string]::IsNullOrEmpty($script:CFG_CLUSTER_PEERS)) {
        return ""
    }
    $peers = $script:CFG_CLUSTER_PEERS -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ }
    $formatted = ($peers | ForEach-Object { "`"$_`"" }) -join ", "
    return $formatted
}

# Configure cluster mode
function Configure-ClusterMode {
    Write-Host ""
    Write-Host "  ━━━ Cluster Configuration ━━━" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  FlyMQ uses Raft consensus for leader election and data replication." -ForegroundColor DarkGray
    Write-Host "  A cluster requires at least 3 nodes for fault tolerance." -ForegroundColor DarkGray
    Write-Host ""

    Write-Host "  Cluster Role:" -ForegroundColor White
    Write-Host "    1) Bootstrap - First node in a new cluster" -ForegroundColor DarkGray
    Write-Host "    2) Join      - Join an existing cluster" -ForegroundColor DarkGray
    Write-Host ""

    $roleChoice = Read-Host "  Cluster role (1=bootstrap, 2=join) [1]"
    if ([string]::IsNullOrEmpty($roleChoice)) { $roleChoice = "1" }

    if ($roleChoice -eq "2" -or $roleChoice -eq "join") {
        $script:CFG_CLUSTER_ROLE = "join"
        Configure-ClusterJoin
    } else {
        $script:CFG_CLUSTER_ROLE = "bootstrap"
        Configure-ClusterBootstrap
    }
}

function Configure-ClusterBootstrap {
    Write-Host ""
    Write-Host "  Bootstrap Node Configuration" -ForegroundColor White
    Write-Host "  This node will be the initial leader of the cluster." -ForegroundColor DarkGray
    Write-Host ""

    # Node ID
    $defaultNodeId = Get-ClusterNodeId
    $nodeId = Read-Host "  Node ID (must be unique) [$defaultNodeId]"
    if ([string]::IsNullOrEmpty($nodeId)) { $nodeId = $defaultNodeId }
    $script:CFG_NODE_ID = $nodeId

    # Advertised address
    $defaultIP = Get-LocalIP
    Write-Host ""
    Write-Host "  Advertised Address" -ForegroundColor White
    Write-Host "  This is the address other nodes will use to connect." -ForegroundColor DarkGray
    $advHost = Read-Host "  Advertised hostname/IP [$defaultIP]"
    if ([string]::IsNullOrEmpty($advHost)) { $advHost = $defaultIP }
    $advPort = Read-Host "  Cluster port [9093]"
    if ([string]::IsNullOrEmpty($advPort)) { $advPort = "9093" }
    $script:CFG_CLUSTER_ADDR = "${advHost}:${advPort}"

    # Expected nodes
    Write-Host ""
    $expectedNodes = Read-Host "  Expected number of nodes in cluster [3]"
    if ([string]::IsNullOrEmpty($expectedNodes)) { $expectedNodes = "3" }
    $expectedNodes = [int]$expectedNodes

    # Replication settings
    Write-Host ""
    Write-Host "  Replication Settings" -ForegroundColor White
    $maxRep = $expectedNodes
    $repFactor = Read-Host "  Replication factor (max: $maxRep) [3]"
    if ([string]::IsNullOrEmpty($repFactor)) { $repFactor = "3" }
    $script:CFG_REPLICATION_FACTOR = [Math]::Min([int]$repFactor, $maxRep)

    $defaultIsr = [Math]::Max(1, [Math]::Floor(($script:CFG_REPLICATION_FACTOR + 1) / 2))
    $minIsr = Read-Host "  Minimum in-sync replicas [$defaultIsr]"
    if ([string]::IsNullOrEmpty($minIsr)) { $minIsr = $defaultIsr }
    $script:CFG_MIN_ISR = [Math]::Min([int]$minIsr, $script:CFG_REPLICATION_FACTOR)

    Write-Host ""
    Write-Success "Bootstrap node configured."
    Write-Host ""
    Write-Host "  ╔════════════════════════════════════════════════════════════╗" -ForegroundColor Green
    Write-Host "  ║              BOOTSTRAP NODE SETUP COMPLETE                 ║" -ForegroundColor Green
    Write-Host "  ╚════════════════════════════════════════════════════════════╝" -ForegroundColor Green
    Write-Host ""
    Write-Host "  This Node:" -ForegroundColor White
    Write-Host "    Node ID:         $($script:CFG_NODE_ID)" -ForegroundColor Cyan
    Write-Host "    Cluster Address: $($script:CFG_CLUSTER_ADDR)" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Next Steps After Installation:" -ForegroundColor White
    Write-Host "  1. Start this bootstrap node first" -ForegroundColor Yellow
    Write-Host "  2. On other nodes, run install.ps1 and choose 'Join' mode" -ForegroundColor DarkGray
    Write-Host "  3. Use this address to join: $($script:CFG_CLUSTER_ADDR)" -ForegroundColor Cyan
    Write-Host ""
}

function Configure-ClusterJoin {
    Write-Host ""
    Write-Host "  Join Existing Cluster" -ForegroundColor White
    Write-Host "  This node will join an existing FlyMQ cluster." -ForegroundColor DarkGray
    Write-Host ""

    # Node ID
    $defaultNodeId = Get-ClusterNodeId
    $nodeId = Read-Host "  Node ID (must be unique) [$defaultNodeId]"
    if ([string]::IsNullOrEmpty($nodeId)) { $nodeId = $defaultNodeId }
    $script:CFG_NODE_ID = $nodeId

    # Peer addresses
    Write-Host ""
    Write-Host "  Cluster Peers" -ForegroundColor White
    Write-Host "  Enter addresses of existing cluster nodes (comma-separated)." -ForegroundColor DarkGray
    Write-Host "  Example: 192.168.1.10:9093,192.168.1.11:9093" -ForegroundColor DarkGray
    Write-Host ""

    $peersValid = $false
    while (-not $peersValid) {
        $peers = Read-Host "  Peer addresses"
        if ([string]::IsNullOrEmpty($peers)) {
            Write-WarningMsg "At least one peer address is required."
            $continue = Read-Host "  Continue without peers? (y/N)"
            if ($continue -eq "y" -or $continue -eq "Y") {
                $peersValid = $true
            }
        } else {
            $allValid = $true
            $peerList = $peers -split ',' | ForEach-Object { $_.Trim() }
            foreach ($p in $peerList) {
                if (-not (Test-PeerAddress $p)) {
                    Write-WarningMsg "Invalid peer format: $p"
                    $allValid = $false
                }
            }
            if ($allValid) {
                $script:CFG_CLUSTER_PEERS = $peers
                $peersValid = $true
                Write-Success "Peer addresses validated: $($peerList.Count) peer(s)"
            } else {
                $continue = Read-Host "  Continue anyway? (y/N)"
                if ($continue -eq "y" -or $continue -eq "Y") {
                    $script:CFG_CLUSTER_PEERS = $peers
                    $peersValid = $true
                }
            }
        }
    }

    # Advertised address
    $defaultIP = Get-LocalIP
    Write-Host ""
    Write-Host "  Advertised Address" -ForegroundColor White
    $advHost = Read-Host "  Advertised hostname/IP [$defaultIP]"
    if ([string]::IsNullOrEmpty($advHost)) { $advHost = $defaultIP }
    $advPort = Read-Host "  Cluster port [9093]"
    if ([string]::IsNullOrEmpty($advPort)) { $advPort = "9093" }
    $script:CFG_CLUSTER_ADDR = "${advHost}:${advPort}"

    # Replication settings
    Write-Host ""
    Write-Host "  Replication Settings (must match cluster)" -ForegroundColor White
    $repFactor = Read-Host "  Replication factor [3]"
    if ([string]::IsNullOrEmpty($repFactor)) { $repFactor = "3" }
    $script:CFG_REPLICATION_FACTOR = [int]$repFactor

    $defaultIsr = [Math]::Max(1, [Math]::Floor(($script:CFG_REPLICATION_FACTOR + 1) / 2))
    $minIsr = Read-Host "  Minimum in-sync replicas [$defaultIsr]"
    if ([string]::IsNullOrEmpty($minIsr)) { $minIsr = $defaultIsr }
    $script:CFG_MIN_ISR = [Math]::Min([int]$minIsr, $script:CFG_REPLICATION_FACTOR)

    Write-Host ""
    Write-Success "Join configuration complete."
    Write-Host ""
    Write-Host "  ╔════════════════════════════════════════════════════════════╗" -ForegroundColor Green
    Write-Host "  ║               JOINING NODE SETUP COMPLETE                  ║" -ForegroundColor Green
    Write-Host "  ╚════════════════════════════════════════════════════════════╝" -ForegroundColor Green
    Write-Host ""
    Write-Host "  This Node:" -ForegroundColor White
    Write-Host "    Node ID:         $($script:CFG_NODE_ID)" -ForegroundColor Cyan
    Write-Host "    Cluster Address: $($script:CFG_CLUSTER_ADDR)" -ForegroundColor Cyan
    Write-Host "    Joining Peers:   $($script:CFG_CLUSTER_PEERS)" -ForegroundColor Cyan
    Write-Host ""
}

# Interactive configuration
function Configure-Interactive {
    Write-Host ""
    Write-Step "Configuration"
    Write-Host ""
    Write-Host "  Press Enter to accept defaults shown in brackets." -ForegroundColor DarkGray
    Write-Host ""

    # Deployment mode
    Write-Host "  ━━━ Deployment Mode ━━━" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Choose how you want to deploy FlyMQ:" -ForegroundColor DarkGray
    Write-Host "    1) Standalone - Single node (development/testing)" -ForegroundColor DarkGray
    Write-Host "    2) Cluster    - Multi-node (production)" -ForegroundColor DarkGray
    Write-Host ""

    $modeChoice = Read-Host "  Deployment mode (1=standalone, 2=cluster) [1]"
    if ([string]::IsNullOrEmpty($modeChoice)) { $modeChoice = "1" }

    if ($modeChoice -eq "2" -or $modeChoice -eq "cluster") {
        $script:CFG_DEPLOYMENT_MODE = "cluster"
        Configure-ClusterMode
    } else {
        $script:CFG_DEPLOYMENT_MODE = "standalone"
        Write-Info "Configuring standalone mode..."

        # Basic network settings for standalone
        Write-Host ""
        $bindAddr = Read-Host "  Client bind address [:9092]"
        if ([string]::IsNullOrEmpty($bindAddr)) { $bindAddr = ":9092" }
        $script:CFG_BIND_ADDR = $bindAddr
    }

    # Storage settings
    Write-Host ""
    Write-Host "  ━━━ Storage Settings ━━━" -ForegroundColor Cyan
    Write-Host ""
    $defaultDataDir = Get-DefaultDataDir
    $dataDir = Read-Host "  Data directory [$defaultDataDir]"
    if ([string]::IsNullOrEmpty($dataDir)) { $dataDir = $defaultDataDir }
    $script:CFG_DATA_DIR = $dataDir

    # Observability
    Write-Host ""
    Write-Host "  ━━━ Observability ━━━" -ForegroundColor Cyan
    Write-Host ""
    $enableMetrics = Read-Host "  Enable Prometheus metrics? (Y/n)"
    $script:CFG_METRICS_ENABLED = -not ($enableMetrics -eq "n" -or $enableMetrics -eq "N")

    $enableAdmin = Read-Host "  Enable Admin REST API? (y/N)"
    $script:CFG_ADMIN_ENABLED = ($enableAdmin -eq "y" -or $enableAdmin -eq "Y")

    # Authentication
    Write-Host ""
    Write-Host "  ━━━ Authentication & Authorization ━━━" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Enable username/password authentication with role-based access control" -ForegroundColor DarkGray
    Write-Host ""
    $enableAuth = Read-Host "  Enable authentication? (y/N)"
    if ($enableAuth -eq "y" -or $enableAuth -eq "Y") {
        $script:CFG_AUTH_ENABLED = $true

        $adminUser = Read-Host "  Admin username [admin]"
        if ([string]::IsNullOrEmpty($adminUser)) { $adminUser = "admin" }
        $script:CFG_AUTH_ADMIN_USER = $adminUser

        Write-Host "  Enter a password for the admin user (leave empty to generate one)" -ForegroundColor DarkGray
        $adminPass = Read-Host "  Admin password" -AsSecureString
        $adminPassPlain = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($adminPass))

        if ([string]::IsNullOrEmpty($adminPassPlain)) {
            # Generate a random password
            $chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%"
            $adminPassPlain = -join ((1..16) | ForEach-Object { $chars[(Get-Random -Maximum $chars.Length)] })
            Write-WarningMsg "Generated admin password: $adminPassPlain"
            Write-WarningMsg "Please save this password securely!"
        }
        $script:CFG_AUTH_ADMIN_PASS = $adminPassPlain

        $allowAnon = Read-Host "  Allow anonymous connections (read-only)? (y/N)"
        $script:CFG_AUTH_ALLOW_ANONYMOUS = ($allowAnon -eq "y" -or $allowAnon -eq "Y")
    }
}

function Build-Binaries {
    Write-Step "Building FlyMQ"
    Write-Host ""
    
    if (-not (Test-Path "bin")) {
        New-Item -ItemType Directory -Path "bin" | Out-Null
    }
    
    Write-Host "  Building flymq.exe..."
    go build -o bin\flymq.exe cmd\flymq\main.go
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to build flymq.exe"
        exit 1
    }
    Write-Success "Built flymq.exe"
    
    Write-Host "  Building flymq-cli.exe..."
    go build -o bin\flymq-cli.exe cmd\flymq-cli\main.go
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to build flymq-cli.exe"
        exit 1
    }
    Write-Success "Built flymq-cli.exe"
}

function Install-Binaries {
    param([string]$prefix)
    
    Write-Step "Installing binaries"
    Write-Host ""
    
    $binDir = Join-Path $prefix "bin"
    Write-Info "Install location: $binDir"
    Write-Host ""
    
    if (-not (Test-Path $binDir)) {
        New-Item -ItemType Directory -Path $binDir -Force | Out-Null
    }
    
    Copy-Item "bin\flymq.exe" "$binDir\flymq.exe" -Force
    Write-Success "Installed flymq.exe"
    
    Copy-Item "bin\flymq-cli.exe" "$binDir\flymq-cli.exe" -Force
    Write-Success "Installed flymq-cli.exe"
    
    # Add to PATH if not already there
    $currentPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if ($currentPath -notlike "*$binDir*") {
        Write-Host ""
        Write-Warning "The bin directory is not in your PATH"
        Write-Info "Add it with: `$env:PATH += ';$binDir'"
        Write-Info "Or permanently: [Environment]::SetEnvironmentVariable('Path', `$env:PATH + ';$binDir', 'User')"
    }
}

function New-DataDirectory {
    param([string]$dataDir)
    
    Write-Step "Creating data directory"
    Write-Host ""
    
    if (-not (Test-Path $dataDir)) {
        New-Item -ItemType Directory -Path $dataDir -Force | Out-Null
    }
    Write-Success "Created: $dataDir"
}

function New-Config {
    param([string]$configDir, [string]$dataDir)

    Write-Step "Generating configuration"
    Write-Host ""

    if (-not (Test-Path $configDir)) {
        New-Item -ItemType Directory -Path $configDir -Force | Out-Null
    }

    $configFile = Join-Path $configDir "flymq.conf"

    # Use configured values or defaults
    $bindAddr = if ($script:CFG_BIND_ADDR) { $script:CFG_BIND_ADDR } else { ":9092" }
    $clusterAddr = if ($script:CFG_CLUSTER_ADDR) { $script:CFG_CLUSTER_ADDR } else { ":9093" }
    $nodeId = if ($script:CFG_NODE_ID) { $script:CFG_NODE_ID } else { $env:COMPUTERNAME }
    $deploymentMode = if ($script:CFG_DEPLOYMENT_MODE) { $script:CFG_DEPLOYMENT_MODE } else { "standalone" }
    $clusterRole = if ($script:CFG_CLUSTER_ROLE) { $script:CFG_CLUSTER_ROLE } else { "bootstrap" }
    $peersArray = Format-PeersArray
    $repFactor = if ($script:CFG_REPLICATION_FACTOR) { $script:CFG_REPLICATION_FACTOR } else { 3 }
    $minIsr = if ($script:CFG_MIN_ISR) { $script:CFG_MIN_ISR } else { 2 }
    $tlsEnabled = if ($script:CFG_TLS_ENABLED) { "true" } else { "false" }
    $encEnabled = if ($script:CFG_ENCRYPTION_ENABLED) { "true" } else { "false" }
    $metricsEnabled = if ($script:CFG_METRICS_ENABLED) { "true" } else { "false" }
    $healthEnabled = if ($script:CFG_HEALTH_ENABLED) { "true" } else { "false" }
    $adminEnabled = if ($script:CFG_ADMIN_ENABLED) { "true" } else { "false" }

    # Authentication
    $authEnabled = if ($script:CFG_AUTH_ENABLED) { "true" } else { "false" }
    $authAllowAnon = if ($script:CFG_AUTH_ALLOW_ANONYMOUS) { "true" } else { "false" }
    $authAdminUser = if ($script:CFG_AUTH_ADMIN_USER) { $script:CFG_AUTH_ADMIN_USER } else { "admin" }
    $authAdminPass = if ($script:CFG_AUTH_ADMIN_PASS) { $script:CFG_AUTH_ADMIN_PASS } else { "" }

    # Schema validation
    $schemaEnabled = if ($script:CFG_SCHEMA_ENABLED) { "true" } else { "false" }
    $schemaValidation = if ($script:CFG_SCHEMA_VALIDATION) { $script:CFG_SCHEMA_VALIDATION } else { "strict" }
    $schemaDir = if ($script:CFG_SCHEMA_REGISTRY_DIR) { $script:CFG_SCHEMA_REGISTRY_DIR } else { "$dataDir\schemas" }

    # DLQ
    $dlqEnabled = if ($script:CFG_DLQ_ENABLED) { "true" } else { "false" }
    $dlqMaxRetries = if ($script:CFG_DLQ_MAX_RETRIES) { $script:CFG_DLQ_MAX_RETRIES } else { 3 }
    $dlqRetryDelay = if ($script:CFG_DLQ_RETRY_DELAY) { $script:CFG_DLQ_RETRY_DELAY } else { 1000 }
    $dlqTopicSuffix = if ($script:CFG_DLQ_TOPIC_SUFFIX) { $script:CFG_DLQ_TOPIC_SUFFIX } else { "-dlq" }

    # TTL
    $ttlDefault = if ($script:CFG_TTL_DEFAULT) { $script:CFG_TTL_DEFAULT } else { 0 }
    $ttlCleanupInterval = if ($script:CFG_TTL_CLEANUP_INTERVAL) { $script:CFG_TTL_CLEANUP_INTERVAL } else { 60 }

    # Delayed
    $delayedEnabled = if ($script:CFG_DELAYED_ENABLED) { "true" } else { "false" }
    $delayedMaxDelay = if ($script:CFG_DELAYED_MAX_DELAY) { $script:CFG_DELAYED_MAX_DELAY } else { 604800 }

    # Transaction
    $txnEnabled = if ($script:CFG_TXN_ENABLED) { "true" } else { "false" }
    $txnTimeout = if ($script:CFG_TXN_TIMEOUT) { $script:CFG_TXN_TIMEOUT } else { 60 }

    # Tracing
    $tracingEnabled = if ($script:CFG_TRACING_ENABLED) { "true" } else { "false" }
    $tracingEndpoint = if ($script:CFG_TRACING_ENDPOINT) { $script:CFG_TRACING_ENDPOINT } else { "localhost:4317" }
    $tracingSampleRate = if ($script:CFG_TRACING_SAMPLE_RATE) { $script:CFG_TRACING_SAMPLE_RATE } else { 0.1 }

    $configContent = @"
# FlyMQ Configuration File
# Copyright (c) 2026 Firefly Software Solutions Inc.
# Generated on $(Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ")

# =============================================================================
# Network Configuration
# =============================================================================

# Address to listen for client connections
bind_addr = "$bindAddr"

# Address for cluster communication
cluster_addr = "$clusterAddr"

# Unique identifier for this node
node_id = "$nodeId"

# =============================================================================
# Cluster Configuration
# =============================================================================

[cluster]
# Deployment mode: standalone or cluster
mode = "$deploymentMode"

# Cluster role: bootstrap (first node) or join
role = "$clusterRole"

# Advertised address for this node (how other nodes reach this node)
advertised_addr = "$clusterAddr"

# List of peer nodes for clustering
peers = [$peersArray]

# Replication factor for partitions (cluster mode only)
replication_factor = $repFactor

# Minimum in-sync replicas required for writes
min_isr = $minIsr

# Election timeout in milliseconds (Raft)
election_timeout_ms = 1000

# Heartbeat interval in milliseconds (Raft)
heartbeat_interval_ms = 100

# =============================================================================
# Storage Configuration
# =============================================================================

# Directory for data storage
data_dir = "$($dataDir -replace '\\', '\\\\')"

# Maximum size of each log segment in bytes (default: 64MB)
segment_bytes = 67108864

# Maximum bytes to retain per topic/partition (0 = unlimited)
retention_bytes = 0

# =============================================================================
# Logging Configuration
# =============================================================================

# Log level: debug, info, warn, error
log_level = "info"

# Output logs in JSON format
log_json = false

# =============================================================================
# Security Configuration
# =============================================================================

[security]
# Enable TLS for client connections
tls_enabled = $tlsEnabled

# Path to TLS certificate file
tls_cert_file = ""

# Path to TLS private key file
tls_key_file = ""

# Enable data-at-rest encryption
encryption_enabled = $encEnabled

# AES-256 encryption key (hex-encoded, 64 characters)
encryption_key = ""

# =============================================================================
# Advanced Configuration
# =============================================================================

[advanced]
# Maximum message size in bytes (default: 32MB)
max_message_size = 33554432

# Connection idle timeout in seconds
idle_timeout = 300

# Enable fsync after each write (slower but more durable)
sync_writes = true

# =============================================================================
# Schema Validation
# =============================================================================

[schema]
# Enable schema validation for messages
enabled = $schemaEnabled

# Validation mode: strict (reject invalid), lenient (warn only), none
validation = "$schemaValidation"

# Directory for schema registry storage
registry_dir = "$($schemaDir -replace '\\', '\\\\')"

# =============================================================================
# Dead Letter Queue
# =============================================================================

[dlq]
# Enable dead letter queues for failed messages
enabled = $dlqEnabled

# Maximum retry attempts before routing to DLQ
max_retries = $dlqMaxRetries

# Delay between retry attempts (milliseconds)
retry_delay = $dlqRetryDelay

# Suffix appended to topic names for DLQ topics
topic_suffix = "$dlqTopicSuffix"

# =============================================================================
# Message TTL
# =============================================================================

[ttl]
# Default message TTL in seconds (0 = no expiry)
default_ttl = $ttlDefault

# Interval for expired message cleanup (seconds)
cleanup_interval = $ttlCleanupInterval

# =============================================================================
# Delayed Message Delivery
# =============================================================================

[delayed]
# Enable delayed/scheduled message delivery
enabled = $delayedEnabled

# Maximum allowed delay in seconds (default: 7 days)
max_delay = $delayedMaxDelay

# =============================================================================
# Transaction Support
# =============================================================================

[transaction]
# Enable transaction support for exactly-once semantics
enabled = $txnEnabled

# Transaction timeout in seconds
timeout = $txnTimeout

# =============================================================================
# Observability - Prometheus Metrics
# =============================================================================

[observability.metrics]
# Enable Prometheus metrics endpoint
enabled = $metricsEnabled

# HTTP address for metrics endpoint
addr = ":9094"

# =============================================================================
# Observability - OpenTelemetry Tracing
# =============================================================================

[observability.tracing]
# Enable OpenTelemetry distributed tracing
enabled = $tracingEnabled

# OTLP exporter endpoint
endpoint = "$tracingEndpoint"

# Sampling rate (0.0 to 1.0)
sample_rate = $tracingSampleRate

# =============================================================================
# Observability - Health Checks
# =============================================================================

[observability.health]
# Enable health check endpoints
enabled = $healthEnabled

# HTTP address for health endpoints
addr = ":9095"

# =============================================================================
# Observability - Admin API
# =============================================================================

[observability.admin]
# Enable Admin REST API
enabled = $adminEnabled

# HTTP address for Admin API
addr = ":9096"

# =============================================================================
# Authentication
# =============================================================================

[auth]
# Enable authentication
enabled = $authEnabled

# Allow anonymous connections (read-only access)
allow_anonymous = $authAllowAnon

# Admin user credentials (created on first startup)
admin_username = "$authAdminUser"
admin_password = "$authAdminPass"
"@

    Set-Content -Path $configFile -Value $configContent
    Write-Success "Generated: $configFile"

    # Generate environment file for cluster deployments
    if ($deploymentMode -eq "cluster") {
        $envFile = Join-Path $configDir "flymq.env"
        $envContent = @"
# FlyMQ Environment Variables
# Generated on $(Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ")

FLYMQ_BIND_ADDR=$bindAddr
FLYMQ_CLUSTER_ADDR=$clusterAddr
FLYMQ_NODE_ID=$nodeId
FLYMQ_CLUSTER_MODE=$deploymentMode
FLYMQ_CLUSTER_ROLE=$clusterRole
FLYMQ_CLUSTER_PEERS=$($script:CFG_CLUSTER_PEERS)
FLYMQ_REPLICATION_FACTOR=$repFactor
FLYMQ_MIN_ISR=$minIsr
FLYMQ_DATA_DIR=$dataDir
FLYMQ_AUTH_ENABLED=$authEnabled
FLYMQ_AUTH_ADMIN_USERNAME=$authAdminUser
FLYMQ_AUTH_ADMIN_PASSWORD=$authAdminPass
"@
        Set-Content -Path $envFile -Value $envContent
        Write-Success "Generated: $envFile"
    }

    # Create schema registry directory if schema validation is enabled
    if ($script:CFG_SCHEMA_ENABLED) {
        $schemaPath = if ($script:CFG_SCHEMA_REGISTRY_DIR) { $script:CFG_SCHEMA_REGISTRY_DIR } else { "$dataDir\schemas" }
        if (-not (Test-Path $schemaPath)) {
            New-Item -ItemType Directory -Path $schemaPath -Force | Out-Null
        }
        Write-Success "Created schema registry: $schemaPath"
    }
}

function Show-PostInstall {
    param([string]$prefix, [string]$configDir)
    
    Write-Host ""
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Green
    Write-Success "FlyMQ v$FLYMQ_VERSION installed successfully!"
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Green
    Write-Host ""
    Write-Host "Installation Summary:"
    Write-Host "  → Binaries:      $(Join-Path $prefix 'bin')" -ForegroundColor Cyan
    Write-Host "  → Configuration: $(Join-Path $configDir 'flymq.conf')" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Quick Start:"
    Write-Host ""
    Write-Host "  1. Start the server:"
    Write-Host "     flymq --config $(Join-Path $configDir 'flymq.conf')" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  2. Produce a message:"
    Write-Host "     flymq-cli produce my-topic `"Hello World`"" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  3. Subscribe to messages:"
    Write-Host "     flymq-cli subscribe my-topic --from earliest" -ForegroundColor Cyan
    Write-Host ""
}

# Main Installation
Show-Banner

if ($Help) {
    Write-Host "Usage: .\install.ps1 [OPTIONS]"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -Prefix PATH        Installation prefix (default: $env:LOCALAPPDATA\FlyMQ)"
    Write-Host "  -Yes                Non-interactive mode with default configuration"
    Write-Host "  -ConfigFile FILE    Use existing configuration file"
    Write-Host "  -Uninstall          Uninstall FlyMQ"
    Write-Host "  -Help               Show this help"
    Write-Host ""
    exit 0
}

if ($Uninstall) {
    Write-Step "Uninstalling FlyMQ"
    Write-Host ""
    
    $defaultPrefix = Get-DefaultPrefix
    $prefix = if ($Prefix) { $Prefix } else { $defaultPrefix }
    
    $binDir = Join-Path $prefix "bin"
    if (Test-Path $binDir) {
        Remove-Item -Path $binDir -Recurse -Force
        Write-Success "Removed binaries"
    }
    
    Write-Info "Configuration and data directories were not removed."
    Write-Info "Remove manually if needed: $prefix"
    exit 0
}

Write-Info "Detected: Windows/$env:PROCESSOR_ARCHITECTURE"
Write-Host ""

# Check Go
Write-Step "Checking dependencies"
Write-Host ""
if (-not (Test-GoInstalled)) {
    Write-Error "Go is not installed. Please install Go 1.21+ first."
    Write-Info "Visit: https://go.dev/dl/"
    exit 1
}
$goVersion = (go version) -replace 'go version go', '' -replace ' .*', ''
Write-Success "Go $goVersion found"

# Set installation paths
$prefix = if ($Prefix) { $Prefix } else { Get-DefaultPrefix }
$dataDir = Get-DefaultDataDir
$configDir = Get-DefaultConfigDir

if (-not $Yes) {
    # Run interactive configuration
    Configure-Interactive

    # Update data dir from configuration
    if ($script:CFG_DATA_DIR) {
        $dataDir = $script:CFG_DATA_DIR
    }

    Write-Host ""
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  Installation Summary" -ForegroundColor White
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Install prefix:    $prefix" -ForegroundColor Cyan
    Write-Host "  Config directory:  $configDir" -ForegroundColor Cyan
    Write-Host "  Data directory:    $dataDir" -ForegroundColor Cyan
    Write-Host "  Deployment mode:   $($script:CFG_DEPLOYMENT_MODE)" -ForegroundColor Cyan
    if ($script:CFG_DEPLOYMENT_MODE -eq "cluster") {
        Write-Host "  Cluster role:      $($script:CFG_CLUSTER_ROLE)" -ForegroundColor Cyan
        Write-Host "  Node ID:           $($script:CFG_NODE_ID)" -ForegroundColor Cyan
        Write-Host "  Cluster address:   $($script:CFG_CLUSTER_ADDR)" -ForegroundColor Cyan
        if ($script:CFG_CLUSTER_PEERS) {
            Write-Host "  Cluster peers:     $($script:CFG_CLUSTER_PEERS)" -ForegroundColor Cyan
        }
    }
    Write-Host ""
    Write-Host "  Observability:" -ForegroundColor White
    Write-Host "  → Prometheus Metrics: $(if ($script:CFG_METRICS_ENABLED) { 'enabled' } else { 'disabled' })" -ForegroundColor Cyan
    Write-Host "  → Health Checks:      enabled" -ForegroundColor Cyan
    Write-Host "  → Admin API:          $(if ($script:CFG_ADMIN_ENABLED) { 'enabled' } else { 'disabled' })" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Security:" -ForegroundColor White
    Write-Host "  → Authentication:     $(if ($script:CFG_AUTH_ENABLED) { 'enabled' } else { 'disabled' })" -ForegroundColor Cyan
    if ($script:CFG_AUTH_ENABLED) {
        Write-Host "  → Admin User:         $($script:CFG_AUTH_ADMIN_USER)" -ForegroundColor Cyan
        Write-Host "  → Allow Anonymous:    $(if ($script:CFG_AUTH_ALLOW_ANONYMOUS) { 'yes' } else { 'no' })" -ForegroundColor Cyan
    }
    Write-Host ""
    Write-Host "  Observability:" -ForegroundColor White
    Write-Host "  → Prometheus Metrics: $(if ($script:CFG_METRICS_ENABLED) { 'enabled' } else { 'disabled' })" -ForegroundColor Cyan
    Write-Host "  → Health Checks:      $(if ($script:CFG_HEALTH_ENABLED) { 'enabled' } else { 'disabled' })" -ForegroundColor Cyan
    Write-Host "  → Admin API:          $(if ($script:CFG_ADMIN_ENABLED) { 'enabled' } else { 'disabled' })" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Security:" -ForegroundColor White
    Write-Host "  → Authentication:     $(if ($script:CFG_AUTH_ENABLED) { 'enabled' } else { 'disabled' })" -ForegroundColor Cyan
    if ($script:CFG_AUTH_ENABLED) {
        Write-Host "  → Admin User:         $($script:CFG_AUTH_ADMIN_USER)" -ForegroundColor Cyan
        Write-Host "  → Allow Anonymous:    $(if ($script:CFG_AUTH_ALLOW_ANONYMOUS) { 'yes' } else { 'no' })" -ForegroundColor Cyan
    }
    Write-Host ""
    $confirm = Read-Host "Proceed with installation? (Y/n)"
    if ($confirm -and $confirm -ne "Y" -and $confirm -ne "y") {
        Write-Info "Installation cancelled"
        exit 0
    }
}

Build-Binaries
Install-Binaries -prefix $prefix
New-DataDirectory -dataDir $dataDir
New-Config -configDir $configDir -dataDir $dataDir
Show-PostInstall -prefix $prefix -configDir $configDir
