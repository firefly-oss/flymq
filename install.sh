#!/bin/bash
#
# FlyMQ Installation Script
# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0
#
# Usage:
#   ./install.sh                    Interactive installation
#   ./install.sh --yes              Non-interactive with defaults
#   ./install.sh --config-file FILE Use existing config file
#   ./install.sh --uninstall        Uninstall FlyMQ
#

set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================

readonly SCRIPT_VERSION="1.26.9"
readonly FLYMQ_VERSION="${FLYMQ_VERSION:-1.26.9}"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"

# Installation options
PREFIX=""
AUTO_CONFIRM=false
UNINSTALL=false
CONFIG_FILE=""

# =============================================================================
# Smart Defaults - Production-Ready Out of the Box
# =============================================================================
# These defaults provide a secure, feature-rich standalone deployment.
# Users can customize by section if needed.

# Configuration values (will be set during interactive setup)
CFG_DATA_DIR=""
CFG_BIND_ADDR=":9092"
CFG_CLUSTER_ADDR=":9093"
CFG_NODE_ID=""
CFG_LOG_LEVEL="info"
CFG_SEGMENT_BYTES="67108864"
CFG_RETENTION_BYTES="0"

# TLS/SSL - Disabled by default (use reverse proxy for TLS in production)
CFG_TLS_ENABLED="false"
CFG_TLS_CERT_FILE=""
CFG_TLS_KEY_FILE=""

# Data-at-Rest Encryption - ENABLED by default (auto-generated key)
CFG_ENCRYPTION_ENABLED="true"
CFG_ENCRYPTION_KEY=""

# Cluster Configuration - Standalone by default
CFG_DEPLOYMENT_MODE="standalone"  # standalone or cluster
CFG_CLUSTER_PEERS=""              # comma-separated list of peer addresses
CFG_ADVERTISE_CLUSTER=""          # advertised cluster address
CFG_REPLICATION_FACTOR="1"        # number of replicas for data
CFG_CLUSTER_ENABLED="false"       # whether clustering is enabled

# Performance Configuration
CFG_ACKS="leader"                 # Durability mode: all, leader, none

# Schema validation - ENABLED by default
CFG_SCHEMA_ENABLED="true"
CFG_SCHEMA_VALIDATION="strict"
CFG_SCHEMA_REGISTRY_DIR=""

# Dead Letter Queue - ENABLED by default
CFG_DLQ_ENABLED="true"
CFG_DLQ_MAX_RETRIES="3"
CFG_DLQ_RETRY_DELAY="1000"
CFG_DLQ_TOPIC_SUFFIX="-dlq"

# Message TTL - ENABLED by default (7 days)
CFG_TTL_DEFAULT="604800"
CFG_TTL_CLEANUP_INTERVAL="60"

# Delayed Message Delivery - ENABLED by default
CFG_DELAYED_ENABLED="true"
CFG_DELAYED_MAX_DELAY="604800"

# Transaction Support - ENABLED by default
CFG_TXN_ENABLED="true"
CFG_TXN_TIMEOUT="60"

# Observability - Metrics - ENABLED by default
CFG_METRICS_ENABLED="true"
CFG_METRICS_ADDR=":9094"

# Observability - Tracing - ENABLED by default
CFG_TRACING_ENABLED="true"
CFG_TRACING_ENDPOINT="localhost:4317"
CFG_TRACING_SAMPLE_RATE="0.1"

# Observability - Health Checks - ENABLED by default
CFG_HEALTH_ENABLED="true"
CFG_HEALTH_ADDR=":9095"
CFG_HEALTH_TLS_ENABLED="false"
CFG_HEALTH_TLS_CERT_FILE=""
CFG_HEALTH_TLS_KEY_FILE=""
CFG_HEALTH_TLS_AUTO_GENERATE="false"
CFG_HEALTH_TLS_USE_ADMIN_CERT="false"

# Observability - Admin API - ENABLED by default
CFG_ADMIN_ENABLED="true"
CFG_ADMIN_ADDR=":9096"
CFG_ADMIN_TLS_ENABLED="false"
CFG_ADMIN_TLS_CERT_FILE=""
CFG_ADMIN_TLS_KEY_FILE=""
CFG_ADMIN_TLS_AUTO_GENERATE="false"

# Authentication - ENABLED by default (auto-generated credentials)
CFG_AUTH_ENABLED="true"
CFG_AUTH_ADMIN_USER="admin"
CFG_AUTH_ADMIN_PASS=""
CFG_AUTH_ALLOW_ANONYMOUS="false"

# Partition Management (Horizontal Scaling) - Sensible defaults
CFG_PARTITION_DISTRIBUTION_STRATEGY="round-robin"
CFG_PARTITION_DEFAULT_REPLICATION_FACTOR="1"
CFG_PARTITION_DEFAULT_PARTITIONS="1"
CFG_PARTITION_AUTO_REBALANCE_ENABLED="false"
CFG_PARTITION_AUTO_REBALANCE_INTERVAL="300"
CFG_PARTITION_REBALANCE_THRESHOLD="0.2"

# Service Discovery - Enable for cluster auto-discovery
CFG_DISCOVERY_ENABLED="false"
CFG_DISCOVERY_CLUSTER_ID=""

# Audit Trail - ENABLED by default for security compliance
CFG_AUDIT_ENABLED="true"
CFG_AUDIT_LOG_DIR=""  # Default: data_dir/audit
CFG_AUDIT_MAX_FILE_SIZE="104857600"  # 100MB
CFG_AUDIT_RETENTION_DAYS="90"

# System service installation
INSTALL_SYSTEMD="false"
INSTALL_LAUNCHD="false"

# Detected system info
OS=""
ARCH=""

# =============================================================================
# Colors and Formatting
# =============================================================================

if [[ -t 1 ]] && [[ -z "${NO_COLOR:-}" ]]; then
    readonly COLOR_ENABLED=true
else
    readonly COLOR_ENABLED=false
fi

if [[ "$COLOR_ENABLED" == true ]]; then
    readonly RESET='\033[0m'
    readonly BOLD='\033[1m'
    readonly DIM='\033[2m'
    readonly RED='\033[31m'
    readonly GREEN='\033[32m'
    readonly YELLOW='\033[33m'
    readonly CYAN='\033[36m'
    readonly WHITE='\033[37m'
else
    readonly RESET='' BOLD='' DIM='' RED='' GREEN='' YELLOW='' CYAN='' WHITE=''
fi

readonly ICON_SUCCESS="✓"
readonly ICON_ERROR="✗"
readonly ICON_WARNING="⚠"
readonly ICON_INFO="ℹ"
readonly ICON_ARROW="→"

# =============================================================================
# Output Functions
# =============================================================================

print_success() { echo -e "${GREEN}${ICON_SUCCESS}${RESET} $1"; }
print_error() { echo -e "${RED}${ICON_ERROR}${RESET} ${RED}$1${RESET}" >&2; }
print_warning() { echo -e "${YELLOW}${ICON_WARNING}${RESET} $1"; }
print_info() { echo -e "${CYAN}${ICON_INFO}${RESET} $1"; }
print_step() { echo -e "\n${CYAN}${BOLD}==>${RESET} ${BOLD}$1${RESET}"; }
print_substep() { echo -e "  ${CYAN}${ICON_ARROW}${RESET} $1"; }
print_section() { echo -e "\n  ${BOLD}$1${RESET}\n"; }

# Spinner for long-running tasks
show_spinner() {
    local pid=$1
    local message="$2"
    local spinner_chars="⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    local i=0
    
    echo -n "  "
    while kill -0 "$pid" 2>/dev/null; do
        printf "\r  ${CYAN}${spinner_chars:$i:1}${RESET} ${message}..."
        i=$(( (i + 1) % ${#spinner_chars} ))
        sleep 0.1
    done
    printf "\r  ${GREEN}${ICON_SUCCESS}${RESET} ${message}\n"
}

# =============================================================================
# Banner - Premium Welcome Experience
# =============================================================================

print_banner() {
    local banner_file="${SCRIPT_DIR}/internal/banner/banner.txt"

    # Clear screen for a clean start (optional, only in interactive mode)
    if [[ -t 1 ]] && [[ "$AUTO_CONFIRM" != true ]]; then
        clear 2>/dev/null || true
    fi

    echo ""
    echo -e "${CYAN}${BOLD}"
    if [[ -f "$banner_file" ]]; then
        while IFS= read -r line; do
            echo "  $line"
        done < "$banner_file"
    else
        echo "  F L Y M Q"
    fi
    echo -e "${RESET}"
    echo ""
    echo -e "  ${GREEN}${BOLD}FlyMQ Installer${RESET} ${DIM}v${FLYMQ_VERSION}${RESET}"
    echo -e "  ${DIM}High-Performance Message Queue for Modern Applications${RESET}"
    echo ""
    echo -e "  ${CYAN}✓${RESET} Sub-millisecond latency     ${CYAN}✓${RESET} Zero external dependencies"
    echo -e "  ${CYAN}✓${RESET} Single binary deployment    ${CYAN}✓${RESET} Built-in encryption & auth"
    echo -e "  ${CYAN}✓${RESET} Consumer groups & offsets   ${CYAN}✓${RESET} Production-ready defaults"
    echo ""
}

# =============================================================================
# System Detection
# =============================================================================

detect_system() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)
    CFG_NODE_ID=$(hostname)

    # Detect Windows environments
    case "$OS" in
        mingw*|msys*|cygwin*)
            OS="windows"
            # Check if running in WSL
            if grep -qEi "(Microsoft|WSL)" /proc/version 2>/dev/null; then
                OS="linux"
                print_info "Detected WSL environment"
            fi
            ;;
    esac

    case "$ARCH" in
        x86_64|amd64) ARCH="amd64" ;;
        aarch64|arm64) ARCH="arm64" ;;
        i386|i686) ARCH="386" ;;
        *) print_error "Unsupported architecture: $ARCH"; exit 1 ;;
    esac

    case "$OS" in
        linux|darwin|windows) ;;
        *) print_error "Unsupported OS: $OS"; exit 1 ;;
    esac
}

get_default_prefix() {
    if [[ "$OS" == "windows" ]]; then
        echo "$HOME/AppData/Local/FlyMQ"
    elif [[ $EUID -eq 0 ]]; then
        echo "/usr/local"
    else
        echo "$HOME/.local"
    fi
}

get_default_data_dir() {
    if [[ "$OS" == "windows" ]]; then
        echo "$HOME/AppData/Local/FlyMQ/data"
    elif [[ $EUID -eq 0 ]]; then
        echo "/var/lib/flymq"
    else
        echo "$HOME/.local/share/flymq"
    fi
}

get_default_config_dir() {
    if [[ "$OS" == "windows" ]]; then
        echo "$HOME/AppData/Local/FlyMQ/config"
    elif [[ $EUID -eq 0 ]]; then
        echo "/etc/flymq"
    else
        echo "$HOME/.config/flymq"
    fi
}

# =============================================================================
# Interactive Configuration
# =============================================================================

prompt_value() {
    local prompt="$1"
    local default="$2"
    local result

    echo -en "  ${prompt} [${DIM}${default}${RESET}]: " >&2
    read -r result </dev/tty
    result="${result:-$default}"
    echo "$result"
}

prompt_yes_no() {
    local prompt="$1"
    local default="$2"
    local result

    while true; do
        if [[ "$default" == "y" ]]; then
            echo -en "  ${prompt} [${GREEN}Y${RESET}/${DIM}n${RESET}]: " >&2
        else
            echo -en "  ${prompt} [${DIM}y${RESET}/${GREEN}N${RESET}]: " >&2
        fi
        read -r result </dev/tty
        result="${result:-$default}"

        if [[ "$result" =~ ^[YyNn]$ ]] || [[ -z "$result" ]]; then
            [[ "$result" =~ ^[Yy] ]] && return 0
            return 1
        fi
        print_warning "Please answer 'y' or 'n'"
    done
}

prompt_number() {
    local prompt="$1"
    local default="$2"
    local min="${3:-0}"
    local max="${4:-}"
    local result

    while true; do
        result=$(prompt_value "$prompt" "$default")
        
        # Check if it's a valid number
        if ! [[ "$result" =~ ^[0-9]+$ ]]; then
            print_warning "Please enter a valid number"
            continue
        fi
        
        # Check minimum
        if [[ -n "$min" ]] && (( result < min )); then
            print_warning "Value must be at least $min"
            continue
        fi
        
        # Check maximum
        if [[ -n "$max" ]] && (( result > max )); then
            print_warning "Value must be at most $max"
            continue
        fi
        
        echo "$result"
        return 0
    done
}

prompt_choice() {
    local prompt="$1"
    local default="$2"
    shift 2
    local valid_choices=("$@")
    local result

    while true; do
        result=$(prompt_value "$prompt" "$default")
        
        # Check if result is in valid choices
        for choice in "${valid_choices[@]}"; do
            if [[ "$result" == "$choice" ]]; then
                echo "$result"
                return 0
            fi
        done
        
        print_warning "Please enter one of: ${valid_choices[*]}"
    done
}

# =============================================================================
# Cluster Mode Configuration
# =============================================================================

# Generate a unique node ID for cluster deployment
generate_cluster_node_id() {
    local hostname
    hostname=$(hostname | tr '[:upper:]' '[:lower:]' | tr -cd '[:alnum:]-')

    # Add a short random suffix for uniqueness
    local suffix
    suffix=$(head -c 4 /dev/urandom 2>/dev/null | xxd -p 2>/dev/null || echo "$(date +%s)" | tail -c 5)
    suffix="${suffix:0:4}"

    echo "${hostname}-${suffix}"
}

# Validate address format (hostname or IP)
validate_address_format() {
    local addr="$1"

    # Check if it's a valid IPv4 address
    if [[ "$addr" =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
        return 0
    fi

    # Check if it's a valid hostname (basic check)
    if [[ "$addr" =~ ^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)*$ ]]; then
        return 0
    fi

    # Check for localhost
    if [[ "$addr" == "localhost" ]]; then
        return 0
    fi

    return 1
}

# Validate peer address format (host:port)
validate_peer_address() {
    local peer="$1"

    # Check format: host:port
    if [[ ! "$peer" =~ ^.+:[0-9]+$ ]]; then
        return 1
    fi

    local host="${peer%:*}"
    local port="${peer##*:}"

    # Validate host
    if ! validate_address_format "$host"; then
        return 1
    fi

    # Validate port range
    if (( port < 1 || port > 65535 )); then
        return 1
    fi

    return 0
}

# Validate cluster size recommendations
validate_cluster_size() {
    local expected_nodes="$1"
    
    # Check for single-node cluster warnings
    if (( expected_nodes == 1 )); then
        print_warning "Single-node cluster provides no fault tolerance"
        print_info "Consider adding more nodes for production use"
    fi

    # Check for even-numbered cluster (split-brain risk)
    if (( expected_nodes > 1 && expected_nodes % 2 == 0 )); then
        print_warning "Even number of nodes ($expected_nodes) may cause split-brain scenarios"
        print_info "Consider using an odd number of nodes (3, 5, 7) for better fault tolerance"
    fi
}

configure_cluster_mode() {
    print_section "Cluster Configuration"
    echo -e "  ${DIM}FlyMQ uses Raft consensus for leader election and data replication.${RESET}"
    echo -e "  ${DIM}A cluster requires at least 3 nodes for fault tolerance.${RESET}"
    echo ""

    # Determine if this is the first node or joining an existing cluster
    echo -e "  ${BOLD}Cluster Role${RESET}"
    echo -e "    ${CYAN}1${RESET}) Bootstrap - First node in a new cluster ${DIM}(no peers needed)${RESET}"
    echo -e "    ${CYAN}2${RESET}) Join      - Join an existing cluster ${DIM}(peers required)${RESET}"
    echo ""

    local role_choice
    role_choice=$(prompt_choice "Cluster role (1=bootstrap, 2=join)" "1" "1" "2" "bootstrap" "join")

    if [[ "$role_choice" == "2" ]] || [[ "$role_choice" == "join" ]]; then
        configure_cluster_join
    else
        configure_cluster_bootstrap
    fi

    # Configure partition management for horizontal scaling
    configure_partition_management
}

configure_partition_management() {
    print_section "Partition Management (Horizontal Scaling)"
    echo -e "  ${DIM}FlyMQ distributes partition leaders across cluster nodes for horizontal scaling.${RESET}"
    echo -e "  ${DIM}Each partition can have a different leader, enabling parallel writes.${RESET}"
    echo ""

    if prompt_yes_no "Configure partition management settings" "n"; then
        echo ""
        echo -e "  ${BOLD}Default Topic Settings${RESET}"
        echo -e "  ${DIM}These defaults apply when creating new topics.${RESET}"
        echo ""

        CFG_PARTITION_DEFAULT_PARTITIONS=$(prompt_value "Default partitions per topic" "${CFG_PARTITION_DEFAULT_PARTITIONS}")
        CFG_PARTITION_DEFAULT_REPLICATION_FACTOR=$(prompt_value "Default replication factor" "${CFG_PARTITION_DEFAULT_REPLICATION_FACTOR}")
        echo ""

        echo -e "  ${BOLD}Distribution Strategy${RESET}"
        echo -e "  ${DIM}How partition leaders are distributed across nodes:${RESET}"
        echo -e "    ${CYAN}round-robin${RESET} - Distribute evenly in order (default)"
        echo -e "    ${CYAN}least-loaded${RESET} - Assign to node with fewest leaders"
        echo -e "    ${CYAN}rack-aware${RESET} - Consider rack placement for fault tolerance"
        echo ""

        local strategy_choice
        strategy_choice=$(prompt_choice "Distribution strategy (1=round-robin, 2=least-loaded, 3=rack-aware)" "1" "1" "2" "3" "round-robin" "least-loaded" "rack-aware")
        case "$strategy_choice" in
            1|round-robin) CFG_PARTITION_DISTRIBUTION_STRATEGY="round-robin" ;;
            2|least-loaded) CFG_PARTITION_DISTRIBUTION_STRATEGY="least-loaded" ;;
            3|rack-aware) CFG_PARTITION_DISTRIBUTION_STRATEGY="rack-aware" ;;
        esac
        echo ""

        echo -e "  ${BOLD}Automatic Rebalancing${RESET}"
        echo -e "  ${DIM}Automatically redistribute partition leaders when nodes join/leave.${RESET}"
        echo ""

        if prompt_yes_no "Enable automatic rebalancing" "n"; then
            CFG_PARTITION_AUTO_REBALANCE_ENABLED="true"
            CFG_PARTITION_AUTO_REBALANCE_INTERVAL=$(prompt_value "Rebalance check interval (seconds)" "${CFG_PARTITION_AUTO_REBALANCE_INTERVAL}")
            CFG_PARTITION_REBALANCE_THRESHOLD=$(prompt_value "Imbalance threshold (0.0-1.0, e.g., 0.2 = 20%)" "${CFG_PARTITION_REBALANCE_THRESHOLD}")
        else
            CFG_PARTITION_AUTO_REBALANCE_ENABLED="false"
        fi
        echo ""

        print_success "Partition management configured"
        echo ""
        echo -e "  ${BOLD}Partition Settings:${RESET}"
        echo -e "    Default Partitions:     ${CYAN}${CFG_PARTITION_DEFAULT_PARTITIONS}${RESET}"
        echo -e "    Replication Factor:     ${CYAN}${CFG_PARTITION_DEFAULT_REPLICATION_FACTOR}${RESET}"
        echo -e "    Distribution Strategy:  ${CYAN}${CFG_PARTITION_DISTRIBUTION_STRATEGY}${RESET}"
        echo -e "    Auto Rebalance:         ${CYAN}${CFG_PARTITION_AUTO_REBALANCE_ENABLED}${RESET}"
        echo ""
    else
        print_info "Using default partition settings (1 partition, 1 replica, round-robin)"
    fi
}

configure_cluster_bootstrap() {
    echo ""
    echo -e "  ${BOLD}Bootstrap Node Configuration${RESET}"
    echo -e "  ${DIM}This node will be the initial leader of the cluster.${RESET}"
    echo -e "  ${DIM}Other nodes will join using this node's cluster address.${RESET}"
    echo ""

    # Generate unique node ID for cluster
    echo -e "  ${BOLD}Node Identity${RESET}"
    local default_node_id
    default_node_id=$(generate_cluster_node_id)
    CFG_NODE_ID=$(prompt_value "Node ID (must be unique in cluster)" "$default_node_id")
    echo ""

    # Get the advertised address for this node
    local default_ip
    default_ip=$(get_local_ip)

    echo -e "  ${BOLD}Cluster Address${RESET}"
    echo -e "  ${DIM}This is the address other nodes will use to connect to this node.${RESET}"
    echo -e "  ${DIM}Use a hostname or IP that is reachable from other cluster nodes.${RESET}"
    local advertised_host
    advertised_host=$(prompt_value "Advertised hostname/IP" "$default_ip")

    # Validate the advertised address format
    if ! validate_address_format "$advertised_host"; then
        print_warning "Address format may be invalid. Proceeding anyway."
    fi

    local advertised_port
    advertised_port=$(prompt_value "Cluster port" "9093")
    CFG_ADVERTISE_CLUSTER="${advertised_host}:${advertised_port}"
    CFG_CLUSTER_ADDR=":${advertised_port}"
    echo ""

    # Expected cluster size for validation
    echo -e "  ${BOLD}Cluster Size${RESET}"
    local expected_nodes
    expected_nodes=$(prompt_number "Expected number of nodes in cluster" "3" "1")
    validate_cluster_size "$expected_nodes"
    echo ""

    print_info "Bootstrap node configured."
    echo ""
    echo -e "  ${GREEN}${BOLD}✓ BOOTSTRAP NODE SETUP COMPLETE${RESET}"
    echo ""
    echo -e "  ${BOLD}This Node:${RESET}"
    echo -e "    Node ID:         ${CYAN}${CFG_NODE_ID}${RESET}"
    echo -e "    Cluster Address: ${CYAN}${CFG_ADVERTISE_CLUSTER}${RESET}"
    echo ""
    echo -e "  ${BOLD}Next Steps After Installation:${RESET}"
    echo ""
    echo -e "  ${YELLOW}1.${RESET} Start this bootstrap node first:"
    echo -e "     ${CYAN}flymq --config /etc/flymq/flymq.json${RESET}"
    echo ""
    echo -e "  ${YELLOW}2.${RESET} On each additional node, run the installer and choose 'Join' mode"
    echo ""
    echo -e "  ${YELLOW}3.${RESET} When prompted for peer addresses on joining nodes, use:"
    echo -e "     ${CYAN}${CFG_ADVERTISE_CLUSTER}${RESET}"
    echo ""
    echo -e "  ${YELLOW}4.${RESET} After all nodes are running, verify cluster status:"
    echo -e "     ${CYAN}flymq-cli cluster status${RESET}"
    echo ""
}

configure_cluster_join() {
    echo ""
    echo -e "  ${BOLD}Join Existing Cluster${RESET}"
    echo -e "  ${DIM}This node will join an existing FlyMQ cluster.${RESET}"
    echo ""

    # Generate unique node ID for this joining node
    echo -e "  ${BOLD}Node Identity${RESET}"
    local default_node_id
    default_node_id=$(generate_cluster_node_id)
    CFG_NODE_ID=$(prompt_value "Node ID (must be unique in cluster)" "$default_node_id")
    echo ""

    # Get peer addresses with validation
    echo -e "  ${BOLD}Cluster Peers${RESET}"
    echo -e "  ${DIM}Enter the addresses of existing cluster nodes (comma-separated).${RESET}"
    echo -e "  ${DIM}Include at least the bootstrap node address.${RESET}"
    echo -e "  ${DIM}Format: host1:port,host2:port${RESET}"
    echo -e "  ${DIM}Example: 192.168.1.10:9093,192.168.1.11:9093${RESET}"
    echo ""

    local peers_valid=false
    while [[ "$peers_valid" == false ]]; do
        CFG_CLUSTER_PEERS=$(prompt_value "Peer addresses" "")

        if [[ -z "$CFG_CLUSTER_PEERS" ]]; then
            print_error "At least one peer address is required to join a cluster."
            if prompt_yes_no "Continue without peers (configure later)" "n"; then
                print_warning "You must configure peers in flymq.json before starting"
                peers_valid=true
            fi
        else
            # Validate each peer address
            local all_valid=true
            local invalid_peers=""
            IFS=',' read -ra peer_array <<< "$CFG_CLUSTER_PEERS"

            for peer in "${peer_array[@]}"; do
                peer=$(echo "$peer" | xargs)  # trim whitespace
                if [[ -n "$peer" ]] && ! validate_peer_address "$peer"; then
                    all_valid=false
                    invalid_peers="${invalid_peers} ${peer}"
                fi
            done

            if [[ "$all_valid" == true ]]; then
                peers_valid=true
                print_success "Peer addresses validated: ${#peer_array[@]} peer(s)"
            else
                print_warning "Invalid peer address format:${invalid_peers}"
                print_info "Expected format: hostname:port or ip:port"
                if prompt_yes_no "Continue anyway" "n"; then
                    peers_valid=true
                fi
            fi
        fi
    done
    echo ""

    # Get the advertised address for this node
    local default_ip
    default_ip=$(get_local_ip)

    echo -e "  ${BOLD}Cluster Address${RESET}"
    echo -e "  ${DIM}This is the address other cluster nodes will use to connect to this node.${RESET}"
    echo -e "  ${DIM}Must be reachable from all other cluster nodes.${RESET}"
    local advertised_host
    advertised_host=$(prompt_value "Advertised hostname/IP" "$default_ip")

    # Validate the advertised address format
    if ! validate_address_format "$advertised_host"; then
        print_warning "Address format may be invalid. Proceeding anyway."
    fi

    local advertised_port
    advertised_port=$(prompt_value "Cluster port" "9093")
    CFG_ADVERTISE_CLUSTER="${advertised_host}:${advertised_port}"
    CFG_CLUSTER_ADDR=":${advertised_port}"
    echo ""

    print_info "Join configuration complete."
    echo ""
    echo -e "  ${GREEN}${BOLD}✓ JOINING NODE SETUP COMPLETE${RESET}"
    echo ""
    echo -e "  ${BOLD}This Node:${RESET}"
    echo -e "    Node ID:         ${CYAN}${CFG_NODE_ID}${RESET}"
    echo -e "    Cluster Address: ${CYAN}${CFG_ADVERTISE_CLUSTER}${RESET}"
    echo -e "    Joining Peers:   ${CYAN}${CFG_CLUSTER_PEERS}${RESET}"
    echo ""
    echo -e "  ${BOLD}Pre-Start Checklist:${RESET}"
    echo -e "    ${DIM}☐ Bootstrap node is running and healthy${RESET}"
    echo -e "    ${DIM}☐ Network connectivity to peers verified${RESET}"
    echo -e "    ${DIM}☐ Firewall allows port ${advertised_port} (cluster) and 9092 (client)${RESET}"
    echo ""
    echo -e "  ${BOLD}Next Steps After Installation:${RESET}"
    echo ""
    echo -e "  ${YELLOW}1.${RESET} Verify the cluster is running:"
    echo -e "     ${CYAN}flymq-cli --server ${CFG_CLUSTER_PEERS%%,*} cluster status${RESET}"
    echo ""
    echo -e "  ${YELLOW}2.${RESET} Start this node:"
    echo -e "     ${CYAN}flymq --config /etc/flymq/flymq.json${RESET}"
    echo ""
    echo -e "  ${YELLOW}3.${RESET} Verify this node joined successfully:"
    echo -e "     ${CYAN}flymq-cli cluster status${RESET}"
    echo ""
}

get_local_ip() {
    # Try to get the primary IP address with timeout protection
    local ip=""
    
    # Check if timeout command exists (GNU coreutils)
    local use_timeout=false
    if command -v timeout &> /dev/null; then
        use_timeout=true
    elif command -v gtimeout &> /dev/null; then
        # macOS with GNU coreutils via Homebrew
        alias timeout='gtimeout'
        use_timeout=true
    fi
    
    # On macOS, use route and ifconfig
    if [[ "$OS" == "darwin" ]]; then
        # Try en0 or en1 directly (most common on macOS)
        for iface in en0 en1; do
            ip=$(ifconfig "$iface" 2>/dev/null | awk '/inet / && !/127\.0\.0\.1/ {print $2; exit}' || echo "")
            [[ -n "$ip" ]] && break
        done
        
        # If still no IP, try the default route interface
        if [[ -z "$ip" ]]; then
            local default_if=$(route -n get default 2>/dev/null | awk '/interface:/ {print $2; exit}' || echo "")
            if [[ -n "$default_if" ]]; then
                ip=$(ifconfig "$default_if" 2>/dev/null | awk '/inet / && !/127\.0\.0\.1/ {print $2; exit}' || echo "")
            fi
        fi
    # On Linux, try ip command first
    elif [[ "$OS" == "linux" ]] && command -v ip &> /dev/null; then
        if [[ "$use_timeout" == true ]]; then
            ip=$(timeout 2 ip route get 1.1.1.1 2>/dev/null | awk '/src/ {print $7; exit}' || echo "")
        else
            ip=$(ip route get 1.1.1.1 2>/dev/null | awk '/src/ {print $7; exit}' || echo "")
        fi
    # On Windows (Git Bash/MSYS2)
    elif [[ "$OS" == "windows" ]]; then
        # Try ipconfig on Windows
        ip=$(ipconfig 2>/dev/null | grep -Eo '([0-9]{1,3}\.){3}[0-9]{1,3}' | grep -v '127.0.0.1' | head -1 || echo "")
    fi
    
    # Generic ifconfig fallback (cross-platform)
    if [[ -z "$ip" ]] || [[ "$ip" == "127."* ]]; then
        if command -v ifconfig &> /dev/null; then
            ip=$(ifconfig 2>/dev/null | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '^127\.' | head -1 || echo "")
        fi
    fi
    
    # Final fallback to localhost
    if [[ -z "$ip" ]] || [[ "$ip" == "127."* ]]; then
        ip="localhost"
    fi

    echo "$ip"
}

# =============================================================================
# Service Discovery Functions
# =============================================================================

# Check if flymq-discover tool is available
has_discovery_tool() {
    command -v flymq-discover &> /dev/null || [[ -x "${SCRIPT_DIR}/flymq-discover" ]]
}

# Discover FlyMQ nodes on the network using mDNS
discover_nodes_mdns() {
    local timeout="${1:-5}"
    local discover_cmd=""

    if command -v flymq-discover &> /dev/null; then
        discover_cmd="flymq-discover"
    elif [[ -x "${SCRIPT_DIR}/flymq-discover" ]]; then
        discover_cmd="${SCRIPT_DIR}/flymq-discover"
    else
        return 1
    fi

    "$discover_cmd" --timeout "$timeout" --quiet 2>/dev/null
}

# Discover nodes using network scanning (fallback)
discover_nodes_scan() {
    local subnet="${1:-}"
    local port="${2:-9093}"

    if [[ -z "$subnet" ]]; then
        # Try to detect local subnet
        local local_ip=$(get_local_ip)
        if [[ "$local_ip" =~ ^([0-9]+\.[0-9]+\.[0-9]+)\.[0-9]+$ ]]; then
            subnet="${BASH_REMATCH[1]}"
        else
            return 1
        fi
    fi

    local found_nodes=""

    # Quick scan of common IPs in subnet
    for i in {1..254}; do
        local target="${subnet}.${i}:${port}"
        # Use timeout to quickly check if port is open
        if (echo >/dev/tcp/${subnet}.${i}/${port}) 2>/dev/null; then
            if [[ -n "$found_nodes" ]]; then
                found_nodes+=","
            fi
            found_nodes+="$target"
        fi
    done &

    # Wait with timeout
    local pid=$!
    sleep 3
    kill $pid 2>/dev/null || true

    echo "$found_nodes"
}

# Interactive node discovery
discover_cluster_nodes() {
    print_section "Cluster Node Discovery"
    echo -e "  ${DIM}FlyMQ can automatically discover existing cluster nodes on your network.${RESET}"
    echo ""

    local discovered_nodes=""
    local discovery_method=""

    # Try mDNS discovery first
    if has_discovery_tool; then
        echo -e "  ${CYAN}Scanning for FlyMQ nodes using mDNS...${RESET}"
        discovered_nodes=$(discover_nodes_mdns 5)
        if [[ -n "$discovered_nodes" ]]; then
            discovery_method="mdns"
        fi
    fi

    # Show results
    if [[ -n "$discovered_nodes" ]]; then
        echo ""
        echo -e "  ${GREEN}${BOLD}✓ Found existing FlyMQ nodes:${RESET}"
        echo ""

        local i=1
        IFS=',' read -ra node_array <<< "$discovered_nodes"
        for node in "${node_array[@]}"; do
            echo -e "    ${CYAN}[$i]${RESET} $node"
            ((i++))
        done
        echo ""

        if prompt_yes_no "Use discovered nodes as cluster peers" "y"; then
            CFG_CLUSTER_PEERS="$discovered_nodes"
            print_success "Using discovered nodes: $discovered_nodes"
            return 0
        fi
    else
        echo -e "  ${YELLOW}No FlyMQ nodes found on the network.${RESET}"
        echo ""
        echo -e "  ${DIM}This could mean:${RESET}"
        echo -e "    ${DIM}- No cluster exists yet (you're setting up the first node)${RESET}"
        echo -e "    ${DIM}- Existing nodes are on a different network${RESET}"
        echo -e "    ${DIM}- mDNS/Bonjour is blocked by firewall${RESET}"
        echo ""
    fi

    # Manual entry option
    echo -e "  ${BOLD}Enter peer addresses manually:${RESET}"
    echo -e "  ${DIM}Format: host1:port,host2:port (e.g., 192.168.1.10:9093,192.168.1.11:9093)${RESET}"
    echo -e "  ${DIM}Leave empty if this is the first node in the cluster.${RESET}"
    echo ""

    CFG_CLUSTER_PEERS=$(prompt_value "Peer addresses" "")

    return 0
}

# Test connectivity to a peer node
test_peer_connectivity() {
    local peer="$1"
    local host="${peer%:*}"
    local port="${peer##*:}"

    # Try to connect with timeout
    if command -v nc &> /dev/null; then
        nc -z -w 2 "$host" "$port" 2>/dev/null
        return $?
    elif command -v timeout &> /dev/null; then
        timeout 2 bash -c "echo >/dev/tcp/$host/$port" 2>/dev/null
        return $?
    else
        # Fallback: just try to connect
        (echo >/dev/tcp/$host/$port) 2>/dev/null
        return $?
    fi
}

# Validate connectivity to all configured peers
validate_peer_connectivity() {
    if [[ -z "$CFG_CLUSTER_PEERS" ]]; then
        return 0
    fi

    echo ""
    echo -e "  ${BOLD}Testing connectivity to peers...${RESET}"

    local all_ok=true
    IFS=',' read -ra peer_array <<< "$CFG_CLUSTER_PEERS"

    for peer in "${peer_array[@]}"; do
        peer=$(echo "$peer" | xargs)  # trim whitespace
        if [[ -z "$peer" ]]; then
            continue
        fi

        echo -n "    Testing $peer... "
        if test_peer_connectivity "$peer"; then
            echo -e "${GREEN}✓ OK${RESET}"
        else
            echo -e "${RED}✗ UNREACHABLE${RESET}"
            all_ok=false
        fi
    done

    if [[ "$all_ok" == false ]]; then
        echo ""
        print_warning "Some peers are unreachable. This may cause issues when starting the cluster."
        echo -e "  ${DIM}Possible causes:${RESET}"
        echo -e "    ${DIM}- Peer nodes are not running yet${RESET}"
        echo -e "    ${DIM}- Firewall blocking port 9093${RESET}"
        echo -e "    ${DIM}- Incorrect peer addresses${RESET}"
        echo ""

        if ! prompt_yes_no "Continue anyway" "y"; then
            return 1
        fi
    else
        echo ""
        print_success "All peers are reachable"
    fi

    return 0
}

# =============================================================================
# JSON Configuration Generation
# =============================================================================

# Format peers array as JSON array
format_peers_json() {
    if [[ -z "$CFG_CLUSTER_PEERS" ]]; then
        echo "[]"
        return
    fi
    
    local result="["
    local first=true
    IFS=',' read -ra peer_array <<< "$CFG_CLUSTER_PEERS"
    for peer in "${peer_array[@]}"; do
        peer=$(echo "$peer" | xargs)  # trim whitespace
        if [[ -n "$peer" ]]; then
            if [[ "$first" == "true" ]]; then
                first=false
            else
                result+=", "
            fi
            result+="\"$peer\""
        fi
    done
    result+="]"
    echo "$result"
}

generate_config() {
    local config_dir="$1"
    local config_file="$config_dir/flymq.json"

    print_step "Generating configuration"
    echo ""
    
    mkdir -p "$config_dir"

    # Build peers array and cluster settings based on deployment mode
    local peers_json
    local cluster_addr_value
    local advertise_cluster_value
    
    if [[ "$CFG_DEPLOYMENT_MODE" == "cluster" ]]; then
        peers_json=$(format_peers_json)
        cluster_addr_value="${CFG_CLUSTER_ADDR}"
        advertise_cluster_value="${CFG_ADVERTISE_CLUSTER}"
    else
        # Standalone mode: no cluster configuration
        peers_json="[]"
        cluster_addr_value=""
        advertise_cluster_value=""
    fi

    cat > "$config_file" << EOF
{
  "_comment": "FlyMQ Configuration - Generated by install.sh on $(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "_mode": "${CFG_DEPLOYMENT_MODE}",

  "bind_addr": "${CFG_BIND_ADDR}",
  "cluster_addr": "${cluster_addr_value}",
  "advertise_cluster": "${advertise_cluster_value}",
  "peers": ${peers_json},
  "node_id": "${CFG_NODE_ID}",

  "data_dir": "${CFG_DATA_DIR}",
  "retention_bytes": ${CFG_RETENTION_BYTES},
  "segment_bytes": ${CFG_SEGMENT_BYTES},

  "log_level": "${CFG_LOG_LEVEL}",

  "security": {
    "tls_enabled": ${CFG_TLS_ENABLED},
    "tls_cert_file": "${CFG_TLS_CERT_FILE}",
    "tls_key_file": "${CFG_TLS_KEY_FILE}",
    "encryption_enabled": ${CFG_ENCRYPTION_ENABLED}
  },

  "auth": {
    "enabled": ${CFG_AUTH_ENABLED},
    "allow_anonymous": ${CFG_AUTH_ALLOW_ANONYMOUS},
    "admin_username": "${CFG_AUTH_ADMIN_USER}",
    "admin_password": "${CFG_AUTH_ADMIN_PASS}"
  },

  "performance": {
    "acks": "${CFG_ACKS}",
    "sync_interval_ms": 5
  },

  "schema": {
    "enabled": ${CFG_SCHEMA_ENABLED},
    "validation": "${CFG_SCHEMA_VALIDATION}"
  },

  "dlq": {
    "enabled": ${CFG_DLQ_ENABLED},
    "max_retries": ${CFG_DLQ_MAX_RETRIES},
    "retry_delay": ${CFG_DLQ_RETRY_DELAY},
    "topic_suffix": "${CFG_DLQ_TOPIC_SUFFIX}"
  },

  "ttl": {
    "default_ttl": ${CFG_TTL_DEFAULT},
    "cleanup_interval": ${CFG_TTL_CLEANUP_INTERVAL}
  },

  "delayed": {
    "enabled": ${CFG_DELAYED_ENABLED},
    "max_delay": ${CFG_DELAYED_MAX_DELAY}
  },

  "transaction": {
    "enabled": ${CFG_TXN_ENABLED},
    "timeout": ${CFG_TXN_TIMEOUT}
  },

  "partition": {
    "distribution_strategy": "${CFG_PARTITION_DISTRIBUTION_STRATEGY}",
    "default_replication_factor": ${CFG_PARTITION_DEFAULT_REPLICATION_FACTOR},
    "default_partitions": ${CFG_PARTITION_DEFAULT_PARTITIONS},
    "auto_rebalance_enabled": ${CFG_PARTITION_AUTO_REBALANCE_ENABLED},
    "auto_rebalance_interval": ${CFG_PARTITION_AUTO_REBALANCE_INTERVAL},
    "rebalance_threshold": ${CFG_PARTITION_REBALANCE_THRESHOLD}
  },

  "discovery": {
    "enabled": ${CFG_DISCOVERY_ENABLED:-false},
    "cluster_id": "${CFG_DISCOVERY_CLUSTER_ID:-}"
  },

  "observability": {
    "metrics": {
      "enabled": ${CFG_METRICS_ENABLED},
      "addr": "${CFG_METRICS_ADDR}"
    },
    "tracing": {
      "enabled": ${CFG_TRACING_ENABLED},
      "endpoint": "${CFG_TRACING_ENDPOINT}",
      "sample_rate": ${CFG_TRACING_SAMPLE_RATE}
    },
    "health": {
      "enabled": ${CFG_HEALTH_ENABLED},
      "addr": "${CFG_HEALTH_ADDR}",
      "tls_enabled": ${CFG_HEALTH_TLS_ENABLED},
      "tls_cert_file": "${CFG_HEALTH_TLS_CERT_FILE}",
      "tls_key_file": "${CFG_HEALTH_TLS_KEY_FILE}",
      "tls_auto_generate": ${CFG_HEALTH_TLS_AUTO_GENERATE},
      "tls_use_admin_cert": ${CFG_HEALTH_TLS_USE_ADMIN_CERT}
    },
    "admin": {
      "enabled": ${CFG_ADMIN_ENABLED},
      "addr": "${CFG_ADMIN_ADDR}",
      "auth_enabled": ${CFG_AUTH_ENABLED},
      "tls_enabled": ${CFG_ADMIN_TLS_ENABLED},
      "tls_cert_file": "${CFG_ADMIN_TLS_CERT_FILE}",
      "tls_key_file": "${CFG_ADMIN_TLS_KEY_FILE}",
      "tls_auto_generate": ${CFG_ADMIN_TLS_AUTO_GENERATE}
    }
  },

  "audit": {
    "enabled": ${CFG_AUDIT_ENABLED},
    "log_dir": "${CFG_AUDIT_LOG_DIR}",
    "max_file_size": ${CFG_AUDIT_MAX_FILE_SIZE},
    "retention_days": ${CFG_AUDIT_RETENTION_DAYS}
  }
}
EOF

    print_success "Generated: ${CYAN}$config_file${RESET}"

    # Generate secrets file when encryption is enabled (always, for security)
    if [[ "$CFG_ENCRYPTION_ENABLED" == "true" ]]; then
        generate_secrets_file "$config_dir"
    fi

    # Generate environment file for cluster deployments
    if [[ "$CFG_DEPLOYMENT_MODE" == "cluster" ]]; then
        generate_env_file "$config_dir"
    fi
}

# Generate environment variables file for systemd/container deployments
generate_env_file() {
    local config_dir="$1"
    local env_file="$config_dir/flymq.env"

    cat > "$env_file" << EOF
# FlyMQ Environment Variables
# Generated by install.sh on $(date -u +"%Y-%m-%dT%H:%M:%SZ")
# Source this file or use with systemd EnvironmentFile=

# Network Configuration
FLYMQ_BIND_ADDR=${CFG_BIND_ADDR}
FLYMQ_CLUSTER_ADDR=${CFG_CLUSTER_ADDR}
FLYMQ_ADVERTISE_CLUSTER=${CFG_ADVERTISE_CLUSTER}
FLYMQ_NODE_ID=${CFG_NODE_ID}
FLYMQ_PEERS=${CFG_CLUSTER_PEERS}

# Storage Configuration
FLYMQ_DATA_DIR=${CFG_DATA_DIR}
FLYMQ_SEGMENT_BYTES=${CFG_SEGMENT_BYTES}
FLYMQ_RETENTION_BYTES=${CFG_RETENTION_BYTES}

# Logging
FLYMQ_LOG_LEVEL=${CFG_LOG_LEVEL}

# Performance
FLYMQ_ACKS=${CFG_ACKS}

# Security (non-sensitive)
FLYMQ_TLS_ENABLED=${CFG_TLS_ENABLED}
FLYMQ_TLS_CERT_FILE=${CFG_TLS_CERT_FILE}
FLYMQ_TLS_KEY_FILE=${CFG_TLS_KEY_FILE}
FLYMQ_ENCRYPTION_ENABLED=${CFG_ENCRYPTION_ENABLED}
# NOTE: FLYMQ_ENCRYPTION_KEY is stored separately in flymq.secrets for security

# Observability
FLYMQ_METRICS_ENABLED=${CFG_METRICS_ENABLED}
FLYMQ_METRICS_ADDR=${CFG_METRICS_ADDR}
FLYMQ_HEALTH_ENABLED=${CFG_HEALTH_ENABLED}
FLYMQ_HEALTH_ADDR=${CFG_HEALTH_ADDR}
FLYMQ_ADMIN_ENABLED=${CFG_ADMIN_ENABLED}
FLYMQ_ADMIN_ADDR=${CFG_ADMIN_ADDR}
EOF

    print_success "Generated: ${CYAN}$env_file${RESET}"
}

# Generate secrets file with restricted permissions (encryption key, passwords)
generate_secrets_file() {
    local config_dir="$1"
    local secrets_file="$config_dir/flymq.secrets"

    # Create secrets file with restricted permissions (owner read/write only)
    touch "$secrets_file"
    chmod 600 "$secrets_file"

    cat > "$secrets_file" << EOF
# FlyMQ Secrets - KEEP THIS FILE SECURE!
# Generated by install.sh on $(date -u +"%Y-%m-%dT%H:%M:%SZ")
# Permissions: 600 (owner read/write only)
#
# SECURITY WARNING:
# - This file contains sensitive cryptographic keys
# - Never commit this file to version control
# - Back up securely (encrypted backup recommended)
# - Use a secrets manager in production (HashiCorp Vault, AWS Secrets Manager, etc.)
#
# CLUSTER REQUIREMENT:
# - ALL nodes in a cluster MUST use the SAME encryption key
# - Nodes with different keys will be rejected from joining
# - Copy this file to all cluster nodes or use a secrets manager
#
# Usage:
#   source $secrets_file && flymq --config $config_dir/flymq.json
#   OR
#   systemd: EnvironmentFile=$secrets_file

# Encryption Key (AES-256, 64 hex characters)
# Required when encryption_enabled=true in config
# IMPORTANT: Must be identical across all cluster nodes!
FLYMQ_ENCRYPTION_KEY=${CFG_ENCRYPTION_KEY}
EOF

    # Ensure permissions are set correctly
    chmod 600 "$secrets_file"

    print_success "Generated: ${CYAN}$secrets_file${RESET} ${DIM}(mode 600)${RESET}"
}

# =============================================================================
# Installation
# =============================================================================

# Track if we need to clone the repo (detected early)
CLONED_REPO_DIR=""
NEEDS_REMOTE_CLONE=false

# Cleanup function for cloned repo - called on exit/error
cleanup_cloned_repo() {
    if [[ -n "${CLONED_REPO_DIR}" ]] && [[ -d "${CLONED_REPO_DIR}" ]]; then
        echo ""
        print_info "Cleaning up temporary files..."
        rm -rf "${CLONED_REPO_DIR}"
        CLONED_REPO_DIR=""
    fi
}

# Detect if we're in the repo or need to clone
detect_source_mode() {
    # Check if we're already in the FlyMQ repository
    if [[ -f "go.mod" ]] && grep -q "module flymq" go.mod 2>/dev/null; then
        NEEDS_REMOTE_CLONE=false
        return 0
    fi

    # Check if we're in a subdirectory of the repo
    if [[ -f "${SCRIPT_DIR}/go.mod" ]] && grep -q "module flymq" "${SCRIPT_DIR}/go.mod" 2>/dev/null; then
        NEEDS_REMOTE_CLONE=false
        return 0
    fi

    # Not in repo - will need to clone
    NEEDS_REMOTE_CLONE=true
}

check_dependencies() {
    print_step "Checking dependencies"
    echo ""

    # Always need Go
    if ! command -v go &> /dev/null; then
        print_error "Go is not installed. Please install Go 1.21+ first."
        print_info "Visit: ${CYAN}https://go.dev/dl/${RESET}"
        exit 1
    fi
    GO_VERSION=$(go version | grep -oE 'go[0-9]+\.[0-9]+' | sed 's/go//')
    print_success "Go $GO_VERSION found"

    # Need Git if running via curl (remote install)
    if [[ "$NEEDS_REMOTE_CLONE" == true ]]; then
        if ! command -v git &> /dev/null; then
            print_error "Git is not installed. Required for remote installation."
            print_info "Install git or clone manually: ${CYAN}git clone https://github.com/firefly-oss/flymq.git${RESET}"
            exit 1
        fi
        print_success "Git found"
    fi
}

ensure_source_code() {
    # Check if we're already in the FlyMQ repository
    if [[ -f "go.mod" ]] && grep -q "module flymq" go.mod 2>/dev/null; then
        print_success "Running from FlyMQ repository"
        return 0
    fi

    # Check if we're in a subdirectory of the repo
    if [[ -f "${SCRIPT_DIR}/go.mod" ]] && grep -q "module flymq" "${SCRIPT_DIR}/go.mod" 2>/dev/null; then
        cd "${SCRIPT_DIR}"
        print_success "Running from FlyMQ repository"
        return 0
    fi

    # Not in repo - clone from GitHub
    print_step "Downloading FlyMQ source code"
    echo ""

    # Create temp directory for clone
    CLONED_REPO_DIR=$(mktemp -d "${TMPDIR:-/tmp}/flymq-install.XXXXXX")
    print_info "Cloning to: ${CYAN}${CLONED_REPO_DIR}${RESET}"

    # Clone main branch
    echo -n "  "
    if ! git clone --depth 1 https://github.com/firefly-oss/flymq.git "${CLONED_REPO_DIR}" 2>&1; then
        print_error "Failed to clone FlyMQ repository"
        rm -rf "${CLONED_REPO_DIR}"
        CLONED_REPO_DIR=""
        exit 1
    fi
    print_success "Cloned FlyMQ repository"

    # Verify the clone was successful
    if [[ ! -f "${CLONED_REPO_DIR}/go.mod" ]]; then
        print_error "Clone verification failed: go.mod not found"
        rm -rf "${CLONED_REPO_DIR}"
        CLONED_REPO_DIR=""
        exit 1
    fi

    # Change to cloned directory
    cd "${CLONED_REPO_DIR}"
}

build_binaries() {
    print_step "Building FlyMQ"
    echo ""
    print_info "Target: ${CYAN}${OS}/${ARCH}${RESET}"
    echo ""

    # Create bin directory
    mkdir -p bin

    # Set binary extensions for Windows
    local server_bin="bin/flymq"
    local cli_bin="bin/flymq-cli"
    if [[ "$OS" == "windows" ]]; then
        server_bin="bin/flymq.exe"
        cli_bin="bin/flymq-cli.exe"
    fi

    # Build server
    echo -n "  "
    if ! GOOS="${OS}" GOARCH="${ARCH}" go build -o "$server_bin" ./cmd/flymq 2>&1; then
        print_error "Failed to build flymq server"
        exit 1
    fi
    print_success "Built flymq server"

    # Build CLI
    echo -n "  "
    if ! GOOS="${OS}" GOARCH="${ARCH}" go build -o "$cli_bin" ./cmd/flymq-cli 2>&1; then
        print_error "Failed to build flymq-cli"
        exit 1
    fi
    print_success "Built flymq-cli"

    # Build discovery tool
    local discover_bin="bin/flymq-discover"
    if [[ "$OS" == "windows" ]]; then
        discover_bin="bin/flymq-discover.exe"
    fi
    echo -n "  "
    if ! GOOS="${OS}" GOARCH="${ARCH}" go build -o "$discover_bin" ./cmd/flymq-discover 2>&1; then
        print_warning "Failed to build flymq-discover (optional)"
    else
        print_success "Built flymq-discover"
    fi
}

install_binaries() {
    local prefix="$1"
    local bin_dir="$prefix/bin"

    print_step "Installing binaries"
    echo ""
    print_info "Install location: ${CYAN}$bin_dir${RESET}"
    echo ""

    mkdir -p "$bin_dir"

    # Set binary names based on OS
    local server_src="bin/flymq"
    local cli_src="bin/flymq-cli"
    local server_dst="$bin_dir/flymq"
    local cli_dst="$bin_dir/flymq-cli"
    
    if [[ "$OS" == "windows" ]]; then
        server_src="bin/flymq.exe"
        cli_src="bin/flymq-cli.exe"
        server_dst="$bin_dir/flymq.exe"
        cli_dst="$bin_dir/flymq-cli.exe"
    fi

    cp "$server_src" "$server_dst"
    [[ "$OS" != "windows" ]] && chmod +x "$server_dst"
    print_success "Installed flymq"

    cp "$cli_src" "$cli_dst"
    [[ "$OS" != "windows" ]] && chmod +x "$cli_dst"
    print_success "Installed flymq-cli"

    # Install discovery tool if it was built
    local discover_src="bin/flymq-discover"
    local discover_dst="$bin_dir/flymq-discover"
    if [[ "$OS" == "windows" ]]; then
        discover_src="bin/flymq-discover.exe"
        discover_dst="$bin_dir/flymq-discover.exe"
    fi
    if [[ -f "$discover_src" ]]; then
        cp "$discover_src" "$discover_dst"
        [[ "$OS" != "windows" ]] && chmod +x "$discover_dst"
        print_success "Installed flymq-discover"
    fi

    # On Windows, add a note about PATH
    if [[ "$OS" == "windows" ]]; then
        echo ""
        print_warning "Windows detected: You may need to add the bin directory to your PATH manually"
        print_info "Add this to your environment variables: ${CYAN}$bin_dir${RESET}"
    fi
}

create_data_dir() {
    print_step "Creating data directory"
    echo ""
    mkdir -p "$CFG_DATA_DIR"
    print_success "Created: ${CYAN}$CFG_DATA_DIR${RESET}"
}

install_system_service() {
    local config_dir="$1"
    local prefix="$2"

    # Linux systemd
    if [[ "$OS" == "linux" ]] && [[ "$INSTALL_SYSTEMD" == "true" ]] && command -v systemctl &> /dev/null; then
        print_step "Installing systemd service"
        echo ""

        local service_file
        local service_name

        if [[ "$CFG_DEPLOYMENT_MODE" == "cluster" ]]; then
            service_file="deploy/systemd/flymq-cluster@.service"
            service_name="flymq-cluster@${CFG_NODE_ID}"
        else
            service_file="deploy/systemd/flymq.service"
            service_name="flymq"
        fi

        if [[ ! -f "$service_file" ]]; then
            print_warning "Systemd service file not found: $service_file"
            return 1
        fi

        # Copy service file
        if [[ "$CFG_DEPLOYMENT_MODE" == "cluster" ]]; then
            sudo cp "$service_file" /etc/systemd/system/flymq-cluster@.service
        else
            sudo cp "$service_file" /etc/systemd/system/flymq.service
        fi

        # Reload systemd
        sudo systemctl daemon-reload

        print_success "Installed systemd service: ${CYAN}$service_name${RESET}"
        echo ""
        print_info "Enable: ${CYAN}sudo systemctl enable $service_name${RESET}"
        print_info "Start:  ${CYAN}sudo systemctl start $service_name${RESET}"
    
    # macOS launchd
    elif [[ "$OS" == "darwin" ]] && [[ "$INSTALL_LAUNCHD" == "true" ]]; then
        print_step "Installing Launch Agent"
        echo ""
        
        local launch_agents_dir="$HOME/Library/LaunchAgents"
        local plist_file="$launch_agents_dir/com.firefly.flymq.plist"
        
        mkdir -p "$launch_agents_dir"
        
        # Generate plist file
        cat > "$plist_file" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.firefly.flymq</string>
    <key>ProgramArguments</key>
    <array>
        <string>$prefix/bin/flymq</string>
        <string>--config</string>
        <string>$config_dir/flymq.json</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$HOME/Library/Logs/flymq.log</string>
    <key>StandardErrorPath</key>
    <string>$HOME/Library/Logs/flymq.error.log</string>
</dict>
</plist>
EOF
        
        print_success "Installed Launch Agent: ${CYAN}$plist_file${RESET}"
        echo ""
        print_info "Load:   ${CYAN}launchctl load $plist_file${RESET}"
        print_info "Unload: ${CYAN}launchctl unload $plist_file${RESET}"
    fi
}

print_post_install() {
    local prefix="$1"
    local config_dir="$2"
    local bin_dir="$prefix/bin"

    echo ""
    echo ""
    echo -e "  ${GREEN}${BOLD}✓ INSTALLATION COMPLETE${RESET}"
    echo ""
    echo -e "  ${BOLD}FlyMQ v${FLYMQ_VERSION}${RESET} is ready to use!"
    echo ""

    # Installation paths - compact
    echo -e "  ${DIM}Binaries${RESET}       ${CYAN}$bin_dir${RESET}"
    echo -e "  ${DIM}Config${RESET}         ${CYAN}$config_dir/flymq.json${RESET}"
    echo -e "  ${DIM}Data${RESET}           ${CYAN}$CFG_DATA_DIR${RESET}"
    echo ""

    # Features - only show enabled ones in a compact format
    local enabled_features=""
    [[ "$CFG_TLS_ENABLED" == "true" ]] && enabled_features+="TLS "
    [[ "$CFG_ENCRYPTION_ENABLED" == "true" ]] && enabled_features+="Encryption "
    [[ "$CFG_AUTH_ENABLED" == "true" ]] && enabled_features+="Auth "
    [[ "$CFG_METRICS_ENABLED" == "true" ]] && enabled_features+="Metrics "
    [[ "$CFG_HEALTH_ENABLED" == "true" ]] && enabled_features+="Health "
    [[ "$CFG_ADMIN_ENABLED" == "true" ]] && enabled_features+="Admin "
    [[ "$CFG_SCHEMA_ENABLED" == "true" ]] && enabled_features+="Schema "
    [[ "$CFG_DLQ_ENABLED" == "true" ]] && enabled_features+="DLQ "
    [[ "$CFG_DELAYED_ENABLED" == "true" ]] && enabled_features+="Delayed "
    [[ "$CFG_TXN_ENABLED" == "true" ]] && enabled_features+="Transactions "
    [[ "$CFG_AUDIT_ENABLED" == "true" ]] && enabled_features+="Audit "

    if [[ -n "$enabled_features" ]]; then
        echo -e "  ${DIM}Features${RESET}       ${GREEN}${enabled_features}${RESET}"
        echo ""
    fi

    # Quick Start
    echo -e "  ${CYAN}${BOLD}QUICK START${RESET}"
    echo ""

    # Show start command with encryption key sourcing if needed
    local start_prefix=""
    if [[ "$CFG_ENCRYPTION_ENABLED" == "true" ]]; then
        start_prefix="source $config_dir/flymq.secrets && "
    fi

    if [[ "$CFG_DEPLOYMENT_MODE" == "cluster" ]]; then
        if [[ -z "$CFG_CLUSTER_PEERS" ]]; then
            echo -e "  ${DIM}# Start bootstrap node (JSON logs by default)${RESET}"
            echo -e "  ${start_prefix}flymq --config $config_dir/flymq.json"
            echo ""
            echo -e "  ${DIM}# Other nodes join with:${RESET} ${CYAN}${CFG_ADVERTISE_CLUSTER}${RESET}"
        else
            echo -e "  ${DIM}# Start this node (will auto-join cluster)${RESET}"
            echo -e "  ${start_prefix}flymq --config $config_dir/flymq.json"
        fi
    else
        echo -e "  ${DIM}# Start server (JSON logs by default)${RESET}"
        echo -e "  ${start_prefix}flymq --config $config_dir/flymq.json"
    fi
    echo ""
    echo -e "  ${DIM}# Start with human-readable logs (for development)${RESET}"
    echo -e "  ${start_prefix}flymq --config $config_dir/flymq.json -human-readable"
    echo ""
    echo -e "  ${DIM}# Start in quiet mode (logs only, no banner)${RESET}"
    echo -e "  ${start_prefix}flymq --config $config_dir/flymq.json -quiet"
    echo ""
    echo -e "  ${DIM}# Send a message${RESET}"
    echo -e "  flymq-cli produce my-topic \"Hello World\""
    echo ""
    echo -e "  ${DIM}# Subscribe to messages${RESET}"
    echo -e "  flymq-cli subscribe my-topic"
    echo ""

    # Endpoints - compact
    local has_endpoints=false
    if [[ "$CFG_METRICS_ENABLED" == "true" ]] || [[ "$CFG_HEALTH_ENABLED" == "true" ]] || [[ "$CFG_ADMIN_ENABLED" == "true" ]]; then
        has_endpoints=true
        echo -e "  ${CYAN}${BOLD}ENDPOINTS${RESET}"
        echo ""
        echo -e "  ${DIM}Client${RESET}         localhost${CFG_BIND_ADDR}"
        [[ "$CFG_METRICS_ENABLED" == "true" ]] && echo -e "  ${DIM}Metrics${RESET}        http://localhost${CFG_METRICS_ADDR}/metrics"
        [[ "$CFG_HEALTH_ENABLED" == "true" ]] && echo -e "  ${DIM}Health${RESET}         http://localhost${CFG_HEALTH_ADDR}/health"
        [[ "$CFG_ADMIN_ENABLED" == "true" ]] && echo -e "  ${DIM}Admin API${RESET}      http://localhost${CFG_ADMIN_ADDR}/api/v1"
        echo ""
    fi

    # Credentials - important, highlighted
    if [[ "$CFG_AUTH_ENABLED" == "true" ]]; then
        echo -e "  ${YELLOW}${BOLD}⚠ SAVE THESE CREDENTIALS${RESET}"
        echo ""
        echo -e "  ${DIM}Username${RESET}       ${CYAN}${CFG_AUTH_ADMIN_USER}${RESET}"
        echo -e "  ${DIM}Password${RESET}       ${CYAN}${CFG_AUTH_ADMIN_PASS}${RESET}"
        echo ""
        echo -e "  ${DIM}# Use with CLI${RESET}"
        echo -e "  flymq-cli -u ${CFG_AUTH_ADMIN_USER} -P '***' produce my-topic \"Hello\""
        echo ""
    fi

    if [[ "$CFG_ENCRYPTION_ENABLED" == "true" ]]; then
        echo -e "  ${YELLOW}${BOLD}⚠ ENCRYPTION KEY - KEEP SECURE${RESET}"
        echo ""
        echo -e "  ${DIM}Secrets file:${RESET}  ${CYAN}$config_dir/flymq.secrets${RESET} ${DIM}(mode 600)${RESET}"
        echo -e "  ${DIM}Key:${RESET}           ${DIM}${CFG_ENCRYPTION_KEY}${RESET}"
        echo ""
        echo -e "  ${DIM}The encryption key is stored in a separate secrets file for security.${RESET}"
        echo -e "  ${DIM}Source it before starting: ${CYAN}source $config_dir/flymq.secrets${RESET}"
        echo ""
        if [[ "$CFG_DEPLOYMENT_MODE" == "cluster" ]]; then
            echo -e "  ${YELLOW}${BOLD}CLUSTER REQUIREMENT:${RESET}"
            echo -e "  ${DIM}All nodes in the cluster MUST use the SAME encryption key.${RESET}"
            echo -e "  ${DIM}Copy ${CYAN}$config_dir/flymq.secrets${RESET}${DIM} to all cluster nodes.${RESET}"
            echo ""
        fi
        echo -e "  ${RED}${BOLD}WARNING:${RESET} ${DIM}Back up this key securely. Data cannot be recovered without it.${RESET}"
        echo ""
    fi

    # PATH warning
    if [[ ":$PATH:" != *":$bin_dir:"* ]]; then
        echo -e "  ${DIM}Add to PATH:${RESET} export PATH=\"$bin_dir:\$PATH\""
        echo ""
    fi

    echo -e "  ${DIM}Docs: https://github.com/firefly-oss/flymq${RESET}"
    echo ""
}

# =============================================================================
# Uninstallation
# =============================================================================

uninstall() {
    print_step "Uninstalling FlyMQ..."

    local prefix="${PREFIX:-$(get_default_prefix)}"
    local bin_dir="$prefix/bin"
    local config_dir="$(get_default_config_dir)"

    if [[ -f "$bin_dir/flymq" ]]; then
        rm -f "$bin_dir/flymq"
        print_success "Removed flymq"
    fi

    if [[ -f "$bin_dir/flymq-cli" ]]; then
        rm -f "$bin_dir/flymq-cli"
        print_success "Removed flymq-cli"
    fi

    if [[ -f "$bin_dir/flymq-discover" ]]; then
        rm -f "$bin_dir/flymq-discover"
        print_success "Removed flymq-discover"
    fi

    # Also check for Windows executables
    if [[ -f "$bin_dir/flymq.exe" ]]; then
        rm -f "$bin_dir/flymq.exe"
        print_success "Removed flymq.exe"
    fi

    if [[ -f "$bin_dir/flymq-cli.exe" ]]; then
        rm -f "$bin_dir/flymq-cli.exe"
        print_success "Removed flymq-cli.exe"
    fi

    if [[ -f "$bin_dir/flymq-discover.exe" ]]; then
        rm -f "$bin_dir/flymq-discover.exe"
        print_success "Removed flymq-discover.exe"
    fi

    print_success "FlyMQ uninstalled"
    print_info "Configuration and data directories were not removed."
    print_info "Remove manually if needed:"
    echo -e "  ${DIM}rm -rf $config_dir${RESET}"
    echo -e "  ${DIM}rm -rf $(get_default_data_dir)${RESET}"
}

# =============================================================================
# Signal Handling
# =============================================================================

cleanup() {
    echo ""
    print_warning "Installation cancelled by user"
    cleanup_cloned_repo
    exit 130
}

trap cleanup SIGINT SIGTERM
trap cleanup_cloned_repo EXIT

# =============================================================================
# Main
# =============================================================================

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --prefix)
                PREFIX="$2"
                shift 2
                ;;
            --yes|-y)
                AUTO_CONFIRM=true
                shift
                ;;
            --config-file)
                CONFIG_FILE="$2"
                AUTO_CONFIRM=true
                shift 2
                ;;
            --uninstall)
                UNINSTALL=true
                shift
                ;;
            --help|-h)
                print_banner
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --prefix PATH        Installation prefix (default: ~/.local or /usr/local)"
                echo "  --yes, -y            Non-interactive mode with default configuration"
                echo "  --config-file FILE   Use existing configuration file (implies --yes)"
                echo "  --uninstall          Uninstall FlyMQ"
                echo "  --help, -h           Show this help"
                echo ""
                echo "Examples:"
                echo "  ./install.sh                           # Interactive installation"
                echo "  ./install.sh --yes                     # Quick install with defaults"
                echo "  ./install.sh --config-file my.conf     # Use existing config"
                echo "  ./install.sh --prefix /opt/flymq       # Custom install location"
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                exit 1
                ;;
        esac
    done
}

# =============================================================================
# New Configuration Flow - Show Defaults First, Customize by Section
# =============================================================================

show_default_configuration() {
    local config_dir="$1"

    echo -e "  ${GREEN}${BOLD}DEFAULT CONFIGURATION${RESET}"
    echo -e "  ${DIM}FlyMQ comes with production-ready defaults. Review below:${RESET}"
    echo ""

    # Section 1: Deployment
    echo -e "  ${WHITE}${BOLD}[${CYAN}1${WHITE}]${RESET} ${BOLD}DEPLOYMENT${RESET}"
    echo -e "      Mode:              ${GREEN}Standalone${RESET} ${DIM}(single node)${RESET}"
    echo -e "      Bind Address:      ${CYAN}${CFG_BIND_ADDR}${RESET}"
    echo -e "      Data Directory:    ${CYAN}${CFG_DATA_DIR}${RESET}"
    echo -e "      Node ID:           ${CYAN}${CFG_NODE_ID}${RESET}"
    echo ""

    # Section 2: Security
    echo -e "  ${WHITE}${BOLD}[${CYAN}2${WHITE}]${RESET} ${BOLD}SECURITY${RESET}"
    echo -e "      TLS/SSL:           ${YELLOW}Disabled${RESET} ${DIM}(use reverse proxy for TLS)${RESET}"
    echo -e "      Data Encryption:   ${GREEN}Enabled${RESET} ${DIM}(AES-256-GCM, auto-generated key)${RESET}"
    echo -e "      Authentication:    ${GREEN}Enabled${RESET} ${DIM}(auto-generated credentials)${RESET}"
    echo -e "      Admin User:        ${CYAN}${CFG_AUTH_ADMIN_USER}${RESET}"
    echo ""

    # Section 3: Advanced Features
    echo -e "  ${WHITE}${BOLD}[${CYAN}3${WHITE}]${RESET} ${BOLD}ADVANCED FEATURES${RESET}"
    echo -e "      Schema Validation: ${GREEN}Enabled${RESET} ${DIM}(strict mode)${RESET}"
    echo -e "      Dead Letter Queue: ${GREEN}Enabled${RESET} ${DIM}(3 retries)${RESET}"
    echo -e "      Message TTL:       ${GREEN}Enabled${RESET} ${DIM}(7 days default)${RESET}"
    echo -e "      Delayed Delivery:  ${GREEN}Enabled${RESET} ${DIM}(up to 7 days)${RESET}"
    echo -e "      Transactions:      ${GREEN}Enabled${RESET} ${DIM}(60s timeout)${RESET}"
    echo ""

    # Section 4: Observability
    echo -e "  ${WHITE}${BOLD}[${CYAN}4${WHITE}]${RESET} ${BOLD}OBSERVABILITY${RESET}"
    echo -e "      Prometheus Metrics: ${GREEN}Enabled${RESET} ${DIM}(${CFG_METRICS_ADDR})${RESET}"
    echo -e "      OpenTelemetry:      ${GREEN}Enabled${RESET} ${DIM}(localhost:4317)${RESET}"
    echo -e "      Health Checks:      ${GREEN}Enabled${RESET} ${DIM}(${CFG_HEALTH_ADDR})${RESET}"
    echo -e "      Admin API:          ${GREEN}Enabled${RESET} ${DIM}(${CFG_ADMIN_ADDR})${RESET}"
    echo -e "      Admin API TLS:      ${YELLOW}Disabled${RESET} ${DIM}(HTTP only)${RESET}"
    echo ""

    # Section 5: Performance
    echo -e "  ${WHITE}${BOLD}[${CYAN}5${WHITE}]${RESET} ${BOLD}PERFORMANCE${RESET}"
    echo -e "      Acks Mode:         ${CYAN}leader${RESET} ${DIM}(balanced durability/latency)${RESET}"
    echo -e "      Segment Size:      ${CYAN}64 MB${RESET}"
    echo -e "      Log Level:         ${CYAN}info${RESET}"
    echo ""
}

configure_by_sections() {
    echo ""
    echo -e "  ${BOLD}Select sections to customize:${RESET}"
    echo -e "    ${CYAN}1${RESET} Deployment    ${CYAN}2${RESET} Security    ${CYAN}3${RESET} Advanced Features"
    echo -e "    ${CYAN}4${RESET} Observability ${CYAN}5${RESET} Performance"
    echo ""
    echo -e "  ${DIM}Enter numbers separated by commas, 'all', or 'none' to skip${RESET}"
    echo ""

    local sections
    sections=$(prompt_value "Sections" "none")

    if [[ "$sections" == "none" ]] || [[ -z "$sections" ]]; then
        return
    fi

    # Parse sections
    if [[ "$sections" == "all" ]]; then
        sections="1,2,3,4,5"
    fi

    IFS=',' read -ra section_array <<< "$sections"

    for section in "${section_array[@]}"; do
        section=$(echo "$section" | xargs)  # trim whitespace
        case "$section" in
            1) configure_section_deployment ;;
            2) configure_section_security ;;
            3) configure_section_advanced ;;
            4) configure_section_observability ;;
            5) configure_section_performance ;;
            *) print_warning "Unknown section: $section" ;;
        esac
    done
}

configure_section_deployment() {
    print_section "Deployment Configuration"

    # Allow changing deployment mode
    echo -e "  ${BOLD}Current Mode:${RESET} ${CYAN}${CFG_DEPLOYMENT_MODE}${RESET}"
    echo ""

    if prompt_yes_no "Change deployment mode" "n"; then
        echo ""
        echo -e "  ${BOLD}Deployment Mode${RESET}"
        echo -e "    ${CYAN}1${RESET}) Standalone - Single node ${DIM}(development/testing/small production)${RESET}"
        echo -e "    ${CYAN}2${RESET}) Cluster    - Multi-node with Raft consensus ${DIM}(high availability)${RESET}"
        echo ""

        local mode_choice
        mode_choice=$(prompt_choice "Deployment mode (1=standalone, 2=cluster)" "1" "1" "2" "standalone" "cluster")

        if [[ "$mode_choice" == "2" ]] || [[ "$mode_choice" == "cluster" ]]; then
            CFG_DEPLOYMENT_MODE="cluster"
            configure_cluster_deployment
        else
            CFG_DEPLOYMENT_MODE="standalone"
            configure_standalone_deployment
        fi
    fi

    # Common settings for both modes
    echo ""
    echo -e "  ${BOLD}Network Settings${RESET}"
    CFG_BIND_ADDR=$(prompt_value "Client bind address" "${CFG_BIND_ADDR}")
    CFG_NODE_ID=$(prompt_value "Node ID" "${CFG_NODE_ID}")

    # Cluster-specific settings
    if [[ "$CFG_DEPLOYMENT_MODE" == "cluster" ]]; then
        echo ""
        echo -e "  ${BOLD}Cluster Settings${RESET}"
        CFG_CLUSTER_ADDR=$(prompt_value "Cluster bind address" "${CFG_CLUSTER_ADDR}")

        echo ""
        if prompt_yes_no "Modify peer nodes" "n"; then
            discover_cluster_nodes
            if [[ -n "$CFG_CLUSTER_PEERS" ]]; then
                validate_peer_connectivity
            fi
        fi

        CFG_REPLICATION_FACTOR=$(prompt_value "Replication factor" "${CFG_REPLICATION_FACTOR}")
    fi

    echo ""
    echo -e "  ${BOLD}Storage Settings${RESET}"
    CFG_DATA_DIR=$(prompt_value "Data directory" "${CFG_DATA_DIR}")
}

configure_section_security() {
    print_section "Security Configuration"

    echo -e "  ${BOLD}TLS/SSL Encryption (Client Connections)${RESET}"
    echo -e "  ${DIM}Enable TLS for encrypted client-server communication.${RESET}"
    echo -e "  ${DIM}Note: For production, consider using a reverse proxy (nginx, traefik) for TLS.${RESET}"
    echo ""
    if prompt_yes_no "Enable TLS encryption" "n"; then
        CFG_TLS_ENABLED="true"
        CFG_TLS_CERT_FILE=$(prompt_value "TLS certificate file" "")
        CFG_TLS_KEY_FILE=$(prompt_value "TLS key file" "")
    else
        CFG_TLS_ENABLED="false"
    fi
    echo ""

    echo -e "  ${BOLD}Data-at-Rest Encryption${RESET}"
    echo -e "  ${DIM}Encrypt all data stored on disk using AES-256-GCM.${RESET}"
    echo ""
    if prompt_yes_no "Enable data-at-rest encryption" "y"; then
        CFG_ENCRYPTION_ENABLED="true"
        local custom_key
        custom_key=$(prompt_value "Encryption key (Enter for auto-generate)" "")
        if [[ -n "$custom_key" ]]; then
            CFG_ENCRYPTION_KEY="$custom_key"
        fi
    else
        CFG_ENCRYPTION_ENABLED="false"
    fi
    echo ""

    echo -e "  ${BOLD}Authentication${RESET}"
    echo -e "  ${DIM}Enable username/password authentication with RBAC.${RESET}"
    echo ""
    if prompt_yes_no "Enable authentication" "y"; then
        CFG_AUTH_ENABLED="true"
        CFG_AUTH_ADMIN_USER=$(prompt_value "Admin username" "${CFG_AUTH_ADMIN_USER}")
        local custom_pass
        echo -e "  ${DIM}Leave empty to use auto-generated password${RESET}"
        read -sp "  Admin password: " custom_pass
        echo ""
        if [[ -n "$custom_pass" ]]; then
            CFG_AUTH_ADMIN_PASS="$custom_pass"
        fi
        if prompt_yes_no "Allow anonymous read-only connections" "n"; then
            CFG_AUTH_ALLOW_ANONYMOUS="true"
        else
            CFG_AUTH_ALLOW_ANONYMOUS="false"
        fi
    else
        CFG_AUTH_ENABLED="false"
    fi
    echo ""

    echo -e "  ${BOLD}Audit Trail${RESET}"
    echo -e "  ${DIM}Log all security-relevant operations for compliance and forensics.${RESET}"
    echo -e "  ${DIM}Tracks authentication, authorization, and administrative actions.${RESET}"
    echo ""
    if prompt_yes_no "Enable audit trail" "y"; then
        CFG_AUDIT_ENABLED="true"
        local custom_retention
        custom_retention=$(prompt_value "Audit log retention (days)" "${CFG_AUDIT_RETENTION_DAYS}")
        if [[ -n "$custom_retention" ]]; then
            CFG_AUDIT_RETENTION_DAYS="$custom_retention"
        fi
    else
        CFG_AUDIT_ENABLED="false"
    fi
}

configure_section_advanced() {
    print_section "Advanced Features"

    echo -e "  ${BOLD}Schema Validation${RESET}"
    echo -e "  ${DIM}Validate messages against JSON Schema, Avro, or Protobuf.${RESET}"
    if prompt_yes_no "Enable schema validation" "y"; then
        CFG_SCHEMA_ENABLED="true"
        CFG_SCHEMA_VALIDATION=$(prompt_choice "Validation mode (strict/lenient/none)" "strict" "strict" "lenient" "none")
    else
        CFG_SCHEMA_ENABLED="false"
    fi
    echo ""

    echo -e "  ${BOLD}Dead Letter Queue${RESET}"
    echo -e "  ${DIM}Route failed messages to a separate queue for analysis.${RESET}"
    if prompt_yes_no "Enable dead letter queues" "y"; then
        CFG_DLQ_ENABLED="true"
        CFG_DLQ_MAX_RETRIES=$(prompt_number "Max retries before DLQ" "${CFG_DLQ_MAX_RETRIES}" "1")
    else
        CFG_DLQ_ENABLED="false"
    fi
    echo ""

    echo -e "  ${BOLD}Message TTL${RESET}"
    echo -e "  ${DIM}Automatically expire messages after a specified time.${RESET}"
    CFG_TTL_DEFAULT=$(prompt_number "Default TTL in seconds (0=no expiry)" "${CFG_TTL_DEFAULT}" "0")
    echo ""

    echo -e "  ${BOLD}Delayed Delivery${RESET}"
    echo -e "  ${DIM}Schedule messages for future delivery.${RESET}"
    if prompt_yes_no "Enable delayed message delivery" "y"; then
        CFG_DELAYED_ENABLED="true"
    else
        CFG_DELAYED_ENABLED="false"
    fi
    echo ""

    echo -e "  ${BOLD}Transactions${RESET}"
    echo -e "  ${DIM}Enable exactly-once semantics with atomic operations.${RESET}"
    if prompt_yes_no "Enable transaction support" "y"; then
        CFG_TXN_ENABLED="true"
    else
        CFG_TXN_ENABLED="false"
    fi
}

configure_section_observability() {
    print_section "Observability"

    echo -e "  ${BOLD}Prometheus Metrics${RESET}"
    if prompt_yes_no "Enable Prometheus metrics" "y"; then
        CFG_METRICS_ENABLED="true"
        CFG_METRICS_ADDR=$(prompt_value "Metrics address" "${CFG_METRICS_ADDR}")
    else
        CFG_METRICS_ENABLED="false"
    fi
    echo ""

    echo -e "  ${BOLD}OpenTelemetry Tracing${RESET}"
    if prompt_yes_no "Enable OpenTelemetry tracing" "y"; then
        CFG_TRACING_ENABLED="true"
        CFG_TRACING_ENDPOINT=$(prompt_value "OTLP endpoint" "${CFG_TRACING_ENDPOINT}")
    else
        CFG_TRACING_ENABLED="false"
    fi
    echo ""

    echo -e "  ${BOLD}Health Check Endpoints${RESET}"
    if prompt_yes_no "Enable health checks" "y"; then
        CFG_HEALTH_ENABLED="true"
        CFG_HEALTH_ADDR=$(prompt_value "Health check address" "${CFG_HEALTH_ADDR}")
    else
        CFG_HEALTH_ENABLED="false"
    fi
    echo ""

    echo -e "  ${BOLD}Admin REST API${RESET}"
    if prompt_yes_no "Enable Admin API" "y"; then
        CFG_ADMIN_ENABLED="true"
        CFG_ADMIN_ADDR=$(prompt_value "Admin API address" "${CFG_ADMIN_ADDR}")
    else
        CFG_ADMIN_ENABLED="false"
    fi
}

configure_section_performance() {
    print_section "Performance"

    echo -e "  ${BOLD}Acknowledgment Mode${RESET}"
    echo -e "    ${DIM}none${RESET}   - Fire-and-forget (fastest, no durability)"
    echo -e "    ${DIM}leader${RESET} - Leader acknowledges (balanced) [DEFAULT]"
    echo -e "    ${DIM}all${RESET}    - All replicas acknowledge (safest)"
    echo ""
    CFG_ACKS=$(prompt_choice "Acks mode" "${CFG_ACKS}" "none" "leader" "all")
    echo ""

    echo -e "  ${BOLD}Storage Settings${RESET}"
    CFG_SEGMENT_BYTES=$(prompt_number "Segment size (bytes)" "${CFG_SEGMENT_BYTES}" "1024")
    CFG_RETENTION_BYTES=$(prompt_number "Retention limit (0=unlimited)" "${CFG_RETENTION_BYTES}" "0")
    echo ""

    echo -e "  ${BOLD}Logging${RESET}"
    CFG_LOG_LEVEL=$(prompt_choice "Log level" "${CFG_LOG_LEVEL}" "debug" "info" "warn" "error")
    echo ""

    echo -e "  ${BOLD}Platform Optimizations${RESET} ${DIM}(automatic)${RESET}"
    case "$OS" in
        linux)
            echo -e "    ${GREEN}✓${RESET} Zero-copy I/O: ${GREEN}sendfile + splice${RESET}"
            ;;
        darwin)
            echo -e "    ${GREEN}✓${RESET} Zero-copy I/O: ${GREEN}sendfile${RESET} ${DIM}(reads only)${RESET}"
            ;;
        *)
            echo -e "    ${YELLOW}○${RESET} Zero-copy I/O: ${YELLOW}Buffered fallback${RESET}"
            ;;
    esac
}

show_final_configuration() {
    local config_dir="$1"

    echo -e "  ${GREEN}${BOLD}FINAL CONFIGURATION${RESET}"
    echo ""

    echo -e "  ${BOLD}Installation Paths:${RESET}"
    echo -e "    Binaries:       ${CYAN}$PREFIX/bin${RESET}"
    echo -e "    Configuration:  ${CYAN}$config_dir/flymq.json${RESET}"
    echo -e "    Data:           ${CYAN}$CFG_DATA_DIR${RESET}"
    echo ""

    echo -e "  ${BOLD}Deployment:${RESET}"
    echo -e "    Mode:           ${CYAN}$CFG_DEPLOYMENT_MODE${RESET}"
    echo -e "    Bind Address:   ${CYAN}$CFG_BIND_ADDR${RESET}"
    if [[ "$CFG_DEPLOYMENT_MODE" == "cluster" ]]; then
        echo -e "    Cluster Addr:   ${CYAN}$CFG_ADVERTISE_CLUSTER${RESET}"
        [[ -n "$CFG_CLUSTER_PEERS" ]] && echo -e "    Peers:          ${CYAN}$CFG_CLUSTER_PEERS${RESET}"
    fi
    echo ""

    echo -e "  ${BOLD}Security:${RESET}"
    [[ "$CFG_TLS_ENABLED" == "true" ]] && echo -e "    TLS:            ${GREEN}Enabled${RESET}" || echo -e "    TLS:            ${YELLOW}Disabled${RESET}"
    [[ "$CFG_ENCRYPTION_ENABLED" == "true" ]] && echo -e "    Encryption:     ${GREEN}Enabled${RESET}" || echo -e "    Encryption:     ${YELLOW}Disabled${RESET}"
    [[ "$CFG_AUTH_ENABLED" == "true" ]] && echo -e "    Authentication: ${GREEN}Enabled${RESET} (user: ${CYAN}${CFG_AUTH_ADMIN_USER}${RESET})" || echo -e "    Authentication: ${YELLOW}Disabled${RESET}"
    echo ""

    echo -e "  ${BOLD}Features:${RESET}"
    local features=""
    [[ "$CFG_SCHEMA_ENABLED" == "true" ]] && features+="Schema "
    [[ "$CFG_DLQ_ENABLED" == "true" ]] && features+="DLQ "
    [[ "$CFG_DELAYED_ENABLED" == "true" ]] && features+="Delayed "
    [[ "$CFG_TXN_ENABLED" == "true" ]] && features+="Transactions "
    [[ -n "$features" ]] && echo -e "    Enabled:        ${GREEN}${features}${RESET}" || echo -e "    Enabled:        ${YELLOW}None${RESET}"
    echo ""

    echo -e "  ${BOLD}Observability:${RESET}"
    [[ "$CFG_METRICS_ENABLED" == "true" ]] && echo -e "    Metrics:        ${GREEN}${CFG_METRICS_ADDR}${RESET}"
    [[ "$CFG_HEALTH_ENABLED" == "true" ]] && echo -e "    Health:         ${GREEN}${CFG_HEALTH_ADDR}${RESET}"
    if [[ "$CFG_ADMIN_ENABLED" == "true" ]]; then
        if [[ "$CFG_ADMIN_TLS_ENABLED" == "true" ]]; then
            echo -e "    Admin API:      ${GREEN}${CFG_ADMIN_ADDR}${RESET} ${DIM}(HTTPS)${RESET}"
        else
            echo -e "    Admin API:      ${GREEN}${CFG_ADMIN_ADDR}${RESET} ${YELLOW}(HTTP)${RESET}"
        fi
    fi

    # Show credentials if auth is enabled
    if [[ "$CFG_AUTH_ENABLED" == "true" ]]; then
        echo ""
        echo -e "  ${YELLOW}${BOLD}⚠ IMPORTANT: Save these credentials securely!${RESET}"
        echo -e "    Admin Username: ${CYAN}${CFG_AUTH_ADMIN_USER}${RESET}"
        echo -e "    Admin Password: ${CYAN}${CFG_AUTH_ADMIN_PASS}${RESET}"
    fi

    if [[ "$CFG_ENCRYPTION_ENABLED" == "true" ]]; then
        echo ""
        echo -e "  ${YELLOW}${BOLD}  Encryption Key (save securely):${RESET}"
        echo -e "    ${DIM}${CFG_ENCRYPTION_KEY}${RESET}"
    fi
}

# =============================================================================
# Deployment Mode Selection (First Step)
# =============================================================================

select_deployment_mode() {
    print_section "Deployment Mode"
    echo -e "  ${DIM}Choose how you want to deploy FlyMQ:${RESET}"
    echo ""
    echo -e "  ${CYAN}${BOLD}[1]${RESET} ${BOLD}Standalone${RESET}"
    echo -e "      ${DIM}Single node deployment for development, testing, or small workloads.${RESET}"
    echo -e "      ${DIM}Simple setup, no clustering overhead.${RESET}"
    echo ""
    echo -e "  ${CYAN}${BOLD}[2]${RESET} ${BOLD}Cluster${RESET}"
    echo -e "      ${DIM}Multi-node deployment with Raft consensus for high availability.${RESET}"
    echo -e "      ${DIM}Automatic failover, data replication, and horizontal scaling.${RESET}"
    echo ""

    local mode_choice
    mode_choice=$(prompt_choice "Select deployment mode" "1" "1" "2" "standalone" "cluster")

    if [[ "$mode_choice" == "2" ]] || [[ "$mode_choice" == "cluster" ]]; then
        CFG_DEPLOYMENT_MODE="cluster"
        configure_cluster_deployment
    else
        CFG_DEPLOYMENT_MODE="standalone"
        configure_standalone_deployment
    fi
}

configure_standalone_deployment() {
    echo ""
    print_success "Standalone mode selected"
    echo ""

    # Set standalone-specific defaults
    CFG_CLUSTER_ENABLED="false"
    CFG_CLUSTER_PEERS=""
    CFG_REPLICATION_FACTOR="1"
}

configure_cluster_deployment() {
    echo ""
    print_success "Cluster mode selected"
    echo ""

    # Enable clustering
    CFG_CLUSTER_ENABLED="true"

    # Determine if this is the first node or joining existing cluster
    echo -e "  ${BOLD}Is this the first node in a new cluster, or joining an existing cluster?${RESET}"
    echo ""
    echo -e "  ${CYAN}${BOLD}[1]${RESET} ${BOLD}Bootstrap new cluster${RESET}"
    echo -e "      ${DIM}This is the first node. Other nodes will join this one.${RESET}"
    echo ""
    echo -e "  ${CYAN}${BOLD}[2]${RESET} ${BOLD}Join existing cluster${RESET}"
    echo -e "      ${DIM}Connect to an existing FlyMQ cluster.${RESET}"
    echo ""

    local cluster_choice
    cluster_choice=$(prompt_choice "Cluster role" "1" "1" "2" "bootstrap" "join")

    if [[ "$cluster_choice" == "2" ]] || [[ "$cluster_choice" == "join" ]]; then
        # Joining existing cluster - try to discover nodes
        discover_cluster_nodes

        # Validate connectivity if peers were specified
        if [[ -n "$CFG_CLUSTER_PEERS" ]]; then
            validate_peer_connectivity
        fi
    else
        # Bootstrap mode - this is the first node
        CFG_CLUSTER_PEERS=""
        echo ""
        print_info "Bootstrap mode: This node will start a new cluster."
        echo -e "  ${DIM}Other nodes can join by specifying this node's address as a peer.${RESET}"
    fi

    # Configure cluster-specific settings
    echo ""
    echo -e "  ${BOLD}Cluster Network Settings${RESET}"

    local local_ip=$(get_local_ip)
    CFG_CLUSTER_ADDR=$(prompt_value "Cluster bind address" "${local_ip}:9093")
    CFG_NODE_ID=$(prompt_value "Node ID (unique in cluster)" "${CFG_NODE_ID}")

    # Replication factor
    echo ""
    echo -e "  ${BOLD}Replication Settings${RESET}"
    echo -e "  ${DIM}Replication factor determines how many copies of data are stored.${RESET}"
    echo -e "  ${DIM}Recommended: 3 for production (tolerates 1 node failure).${RESET}"
    echo ""

    if [[ -z "$CFG_CLUSTER_PEERS" ]]; then
        # Bootstrap mode - start with 1, can increase later
        CFG_REPLICATION_FACTOR=$(prompt_value "Replication factor" "1")
        echo -e "  ${DIM}Note: Increase replication factor after more nodes join.${RESET}"
    else
        CFG_REPLICATION_FACTOR=$(prompt_value "Replication factor" "3")
    fi

    # Enable service discovery for cluster mode
    echo ""
    echo -e "  ${BOLD}Service Discovery${RESET}"
    echo -e "  ${DIM}mDNS service discovery allows nodes to automatically find each other.${RESET}"
    echo ""

    if prompt_yes_no "Enable service discovery (mDNS/Bonjour)" "y"; then
        CFG_DISCOVERY_ENABLED="true"
        CFG_DISCOVERY_CLUSTER_ID=$(prompt_value "Cluster ID (for filtering)" "${CFG_DISCOVERY_CLUSTER_ID:-flymq-cluster}")
    else
        CFG_DISCOVERY_ENABLED="false"
    fi
}

# =============================================================================
# Iterative Configuration Loop
# =============================================================================

show_configuration_summary() {
    local config_dir="$1"

    clear_screen_if_interactive

    echo ""
    echo -e "  ${GREEN}${BOLD}CONFIGURATION SUMMARY${RESET}"
    echo -e "  ${DIM}Review your settings. Select a section number to modify, or confirm to proceed.${RESET}"
    echo ""

    # Section 1: Deployment
    echo -e "  ${WHITE}${BOLD}[${CYAN}1${WHITE}]${RESET} ${BOLD}DEPLOYMENT${RESET}"
    if [[ "$CFG_DEPLOYMENT_MODE" == "cluster" ]]; then
        echo -e "      Mode:              ${GREEN}Cluster${RESET} ${DIM}(high availability)${RESET}"
        echo -e "      Cluster Address:   ${CYAN}${CFG_CLUSTER_ADDR}${RESET}"
        if [[ -n "$CFG_CLUSTER_PEERS" ]]; then
            echo -e "      Peers:             ${CYAN}${CFG_CLUSTER_PEERS}${RESET}"
        else
            echo -e "      Peers:             ${YELLOW}Bootstrap mode (first node)${RESET}"
        fi
        echo -e "      Replication:       ${CYAN}${CFG_REPLICATION_FACTOR}x${RESET}"
        if [[ "$CFG_DISCOVERY_ENABLED" == "true" ]]; then
            echo -e "      Service Discovery: ${GREEN}Enabled${RESET} ${DIM}(mDNS)${RESET}"
            [[ -n "$CFG_DISCOVERY_CLUSTER_ID" ]] && echo -e "      Cluster ID:        ${CYAN}${CFG_DISCOVERY_CLUSTER_ID}${RESET}"
        else
            echo -e "      Service Discovery: ${YELLOW}Disabled${RESET}"
        fi
    else
        echo -e "      Mode:              ${GREEN}Standalone${RESET} ${DIM}(single node)${RESET}"
    fi
    echo -e "      Bind Address:      ${CYAN}${CFG_BIND_ADDR}${RESET}"
    echo -e "      Data Directory:    ${CYAN}${CFG_DATA_DIR}${RESET}"
    echo -e "      Node ID:           ${CYAN}${CFG_NODE_ID}${RESET}"
    echo ""

    # Section 2: Security
    echo -e "  ${WHITE}${BOLD}[${CYAN}2${WHITE}]${RESET} ${BOLD}SECURITY${RESET}"
    if [[ "$CFG_TLS_ENABLED" == "true" ]]; then
        echo -e "      TLS/SSL:           ${GREEN}Enabled${RESET}"
        [[ -n "$CFG_TLS_CERT_FILE" ]] && echo -e "      TLS Cert:          ${CYAN}${CFG_TLS_CERT_FILE}${RESET}"
    else
        echo -e "      TLS/SSL:           ${YELLOW}Disabled${RESET} ${DIM}(use reverse proxy for TLS)${RESET}"
    fi
    if [[ "$CFG_ENCRYPTION_ENABLED" == "true" ]]; then
        echo -e "      Data Encryption:   ${GREEN}Enabled${RESET} ${DIM}(AES-256-GCM)${RESET}"
    else
        echo -e "      Data Encryption:   ${YELLOW}Disabled${RESET}"
    fi
    if [[ "$CFG_AUTH_ENABLED" == "true" ]]; then
        echo -e "      Authentication:    ${GREEN}Enabled${RESET}"
        echo -e "      Admin User:        ${CYAN}${CFG_AUTH_ADMIN_USER}${RESET}"
        if [[ -n "$CFG_AUTH_ADMIN_PASS" ]]; then
            # Check if password was auto-generated (will be set in main)
            echo -e "      Admin Password:    ${CYAN}(configured)${RESET}"
        else
            echo -e "      Admin Password:    ${YELLOW}(will be auto-generated)${RESET}"
        fi
    else
        echo -e "      Authentication:    ${YELLOW}Disabled${RESET}"
    fi
    if [[ "$CFG_AUDIT_ENABLED" == "true" ]]; then
        echo -e "      Audit Trail:       ${GREEN}Enabled${RESET} ${DIM}(${CFG_AUDIT_RETENTION_DAYS} days retention)${RESET}"
    else
        echo -e "      Audit Trail:       ${YELLOW}Disabled${RESET}"
    fi
    echo ""

    # Section 3: Advanced Features
    echo -e "  ${WHITE}${BOLD}[${CYAN}3${WHITE}]${RESET} ${BOLD}ADVANCED FEATURES${RESET}"
    if [[ "$CFG_SCHEMA_ENABLED" == "true" ]]; then
        echo -e "      Schema Validation: ${GREEN}Enabled${RESET} ${DIM}(${CFG_SCHEMA_VALIDATION} mode)${RESET}"
    else
        echo -e "      Schema Validation: ${YELLOW}Disabled${RESET}"
    fi
    if [[ "$CFG_DLQ_ENABLED" == "true" ]]; then
        echo -e "      Dead Letter Queue: ${GREEN}Enabled${RESET} ${DIM}(${CFG_DLQ_MAX_RETRIES} retries)${RESET}"
    else
        echo -e "      Dead Letter Queue: ${YELLOW}Disabled${RESET}"
    fi
    # Format TTL nicely (convert seconds to days if large)
    local ttl_display="${CFG_TTL_DEFAULT}s"
    if [[ "$CFG_TTL_DEFAULT" -ge 86400 ]]; then
        ttl_display="$((CFG_TTL_DEFAULT / 86400)) days"
    elif [[ "$CFG_TTL_DEFAULT" -ge 3600 ]]; then
        ttl_display="$((CFG_TTL_DEFAULT / 3600)) hours"
    fi
    echo -e "      Message TTL:       ${GREEN}Enabled${RESET} ${DIM}(${ttl_display})${RESET}"
    if [[ "$CFG_DELAYED_ENABLED" == "true" ]]; then
        echo -e "      Delayed Delivery:  ${GREEN}Enabled${RESET}"
    else
        echo -e "      Delayed Delivery:  ${YELLOW}Disabled${RESET}"
    fi
    if [[ "$CFG_TXN_ENABLED" == "true" ]]; then
        echo -e "      Transactions:      ${GREEN}Enabled${RESET} ${DIM}(${CFG_TXN_TIMEOUT}s timeout)${RESET}"
    else
        echo -e "      Transactions:      ${YELLOW}Disabled${RESET}"
    fi
    echo ""

    # Section 4: Observability
    echo -e "  ${WHITE}${BOLD}[${CYAN}4${WHITE}]${RESET} ${BOLD}OBSERVABILITY${RESET}"
    if [[ "$CFG_METRICS_ENABLED" == "true" ]]; then
        echo -e "      Prometheus Metrics: ${GREEN}Enabled${RESET} ${DIM}(${CFG_METRICS_ADDR})${RESET}"
    else
        echo -e "      Prometheus Metrics: ${YELLOW}Disabled${RESET}"
    fi
    if [[ "$CFG_TRACING_ENABLED" == "true" ]]; then
        echo -e "      OpenTelemetry:      ${GREEN}Enabled${RESET} ${DIM}(${CFG_TRACING_ENDPOINT})${RESET}"
    else
        echo -e "      OpenTelemetry:      ${YELLOW}Disabled${RESET}"
    fi
    if [[ "$CFG_HEALTH_ENABLED" == "true" ]]; then
        echo -e "      Health Checks:      ${GREEN}Enabled${RESET} ${DIM}(${CFG_HEALTH_ADDR})${RESET}"
    else
        echo -e "      Health Checks:      ${YELLOW}Disabled${RESET}"
    fi
    if [[ "$CFG_ADMIN_ENABLED" == "true" ]]; then
        if [[ "$CFG_ADMIN_TLS_ENABLED" == "true" ]]; then
            echo -e "      Admin API:          ${GREEN}Enabled${RESET} ${DIM}(${CFG_ADMIN_ADDR}, HTTPS)${RESET}"
        else
            echo -e "      Admin API:          ${GREEN}Enabled${RESET} ${DIM}(${CFG_ADMIN_ADDR}, HTTP)${RESET}"
        fi
    else
        echo -e "      Admin API:          ${YELLOW}Disabled${RESET}"
    fi
    echo ""

    # Section 5: Performance
    echo -e "  ${WHITE}${BOLD}[${CYAN}5${WHITE}]${RESET} ${BOLD}PERFORMANCE${RESET}"
    echo -e "      Acks Mode:         ${CYAN}${CFG_ACKS}${RESET}"
    # Format segment size nicely
    local segment_display="${CFG_SEGMENT_BYTES} bytes"
    if [[ "$CFG_SEGMENT_BYTES" -ge 1073741824 ]]; then
        segment_display="$((CFG_SEGMENT_BYTES / 1073741824)) GB"
    elif [[ "$CFG_SEGMENT_BYTES" -ge 1048576 ]]; then
        segment_display="$((CFG_SEGMENT_BYTES / 1048576)) MB"
    fi
    echo -e "      Segment Size:      ${CYAN}${segment_display}${RESET}"
    echo -e "      Log Level:         ${CYAN}${CFG_LOG_LEVEL}${RESET}"
    echo ""

    # Show credentials reminder if auth is enabled
    if [[ "$CFG_AUTH_ENABLED" == "true" ]]; then
        echo -e "  ${YELLOW}${BOLD}⚠ CREDENTIALS${RESET}"
        echo -e "      Admin Username:    ${CYAN}${CFG_AUTH_ADMIN_USER}${RESET}"
        if [[ -n "$CFG_AUTH_ADMIN_PASS" ]]; then
            echo -e "      Admin Password:    ${CYAN}${CFG_AUTH_ADMIN_PASS}${RESET}"
        else
            echo -e "      Admin Password:    ${DIM}(will be auto-generated during install)${RESET}"
        fi
        echo ""
    fi

    # Show encryption key reminder if encryption is enabled
    if [[ "$CFG_ENCRYPTION_ENABLED" == "true" ]] && [[ -n "$CFG_ENCRYPTION_KEY" ]]; then
        echo -e "  ${YELLOW}${BOLD}⚠ ENCRYPTION KEY${RESET}"
        echo -e "      ${DIM}${CFG_ENCRYPTION_KEY}${RESET}"
        echo -e "      ${DIM}(Save this key securely - required for data recovery)${RESET}"
        echo ""
    fi
}

clear_screen_if_interactive() {
    # Only clear screen if we're in an interactive terminal
    if [[ -t 1 ]] && [[ "$AUTO_CONFIRM" != true ]]; then
        # Use tput if available, otherwise ANSI escape
        if command -v tput &> /dev/null; then
            tput clear 2>/dev/null || true
        else
            echo -e "\033[2J\033[H" 2>/dev/null || true
        fi
    fi
}

iterative_configuration_loop() {
    local config_dir="$1"

    while true; do
        show_configuration_summary "$config_dir"

        echo -e "  ${BOLD}Options:${RESET}"
        echo -e "    ${CYAN}1-5${RESET}  Modify a section"
        echo -e "    ${CYAN}c${RESET}    Confirm and proceed with installation"
        echo -e "    ${CYAN}q${RESET}    Cancel installation"
        echo ""

        local choice
        choice=$(prompt_value "Enter choice" "c")

        case "$choice" in
            1)
                configure_section_deployment
                ;;
            2)
                configure_section_security
                ;;
            3)
                configure_section_advanced
                ;;
            4)
                configure_section_observability
                ;;
            5)
                configure_section_performance
                ;;
            c|C|confirm)
                echo ""
                if prompt_yes_no "Proceed with installation" "y"; then
                    return 0
                fi
                ;;
            q|Q|quit|cancel)
                print_info "Installation cancelled"
                exit 0
                ;;
            *)
                print_warning "Invalid choice: $choice"
                sleep 1
                ;;
        esac
    done
}

# =============================================================================
# Main Entry Point
# =============================================================================

main() {
    parse_args "$@"

    print_banner
    detect_system

    if [[ "$UNINSTALL" == true ]]; then
        uninstall
        exit 0
    fi

    print_info "Detected: ${CYAN}$OS/$ARCH${RESET}"
    echo ""

    # Set default prefix if not specified
    if [[ -z "$PREFIX" ]]; then
        PREFIX=$(get_default_prefix)
    fi

    # Set default data dir and config dir
    CFG_DATA_DIR=$(get_default_data_dir)
    local config_dir=$(get_default_config_dir)
    CFG_NODE_ID=$(hostname)
    CFG_SCHEMA_REGISTRY_DIR="${CFG_DATA_DIR}/schemas"

    # Auto-generate encryption key and admin password for defaults
    if [[ -z "$CFG_ENCRYPTION_KEY" ]]; then
        CFG_ENCRYPTION_KEY=$(openssl rand -hex 32 2>/dev/null || head -c 32 /dev/urandom | xxd -p -c 64)
    fi
    if [[ -z "$CFG_AUTH_ADMIN_PASS" ]]; then
        CFG_AUTH_ADMIN_PASS=$(openssl rand -base64 16 2>/dev/null || head -c 16 /dev/urandom | base64)
    fi

    # Interactive configuration or use defaults
    if [[ "$AUTO_CONFIRM" != true ]]; then
        # STEP 1: Ask deployment mode first (most important decision)
        select_deployment_mode

        # STEP 2: Show configuration summary and allow iterative modification
        iterative_configuration_loop "$config_dir"
    fi

    echo ""
    detect_source_mode
    check_dependencies
    ensure_source_code
    build_binaries
    install_binaries "$PREFIX"
    create_data_dir
    generate_config "$config_dir"
    install_system_service "$config_dir" "$PREFIX"
    print_post_install "$PREFIX" "$config_dir"
    # Note: cleanup_cloned_repo is called automatically via EXIT trap
}

main "$@"
