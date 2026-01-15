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

readonly SCRIPT_VERSION="1.26.7"
readonly FLYMQ_VERSION="${FLYMQ_VERSION:-1.26.7}"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"

# Installation options
PREFIX=""
AUTO_CONFIRM=false
UNINSTALL=false
CONFIG_FILE=""

# Configuration values (will be set during interactive setup)
CFG_DATA_DIR=""
CFG_BIND_ADDR=":9092"
CFG_CLUSTER_ADDR=":9093"
CFG_NODE_ID=""
CFG_LOG_LEVEL="info"
CFG_SEGMENT_BYTES="67108864"
CFG_RETENTION_BYTES="0"
CFG_TLS_ENABLED="false"
CFG_TLS_CERT_FILE=""
CFG_TLS_KEY_FILE=""
CFG_ENCRYPTION_ENABLED="false"
CFG_ENCRYPTION_KEY=""

# Cluster Configuration
CFG_DEPLOYMENT_MODE="standalone"  # standalone or cluster
CFG_CLUSTER_PEERS=""              # comma-separated list of peer addresses
CFG_ADVERTISE_CLUSTER=""          # advertised cluster address

# Performance Configuration
CFG_ACKS="leader"                 # Durability mode: all, leader, none

# Schema validation
CFG_SCHEMA_ENABLED="false"
CFG_SCHEMA_VALIDATION="strict"
CFG_SCHEMA_REGISTRY_DIR=""

# Dead Letter Queue
CFG_DLQ_ENABLED="false"
CFG_DLQ_MAX_RETRIES="3"
CFG_DLQ_RETRY_DELAY="1000"
CFG_DLQ_TOPIC_SUFFIX="-dlq"

# Message TTL
CFG_TTL_DEFAULT="0"
CFG_TTL_CLEANUP_INTERVAL="60"

# Delayed Message Delivery
CFG_DELAYED_ENABLED="false"
CFG_DELAYED_MAX_DELAY="604800"

# Transaction Support
CFG_TXN_ENABLED="false"
CFG_TXN_TIMEOUT="60"

# Observability - Metrics
CFG_METRICS_ENABLED="false"
CFG_METRICS_ADDR=":9094"

# Observability - Tracing
CFG_TRACING_ENABLED="false"
CFG_TRACING_ENDPOINT="localhost:4317"
CFG_TRACING_SAMPLE_RATE="0.1"

# Observability - Health Checks
CFG_HEALTH_ENABLED="true"
CFG_HEALTH_ADDR=":9095"
CFG_HEALTH_TLS_ENABLED="false"
CFG_HEALTH_TLS_CERT_FILE=""
CFG_HEALTH_TLS_KEY_FILE=""
CFG_HEALTH_TLS_AUTO_GENERATE="false"
CFG_HEALTH_TLS_USE_ADMIN_CERT="false"

# Observability - Admin API
CFG_ADMIN_ENABLED="false"
CFG_ADMIN_ADDR=":9096"
CFG_ADMIN_TLS_ENABLED="false"
CFG_ADMIN_TLS_CERT_FILE=""
CFG_ADMIN_TLS_KEY_FILE=""
CFG_ADMIN_TLS_AUTO_GENERATE="false"

# Authentication
CFG_AUTH_ENABLED="false"
CFG_AUTH_ADMIN_USER="admin"
CFG_AUTH_ADMIN_PASS=""
CFG_AUTH_ALLOW_ANONYMOUS="true"

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
print_section() { echo -e "\n  ${BOLD}${CYAN}━━━ $1 ━━━${RESET}\n"; }

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
# Banner - Read from banner.txt
# =============================================================================

print_banner() {
    local banner_file="${SCRIPT_DIR}/internal/banner/banner.txt"
    echo ""
    if [[ -f "$banner_file" ]]; then
        while IFS= read -r line; do
            # Output line with colors, then reset (avoids backslash escaping RESET)
            echo -ne "${CYAN}${BOLD}"
            echo -e "${line}"
            echo -ne "${RESET}"
        done < "$banner_file"
    else
        echo -e "${CYAN}${BOLD}  FlyMQ${RESET}"
    fi
    echo ""
    echo -e "  ${BOLD}High-Performance Message Queue${RESET}"
    echo -e "  ${DIM}Version ${FLYMQ_VERSION}${RESET}"
    echo ""
    echo -e "  ${DIM}Copyright (c) 2026 Firefly Software Solutions Inc.${RESET}"
    echo -e "  ${DIM}Licensed under the Apache License 2.0${RESET}"
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
            echo -en "  ${prompt} [${DIM}Y/n${RESET}]: " >&2
        else
            echo -en "  ${prompt} [${DIM}y/N${RESET}]: " >&2
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
    echo -e "    ${DIM}1)${RESET} Bootstrap - First node in a new cluster (no peers needed)"
    echo -e "    ${DIM}2)${RESET} Join      - Join an existing cluster (peers required)"
    echo ""

    local role_choice
    role_choice=$(prompt_choice "Cluster role (1=bootstrap, 2=join)" "1" "1" "2" "bootstrap" "join")

    if [[ "$role_choice" == "2" ]] || [[ "$role_choice" == "join" ]]; then
        configure_cluster_join
    else
        configure_cluster_bootstrap
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
    echo -e "  ${GREEN}${BOLD}╔════════════════════════════════════════════════════════════╗${RESET}"
    echo -e "  ${GREEN}${BOLD}║              BOOTSTRAP NODE SETUP COMPLETE                 ║${RESET}"
    echo -e "  ${GREEN}${BOLD}╚════════════════════════════════════════════════════════════╝${RESET}"
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
    echo -e "  ${GREEN}${BOLD}╔════════════════════════════════════════════════════════════╗${RESET}"
    echo -e "  ${GREEN}${BOLD}║               JOINING NODE SETUP COMPLETE                  ║${RESET}"
    echo -e "  ${GREEN}${BOLD}╚════════════════════════════════════════════════════════════╝${RESET}"
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

configure_interactive() {
    echo ""
    print_step "Configuration"
    echo ""
    echo -e "  ${DIM}Press Enter to accept defaults shown in brackets.${RESET}"
    echo -e "  ${DIM}Press Ctrl+C at any time to cancel.${RESET}"
    echo ""

    # =========================================================================
    # Deployment Mode Selection
    # =========================================================================
    print_section "Deployment Mode"
    echo -e "  ${DIM}Choose how you want to deploy FlyMQ:${RESET}"
    echo -e "    ${DIM}1)${RESET} Standalone - Single node deployment (development/testing)"
    echo -e "    ${DIM}2)${RESET} Cluster    - Multi-node deployment (production)"
    echo ""

    local mode_choice
    mode_choice=$(prompt_choice "Deployment mode (1=standalone, 2=cluster)" "1" "1" "2" "standalone" "cluster")

    if [[ "$mode_choice" == "2" ]] || [[ "$mode_choice" == "cluster" ]]; then
        CFG_DEPLOYMENT_MODE="cluster"
        configure_cluster_mode
    else
        CFG_DEPLOYMENT_MODE="standalone"
        print_info "Configuring standalone mode..."
    fi
    echo ""

    # Network Configuration (only if not already configured in cluster mode)
    if [[ "$CFG_DEPLOYMENT_MODE" == "standalone" ]]; then
        echo ""
        echo -e "  ${BOLD}Network Settings${RESET}"
        echo ""
        CFG_BIND_ADDR=$(prompt_value "Client bind address" ":9092")
        CFG_NODE_ID=$(prompt_value "Node ID" "$CFG_NODE_ID")
        echo ""
    fi

    # Storage Configuration
    echo -e "  ${BOLD}Storage Settings${RESET}"
    echo ""
    CFG_DATA_DIR=$(prompt_value "Data directory" "$(get_default_data_dir)")
    CFG_SEGMENT_BYTES=$(prompt_number "Segment size (bytes)" "67108864" "1024")
    CFG_RETENTION_BYTES=$(prompt_number "Retention limit (0=unlimited)" "0" "0")
    echo ""

    # Logging Configuration
    echo -e "  ${BOLD}Logging Settings${RESET}"
    echo -e "  ${DIM}Available levels: debug, info, warn, error${RESET}"
    echo ""
    CFG_LOG_LEVEL=$(prompt_choice "Log level" "info" "debug" "info" "warn" "error")
    echo ""

    # Performance Configuration
    echo -e "  ${BOLD}Performance Settings${RESET}"
    echo -e "  ${DIM}Acks mode controls durability vs. latency trade-off:${RESET}"
    echo -e "    ${DIM}none${RESET}   - Fire-and-forget, maximum throughput (like Kafka acks=0)"
    echo -e "    ${DIM}leader${RESET} - Leader acknowledges, balanced (like Kafka acks=1) [DEFAULT]"
    echo -e "    ${DIM}all${RESET}    - All replicas acknowledge, maximum durability (like Kafka acks=-1)"
    echo ""
    CFG_ACKS=$(prompt_choice "Acks mode" "leader" "none" "leader" "all")
    echo ""

    # Security Configuration
    echo -e "  ${BOLD}Security Settings${RESET}"
    echo ""
    if prompt_yes_no "Enable TLS encryption" "n"; then
        CFG_TLS_ENABLED="true"
        CFG_TLS_CERT_FILE=$(prompt_value "TLS certificate file" "")
        CFG_TLS_KEY_FILE=$(prompt_value "TLS key file" "")
        if [[ -z "$CFG_TLS_CERT_FILE" ]] || [[ -z "$CFG_TLS_KEY_FILE" ]]; then
            print_warning "TLS files not specified. You can configure them later in flymq.json"
        fi
    fi
    echo ""

    if prompt_yes_no "Enable data-at-rest encryption" "n"; then
        CFG_ENCRYPTION_ENABLED="true"
        CFG_ENCRYPTION_KEY=$(prompt_value "Encryption key (leave empty to auto-generate)" "")
        if [[ -z "$CFG_ENCRYPTION_KEY" ]]; then
            CFG_ENCRYPTION_KEY=$(openssl rand -hex 32 2>/dev/null || head -c 32 /dev/urandom | xxd -p -c 64)
            print_info "Generated encryption key (save this securely)"
        fi
    fi
    echo ""

    # =========================================================================
    # Advanced Features
    # =========================================================================

    print_section "Advanced Features"
    echo -e "  ${DIM}Configure optional advanced features for enhanced functionality.${RESET}"
    echo ""

    # Schema Validation
    echo -e "  ${BOLD}Schema Validation${RESET}"
    echo -e "  ${DIM}Validate messages against JSON Schema, Avro, or Protobuf schemas${RESET}"
    if prompt_yes_no "Enable schema validation" "n"; then
        CFG_SCHEMA_ENABLED="true"
        echo -e "  ${DIM}Validation modes: strict (reject invalid), lenient (warn only), none${RESET}"
        CFG_SCHEMA_VALIDATION=$(prompt_choice "Validation mode" "strict" "strict" "lenient" "none")
        CFG_SCHEMA_REGISTRY_DIR=$(prompt_value "Schema registry directory" "${CFG_DATA_DIR}/schemas")
    fi
    echo ""

    # Dead Letter Queue
    echo -e "  ${BOLD}Dead Letter Queue (DLQ)${RESET}"
    echo -e "  ${DIM}Route failed messages to a separate queue for later analysis${RESET}"
    if prompt_yes_no "Enable dead letter queues" "n"; then
        CFG_DLQ_ENABLED="true"
        CFG_DLQ_MAX_RETRIES=$(prompt_number "Max retry attempts before DLQ" "3" "1")
        CFG_DLQ_RETRY_DELAY=$(prompt_number "Retry delay (milliseconds)" "1000" "100")
        CFG_DLQ_TOPIC_SUFFIX=$(prompt_value "DLQ topic suffix" "-dlq")
    fi
    echo ""

    # Message TTL
    echo -e "  ${BOLD}Message TTL (Time-To-Live)${RESET}"
    echo -e "  ${DIM}Automatically expire messages after a specified time${RESET}"
    CFG_TTL_DEFAULT=$(prompt_number "Default TTL in seconds (0=no expiry)" "0" "0")
    if [[ "$CFG_TTL_DEFAULT" != "0" ]]; then
        CFG_TTL_CLEANUP_INTERVAL=$(prompt_number "Cleanup interval (seconds)" "60" "1")
    fi
    echo ""

    # Delayed Message Delivery
    echo -e "  ${BOLD}Delayed Message Delivery${RESET}"
    echo -e "  ${DIM}Schedule messages for future delivery${RESET}"
    if prompt_yes_no "Enable delayed message delivery" "n"; then
        CFG_DELAYED_ENABLED="true"
        CFG_DELAYED_MAX_DELAY=$(prompt_number "Maximum delay (seconds)" "604800" "1")
        print_info "Max delay: $(( CFG_DELAYED_MAX_DELAY / 86400 )) days"
    fi
    echo ""

    # Transaction Support
    echo -e "  ${BOLD}Transaction Support${RESET}"
    echo -e "  ${DIM}Enable exactly-once semantics with atomic message operations${RESET}"
    if prompt_yes_no "Enable transaction support" "n"; then
        CFG_TXN_ENABLED="true"
        CFG_TXN_TIMEOUT=$(prompt_number "Transaction timeout (seconds)" "60" "1")
    fi
    echo ""

    # =========================================================================
    # Observability Features
    # =========================================================================

    print_section "Observability Features"
    echo -e "  ${DIM}Enable monitoring and debugging capabilities.${RESET}"
    echo ""

    # Prometheus Metrics
    echo -e "  ${BOLD}Prometheus Metrics${RESET}"
    echo -e "  ${DIM}Expose metrics for monitoring throughput, latency, and errors${RESET}"
    if prompt_yes_no "Enable Prometheus metrics endpoint" "y"; then
        CFG_METRICS_ENABLED="true"
        CFG_METRICS_ADDR=$(prompt_value "Metrics HTTP address" ":9094")
    fi
    echo ""

    # OpenTelemetry Tracing
    echo -e "  ${BOLD}OpenTelemetry Tracing${RESET}"
    echo -e "  ${DIM}Distributed tracing for debugging and performance analysis${RESET}"
    if prompt_yes_no "Enable OpenTelemetry tracing" "n"; then
        CFG_TRACING_ENABLED="true"
        CFG_TRACING_ENDPOINT=$(prompt_value "OTLP endpoint" "localhost:4317")
        CFG_TRACING_SAMPLE_RATE=$(prompt_value "Sample rate (0.0-1.0)" "0.1")
    fi
    echo ""

    # Health Check Endpoints
    echo -e "  ${BOLD}Health Check Endpoints${RESET}"
    echo -e "  ${DIM}Kubernetes-compatible liveness and readiness probes${RESET}"
    if prompt_yes_no "Enable health check endpoints" "y"; then
        CFG_HEALTH_ENABLED="true"
        CFG_HEALTH_ADDR=$(prompt_value "Health check HTTP address" ":9095")
        echo ""
        echo -e "  ${BOLD}Health Endpoint HTTPS/TLS${RESET}"
        echo -e "  ${DIM}Secure health endpoints with TLS/SSL encryption${RESET}"
        if prompt_yes_no "Enable HTTPS for health endpoints" "n"; then
            CFG_HEALTH_TLS_ENABLED="true"
            echo -e "  ${DIM}You can share Admin API certs, provide your own, or auto-generate${RESET}"
            if prompt_yes_no "Use Admin API TLS certificate (if Admin API HTTPS is enabled later)" "y"; then
                CFG_HEALTH_TLS_USE_ADMIN_CERT="true"
                print_info "Health endpoints will share Admin API TLS certificates"
            elif prompt_yes_no "Auto-generate self-signed certificate" "y"; then
                CFG_HEALTH_TLS_AUTO_GENERATE="true"
                print_info "A self-signed certificate will be generated on first startup"
            else
                CFG_HEALTH_TLS_CERT_FILE=$(prompt_value "TLS certificate file" "")
                CFG_HEALTH_TLS_KEY_FILE=$(prompt_value "TLS key file" "")
                if [[ -z "$CFG_HEALTH_TLS_CERT_FILE" ]] || [[ -z "$CFG_HEALTH_TLS_KEY_FILE" ]]; then
                    print_warning "TLS files not specified. You can configure them later in flymq.json"
                fi
            fi
        fi
    fi
    echo ""

    # Admin API
    echo -e "  ${BOLD}Admin REST API${RESET}"
    echo -e "  ${DIM}REST API for cluster management, topic operations, and monitoring${RESET}"
    if prompt_yes_no "Enable Admin REST API" "n"; then
        CFG_ADMIN_ENABLED="true"
        CFG_ADMIN_ADDR=$(prompt_value "Admin API HTTP address" ":9096")
        echo ""
        echo -e "  ${BOLD}Admin API HTTPS/TLS${RESET}"
        echo -e "  ${DIM}Secure the Admin API with TLS/SSL encryption${RESET}"
        if prompt_yes_no "Enable HTTPS for Admin API" "n"; then
            CFG_ADMIN_TLS_ENABLED="true"
            echo -e "  ${DIM}You can provide your own certificates or auto-generate self-signed ones${RESET}"
            if prompt_yes_no "Auto-generate self-signed certificate" "y"; then
                CFG_ADMIN_TLS_AUTO_GENERATE="true"
                print_info "A self-signed certificate will be generated on first startup"
            else
                CFG_ADMIN_TLS_CERT_FILE=$(prompt_value "TLS certificate file" "")
                CFG_ADMIN_TLS_KEY_FILE=$(prompt_value "TLS key file" "")
                if [[ -z "$CFG_ADMIN_TLS_CERT_FILE" ]] || [[ -z "$CFG_ADMIN_TLS_KEY_FILE" ]]; then
                    print_warning "TLS files not specified. You can configure them later in flymq.json"
                fi
            fi
        fi
    fi
    echo ""

    # =========================================================================
    # Authentication
    # =========================================================================

    print_section "Authentication"
    echo -e "  ${BOLD}Authentication & Authorization${RESET}"
    echo -e "  ${DIM}Enable username/password authentication with role-based access control${RESET}"
    if prompt_yes_no "Enable authentication" "n"; then
        CFG_AUTH_ENABLED="true"
        CFG_AUTH_ADMIN_USER=$(prompt_value "Admin username" "admin")
        echo -e "  ${DIM}Enter a password for the admin user (leave empty to generate one)${RESET}"
        read -sp "  Admin password: " CFG_AUTH_ADMIN_PASS
        echo ""
        if [[ -z "$CFG_AUTH_ADMIN_PASS" ]]; then
            CFG_AUTH_ADMIN_PASS=$(openssl rand -base64 16 2>/dev/null || head -c 16 /dev/urandom | base64)
            echo -e "  ${YELLOW}Generated admin password: ${CYAN}$CFG_AUTH_ADMIN_PASS${RESET}"
            echo -e "  ${YELLOW}Please save this password securely!${RESET}"
        fi
        if prompt_yes_no "Allow anonymous connections (read-only)" "n"; then
            CFG_AUTH_ALLOW_ANONYMOUS="true"
        else
            CFG_AUTH_ALLOW_ANONYMOUS="false"
        fi
    fi
    echo ""

    # =========================================================================
    # System Integration
    # =========================================================================

    # System Integration - Platform specific
    if [[ "$OS" == "linux" ]] && command -v systemctl &> /dev/null; then
        print_section "System Integration"
        echo -e "  ${BOLD}Systemd Service${RESET}"
        echo -e "  ${DIM}Install systemd service for automatic startup and management${RESET}"
        if prompt_yes_no "Install systemd service" "y"; then
            INSTALL_SYSTEMD="true"
        fi
        echo ""
    elif [[ "$OS" == "darwin" ]]; then
        print_section "System Integration"
        echo -e "  ${BOLD}Launch Agent (macOS)${RESET}"
        echo -e "  ${DIM}Install launchd service for automatic startup${RESET}"
        if prompt_yes_no "Install Launch Agent" "n"; then
            INSTALL_LAUNCHD="true"
        fi
        echo ""
    elif [[ "$OS" == "windows" ]]; then
        print_section "System Integration"
        echo -e "  ${BOLD}Windows Service${RESET}"
        echo -e "  ${DIM}Note: Windows service installation requires additional tools (NSSM or sc.exe)${RESET}"
        print_info "See documentation for Windows service setup"
        echo ""
    fi
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
    "encryption_enabled": ${CFG_ENCRYPTION_ENABLED},
    "encryption_key": "${CFG_ENCRYPTION_KEY}"
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
  }
}
EOF

    print_success "Generated: ${CYAN}$config_file${RESET}"

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

# Security
FLYMQ_TLS_ENABLED=${CFG_TLS_ENABLED}
FLYMQ_TLS_CERT_FILE=${CFG_TLS_CERT_FILE}
FLYMQ_TLS_KEY_FILE=${CFG_TLS_KEY_FILE}
FLYMQ_ENCRYPTION_ENABLED=${CFG_ENCRYPTION_ENABLED}
FLYMQ_ENCRYPTION_KEY=${CFG_ENCRYPTION_KEY}

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

# =============================================================================
# Installation
# =============================================================================

check_dependencies() {
    print_step "Checking dependencies"
    echo ""

    if ! command -v go &> /dev/null; then
        print_error "Go is not installed. Please install Go 1.21+ first."
        print_info "Visit: ${CYAN}https://go.dev/dl/${RESET}"
        exit 1
    fi

    GO_VERSION=$(go version | grep -oE 'go[0-9]+\.[0-9]+' | sed 's/go//')
    print_success "Go $GO_VERSION found"
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
    if ! GOOS="${OS}" GOARCH="${ARCH}" go build -o "$server_bin" cmd/flymq/main.go 2>&1; then
        print_error "Failed to build flymq server"
        exit 1
    fi
    print_success "Built flymq server"

    # Build CLI
    echo -n "  "
    if ! GOOS="${OS}" GOARCH="${ARCH}" go build -o "$cli_bin" cmd/flymq-cli/main.go 2>&1; then
        print_error "Failed to build flymq-cli"
        exit 1
    fi
    print_success "Built flymq-cli"
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
    echo -e "${GREEN}${BOLD}════════════════════════════════════════════════════════════════${RESET}"
    print_success "FlyMQ v${FLYMQ_VERSION} installed successfully!"
    echo -e "${GREEN}${BOLD}════════════════════════════════════════════════════════════════${RESET}"
    echo ""
    echo -e "${BOLD}Installation Summary:${RESET}"
    echo -e "  ${ICON_ARROW} Binaries:      ${CYAN}$bin_dir${RESET}"
    echo -e "  ${ICON_ARROW} Configuration: ${CYAN}$config_dir/flymq.json${RESET}"
    echo -e "  ${ICON_ARROW} Data:          ${CYAN}$CFG_DATA_DIR${RESET}"
    echo ""

    # Show enabled features
    echo -e "${BOLD}Enabled Features:${RESET}"
    echo -e "  ${ICON_ARROW} TLS Encryption:     ${CYAN}$CFG_TLS_ENABLED${RESET}"
    echo -e "  ${ICON_ARROW} Data Encryption:    ${CYAN}$CFG_ENCRYPTION_ENABLED${RESET}"
    echo -e "  ${ICON_ARROW} Schema Validation:  ${CYAN}$CFG_SCHEMA_ENABLED${RESET}"
    echo -e "  ${ICON_ARROW} Dead Letter Queue:  ${CYAN}$CFG_DLQ_ENABLED${RESET}"
    echo -e "  ${ICON_ARROW} Delayed Delivery:   ${CYAN}$CFG_DELAYED_ENABLED${RESET}"
    echo -e "  ${ICON_ARROW} Transactions:       ${CYAN}$CFG_TXN_ENABLED${RESET}"
    echo -e "  ${ICON_ARROW} Prometheus Metrics: ${CYAN}$CFG_METRICS_ENABLED${RESET}"
    echo -e "  ${ICON_ARROW} Health Checks:      ${CYAN}$CFG_HEALTH_ENABLED${RESET}"
    echo -e "  ${ICON_ARROW} Admin API:          ${CYAN}$CFG_ADMIN_ENABLED${RESET}"
    echo -e "  ${ICON_ARROW} Authentication:     ${CYAN}$CFG_AUTH_ENABLED${RESET}"
    echo ""

    echo -e "${BOLD}Quick Start:${RESET}"
    echo ""

    if [[ "$CFG_DEPLOYMENT_MODE" == "cluster" ]]; then
        # Bootstrap node has no peers, joining node has peers
        if [[ -z "$CFG_CLUSTER_PEERS" ]]; then
            echo -e "  ${BOLD}Bootstrap Node (First Node):${RESET}"
            echo ""
            echo "  1. Start this node:"
            echo -e "     ${CYAN}flymq --config $config_dir/flymq.json${RESET}"
            echo ""
            echo "  2. On other nodes, run install.sh and choose 'Join' mode"
            echo -e "     Use this address to join: ${CYAN}${CFG_ADVERTISE_CLUSTER}${RESET}"
            echo ""
            echo "  3. Once all nodes are running, check cluster status:"
            echo -e "     ${CYAN}flymq-cli cluster status${RESET}"
            echo ""
        else
            echo -e "  ${BOLD}Joining Node:${RESET}"
            echo ""
            echo "  1. Ensure the cluster is running"
            echo ""
            echo "  2. Start this node:"
            echo -e "     ${CYAN}flymq --config $config_dir/flymq.json${RESET}"
            echo ""
            echo "  3. This node will automatically join the cluster"
            echo ""
            echo "  4. Check cluster status:"
            echo -e "     ${CYAN}flymq-cli cluster status${RESET}"
            echo ""
        fi
    else
        echo "  1. Start the server:"
        echo -e "     ${CYAN}flymq --config $config_dir/flymq.json${RESET}"
        echo ""
    fi

    echo "  2. Produce a message:"
    echo -e "     ${CYAN}flymq-cli produce my-topic \"Hello World\"${RESET}"
    echo ""
    echo "  3. Subscribe to messages:"
    echo -e "     ${CYAN}flymq-cli subscribe my-topic --from earliest${RESET}"
    echo ""
    echo "  4. List topics:"
    echo -e "     ${CYAN}flymq-cli topics${RESET}"
    echo ""

    # Show advanced feature examples if enabled
    if [[ "$CFG_SCHEMA_ENABLED" == "true" ]] || [[ "$CFG_DLQ_ENABLED" == "true" ]] || \
       [[ "$CFG_DELAYED_ENABLED" == "true" ]] || [[ "$CFG_TXN_ENABLED" == "true" ]]; then
        echo -e "${BOLD}Advanced Feature Examples:${RESET}"
        echo ""

        if [[ "$CFG_SCHEMA_ENABLED" == "true" ]]; then
            echo "  # Register a JSON schema"
            echo -e "  ${CYAN}flymq-cli schema register user-schema json '{\"type\":\"object\"}'${RESET}"
            echo ""
        fi

        if [[ "$CFG_DLQ_ENABLED" == "true" ]]; then
            echo "  # View dead letter queue messages"
            echo -e "  ${CYAN}flymq-cli dlq list my-topic${RESET}"
            echo ""
        fi

        if [[ "$CFG_DELAYED_ENABLED" == "true" ]]; then
            echo "  # Send a delayed message (5 second delay)"
            echo -e "  ${CYAN}flymq-cli produce-delayed my-topic \"Delayed message\" 5000${RESET}"
            echo ""
        fi

        if [[ "$CFG_TXN_ENABLED" == "true" ]]; then
            echo "  # Send messages in a transaction"
            echo -e "  ${CYAN}flymq-cli txn my-topic \"Message 1\" \"Message 2\"${RESET}"
            echo ""
        fi
    fi

    # Show observability endpoints if enabled
    if [[ "$CFG_METRICS_ENABLED" == "true" ]] || [[ "$CFG_HEALTH_ENABLED" == "true" ]] || \
       [[ "$CFG_ADMIN_ENABLED" == "true" ]]; then
        echo -e "${BOLD}Observability Endpoints:${RESET}"
        echo ""

        if [[ "$CFG_METRICS_ENABLED" == "true" ]]; then
            echo -e "  ${ICON_ARROW} Prometheus Metrics: ${CYAN}http://localhost${CFG_METRICS_ADDR}/metrics${RESET}"
        fi

        if [[ "$CFG_HEALTH_ENABLED" == "true" ]]; then
            local health_protocol="http"
            if [[ "$CFG_HEALTH_TLS_ENABLED" == "true" ]]; then
                health_protocol="https"
            fi
            echo -e "  ${ICON_ARROW} Health Check:       ${CYAN}${health_protocol}://localhost${CFG_HEALTH_ADDR}/health${RESET}"
            echo -e "  ${ICON_ARROW} Liveness Probe:     ${CYAN}${health_protocol}://localhost${CFG_HEALTH_ADDR}/health/live${RESET}"
            echo -e "  ${ICON_ARROW} Readiness Probe:    ${CYAN}${health_protocol}://localhost${CFG_HEALTH_ADDR}/health/ready${RESET}"
        fi

        if [[ "$CFG_ADMIN_ENABLED" == "true" ]]; then
            local admin_protocol="http"
            if [[ "$CFG_ADMIN_TLS_ENABLED" == "true" ]]; then
                admin_protocol="https"
            fi
            echo -e "  ${ICON_ARROW} Admin API:          ${CYAN}${admin_protocol}://localhost${CFG_ADMIN_ADDR}/api/v1${RESET}"
            echo -e "  ${ICON_ARROW} Swagger UI:         ${CYAN}${admin_protocol}://localhost${CFG_ADMIN_ADDR}/swagger/${RESET}"
        fi
        echo ""
    fi

    # Check if bin_dir is in PATH
    if [[ ":$PATH:" != *":$bin_dir:"* ]]; then
        print_warning "Add $bin_dir to your PATH:"
        echo ""
        echo -e "  ${CYAN}export PATH=\"$bin_dir:\$PATH\"${RESET}"
        echo ""
    fi

    if [[ "$CFG_ENCRYPTION_ENABLED" == "true" ]]; then
        print_warning "IMPORTANT: Save your encryption key securely!"
        echo -e "  ${DIM}Key: $CFG_ENCRYPTION_KEY${RESET}"
        echo ""
    fi

    # Show authentication info if enabled
    if [[ "$CFG_AUTH_ENABLED" == "true" ]]; then
        echo -e "${BOLD}${YELLOW}═══════════════════════════════════════════════════════════════${RESET}"
        echo -e "${BOLD}${YELLOW}  AUTHENTICATION CREDENTIALS - SAVE THESE!${RESET}"
        echo -e "${BOLD}${YELLOW}═══════════════════════════════════════════════════════════════${RESET}"
        echo ""
        echo -e "  ${BOLD}Username:${RESET} ${CYAN}${CFG_AUTH_ADMIN_USER}${RESET}"
        echo -e "  ${BOLD}Password:${RESET} ${CYAN}${CFG_AUTH_ADMIN_PASS}${RESET}"
        echo ""
        echo -e "${BOLD}CLI Usage Examples:${RESET}"
        echo ""
        echo -e "  ${DIM}# Binary protocol commands (produce, consume, subscribe):${RESET}"
        echo -e "  ${CYAN}flymq-cli -u ${CFG_AUTH_ADMIN_USER} -P '${CFG_AUTH_ADMIN_PASS}' produce my-topic \"Hello\"${RESET}"
        echo -e "  ${CYAN}flymq-cli -u ${CFG_AUTH_ADMIN_USER} -P '${CFG_AUTH_ADMIN_PASS}' subscribe my-topic --from earliest${RESET}"
        echo ""
        if [[ "$CFG_ADMIN_ENABLED" == "true" ]]; then
            local admin_protocol="http"
            local tls_flags=""
            if [[ "$CFG_ADMIN_TLS_ENABLED" == "true" ]]; then
                admin_protocol="https"
                if [[ "$CFG_ADMIN_TLS_AUTO_GENERATE" == "true" ]]; then
                    tls_flags=" --admin-tls --admin-insecure"
                else
                    tls_flags=" --admin-tls"
                fi
            fi
            echo -e "  ${DIM}# Admin API commands (cluster, topics list, groups):${RESET}"
            echo -e "  ${CYAN}flymq-cli${tls_flags} --admin-user ${CFG_AUTH_ADMIN_USER} --admin-pass '${CFG_AUTH_ADMIN_PASS}' cluster status${RESET}"
            echo -e "  ${CYAN}flymq-cli${tls_flags} --admin-user ${CFG_AUTH_ADMIN_USER} --admin-pass '${CFG_AUTH_ADMIN_PASS}' topics${RESET}"
            echo ""
        fi
        echo -e "  ${DIM}# Or set environment variables:${RESET}"
        echo -e "  ${CYAN}export FLYMQ_USERNAME=${CFG_AUTH_ADMIN_USER}${RESET}"
        echo -e "  ${CYAN}export FLYMQ_PASSWORD='${CFG_AUTH_ADMIN_PASS}'${RESET}"
        if [[ "$CFG_ADMIN_ENABLED" == "true" ]]; then
            echo -e "  ${CYAN}export FLYMQ_ADMIN_USER=${CFG_AUTH_ADMIN_USER}${RESET}"
            echo -e "  ${CYAN}export FLYMQ_ADMIN_PASS='${CFG_AUTH_ADMIN_PASS}'${RESET}"
        fi
        echo ""
        echo -e "${BOLD}${YELLOW}═══════════════════════════════════════════════════════════════${RESET}"
        echo ""
    fi

    echo -e "${DIM}For more information, visit: https://github.com/firefly-oss/flymq${RESET}"
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
    exit 130
}

trap cleanup SIGINT SIGTERM

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

main() {
    parse_args "$@"

    print_banner
    detect_system

    if [[ "$UNINSTALL" == true ]]; then
        uninstall
        exit 0
    fi

    print_info "Detected: $OS/$ARCH"

    # Set default prefix if not specified
    if [[ -z "$PREFIX" ]]; then
        PREFIX=$(get_default_prefix)
    fi

    # Set default data dir
    CFG_DATA_DIR=$(get_default_data_dir)
    local config_dir=$(get_default_config_dir)

    # Interactive configuration or use defaults
    if [[ "$AUTO_CONFIRM" != true ]]; then
        echo ""
        echo -e "  ${BOLD}Welcome to the FlyMQ installer!${RESET}"
        echo -e "  ${DIM}This wizard will guide you through the installation process.${RESET}"
        echo ""

        if prompt_yes_no "Run interactive configuration" "y"; then
            configure_interactive
        else
            print_info "Using default configuration"
        fi

        echo ""
        echo -e "  ${BOLD}Installation Summary:${RESET}"
        echo -e "  ${ICON_ARROW} Install prefix: ${CYAN}$PREFIX${RESET}"
        echo -e "  ${ICON_ARROW} Config dir:     ${CYAN}$config_dir${RESET}"
        echo -e "  ${ICON_ARROW} Data dir:       ${CYAN}$CFG_DATA_DIR${RESET}"
        echo -e "  ${ICON_ARROW} Bind address:   ${CYAN}$CFG_BIND_ADDR${RESET}"
        echo ""
        echo -e "  ${BOLD}Deployment:${RESET}"
        echo -e "  ${ICON_ARROW} Mode:           ${CYAN}$CFG_DEPLOYMENT_MODE${RESET}"
        if [[ "$CFG_DEPLOYMENT_MODE" == "cluster" ]]; then
            echo -e "  ${ICON_ARROW} Cluster addr:   ${CYAN}$CFG_ADVERTISE_CLUSTER${RESET}"
            if [[ -n "$CFG_CLUSTER_PEERS" ]]; then
                echo -e "  ${ICON_ARROW} Peers:          ${CYAN}$CFG_CLUSTER_PEERS${RESET}"
            fi
        fi
        echo ""
        echo -e "  ${BOLD}Security:${RESET}"
        echo -e "  ${ICON_ARROW} TLS enabled:    ${CYAN}$CFG_TLS_ENABLED${RESET}"
        echo -e "  ${ICON_ARROW} Encryption:     ${CYAN}$CFG_ENCRYPTION_ENABLED${RESET}"
        echo ""
        echo -e "  ${BOLD}Advanced Features:${RESET}"
        echo -e "  ${ICON_ARROW} Schema Validation:  ${CYAN}$CFG_SCHEMA_ENABLED${RESET}"
        echo -e "  ${ICON_ARROW} Dead Letter Queue:  ${CYAN}$CFG_DLQ_ENABLED${RESET}"
        echo -e "  ${ICON_ARROW} Delayed Delivery:   ${CYAN}$CFG_DELAYED_ENABLED${RESET}"
        echo -e "  ${ICON_ARROW} Transactions:       ${CYAN}$CFG_TXN_ENABLED${RESET}"
        echo ""
        echo -e "  ${BOLD}Observability:${RESET}"
        echo -e "  ${ICON_ARROW} Metrics (${CFG_METRICS_ADDR}):  ${CYAN}$CFG_METRICS_ENABLED${RESET}"
        echo -e "  ${ICON_ARROW} Health (${CFG_HEALTH_ADDR}):   ${CYAN}$CFG_HEALTH_ENABLED${RESET}"
        echo -e "  ${ICON_ARROW} Admin API (${CFG_ADMIN_ADDR}): ${CYAN}$CFG_ADMIN_ENABLED${RESET}"
        echo ""
        echo -e "  ${BOLD}Security:${RESET}"
        echo -e "  ${ICON_ARROW} Authentication:     ${CYAN}$CFG_AUTH_ENABLED${RESET}"
        if [[ "$CFG_AUTH_ENABLED" == "true" ]]; then
            echo -e "  ${ICON_ARROW} Admin User:         ${CYAN}$CFG_AUTH_ADMIN_USER${RESET}"
            echo -e "  ${ICON_ARROW} Allow Anonymous:    ${CYAN}$CFG_AUTH_ALLOW_ANONYMOUS${RESET}"
        fi
        echo ""

        if ! prompt_yes_no "Proceed with installation" "y"; then
            print_info "Installation cancelled"
            exit 0
        fi
    fi

    echo ""
    check_dependencies
    build_binaries
    install_binaries "$PREFIX"
    create_data_dir
    generate_config "$config_dir"
    install_system_service "$config_dir" "$PREFIX"
    print_post_install "$PREFIX" "$config_dir"
}

main "$@"
