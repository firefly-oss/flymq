#!/bin/bash
# =============================================================================
# FlyMQ Docker Entrypoint
# =============================================================================
#
# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0
#
# Handles cluster configuration for bootstrap servers and agent nodes.
#
# ROLES:
#   bootstrap  - First cluster node, generates join tokens
#   agent      - Remote node that joins via token
#   standalone - Single node, no clustering (default)
#
# ENVIRONMENT:
#   Bootstrap Mode:
#     FLYMQ_CLUSTER_TOKEN    - Cluster secret (auto-generated if not set)
#     FLYMQ_ADVERTISE_ADDR   - External IP to advertise (auto-detected)
#
#   Agent Mode:
#     FLYMQ_SERVER           - Bootstrap server address (required)
#     FLYMQ_JOIN_TOKEN       - Cluster join token (required)
#
# =============================================================================

set -e

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly BANNER_FILE="${SCRIPT_DIR}/banner.txt"
readonly TOKEN_FILE="/var/lib/flymq/cluster-token"
readonly VERSION="1.26.1"

# -----------------------------------------------------------------------------
# ANSI Colors (matching internal/banner/banner.go)
# -----------------------------------------------------------------------------
readonly RESET='\033[0m'
readonly BOLD='\033[1m'
readonly DIM='\033[2m'
readonly RED='\033[31m'
readonly GREEN='\033[32m'
readonly YELLOW='\033[33m'
readonly CYAN='\033[36m'

# Icons
readonly ICON_SUCCESS="✓"
readonly ICON_ERROR="✗"
readonly ICON_WARNING="⚠"
readonly ICON_INFO="ℹ"
readonly ICON_ARROW="→"

# -----------------------------------------------------------------------------
# Output Functions
# -----------------------------------------------------------------------------
print_success() { echo -e "${GREEN}${ICON_SUCCESS}${RESET} $1"; }
print_error()   { echo -e "${RED}${ICON_ERROR}${RESET} ${RED}$1${RESET}" >&2; }
print_warning() { echo -e "${YELLOW}${ICON_WARNING}${RESET} $1"; }
print_info()    { echo -e "${CYAN}${ICON_INFO}${RESET} $1"; }
print_step()    { echo -e "${CYAN}${BOLD}${ICON_ARROW}${RESET} ${BOLD}$1${RESET}"; }

# -----------------------------------------------------------------------------
# Banner Display (reads from banner.txt like install.sh)
# -----------------------------------------------------------------------------
print_banner() {
    echo ""
    if [[ -f "$BANNER_FILE" ]]; then
        while IFS= read -r line; do
            echo -e "${CYAN}${BOLD}${line}${RESET}"
        done < "$BANNER_FILE"
    else
        echo -e "${CYAN}${BOLD}  FlyMQ${RESET}"
    fi
    echo ""
    echo -e "  ${BOLD}High-Performance Message Queue${RESET}"
    echo -e "  ${DIM}Version ${VERSION}${RESET}"
    echo ""
    echo -e "  ${DIM}Copyright (c) 2026 Firefly Software Solutions Inc.${RESET}"
    echo -e "  ${DIM}Licensed under the Apache License 2.0${RESET}"
    echo ""
}

# -----------------------------------------------------------------------------
# Utility Functions
# -----------------------------------------------------------------------------

# Generate node ID if not provided
generate_node_id() {
    if [[ -z "$FLYMQ_NODE_ID" ]]; then
        if [[ -n "$HOSTNAME" ]]; then
            export FLYMQ_NODE_ID="$HOSTNAME"
        else
            export FLYMQ_NODE_ID="flymq-$(openssl rand -hex 4)"
        fi
    fi
}

# Generate or load cluster token
generate_cluster_token() {
    if [[ -z "$FLYMQ_CLUSTER_TOKEN" ]]; then
        if [[ -f "$TOKEN_FILE" ]]; then
            export FLYMQ_CLUSTER_TOKEN=$(cat "$TOKEN_FILE")
        else
            export FLYMQ_CLUSTER_TOKEN=$(openssl rand -hex 32)
            echo "$FLYMQ_CLUSTER_TOKEN" > "$TOKEN_FILE" 2>/dev/null || true
        fi
    fi
}

# Detect external IP address for advertising to other nodes
# This handles Docker, Kubernetes, cloud, and NAT environments
detect_advertise_ip() {
    # Priority 1: Explicit advertise address (user override)
    if [[ -n "$FLYMQ_ADVERTISE_ADDR" ]]; then
        echo "$FLYMQ_ADVERTISE_ADDR"
        return
    fi

    # Priority 2: Kubernetes pod IP (if running in K8s)
    if [[ -n "$POD_IP" ]]; then
        echo "$POD_IP"
        return
    fi

    # Priority 3: AWS EC2 metadata (private IP)
    local aws_ip=$(curl -sf --connect-timeout 1 http://169.254.169.254/latest/meta-data/local-ipv4 2>/dev/null || true)
    if [[ -n "$aws_ip" ]]; then
        echo "$aws_ip"
        return
    fi

    # Priority 4: GCP metadata (internal IP)
    local gcp_ip=$(curl -sf --connect-timeout 1 -H "Metadata-Flavor: Google" \
        http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip 2>/dev/null || true)
    if [[ -n "$gcp_ip" ]]; then
        echo "$gcp_ip"
        return
    fi

    # Priority 5: Azure IMDS (private IP)
    local azure_ip=$(curl -sf --connect-timeout 1 -H "Metadata: true" \
        "http://169.254.169.254/metadata/instance/network/interface/0/ipv4/ipAddress/0/privateIpAddress?api-version=2021-02-01&format=text" 2>/dev/null || true)
    if [[ -n "$azure_ip" ]]; then
        echo "$azure_ip"
        return
    fi

    # Priority 6: Docker container IP via default route
    local docker_ip=$(ip route get 1 2>/dev/null | awk '{print $7; exit}' || true)
    if [[ -n "$docker_ip" && "$docker_ip" != "127.0.0.1" ]]; then
        echo "$docker_ip"
        return
    fi

    # Priority 7: First non-loopback interface IP
    local iface_ip=$(ip -4 addr show scope global 2>/dev/null | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | head -1 || true)
    if [[ -n "$iface_ip" ]]; then
        echo "$iface_ip"
        return
    fi

    # Priority 8: hostname -i fallback
    local host_ip=$(hostname -i 2>/dev/null | awk '{print $1}' || true)
    if [[ -n "$host_ip" && "$host_ip" != "127.0.0.1" ]]; then
        echo "$host_ip"
        return
    fi

    # Fallback: return empty (will use bind address)
    echo ""
}

# Get external IP (for display purposes)
get_external_ip() {
    local ip=$(detect_advertise_ip)
    if [[ -n "$ip" ]]; then
        echo "$ip"
    else
        echo "<YOUR_SERVER_IP>"
    fi
}

# Setup advertise addresses for cluster communication
setup_advertise_addresses() {
    local detected_ip=$(detect_advertise_ip)

    if [[ -z "$detected_ip" ]]; then
        print_warning "Could not auto-detect IP address. Set FLYMQ_ADVERTISE_CLUSTER manually for multi-node clusters."
        return
    fi

    # Set advertise cluster address if not already set
    if [[ -z "$FLYMQ_ADVERTISE_CLUSTER" ]]; then
        local cluster_port="${FLYMQ_CLUSTER_PORT:-9093}"
        export FLYMQ_ADVERTISE_CLUSTER="${detected_ip}:${cluster_port}"
        print_info "Auto-detected advertise address: ${FLYMQ_ADVERTISE_CLUSTER}"
    fi

    # Set advertise client address if not already set
    if [[ -z "$FLYMQ_ADVERTISE_ADDR" ]]; then
        local client_port="${FLYMQ_CLIENT_PORT:-9092}"
        export FLYMQ_ADVERTISE_ADDR="${detected_ip}:${client_port}"
    fi
}

# Setup data directory
setup_data_dir() {
    local data_dir="${FLYMQ_DATA_DIR:-/data}"
    local node_data_dir="$data_dir/$FLYMQ_NODE_ID"

    mkdir -p "$node_data_dir" 2>/dev/null || true
    export FLYMQ_DATA_DIR="$node_data_dir"
}

# -----------------------------------------------------------------------------
# Role: Bootstrap (First cluster node)
# -----------------------------------------------------------------------------
run_bootstrap() {
    print_step "Initializing Bootstrap Server"

    generate_cluster_token

    local cluster_port="${FLYMQ_CLUSTER_PORT:-9093}"
    # Use the advertise cluster address if set, otherwise construct from IP
    local server_addr="${FLYMQ_ADVERTISE_CLUSTER:-}"
    if [[ -z "$server_addr" ]]; then
        local external_ip=$(get_external_ip)
        # Remove any port suffix if present
        external_ip="${external_ip%%:*}"
        server_addr="${external_ip}:${cluster_port}"
    fi

    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo -e "${CYAN}${BOLD}  Add Remote Nodes to This Cluster${RESET}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo ""
    echo -e "  Run this command on each remote host:"
    echo ""
    echo -e "  ${YELLOW}docker run -d --name flymq-agent \\\\${RESET}"
    echo -e "  ${YELLOW}  --restart unless-stopped \\\\${RESET}"
    echo -e "  ${YELLOW}  -p 9092:9092 -p 9093:9093 -p 9095:9095 -p 9096:9096 \\\\${RESET}"
    echo -e "  ${YELLOW}  -v flymq-data:/data \\\\${RESET}"
    echo -e "  ${YELLOW}  -e FLYMQ_ROLE=agent \\\\${RESET}"
    echo -e "  ${YELLOW}  -e FLYMQ_SERVER=${server_addr} \\\\${RESET}"
    echo -e "  ${YELLOW}  -e FLYMQ_JOIN_TOKEN=${FLYMQ_CLUSTER_TOKEN} \\\\${RESET}"
    echo -e "  ${YELLOW}  flymq:latest${RESET}"
    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo ""

    # Bootstrap has no initial peers but needs cluster enabled
    export FLYMQ_PEERS=""
    # Ensure cluster address is set for bootstrap mode
    export FLYMQ_CLUSTER_ADDR="${FLYMQ_CLUSTER_ADDR:-0.0.0.0:${cluster_port}}"

    print_success "Bootstrap server initialized"
}


# -----------------------------------------------------------------------------
# Role: Agent (Join existing cluster)
# -----------------------------------------------------------------------------
run_agent() {
    print_step "Initializing Agent Node"

    # Validate required environment
    if [[ -z "$FLYMQ_SERVER" ]]; then
        print_error "FLYMQ_SERVER is required in agent mode"
        print_error "Set it to the bootstrap server address (e.g., 192.168.1.100:9093)"
        exit 1
    fi

    if [[ -z "$FLYMQ_JOIN_TOKEN" ]]; then
        print_error "FLYMQ_JOIN_TOKEN is required in agent mode"
        print_error "Get the token from the bootstrap server output"
        exit 1
    fi

    print_info "Joining cluster via: $FLYMQ_SERVER"

    # Wait for bootstrap server
    local max_attempts=60
    local attempt=0
    local server_host="${FLYMQ_SERVER%:*}"
    local health_port="${FLYMQ_SERVER_HEALTH_PORT:-9095}"

    print_info "Waiting for bootstrap server..."
    while [[ $attempt -lt $max_attempts ]]; do
        if curl -sf "http://${server_host}:${health_port}/health" > /dev/null 2>&1; then
            print_success "Bootstrap server is ready"
            break
        fi
        attempt=$((attempt + 1))
        if [[ $((attempt % 10)) -eq 0 ]]; then
            print_info "Still waiting... (attempt $attempt/$max_attempts)"
        fi
        sleep 2
    done

    if [[ $attempt -eq $max_attempts ]]; then
        print_warning "Bootstrap server not responding, attempting to join anyway"
    fi

    # Set peers to bootstrap server
    export FLYMQ_PEERS="$FLYMQ_SERVER"

    local my_ip=$(get_external_ip)
    print_success "Agent initialized (advertising as ${my_ip}:${FLYMQ_CLUSTER_PORT:-9093})"
}

# -----------------------------------------------------------------------------
# Role: Standalone (Single node, no clustering)
# -----------------------------------------------------------------------------
run_standalone() {
    print_step "Initializing Standalone Node"
    export FLYMQ_PEERS=""
    export FLYMQ_CLUSTER_ADDR=""
    print_success "Standalone mode - clustering disabled"
}

# -----------------------------------------------------------------------------
# Role: Cluster (Multi-node with FLYMQ_PEERS)
# -----------------------------------------------------------------------------
run_cluster() {
    print_step "Initializing Cluster Node"
    
    if [[ -z "$FLYMQ_PEERS" ]]; then
        print_warning "FLYMQ_PEERS not set - this node will start without initial peers"
    else
        print_info "Peers: $FLYMQ_PEERS"
    fi
    
    # Ensure cluster address is set
    local cluster_port="${FLYMQ_CLUSTER_PORT:-9093}"
    export FLYMQ_CLUSTER_ADDR="${FLYMQ_CLUSTER_ADDR:-0.0.0.0:${cluster_port}}"
    
    print_success "Cluster mode enabled"
}

# -----------------------------------------------------------------------------
# Configuration Summary
# -----------------------------------------------------------------------------
print_config() {
    echo ""
    echo -e "${CYAN}━━━ Node Configuration ━━━${RESET}"
    echo ""
    echo -e "  ${DIM}Role:${RESET}         ${BOLD}${FLYMQ_ROLE:-standalone}${RESET}"
    echo -e "  ${DIM}Node ID:${RESET}      ${BOLD}$FLYMQ_NODE_ID${RESET}"
    echo -e "  ${DIM}Bind:${RESET}         ${BOLD}${FLYMQ_BIND_ADDR:-0.0.0.0:9092}${RESET}"
    if [[ -n "$FLYMQ_CLUSTER_ADDR" ]]; then
        echo -e "  ${DIM}Cluster:${RESET}      ${BOLD}${FLYMQ_CLUSTER_ADDR}${RESET}"
    fi
    if [[ -n "$FLYMQ_ADVERTISE_CLUSTER" ]]; then
        echo -e "  ${DIM}Advertise:${RESET}    ${BOLD}${FLYMQ_ADVERTISE_CLUSTER}${RESET}"
    fi
    echo -e "  ${DIM}Data:${RESET}         ${BOLD}${FLYMQ_DATA_DIR}${RESET}"
    if [[ -n "$FLYMQ_PEERS" ]]; then
        echo -e "  ${DIM}Peers:${RESET}        ${BOLD}$FLYMQ_PEERS${RESET}"
    fi
    echo ""
}

# -----------------------------------------------------------------------------
# Main Entrypoint
# -----------------------------------------------------------------------------
main() {
    # Display banner
    print_banner

    # Generate node ID
    generate_node_id

    # Setup advertise addresses (auto-detect IP for Docker/K8s/Cloud)
    setup_advertise_addresses

    # Handle role-specific initialization
    case "${FLYMQ_ROLE:-standalone}" in
        bootstrap|server)
            run_bootstrap
            ;;
        agent|join)
            run_agent
            ;;
        cluster|multi)
            run_cluster
            ;;
        standalone|single)
            run_standalone
            ;;
        *)
            print_error "Unknown role: $FLYMQ_ROLE"
            print_error "Valid roles: bootstrap, agent, cluster, standalone"
            exit 1
            ;;
    esac

    # Setup data directory
    setup_data_dir

    # Print configuration summary
    print_config

    # Start FlyMQ
    print_step "Starting FlyMQ server..."
    exec "$@"
}

main "$@"

