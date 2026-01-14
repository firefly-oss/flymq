#!/bin/bash
# =============================================================================
# FlyMQ Enterprise Setup - Rancher-Style Deployment
# =============================================================================
#
# Simple two-command cluster deployment:
#
# 1. Start server (outputs a join command):
#    ./setup-enterprise.sh server
#
# 2. On agent nodes, run the join command:
#    ./setup-enterprise.sh agent --bundle <path-to-bundle.tar.gz>
#
# The server creates a "join bundle" containing everything agents need.
# Just copy the bundle file to agent nodes - no manual cert/key management!
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$(dirname "$DEPLOY_DIR")")"
CERTS_DIR="$DEPLOY_DIR/certs"
ENV_FILE="$DEPLOY_DIR/.env.enterprise"
BANNER_FILE="$PROJECT_ROOT/internal/banner/banner.txt"
BUNDLE_DIR="$DEPLOY_DIR/join-bundle"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

print_step() { echo -e "${CYAN}→${NC} $1"; }
print_success() { echo -e "${GREEN}✓${NC} $1"; }
print_warning() { echo -e "${YELLOW}⚠${NC} $1"; }
print_error() { echo -e "${RED}✗${NC} $1" >&2; }
print_info() { echo -e "${DIM}ℹ${NC} $1"; }

show_banner() {
    echo ""
    if [[ -f "$BANNER_FILE" ]]; then
        while IFS= read -r line || [[ -n "$line" ]]; do
            printf -v padded_line "%-47s" "$line"
            echo -e "${CYAN}${BOLD}   ${MAGENTA}${padded_line}${CYAN}             ${NC}"
        done < "$BANNER_FILE"
    else
        echo -e "${CYAN}${BOLD}                      ${MAGENTA}FlyMQ${CYAN}                                  ${NC}"
    fi
    echo -e "${CYAN}${BOLD}            ${NC}${BOLD}Enterprise Cluster Deployment${CYAN}${BOLD}                   ${NC}"
    echo ""
}

show_usage() {
    echo -e "${BOLD}Usage:${NC}"
    echo "  $0 server [options]     Start a new FlyMQ server"
    echo "  $0 agent [options]      Join an existing cluster"
    echo "  $0 stop [--volumes]     Stop the local node"
    echo "  $0 status               Show node status"
    echo "  $0 logs                 Show logs"
    echo "  $0 bundle               Create/show join bundle for agents"
    echo ""
    echo -e "${BOLD}Server Options:${NC}"
    echo "  --admin-password PASS   Set admin password (default: auto-generated)"
    echo "  --node-id ID            Set node ID (default: flymq-server)"
    echo ""
    echo -e "${BOLD}Agent Options:${NC}"
    echo "  --bundle PATH           Path to join bundle from server (recommended)"
    echo "  --server HOST:PORT      Server address (if not using bundle)"
    echo "  --token TOKEN           Join token (if not using bundle)"
    echo ""
    echo -e "${BOLD}Rancher-Style Workflow:${NC}"
    echo ""
    echo "  ${BOLD}Step 1:${NC} Start server (creates join bundle automatically)"
    echo "    $0 server"
    echo ""
    echo "  ${BOLD}Step 2:${NC} Copy the join bundle to agent nodes"
    echo "    scp deploy/docker/flymq-join-bundle.tar.gz agent:/path/to/flymq/deploy/docker/"
    echo ""
    echo "  ${BOLD}Step 3:${NC} On each agent, run:"
    echo "    $0 agent --bundle deploy/docker/flymq-join-bundle.tar.gz"
    echo ""
}

# Generate a secure random token
generate_token() {
    openssl rand -base64 32 | tr -d '/+=' | head -c 32
}

# Generate a secure random password
generate_password() {
    openssl rand -base64 24 | tr -d '/+='
}

# =============================================================================
# CREATE JOIN BUNDLE - Single file containing everything agents need
# =============================================================================
create_join_bundle() {
    local server_addr="$1"  # Already includes port (e.g., 192.168.1.100:9093)
    local token="$2"
    local encryption_key="$3"
    local admin_password="$4"

    local bundle_dir="$DEPLOY_DIR/.join-bundle-tmp"
    local bundle_file="$DEPLOY_DIR/flymq-join-bundle.tar.gz"

    # Clean up any existing temp directory
    rm -rf "$bundle_dir"
    mkdir -p "$bundle_dir/certs"

    # Copy certificates
    cp "$CERTS_DIR"/*.crt "$bundle_dir/certs/" 2>/dev/null || true
    cp "$CERTS_DIR"/*.key "$bundle_dir/certs/" 2>/dev/null || true

    # Create join config file (sourced by agent setup)
    cat > "$bundle_dir/join-config.sh" << EOF
# FlyMQ Agent Join Configuration
# Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
# Copy this bundle to agent nodes and run:
#   ./setup-enterprise.sh agent --bundle flymq-join-bundle.tar.gz

FLYMQ_SERVER="$server_addr"
FLYMQ_JOIN_TOKEN="$token"
FLYMQ_ENCRYPTION_KEY="$encryption_key"
FLYMQ_AUTH_ADMIN_PASSWORD="$admin_password"
EOF
    chmod 600 "$bundle_dir/join-config.sh"

    # Create the bundle tarball
    tar -czf "$bundle_file" -C "$bundle_dir" . 2>/dev/null

    # Clean up temp directory
    rm -rf "$bundle_dir"

    # Set secure permissions on bundle
    chmod 600 "$bundle_file"
}

# =============================================================================
# SERVER MODE - Bootstrap a new cluster
# =============================================================================
setup_server() {
    local admin_password=""
    local node_id="flymq-server"
    local data_dir="./flymq-data"
    local enable_tls=true
    local run_tests=false

    # Parse server options
    while [[ $# -gt 0 ]]; do
        case $1 in
            --admin-password) admin_password="$2"; shift 2 ;;
            --node-id) node_id="$2"; shift 2 ;;
            --data-dir) data_dir="$2"; shift 2 ;;
            --no-tls) enable_tls=false; shift ;;
            --test) run_tests=true; shift ;;
            *) shift ;;
        esac
    done

    show_banner
    echo -e "${BOLD}Mode: ${GREEN}Server (Bootstrap)${NC}"
    echo ""

    # Generate admin password if not provided
    if [[ -z "$admin_password" ]]; then
        admin_password=$(generate_password)
        print_info "Generated admin password (save this!)"
    fi

    # Generate cluster token
    local cluster_token=$(generate_token)

    # Generate encryption key
    print_step "Generating encryption key..."
    local encryption_key=$("$SCRIPT_DIR/generate-encryption-key.sh" 2>/dev/null)
    print_success "Encryption key generated"

    # Generate TLS certificates
    if [[ "$enable_tls" == "true" ]]; then
        print_step "Generating TLS certificates..."
        if [[ ! -f "$CERTS_DIR/server.crt" ]]; then
            "$SCRIPT_DIR/generate-certs.sh" "$CERTS_DIR" >/dev/null 2>&1
            print_success "TLS certificates generated"
        else
            print_warning "TLS certificates already exist, reusing"
        fi
    fi

    # Save environment file
    print_step "Saving configuration..."
    cat > "$ENV_FILE" << EOF
# FlyMQ Enterprise Configuration
# Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
# Mode: Server

# Cluster credentials (KEEP SECURE!)
FLYMQ_CLUSTER_TOKEN=$cluster_token
FLYMQ_ENCRYPTION_KEY=$encryption_key
FLYMQ_AUTH_ADMIN_PASSWORD=$admin_password

# Node configuration
FLYMQ_NODE_ID=$node_id

# TLS
FLYMQ_TLS_ENABLED=$enable_tls
EOF
    chmod 600 "$ENV_FILE"
    print_success "Configuration saved to .env.enterprise"

    # Build Docker image
    print_step "Building FlyMQ Docker image..."
    cd "$PROJECT_ROOT"
    docker compose -f deploy/docker/docker-compose.enterprise.yml build --quiet 2>/dev/null
    print_success "Docker image built"

    # Create Docker volumes and copy certificates
    print_step "Setting up Docker volumes..."

    # Create the flymq-certs volume if it doesn't exist
    docker volume create flymq-certs >/dev/null 2>&1 || true

    # Copy certificates to the Docker volume with correct ownership (uid 1000 = flymq user)
    if [[ "$enable_tls" == "true" && -d "$CERTS_DIR" ]]; then
        print_step "Copying TLS certificates to Docker volume..."
        docker run --rm \
            -v flymq-certs:/certs \
            -v "$CERTS_DIR":/src:ro \
            alpine sh -c "cp -r /src/. /certs/ && chmod 644 /certs/*.crt && chmod 640 /certs/*.key && chown -R 1000:1000 /certs/" 2>/dev/null
        print_success "TLS certificates copied to flymq-certs volume"
    fi

    # Start server
    print_step "Starting FlyMQ server..."
    cd "$DEPLOY_DIR"

    # Export environment variables
    export FLYMQ_CLUSTER_TOKEN="$cluster_token"
    export FLYMQ_ENCRYPTION_KEY="$encryption_key"
    export FLYMQ_AUTH_ADMIN_PASSWORD="$admin_password"
    export FLYMQ_NODE_ID="$node_id"

    docker compose -f docker-compose.enterprise.yml up -d 2>/dev/null
    print_success "FlyMQ server started"

    # Wait for health
    print_step "Waiting for server to be ready..."
    local ready=false
    for i in {1..30}; do
        if curl -sf "http://localhost:9095/health" > /dev/null 2>&1; then
            ready=true
            break
        fi
        sleep 2
    done

    if [[ "$ready" == "true" ]]; then
        print_success "Server is ready!"
    else
        print_error "Server failed to start. Check logs with: $0 logs"
        exit 1
    fi

    # Run tests if requested
    if [[ "$run_tests" == "true" ]]; then
        print_step "Running end-to-end tests..."
        "$SCRIPT_DIR/e2e-test.sh"
    fi

    # Get external IP for agent connection
    local external_ip=$(curl -sf https://api.ipify.org 2>/dev/null || hostname -I 2>/dev/null | awk '{print $1}' || echo "YOUR_SERVER_IP")

    # Create join bundle (single file containing everything agents need)
    print_step "Creating join bundle for agents..."
    create_join_bundle "${external_ip}:9093" "$cluster_token" "$encryption_key" "$admin_password"
    local bundle_file="$DEPLOY_DIR/flymq-join-bundle.tar.gz"
    print_success "Join bundle created: $bundle_file"

    # Print connection info
    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}${BOLD}  ✓ FlyMQ Server Started Successfully${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  ${BOLD}Admin Credentials:${NC}"
    echo -e "    Username: ${CYAN}admin${NC}"
    echo -e "    Password: ${CYAN}$admin_password${NC}"
    echo ""
    echo -e "  ${BOLD}Endpoints:${NC}"
    echo -e "    Client:   ${CYAN}localhost:9092${NC} (TLS)"
    echo -e "    Health:   ${CYAN}http://localhost:9095/health${NC}"
    echo -e "    Metrics:  ${CYAN}http://localhost:9094/metrics${NC}"
    echo ""
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}${BOLD}  To Add Agent Nodes (Rancher-style):${NC}"
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  ${BOLD}Step 1:${NC} Copy the join bundle to agent nodes:"
    echo -e "    ${CYAN}scp $bundle_file agent-host:/path/to/flymq/deploy/docker/${NC}"
    echo ""
    echo -e "  ${BOLD}Step 2:${NC} On each agent, run:"
    echo -e "    ${CYAN}./deploy/docker/scripts/setup-enterprise.sh agent --bundle flymq-join-bundle.tar.gz${NC}"
    echo ""
    echo -e "  ${DIM}That's it! The bundle contains certificates, encryption key, and server address.${NC}"
    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}${BOLD}  CLI Example:${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  ${CYAN}./bin/flymq-cli --tls --ca-cert deploy/docker/certs/ca.crt \\\\${NC}"
    echo -e "  ${CYAN}  -u admin -P '$admin_password' topics${NC}"
    echo ""
}

# =============================================================================
# EXTRACT JOIN BUNDLE - Unpack bundle and set up certificates
# =============================================================================
extract_join_bundle() {
    local bundle_path="$1"

    # Handle relative paths
    if [[ ! "$bundle_path" = /* ]]; then
        bundle_path="$DEPLOY_DIR/$bundle_path"
    fi

    if [[ ! -f "$bundle_path" ]]; then
        print_error "Bundle file not found: $bundle_path"
        exit 1
    fi

    print_step "Extracting join bundle..."

    # Create certs directory
    mkdir -p "$CERTS_DIR"

    # Extract bundle to temp directory
    local tmp_dir="$DEPLOY_DIR/.bundle-extract-tmp"
    rm -rf "$tmp_dir"
    mkdir -p "$tmp_dir"

    tar -xzf "$bundle_path" -C "$tmp_dir" 2>/dev/null

    # Copy certificates
    if [[ -d "$tmp_dir/certs" ]]; then
        cp "$tmp_dir/certs"/* "$CERTS_DIR/" 2>/dev/null || true
        chmod 644 "$CERTS_DIR"/*.crt 2>/dev/null || true
        chmod 600 "$CERTS_DIR"/*.key 2>/dev/null || true
        print_success "Certificates extracted"
    fi

    # Source the join config
    if [[ -f "$tmp_dir/join-config.sh" ]]; then
        source "$tmp_dir/join-config.sh"
        print_success "Configuration loaded from bundle"
    fi

    # Cleanup
    rm -rf "$tmp_dir"
}

# =============================================================================
# AGENT MODE - Join an existing cluster
# =============================================================================
setup_agent() {
    local bundle_path=""
    local server_addr=""
    local join_token=""
    local node_id="flymq-agent-$(hostname -s 2>/dev/null || echo $RANDOM)"
    local encryption_key=""
    local admin_password=""

    # Parse agent options
    while [[ $# -gt 0 ]]; do
        case $1 in
            --bundle) bundle_path="$2"; shift 2 ;;
            --server) server_addr="$2"; shift 2 ;;
            --token) join_token="$2"; shift 2 ;;
            --node-id) node_id="$2"; shift 2 ;;
            --encryption-key) encryption_key="$2"; shift 2 ;;
            --admin-password) admin_password="$2"; shift 2 ;;
            *) shift ;;
        esac
    done

    show_banner
    echo -e "${BOLD}Mode: ${YELLOW}Agent (Join Cluster)${NC}"
    echo ""

    # If bundle provided, extract it first (this sets all the variables)
    if [[ -n "$bundle_path" ]]; then
        extract_join_bundle "$bundle_path"
        # Variables are now set from the bundle's join-config.sh
        server_addr="${server_addr:-$FLYMQ_SERVER}"
        join_token="${join_token:-$FLYMQ_JOIN_TOKEN}"
        encryption_key="${encryption_key:-$FLYMQ_ENCRYPTION_KEY}"
        admin_password="${admin_password:-$FLYMQ_AUTH_ADMIN_PASSWORD}"
    fi

    # Validate required options
    if [[ -z "$server_addr" ]]; then
        print_error "Missing server address"
        echo ""
        echo "  Use one of these methods:"
        echo "    $0 agent --bundle flymq-join-bundle.tar.gz"
        echo "    $0 agent --server <host:port> --token <token>"
        exit 1
    fi

    if [[ -z "$join_token" ]]; then
        print_error "Missing join token"
        echo ""
        echo "  Use one of these methods:"
        echo "    $0 agent --bundle flymq-join-bundle.tar.gz"
        echo "    $0 agent --server <host:port> --token <token>"
        exit 1
    fi

    # Check for certificates
    if [[ ! -f "$CERTS_DIR/ca.crt" ]]; then
        print_error "TLS certificates not found"
        echo ""
        echo "  Use the bundle method (recommended):"
        echo "    $0 agent --bundle flymq-join-bundle.tar.gz"
        echo ""
        echo "  Or manually copy certificates from the server:"
        echo "    scp -r server:/path/to/flymq/deploy/docker/certs/ $CERTS_DIR/"
        exit 1
    fi
    print_success "TLS certificates found"

    # Check for encryption key
    if [[ -z "$encryption_key" ]]; then
        if [[ -f "$ENV_FILE" ]]; then
            source "$ENV_FILE"
            encryption_key="$FLYMQ_ENCRYPTION_KEY"
        fi
    fi

    if [[ -z "$encryption_key" ]]; then
        print_error "Encryption key required"
        echo ""
        echo "  Use the bundle method (recommended):"
        echo "    $0 agent --bundle flymq-join-bundle.tar.gz"
        exit 1
    fi
    print_success "Encryption key configured"

    # Get admin password (optional for agents, use default if not provided)
    if [[ -z "$admin_password" ]]; then
        admin_password="agent-default"
    fi

    # Save agent environment file
    print_step "Saving agent configuration..."
    cat > "$ENV_FILE" << EOF
# FlyMQ Enterprise Configuration
# Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
# Mode: Agent

# Server connection
FLYMQ_SERVER=$server_addr
FLYMQ_JOIN_TOKEN=$join_token

# Cluster credentials
FLYMQ_ENCRYPTION_KEY=$encryption_key
FLYMQ_AUTH_ADMIN_PASSWORD=$admin_password

# Node configuration
FLYMQ_NODE_ID=$node_id
EOF
    chmod 600 "$ENV_FILE"
    print_success "Configuration saved"

    # Build Docker image
    print_step "Building FlyMQ Docker image..."
    cd "$PROJECT_ROOT"
    docker compose -f deploy/docker/docker-compose.enterprise-agent.yml build --quiet 2>/dev/null
    print_success "Docker image built"

    # Create Docker volumes and copy certificates
    print_step "Setting up Docker volumes..."
    docker volume create flymq-certs >/dev/null 2>&1 || true

    if [[ -d "$CERTS_DIR" ]]; then
        docker run --rm \
            -v flymq-certs:/certs \
            -v "$CERTS_DIR":/src:ro \
            alpine sh -c "cp -r /src/. /certs/ && chmod 644 /certs/*.crt && chmod 640 /certs/*.key && chown -R 1000:1000 /certs/" 2>/dev/null
        print_success "TLS certificates ready"
    fi

    # Start agent
    print_step "Starting FlyMQ agent..."
    cd "$DEPLOY_DIR"

    # Export environment variables
    export FLYMQ_SERVER="$server_addr"
    export FLYMQ_JOIN_TOKEN="$join_token"
    export FLYMQ_ENCRYPTION_KEY="$encryption_key"
    export FLYMQ_AUTH_ADMIN_PASSWORD="$admin_password"
    export FLYMQ_NODE_ID="$node_id"

    docker compose -f docker-compose.enterprise-agent.yml up -d 2>/dev/null
    print_success "FlyMQ agent started"

    # Wait for health
    print_step "Waiting for agent to be ready..."
    local ready=false
    for i in {1..30}; do
        if curl -sf "http://localhost:9095/health" > /dev/null 2>&1; then
            ready=true
            break
        fi
        sleep 2
    done

    if [[ "$ready" == "true" ]]; then
        print_success "Agent is ready and connected to cluster!"
    else
        print_warning "Agent started but health check pending. Check logs with: $0 logs"
    fi

    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}${BOLD}  ✓ FlyMQ Agent Started${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  ${BOLD}Node ID:${NC}     ${CYAN}$node_id${NC}"
    echo -e "  ${BOLD}Server:${NC}      ${CYAN}$server_addr${NC}"
    echo -e "  ${BOLD}Health:${NC}      ${CYAN}http://localhost:9095/health${NC}"
    echo ""
}

# =============================================================================
# UTILITY COMMANDS
# =============================================================================
cmd_stop() {
    local clean_volumes=false

    # Parse options
    while [[ $# -gt 0 ]]; do
        case $1 in
            --volumes|-v) clean_volumes=true; shift ;;
            *) shift ;;
        esac
    done

    print_step "Stopping FlyMQ..."
    cd "$DEPLOY_DIR"

    # Try both compose files
    if [[ "$clean_volumes" == "true" ]]; then
        docker compose -f docker-compose.enterprise.yml down -v 2>/dev/null || true
        docker compose -f docker-compose.enterprise-agent.yml down -v 2>/dev/null || true
        print_success "FlyMQ stopped and volumes removed"
    else
        docker compose -f docker-compose.enterprise.yml down 2>/dev/null || true
        docker compose -f docker-compose.enterprise-agent.yml down 2>/dev/null || true
        print_success "FlyMQ stopped"
        print_info "Use '$0 stop --volumes' to also remove data volumes"
    fi
}

cmd_status() {
    echo ""
    echo -e "${BOLD}FlyMQ Cluster Status${NC}"
    echo ""

    # Check if running
    if curl -sf "http://localhost:9095/health" > /dev/null 2>&1; then
        local health=$(curl -sf "http://localhost:9095/health")
        echo -e "  ${GREEN}●${NC} Node is ${GREEN}running${NC}"
        echo ""
        echo -e "  ${BOLD}Health:${NC}"
        echo "$health" | jq -r '
            "    Status:  \(.status)",
            "    Version: \(.version)"
        ' 2>/dev/null || echo "    $health"
    else
        echo -e "  ${RED}●${NC} Node is ${RED}not running${NC}"
    fi
    echo ""

    # Show containers
    echo -e "  ${BOLD}Containers:${NC}"
    docker ps --filter "name=flymq" --format "    {{.Names}}: {{.Status}}" 2>/dev/null || echo "    None"
    echo ""

    # Show volumes
    echo -e "  ${BOLD}Volumes:${NC}"
    docker volume ls --filter "name=flymq" --format "    {{.Name}}" 2>/dev/null || echo "    None"
    echo ""
}

cmd_logs() {
    local container=$(docker ps --filter "name=flymq" --format "{{.Names}}" | head -1)
    if [[ -n "$container" ]]; then
        docker logs -f "$container"
    else
        print_error "No FlyMQ container running"
        exit 1
    fi
}

cmd_token() {
    if [[ -f "$ENV_FILE" ]]; then
        source "$ENV_FILE"
        if [[ -n "$FLYMQ_CLUSTER_TOKEN" ]]; then
            echo ""
            echo -e "${BOLD}Current Join Token:${NC}"
            echo -e "  ${CYAN}$FLYMQ_CLUSTER_TOKEN${NC}"
            echo ""
        else
            print_error "No cluster token found. Is this a server node?"
        fi
    else
        print_error "No configuration found. Run 'server' first."
    fi
}

cmd_bundle() {
    local bundle_file="$DEPLOY_DIR/flymq-join-bundle.tar.gz"

    if [[ -f "$bundle_file" ]]; then
        echo ""
        echo -e "${BOLD}Join Bundle:${NC}"
        echo -e "  ${CYAN}$bundle_file${NC}"
        echo ""
        echo -e "${BOLD}Bundle Contents:${NC}"
        tar -tzf "$bundle_file" 2>/dev/null | while read line; do
            echo "    $line"
        done
        echo ""
        echo -e "${BOLD}To add agent nodes:${NC}"
        echo ""
        echo -e "  ${BOLD}Step 1:${NC} Copy the bundle to agent nodes:"
        echo -e "    ${CYAN}scp $bundle_file agent:/path/to/flymq/deploy/docker/${NC}"
        echo ""
        echo -e "  ${BOLD}Step 2:${NC} On each agent, run:"
        echo -e "    ${CYAN}./deploy/docker/scripts/setup-enterprise.sh agent --bundle flymq-join-bundle.tar.gz${NC}"
        echo ""
    else
        print_error "No join bundle found. Run 'server' first to create one."
        echo ""
        echo "  The server command automatically creates a join bundle at:"
        echo "    $bundle_file"
        echo ""
    fi
}

cmd_export_certs() {
    local output_dir="${1:-./flymq-certs-export}"

    print_step "Exporting TLS certificates..."

    # Check if certs exist in local directory
    if [[ -d "$CERTS_DIR" && -f "$CERTS_DIR/ca.crt" ]]; then
        mkdir -p "$output_dir"
        cp -r "$CERTS_DIR"/* "$output_dir/"
        chmod 644 "$output_dir"/*.crt 2>/dev/null || true
        chmod 600 "$output_dir"/*.key 2>/dev/null || true
        print_success "Certificates exported to: $output_dir"
        echo ""
        echo -e "  ${BOLD}Files exported:${NC}"
        ls -la "$output_dir" | tail -n +2 | while read line; do
            echo "    $line"
        done
        echo ""
        echo -e "  ${BOLD}To use on agent nodes:${NC}"
        echo "    1. Copy this directory to the agent node"
        echo "    2. Place in deploy/docker/certs/"
        echo "    3. Run: $0 agent --server <server>:9093 --token <token>"
        echo ""
    else
        # Try to export from Docker volume
        if docker volume inspect flymq-certs >/dev/null 2>&1; then
            mkdir -p "$output_dir"
            docker run --rm \
                -v flymq-certs:/certs:ro \
                -v "$(cd "$output_dir" && pwd)":/export \
                alpine sh -c "cp -r /certs/. /export/" 2>/dev/null
            chmod 644 "$output_dir"/*.crt 2>/dev/null || true
            chmod 600 "$output_dir"/*.key 2>/dev/null || true
            print_success "Certificates exported from Docker volume to: $output_dir"
        else
            print_error "No certificates found. Run 'server' first to generate certificates."
            exit 1
        fi
    fi
}

# =============================================================================
# MAIN COMMAND DISPATCHER
# =============================================================================
main() {
    if [[ $# -lt 1 ]]; then
        show_banner
        show_usage
        exit 0
    fi

    local command="$1"
    shift

    case "$command" in
        server)
            setup_server "$@"
            ;;
        agent)
            setup_agent "$@"
            ;;
        stop)
            cmd_stop "$@"
            ;;
        status)
            cmd_status
            ;;
        logs)
            cmd_logs
            ;;
        token)
            cmd_token
            ;;
        bundle)
            cmd_bundle
            ;;
        export-certs)
            cmd_export_certs "$@"
            ;;
        help|--help|-h)
            show_banner
            show_usage
            ;;
        *)
            print_error "Unknown command: $command"
            show_usage
            exit 1
            ;;
    esac
}

main "$@"
