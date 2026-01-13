#!/bin/bash
# FlyMQ Benchmark Runner
# Runs benchmarks for standalone, 2-node, and 3-node cluster configurations

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="/tmp/flymq-benchmark"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Cleanup function
cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    pkill -f "flymq.*benchmark" 2>/dev/null || true
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" down 2>/dev/null || true
    rm -rf "$DATA_DIR"
}

trap cleanup EXIT

# Build FlyMQ
build_flymq() {
    echo -e "${BLUE}Building FlyMQ...${NC}"
    cd "$PROJECT_DIR"
    go build -o bin/flymq ./cmd/flymq
    go build -o benchmarks/benchmark ./benchmarks
    echo -e "${GREEN}Build complete${NC}"
}

# Start FlyMQ standalone
start_standalone() {
    echo -e "${BLUE}Starting FlyMQ standalone...${NC}"
    mkdir -p "$DATA_DIR/standalone"
    FLYMQ_BIND_ADDR=":19092" \
    FLYMQ_DATA_DIR="$DATA_DIR/standalone" \
    FLYMQ_LOG_LEVEL="info" \
    "$PROJECT_DIR/bin/flymq" &
    sleep 2
    echo -e "${GREEN}FlyMQ standalone started on port 19092${NC}"
}

# Start FlyMQ 2-node cluster
start_2node_cluster() {
    echo -e "${BLUE}Starting FlyMQ 2-node cluster...${NC}"
    mkdir -p "$DATA_DIR/node1" "$DATA_DIR/node2"

    # Node 1
    FLYMQ_NODE_ID="node1" \
    FLYMQ_BIND_ADDR=":19092" \
    FLYMQ_CLUSTER_ADDR=":19093" \
    FLYMQ_ADVERTISE_ADDR="127.0.0.1:19092" \
    FLYMQ_ADVERTISE_CLUSTER="127.0.0.1:19093" \
    FLYMQ_DATA_DIR="$DATA_DIR/node1" \
    FLYMQ_PEERS="127.0.0.1:19095" \
    FLYMQ_LOG_LEVEL="info" \
    "$PROJECT_DIR/bin/flymq" &

    sleep 1

    # Node 2
    FLYMQ_NODE_ID="node2" \
    FLYMQ_BIND_ADDR=":19094" \
    FLYMQ_CLUSTER_ADDR=":19095" \
    FLYMQ_ADVERTISE_ADDR="127.0.0.1:19094" \
    FLYMQ_ADVERTISE_CLUSTER="127.0.0.1:19095" \
    FLYMQ_DATA_DIR="$DATA_DIR/node2" \
    FLYMQ_PEERS="127.0.0.1:19093" \
    FLYMQ_LOG_LEVEL="info" \
    "$PROJECT_DIR/bin/flymq" &

    sleep 3
    echo -e "${GREEN}FlyMQ 2-node cluster started (ports 19092, 19094)${NC}"
}

# Start FlyMQ 3-node cluster
start_3node_cluster() {
    echo -e "${BLUE}Starting FlyMQ 3-node cluster...${NC}"
    mkdir -p "$DATA_DIR/node1" "$DATA_DIR/node2" "$DATA_DIR/node3"
    
    # Node 1
    "$PROJECT_DIR/bin/flymq" \
        --node-id node1 \
        --port 19092 \
        --cluster-port 19093 \
        --data-dir "$DATA_DIR/node1" \
        --peers "127.0.0.1:19095,127.0.0.1:19097" \
        --log-level info &
    
    sleep 1
    
    # Node 2
    "$PROJECT_DIR/bin/flymq" \
        --node-id node2 \
        --port 19094 \
        --cluster-port 19095 \
        --data-dir "$DATA_DIR/node2" \
        --peers "127.0.0.1:19093,127.0.0.1:19097" \
        --log-level info &
    
    sleep 1
    
    # Node 3
    "$PROJECT_DIR/bin/flymq" \
        --node-id node3 \
        --port 19096 \
        --cluster-port 19097 \
        --data-dir "$DATA_DIR/node3" \
        --peers "127.0.0.1:19093,127.0.0.1:19095" \
        --log-level info &
    
    sleep 3
    echo -e "${GREEN}FlyMQ 3-node cluster started (ports 19092, 19094, 19096)${NC}"
}

# Start Kafka for comparison
start_kafka() {
    local profile=$1
    echo -e "${BLUE}Starting Kafka ($profile)...${NC}"
    cd "$SCRIPT_DIR"
    docker compose --profile "$profile" up -d
    echo -e "${YELLOW}Waiting for Kafka to be ready...${NC}"
    sleep 30
    echo -e "${GREEN}Kafka started${NC}"
}

# Run benchmark
run_benchmark() {
    local mode=$1
    local output=$2
    echo -e "${BLUE}Running benchmark: $mode${NC}"
    cd "$SCRIPT_DIR"
    ./benchmark --mode "$mode" --output "$output" --warmup 2
}

# Main
case "${1:-all}" in
    standalone)
        build_flymq
        cleanup
        start_standalone
        start_kafka "standalone"
        run_benchmark "standalone" "benchmark-standalone.json"
        ;;
    cluster-2)
        build_flymq
        cleanup
        start_2node_cluster
        start_kafka "cluster-2"
        run_benchmark "cluster" "benchmark-2node.json"
        ;;
    cluster-3)
        build_flymq
        cleanup
        start_3node_cluster
        start_kafka "cluster-3"
        run_benchmark "cluster" "benchmark-3node.json"
        ;;
    all)
        build_flymq
        echo -e "${BLUE}=== STANDALONE BENCHMARK ===${NC}"
        cleanup
        start_standalone
        start_kafka "standalone"
        run_benchmark "standalone" "benchmark-standalone.json"
        
        echo -e "${BLUE}=== 2-NODE CLUSTER BENCHMARK ===${NC}"
        cleanup
        start_2node_cluster
        start_kafka "cluster-2"
        run_benchmark "cluster" "benchmark-2node.json"
        
        echo -e "${BLUE}=== 3-NODE CLUSTER BENCHMARK ===${NC}"
        cleanup
        start_3node_cluster
        start_kafka "cluster-3"
        run_benchmark "cluster" "benchmark-3node.json"
        ;;
    *)
        echo "Usage: $0 [standalone|cluster-2|cluster-3|all]"
        exit 1
        ;;
esac

echo -e "${GREEN}Benchmark complete!${NC}"

