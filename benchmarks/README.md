# FlyMQ Performance Benchmarks

This directory contains comprehensive benchmarking tools to compare FlyMQ performance against Apache Kafka.

## Overview

The benchmark suite measures:
- **Throughput**: Messages per second and MB/s
- **Latency**: p50, p95, p99, min, max, and average latencies
- **Scalability**: Performance under various concurrency levels
- **Message Integrity**: Verification that messages are correctly produced and stored

## Prerequisites

### Required Software
- **Go 1.21+**: For building the benchmark tool
- **Docker & Docker Compose**: For running Kafka instances
- **FlyMQ**: Built and available (run `./install.sh` from project root)

### System Requirements
- Minimum 4 CPU cores recommended
- At least 4GB RAM available
- SSD storage recommended for accurate results

## Quick Start

### 1. Build the Benchmark Tool

```bash
cd benchmarks
go build -o benchmark benchmark.go
```

### 2. Start Kafka (Standalone Mode)

```bash
# Start standalone Kafka
docker compose --profile standalone up -d

# Wait for Kafka to be ready (about 30 seconds)
docker compose --profile standalone logs -f kafka-standalone
# Look for "Kafka Server started" message, then Ctrl+C
```

### 3. Start FlyMQ

```bash
# In a separate terminal, from project root
./bin/flymq
```

### 4. Run Benchmarks

```bash
# Quick test (2 tests, ~30 seconds)
./benchmark -quick

# Full standalone benchmark
./benchmark -mode=standalone

# Full cluster benchmark (requires cluster setup)
./benchmark -mode=cluster

# Run both modes
./benchmark -mode=both
```

## Benchmark Modes

### Standalone Mode
Compares single-node FlyMQ against single-node Kafka:
- FlyMQ: `localhost:19092`
- Kafka: `localhost:29092`

### Cluster Mode
Compares 2-node FlyMQ cluster against 2-node Kafka cluster:
- FlyMQ: `localhost:19092`, `localhost:19094`
- Kafka: `localhost:39092`, `localhost:39093`

## Command-Line Options

| Flag | Default | Description |
|------|---------|-------------|
| `-mode` | `standalone` | Benchmark mode: `standalone`, `cluster`, or `both` |
| `-quick` | `false` | Quick mode with fewer tests |
| `-verify` | `true` | Enable message integrity verification |
| `-warmup` | `2` | Number of warmup runs before benchmarking |
| `-output` | `benchmark-results.json` | Output file for JSON results |
| `-v` | `false` | Verbose output with error details |
| `-flymq-standalone` | `localhost:19092` | FlyMQ standalone address |
| `-kafka-standalone` | `localhost:29092` | Kafka standalone address |
| `-flymq-cluster` | `localhost:19092,localhost:19094` | FlyMQ cluster addresses |
| `-kafka-cluster` | `localhost:39092,localhost:39093` | Kafka cluster addresses |

## Test Configurations

The benchmark runs the following tests (in full mode):

| Test | Message Size | Count | Concurrency |
|------|-------------|-------|-------------|
| Tiny messages | 100B | 5,000 | 1 |
| Small messages | 1KB | 5,000 | 1 |
| Medium messages | 10KB | 2,000 | 1 |
| Large messages | 100KB | 500 | 1 |
| Concurrent (4 workers) | 1KB | 10,000 | 4 |
| Concurrent (8 workers) | 1KB | 10,000 | 8 |
| High concurrency | 1KB | 20,000 | 16 |
| Sustained load | 1KB | 50,000 | 8 |

## Cluster Setup

### FlyMQ Cluster (2 nodes)

```bash
# Terminal 1: Node 1 (bootstrap)
FLYMQ_BIND_ADDR=:19092 FLYMQ_CLUSTER_ADDR=:19093 \
FLYMQ_DATA_DIR=./data-node1 FLYMQ_NODE_ID=node1 ./bin/flymq

# Terminal 2: Node 2 (join node 1)
FLYMQ_BIND_ADDR=:19094 FLYMQ_CLUSTER_ADDR=:19095 \
FLYMQ_DATA_DIR=./data-node2 FLYMQ_NODE_ID=node2 \
FLYMQ_CLUSTER_PEERS=localhost:19093 ./bin/flymq


## Understanding Results

### Output Format

The benchmark produces both terminal output and a JSON file with detailed results.

#### Terminal Output Example
```
[STANDALONE 1/8] Tiny messages (100B x 5000)
  Size: 100B | Count: 5.0K | Concurrency: 1
  ▶ FlyMQ: 33034 msgs/s | 3.15 MB/s | p50=0.03ms p99=0.09ms
  ▶ Kafka: 4869 msgs/s | 0.46 MB/s | p50=0.19ms p99=0.38ms
```

#### JSON Output Structure
```json
{
  "timestamp": "2026-01-15T10:00:00Z",
  "system_info": {
    "os": "darwin",
    "arch": "arm64",
    "cpus": 10,
    "go_version": "go1.25.5"
  },
  "results": [
    {
      "system": "FlyMQ",
      "mode": "standalone",
      "name": "Tiny messages (100B x 5000)",
      "message_size_bytes": 100,
      "message_count": 5000,
      "throughput_msgs_per_sec": 33034,
      "throughput_mb_per_sec": 3.15,
      "latency_p50_ms": 0.03,
      "latency_p99_ms": 0.09,
      "success": true
    }
  ]
}
```

### Interpreting Metrics

| Metric | Description | Good Values |
|--------|-------------|-------------|
| **Throughput (msgs/s)** | Messages processed per second | Higher is better |
| **Throughput (MB/s)** | Data throughput | Higher is better |
| **p50 Latency** | Median latency (50th percentile) | Lower is better, <1ms is excellent |
| **p99 Latency** | Tail latency (99th percentile) | Lower is better, <10ms is good |
| **Errors** | Failed operations | Should be 0 |

## Benchmark Results

> **Latest Run**: January 16, 2026
> **Test Environment**: Apple M3 Pro, 12 cores, 36GB RAM, NVMe SSD
> **Docker Containers**: Resource-limited to simulate production conditions

### Standalone Mode (Single Node)

| Test | FlyMQ | Kafka | FlyMQ Advantage |
|------|-------|-------|-----------------|
| Tiny (100B) | 7,431 msgs/s | 4,180 msgs/s | **1.78x faster** |
| Small (1KB) | 7,467 msgs/s | 4,733 msgs/s | **1.58x faster** |
| Medium (10KB) | 5,083 msgs/s | 2,105 msgs/s | **2.41x faster** |
| Large (100KB) | 1,005 msgs/s | 916 msgs/s | **1.10x faster** |
| 4 workers | 20,272 msgs/s | 12,663 msgs/s | **1.60x faster** |
| 8 workers | 26,193 msgs/s | 16,411 msgs/s | **1.60x faster** |
| 16 workers | 30,689 msgs/s | 20,350 msgs/s | **1.51x faster** |
| Sustained | 25,166 msgs/s | 22,104 msgs/s | **1.14x faster** |

**Summary**: FlyMQ averages **15,413 msgs/s** vs Kafka **10,433 msgs/s** → **1.48x faster throughput** with **1.29x lower latency** (p50: 0.30ms vs 0.38ms)

### Cluster Mode (2 Nodes)

| Test | FlyMQ | Kafka | FlyMQ Advantage |
|------|-------|-------|-----------------|
| Tiny (100B) | 7,175 msgs/s | 2,818 msgs/s | **2.55x faster** |
| Small (1KB) | 7,114 msgs/s | 2,978 msgs/s | **2.39x faster** |
| Medium (10KB) | 4,660 msgs/s | 2,928 msgs/s | **1.59x faster** |
| Large (100KB) | 1,120 msgs/s | 849 msgs/s | **1.32x faster** |
| 4 workers | 19,023 msgs/s | 10,045 msgs/s | **1.89x faster** |
| 8 workers | 23,203 msgs/s | 13,745 msgs/s | **1.69x faster** |
| 16 workers | 28,027 msgs/s | 18,218 msgs/s | **1.54x faster** |
| Sustained | 22,333 msgs/s | 13,708 msgs/s | **1.63x faster** |

**Summary**: FlyMQ averages **14,082 msgs/s** vs Kafka **8,161 msgs/s** → **1.73x faster throughput** with **1.53x lower latency** (p50: 0.31ms vs 0.47ms)

### Cluster Mode (3 Nodes)

| Test | FlyMQ | Kafka | FlyMQ Advantage |
|------|-------|-------|-----------------|
| Tiny (100B) | 6,337 msgs/s | 1,899 msgs/s | **3.34x faster** |
| Small (1KB) | 6,781 msgs/s | 2,700 msgs/s | **2.51x faster** |
| Medium (10KB) | 4,426 msgs/s | 2,305 msgs/s | **1.92x faster** |
| Large (100KB) | 1,062 msgs/s | 734 msgs/s | **1.45x faster** |
| 4 workers | 13,297 msgs/s | 3,846 msgs/s | **3.46x faster** |
| 8 workers | 18,119 msgs/s | 9,460 msgs/s | **1.92x faster** |
| 16 workers | 22,713 msgs/s | 14,952 msgs/s | **1.52x faster** |
| Sustained | 21,357 msgs/s | 12,105 msgs/s | **1.76x faster** |

**Summary**: FlyMQ averages **11,762 msgs/s** vs Kafka **6,000 msgs/s** → **1.96x faster throughput** with **1.86x lower latency** (p50: 0.35ms vs 0.64ms)

## Cleanup

```bash
# Stop Kafka containers
docker compose --profile standalone down
docker compose --profile cluster down

# Remove volumes (optional)
docker compose --profile all down -v
```

## Troubleshooting

### Kafka Not Starting
```bash
# Check logs
docker compose --profile standalone logs kafka-standalone

# Restart with clean state
docker compose --profile standalone down -v
docker compose --profile standalone up -d
```

### Connection Refused Errors
- Ensure FlyMQ is running before starting benchmarks
- Wait for Kafka to fully start (check health status)
- Verify ports are not in use by other applications

### Inconsistent Results
- Close other applications to reduce system load
- Run benchmarks multiple times and average results
- Increase warmup runs with `-warmup=5`
