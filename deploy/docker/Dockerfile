# =============================================================================
# FlyMQ Docker Image
# =============================================================================
#
# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0
#
# Multi-stage build for optimized production containers.
#
# ROLES:
#   bootstrap  - First cluster node, generates join tokens for agents
#   agent      - Remote node that joins an existing cluster via token
#   standalone - Single node, no clustering (default)
#
# USAGE:
#   # Build
#   docker build -t flymq:latest -f deploy/docker/Dockerfile .
#
#   # Bootstrap (first node)
#   docker run -d -p 9092:9092 -p 9093:9093 -e FLYMQ_ROLE=bootstrap flymq:latest
#
#   # Agent (join cluster)
#   docker run -d -e FLYMQ_ROLE=agent \
#     -e FLYMQ_SERVER=<bootstrap-ip>:9093 \
#     -e FLYMQ_JOIN_TOKEN=<token> flymq:latest
#
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Build
# -----------------------------------------------------------------------------
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /build

# Copy dependency files first for better layer caching
COPY go.mod go.sum* ./
RUN go mod download 2>/dev/null || true

# Copy source code
COPY . .

# Build FlyMQ server with version info
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-w -s -X flymq/internal/banner.Version=$(git describe --tags --always 2>/dev/null || echo 'dev')" \
    -o /build/bin/flymq cmd/flymq/main.go

# Build CLI tool
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-w -s" \
    -o /build/bin/flymq-cli cmd/flymq-cli/main.go 2>/dev/null || true

# Build discovery tool
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-w -s" \
    -o /build/bin/flymq-discover cmd/flymq-discover/main.go 2>/dev/null || true

# -----------------------------------------------------------------------------
# Stage 2: Runtime
# -----------------------------------------------------------------------------
FROM alpine:3.19

# OCI Image Labels
LABEL org.opencontainers.image.title="FlyMQ"
LABEL org.opencontainers.image.description="High-Performance Distributed Message Queue"
LABEL org.opencontainers.image.vendor="Firefly Software Solutions Inc."
LABEL org.opencontainers.image.url="https://github.com/firefly-oss/flymq"
LABEL org.opencontainers.image.source="https://github.com/firefly-oss/flymq"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    curl \
    bash \
    jq \
    tzdata \
    openssl \
    && rm -rf /var/cache/apk/*

# Create non-root user for security
RUN addgroup -g 1000 flymq && \
    adduser -u 1000 -G flymq -h /home/flymq -D flymq

# Create required directories
RUN mkdir -p /data /config /app /var/lib/flymq && \
    chown -R flymq:flymq /data /config /app /var/lib/flymq

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /build/bin/flymq /app/flymq
COPY --from=builder /build/bin/flymq-cli /app/flymq-cli
COPY --from=builder /build/bin/flymq-discover /app/flymq-discover

# Copy banner for display
COPY --from=builder /build/internal/banner/banner.txt /app/banner.txt

# Copy entrypoint scripts
COPY deploy/docker/docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh /app/flymq /app/flymq-cli /app/flymq-discover

# Add to PATH
ENV PATH="/app:${PATH}"

# -----------------------------------------------------------------------------
# Default Configuration
# -----------------------------------------------------------------------------
ENV FLYMQ_DATA_DIR=/data
ENV FLYMQ_BIND_ADDR=0.0.0.0:9092
ENV FLYMQ_CLUSTER_ADDR=0.0.0.0:9093
ENV FLYMQ_LOG_LEVEL=info
ENV FLYMQ_HEALTH_ENABLED=true
ENV FLYMQ_HEALTH_ADDR=0.0.0.0:9095
ENV FLYMQ_ADMIN_ENABLED=true
ENV FLYMQ_ADMIN_ADDR=0.0.0.0:9096
ENV FLYMQ_ROLE=standalone

# -----------------------------------------------------------------------------
# Ports
# -----------------------------------------------------------------------------
# 9092 - Client connections (MQTT/Custom protocol)
# 9093 - Cluster communication (Raft consensus + Gossip)
# 9095 - Health check endpoint
# 9096 - Admin REST API
EXPOSE 9092 9093 9095 9096

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -sf http://localhost:9095/health || exit 1

# Data volume
VOLUME ["/data"]

# Run as non-root
USER flymq

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["/app/flymq"]

