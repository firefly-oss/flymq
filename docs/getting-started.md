# Getting Started with FlyMQ

This guide will help you get up and running with FlyMQ quickly.

## Installation

### Using Go Modules

```bash
go get github.com/firefly-oss/flymq
```

### Building from Source

```bash
git clone https://github.com/firefly-oss/flymq.git
cd flymq
go build ./...
```

## Quick Start

### Embedded Mode

The simplest way to use FlyMQ is in embedded mode, where the server runs within your application.

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/firefly-oss/flymq/pkg/flymq"
)

func main() {
    // Create and start the server
    server := flymq.NewServer(
        flymq.WithDataDir("./data"),
        flymq.WithDefaultPartitions(3),
    )
    
    if err := server.Start(); err != nil {
        log.Fatal(err)
    }
    defer server.Stop()

    // Create a topic
    if err := server.CreateTopic("events", 3); err != nil {
        log.Fatal(err)
    }

    // Create a producer
    producer, err := server.NewProducer()
    if err != nil {
        log.Fatal(err)
    }
    defer producer.Close()

    // Send a message
    ctx := context.Background()
    msg := &flymq.Message{
        Topic: "events",
        Key:   []byte("user-123"),
        Value: []byte(`{"action": "login", "timestamp": "2024-01-15T10:30:00Z"}`),
    }
    
    meta, err := producer.Send(ctx, msg)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Message sent to partition %d at offset %d", meta.Partition, meta.Offset)

    // Create a consumer
    consumer, err := server.NewConsumer(
        flymq.WithGroupID("my-group"),
        flymq.WithTopics("events"),
        flymq.WithOffsetReset(flymq.OffsetEarliest),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close()

    // Poll for messages
    messages, err := consumer.Poll(5 * time.Second)
    if err != nil {
        log.Fatal(err)
    }
    
    for _, m := range messages {
        log.Printf("Received: %s", string(m.Value))
    }
}
```

### Interactive Installation (Recommended)

The interactive installer provides a guided setup experience:

```bash
git clone https://github.com/firefly-oss/flymq
cd flymq
./install.sh
```

The installer will:
1. Ask for deployment mode (standalone or cluster)
2. For cluster mode: discover existing nodes via mDNS or accept manual peer entry
3. Show a configuration summary with all settings
4. Allow iterative modification of any section
5. Build, install, and configure the system service

**Quick install with defaults:**

```bash
./install.sh --yes
```

### Standalone Server Mode

For production deployments, run FlyMQ as a standalone server.

**Start the server:**

```bash
# Default: JSON logs (ideal for production/log aggregators)
flymq --config /etc/flymq/flymq.json

# Human-readable logs (for development)
flymq --config /etc/flymq/flymq.json -human-readable

# Quiet mode: logs only, no banner (ideal for containers)
flymq --config /etc/flymq/flymq.json -quiet
```

**Example flymq.json:**

```json
{
  "bind_addr": ":9092",
  "data_dir": "/var/lib/flymq",
  "log_level": "info",
  "segment_bytes": 67108864,
  "retention_bytes": 10737418240,
  "performance": {
    "acks": "leader",
    "sync_interval_ms": 5
  },
  "observability": {
    "metrics": { "enabled": true, "addr": ":9094" },
    "health": { "enabled": true, "addr": ":9095" }
  }
}
```

**Connect from a client:**

```go
client, err := flymq.NewClient("localhost:9092")
if err != nil {
    log.Fatal(err)
}
defer client.Close()

producer, err := client.NewProducer()
// ... use producer as shown above
```

**Connect with authentication:**

```go
client, err := flymq.NewClientWithOptions("localhost:9092", flymq.ClientOptions{
    Username: "myuser",
    Password: "mypassword",
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

**Using the CLI with authentication:**

```bash
# Set credentials via environment variables
export FLYMQ_USERNAME=admin
export FLYMQ_PASSWORD=secret

# Or pass them as flags
flymq-cli produce my-topic "Hello" --username admin --password secret

# Check authentication status
flymq-cli whoami
```

**Admin API authentication (for cluster management commands):**

```bash
# Set Admin API credentials via environment variables
export FLYMQ_ADMIN_USER=admin
export FLYMQ_ADMIN_PASS=secret

# Or pass them as flags
flymq-cli cluster status --admin-user admin --admin-pass secret

# With HTTPS enabled on Admin API
flymq-cli cluster status --admin-tls --admin-ca-cert /etc/flymq/ca.crt \
  --admin-user admin --admin-pass secret

# With self-signed certificate (skip verification)
flymq-cli cluster status --admin-tls --admin-insecure \
  --admin-user admin --admin-pass secret
```

> **Note:** The binary protocol authentication (`--username`, `--password`) and Admin API authentication (`--admin-user`, `--admin-pass`) are separate. Commands that use the binary protocol (produce, consume, subscribe) use the former, while commands that call the Admin API (cluster, topics list, groups) use the latter.

## Common Patterns

### Producer with Retries

```go
producer, err := server.NewProducer(
    flymq.WithRetries(3),
    flymq.WithAcks(flymq.AcksAll),
)
```

### Batched Producer

```go
producer, err := server.NewProducer(
    flymq.WithBatchSize(100),
    flymq.WithLingerMs(10),
)
```

### Consumer Group

```go
// Multiple consumers in the same group share partitions
consumer1, _ := server.NewConsumer(
    flymq.WithGroupID("analytics"),
    flymq.WithTopics("events"),
)

consumer2, _ := server.NewConsumer(
    flymq.WithGroupID("analytics"),
    flymq.WithTopics("events"),
)
// Partitions are automatically distributed between consumer1 and consumer2
```

### Manual Offset Commit

```go
consumer, err := server.NewConsumer(
    flymq.WithGroupID("processor"),
    flymq.WithTopics("events"),
    flymq.WithAutoCommit(false),
)

for {
    messages, _ := consumer.Poll(100 * time.Millisecond)
    for _, msg := range messages {
        if err := processMessage(msg); err != nil {
            log.Printf("processing failed: %v", err)
            continue
        }
        // Commit only after successful processing
        consumer.CommitMessage(msg)
    }
}
```

## Advanced Features

FlyMQ supports several advanced messaging patterns out of the box.

### Delayed Messages

Schedule messages for future delivery:

```bash
# CLI: Deliver message after 5 seconds
flymq-cli produce-delayed my-topic "Delayed message" 5000
```

```go
// Go client
offset, err := client.ProduceDelayed("my-topic", []byte("message"), 5000)
```

### Message TTL

Set expiration time for messages:

```bash
# CLI: Message expires after 60 seconds
flymq-cli produce-ttl my-topic "Expiring message" 60000
```

```go
// Go client
offset, err := client.ProduceWithTTL("my-topic", []byte("message"), 60000)
```

### Dead Letter Queue

Handle failed messages:

```bash
# List DLQ messages
flymq-cli dlq list my-topic

# Replay a failed message
flymq-cli dlq replay my-topic <message-id>

# Purge all DLQ messages
flymq-cli dlq purge my-topic
```

```go
// Go client
messages, err := client.FetchDLQ("my-topic", 100)
err = client.ReplayDLQ("my-topic", messageID)
err = client.PurgeDLQ("my-topic")
```

### Transactions

Atomic multi-message operations:

```bash
# CLI: Produce multiple messages atomically
flymq-cli txn my-topic "message1" "message2" "message3"
```

```go
// Go client
txn, err := client.BeginTransaction()
txn.Produce("topic1", []byte("msg1"))
txn.Produce("topic2", []byte("msg2"))
err = txn.Commit()  // or txn.Rollback()
```

See the [API Reference](./api-reference.md) for complete documentation of these features.

## Next Steps

- Read the [Architecture Guide](./architecture.md) to understand FlyMQ internals
- Check the [API Reference](./api-reference.md) for complete API documentation
- Review the [Configuration Guide](./configuration.md) for all configuration options
- Explore the [Admin REST API](./admin-api.md) for cluster management
- Set up monitoring with the [Observability Guide](./observability.md)
- Understand trade-offs in [Design Decisions](./design-decisions.md)
- See [Examples](../examples/) for more usage patterns

