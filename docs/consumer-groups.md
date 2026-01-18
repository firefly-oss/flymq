# Consumer Groups and Offset Management

This guide explains how FlyMQ handles message consumption, consumer groups, and offset tracking.

## Quick Reference

| Concept | Description | Required? |
|---------|-------------|-----------|
| **Consumer Group** | A named group of consumers that share message processing | Optional (defaults to auto-generated) |
| **Offset** | Position in the message log (0, 1, 2, ...) | Tracked automatically |
| **Commit** | Saving your position so you don't reprocess messages | Automatic by default |

## The Two Ways to Consume Messages

### 1. Simple Consumption (No Tracking)

Use this when you just want to read messages without tracking progress:

```bash
# CLI: Read 10 messages starting from offset 0
flymq-cli consume my-topic --offset 0 --count 10
```

```python
# Python: One-time read
client = FlyMQClient("localhost:9092")
messages = client.fetch("my-topic", partition=0, offset=0, count=10)
```

**When to use**: Debugging, one-time reads, or when you manage offsets yourself.

### 2. Consumer Groups (Recommended)

Use this for production workloads. FlyMQ tracks your progress automatically:

```bash
# CLI: Subscribe with a consumer group
flymq-cli subscribe my-topic --group my-app
```

```python
# Python: High-level consumer
with client.consumer("my-topic", group_id="my-app") as consumer:
    for message in consumer:
        process(message)
        # Offset is committed automatically
```

**When to use**: Production applications, when you need exactly-once processing.

## How Consumer Groups Work

### The Problem They Solve

Without consumer groups:
1. You read message at offset 5
2. Your app crashes
3. You restart - where do you continue from?

With consumer groups:
1. You read message at offset 5
2. FlyMQ commits offset 6 (next message to read)
3. Your app crashes
4. You restart - FlyMQ tells you to continue from offset 6

### Visual Example

```
Topic: orders
┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐
│  0  │  1  │  2  │  3  │  4  │  5  │  6  │  7  │  ← Messages
└─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘
                          ↑
                    Committed Offset = 5
                    (Consumer will resume here)
```

### Multiple Consumers in a Group

When multiple consumers share the same group ID, partitions are distributed:

```
Consumer Group: "order-processors"

Consumer A ──────► Partition 0
Consumer B ──────► Partition 1
Consumer C ──────► Partition 2

If Consumer B crashes:
Consumer A ──────► Partition 0, Partition 1 (rebalanced)
Consumer C ──────► Partition 2
```

## Offset Reset Modes

When a consumer group has no committed offset (first time consuming):

| Mode | Behavior | Use Case |
|------|----------|----------|
| `earliest` | Start from first message | Process all historical data |
| `latest` | Start from new messages only | Only care about new data |
| `committed` | Resume from last commit | Normal operation after restart |

```bash
# CLI examples
flymq-cli subscribe my-topic --group my-app --from-beginning  # earliest
flymq-cli subscribe my-topic --group my-app --from-latest     # latest (default)
```

```python
# Python
consumer = client.consumer("my-topic", group_id="my-app", auto_offset_reset="earliest")
```

## Auto-Commit vs Manual Commit

### Auto-Commit (Default)

Offsets are committed automatically every 5 seconds:

```python
# Auto-commit enabled by default
with client.consumer("my-topic", group_id="my-app") as consumer:
    for message in consumer:
        process(message)  # If this crashes, message may be reprocessed
```

**Trade-off**: Simpler, but may reprocess messages if you crash between processing and commit.

### Manual Commit

You control exactly when offsets are committed:

```python
# Disable auto-commit
with client.consumer("my-topic", group_id="my-app", auto_commit=False) as consumer:
    for message in consumer:
        process(message)
        consumer.commit()  # Commit after successful processing
```

**Trade-off**: More control, but you must remember to commit.

## Best Practices

### 1. Always Use Consumer Groups in Production

```python
# ✅ Good: Uses consumer group
consumer = client.consumer("orders", group_id="order-service")

# ❌ Bad: No tracking, will reprocess on restart
messages = client.fetch("orders", offset=0)
```

### 2. Use Meaningful Group Names

```python
# ✅ Good: Descriptive name
consumer = client.consumer("orders", group_id="payment-processor")

# ❌ Bad: Generic name
consumer = client.consumer("orders", group_id="consumer1")
```

### 3. One Group Per Application Purpose

```python
# ✅ Good: Each service has its own group
analytics_consumer = client.consumer("orders", group_id="analytics-service")
billing_consumer = client.consumer("orders", group_id="billing-service")

# Both services receive ALL messages independently
```

### 4. Commit After Processing

```python
# ✅ Good: Commit after successful processing
for message in consumer:
    result = process(message)
    if result.success:
        consumer.commit()

# ❌ Bad: Commit before processing
for message in consumer:
    consumer.commit()  # If process() fails, message is lost!
    process(message)
```

## CLI Commands Reference

```bash
# Subscribe with consumer group (recommended for production)
flymq-cli subscribe <topic> --group <group-id> [options]

# Simple consume (for debugging)
flymq-cli consume <topic> --offset <n> --count <n>

# List all consumer groups
flymq-cli groups list

# Check consumer group details and state
flymq-cli groups describe <group-id>

# Check consumer group lag
flymq-cli groups lag <group-id>

# Reset consumer group offset
flymq-cli groups reset <group-id>
```

## Troubleshooting

### "Why am I seeing duplicate messages?"

1. **Auto-commit timing**: Messages processed between commits may be reprocessed after crash
2. **Solution**: Use manual commit after processing, or ensure idempotent processing

### "Why am I missing messages?"

1. **Started with `--from-latest`**: Only sees new messages
2. **Solution**: Use `--from-beginning` to read historical messages

### "How do I reprocess all messages?"

```bash
# Reset offset to beginning
flymq-cli groups reset my-app
```

