# Consumer Guide - Java SDK

Best practices for consuming messages with FlyMQ Java client.

## Quick Start

```java
import com.firefly.flymq.FlyMQClient;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    try (var consumer = client.consumer("my-topic", "my-group")) {
        consumer.subscribe();

        for (var msg : consumer.poll(Duration.ofSeconds(1))) {
            System.out.println("Received: " + msg.dataAsString());
        }
    }
}
```

## High-Level Consumer (Recommended)

The `Consumer` class provides a Kafka-like API with auto-commit and poll-based consumption.

### Basic Usage

```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.consumer.Consumer;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    try (Consumer consumer = client.consumer("my-topic", "my-group")) {
        consumer.subscribe();

        while (true) {
            var messages = consumer.poll(Duration.ofSeconds(1));
            for (var msg : messages) {
                System.out.println("Key: " + msg.keyAsString());
                System.out.println("Value: " + msg.dataAsString());
                System.out.println("Offset: " + msg.offset());
            }
            // Auto-commit enabled by default
        }
    }
}
```

### Consumer Configuration

```java
import com.firefly.flymq.consumer.ConsumerConfig;

ConsumerConfig config = ConsumerConfig.builder()
    .maxPollRecords(100)           // Max messages per poll
    .enableAutoCommit(true)        // Enable auto-commit
    .autoCommitIntervalMs(5000)    // Commit every 5 seconds
    .sessionTimeoutMs(30000)       // Session timeout
    .build();

try (Consumer consumer = client.consumer("my-topic", "my-group", 0, config)) {
    consumer.subscribe();
    // ...
}
```

### Manual Commit

```java
ConsumerConfig config = ConsumerConfig.builder()
    .enableAutoCommit(false)  // Disable auto-commit
    .build();

try (Consumer consumer = client.consumer("my-topic", "my-group", 0, config)) {
    consumer.subscribe();

    while (true) {
        var messages = consumer.poll(Duration.ofSeconds(1));
        for (var msg : messages) {
            process(msg);
        }
        consumer.commitSync();  // Manual commit after processing
    }
}
```

### Async Commit

```java
consumer.commitAsync((offsets, error) -> {
    if (error != null) {
        System.err.println("Commit failed: " + error.getMessage());
    } else {
        System.out.println("Committed: " + offsets);
    }
});
```

## Basic Consumer (Low-Level)

```java
try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    // Simple consume by offset
    ConsumedMessage msg = client.consume("my-topic", 0);
    System.out.println("Received: " + msg.dataAsString());
}
```

## Consumer Groups

Multiple consumers sharing work:

```java
// Consumer 1
try (Consumer consumer1 = client.consumer("orders", "order-processors")) {
    consumer1.subscribe();
    // Processes partitions 0, 1
}

// Consumer 2 (same group - shares partitions)
try (Consumer consumer2 = client.consumer("orders", "order-processors")) {
    consumer2.subscribe();
    // Processes partitions 2, 3
}

// Messages are distributed across consumers in the group
```

## Subscribe Modes

Control where consumption starts:

```java
// Start from committed offset (default)
consumer.subscribe(SubscribeMode.COMMIT);

// Start from beginning
consumer.subscribe(SubscribeMode.EARLIEST);

// Start from end (new messages only)
consumer.subscribe(SubscribeMode.LATEST);
```

## Offset Management

### Manual Commit

```java
try (Consumer consumer = new Consumer(client, "topic", "group")) {
    consumer.subscribe();
    
    List<ConsumedMessage> messages = consumer.poll(Duration.ofSeconds(1));
    for (ConsumedMessage msg : messages) {
        process(msg);
    }
    
    // Commit after processing
    consumer.commitSync();
}
```

### Commit Specific Offset

```java
// Commit a specific offset
consumer.commitSync(42);
```

### Auto-Commit

```java
ConsumerConfig config = ConsumerConfig.builder()
    .enableAutoCommit(true)
    .autoCommitIntervalMs(5000)  // Commit every 5 seconds
    .build();

Consumer consumer = new Consumer(client, "topic", "group", 0, config);
```

## Seeking

Navigate to specific positions:

```java
// Seek to specific offset
consumer.seek(100);

// Seek to beginning
consumer.seekToBeginning();

// Seek to end
consumer.seekToEnd();

// Get current position
long position = consumer.position();
```

## Consumer Lag

Monitor consumer lag:

```java
ConsumerLag lag = consumer.lag();
System.out.println("Current offset: " + lag.currentOffset());
System.out.println("End offset: " + lag.endOffset());
System.out.println("Lag: " + lag.lag());
```

## Batch Fetching

Fetch multiple messages efficiently:

```java
FetchResult result = client.fetch("topic", 0, startOffset, 100);
for (ConsumedMessage msg : result.messages()) {
    process(msg);
}
long nextOffset = result.nextOffset();
```

## Error Handling

```java
try (Consumer consumer = new Consumer(client, "topic", "group")) {
    consumer.subscribe();
    
    while (true) {
        try {
            List<ConsumedMessage> messages = consumer.poll(Duration.ofSeconds(1));
            for (ConsumedMessage msg : messages) {
                try {
                    process(msg);
                } catch (Exception e) {
                    // Handle processing error
                    handleError(msg, e);
                }
            }
            consumer.commitSync();
        } catch (FlyMQException e) {
            // Handle FlyMQ errors
            if (isRetryable(e)) {
                Thread.sleep(1000);
                continue;
            }
            throw e;
        }
    }
}
```

## Configuration Options

```java
ConsumerConfig config = ConsumerConfig.builder()
    .maxPollRecords(500)           // Max messages per poll
    .enableAutoCommit(false)       // Manual commit
    .autoCommitIntervalMs(5000)    // Auto-commit interval
    .build();
```

## Best Practices

✅ DO:
- Use consumer groups for parallel processing
- Commit offsets after successful processing
- Handle errors gracefully with retry logic
- Monitor consumer lag
- Close consumers properly (use try-with-resources)

❌ DON'T:
- Commit before processing completes
- Create new consumer for each message
- Ignore processing errors
- Leave consumers running without health checks
- Process messages without timeout handling

