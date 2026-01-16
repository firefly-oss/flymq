# Consumer Guide - Java SDK

Best practices for consuming messages with FlyMQ Java client.

## Basic Consumer

```java
try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
    // Simple consume by offset
    ConsumedMessage msg = client.consume("my-topic", 0);
    System.out.println("Received: " + msg.dataAsString());
}
```

## High-Level Consumer

Use the `Consumer` class for Kafka-like consumption:

```java
try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
    try (Consumer consumer = new Consumer(client, "my-topic", "my-group")) {
        consumer.subscribe();
        
        while (true) {
            List<ConsumedMessage> messages = consumer.poll(Duration.ofSeconds(1));
            for (ConsumedMessage msg : messages) {
                process(msg);
            }
            consumer.commitSync();
        }
    }
}
```

## Consumer Groups

Multiple consumers sharing work:

```java
// Consumer 1
Consumer consumer1 = new Consumer(client, "orders", "order-processors");
consumer1.subscribe();

// Consumer 2 (same group - shares partitions)
Consumer consumer2 = new Consumer(client, "orders", "order-processors");
consumer2.subscribe();

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

