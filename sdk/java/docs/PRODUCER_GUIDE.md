# Producer Guide - Java SDK

Best practices for producing messages with FlyMQ Java client.

## Quick Start

```java
import com.firefly.flymq.FlyMQClient;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    // produce() returns RecordMetadata (Kafka-like)
    var meta = client.produce("my-topic", "Hello".getBytes());
    System.out.println("Produced to " + meta.topic() + " at offset " + meta.offset());
}
```

## High-Level Producer (Recommended)

The `HighLevelProducer` provides batching, callbacks, and automatic retries for production workloads.

```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.producer.HighLevelProducer;
import com.firefly.flymq.producer.ProducerConfig;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {

    ProducerConfig config = ProducerConfig.builder()
        .batchSize(100)        // Max messages per batch
        .lingerMs(10)          // Wait time for batching
        .maxRetries(3)         // Retry attempts
        .retryBackoffMs(100)   // Backoff between retries
        .build();

    try (HighLevelProducer producer = client.producer(config)) {
        // Send with callback
        producer.send("events", "{\"event\": \"click\"}".getBytes())
            .whenComplete((metadata, error) -> {
                if (error != null) {
                    System.err.println("Failed: " + error.getMessage());
                } else {
                    System.out.println("✓ Sent to " + metadata.topic() +
                        "[" + metadata.partition() + "] @ offset " + metadata.offset());
                }
            });

        // Wait for result if needed
        var metadata = producer.send("events", "data".getBytes()).get();
        System.out.println("Offset: " + metadata.offset());

        producer.flush();
    }
}
```

### High-Throughput Pattern

```java
try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    ProducerConfig config = ProducerConfig.builder()
        .batchSize(1000)
        .lingerMs(50)
        .build();

    try (HighLevelProducer producer = client.producer(config)) {
        // Send 100k messages efficiently
        for (int i = 0; i < 100000; i++) {
            producer.send("events", ("event-" + i).getBytes());
        }
        producer.flush();
        System.out.println("All messages sent!");
    }
}
```

### Fire-and-Forget vs Wait

```java
// Fire-and-forget (fastest)
producer.send("topic", data);

// Wait for acknowledgment
var future = producer.send("topic", data);
var metadata = future.get();  // Blocks until sent

// Check if sent without blocking
if (future.isDone()) {
    System.out.println("Sent at offset " + future.get().offset());
}
```

## Basic Producer (Low-Level)

```java
try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    // Simple produce - returns RecordMetadata
    var meta = client.produce("my-topic", "Hello".getBytes());
    System.out.println("Produced to " + meta.topic() + " at offset " + meta.offset());
}
```

## Key-Based Produce

Messages with the same key go to the same partition:

```java
// With HighLevelProducer
producer.send("orders", orderJson.getBytes(), "user-123".getBytes());
producer.send("orders", anotherOrder.getBytes(), "user-123".getBytes()); // Same partition

// With low-level client
client.produceWithKey("orders", "user-123", orderJson.getBytes());
```

## Batch Producing

### With HighLevelProducer (Recommended)

```java
try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    try (HighLevelProducer producer = client.producer()) {
        for (int i = 0; i < 1000; i++) {
            producer.send("my-topic", ("Message " + i).getBytes());
        }
        producer.flush();  // Ensure all sent
    }
}
```

### With Low-Level Client

```java
try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    for (int i = 0; i < 1000; i++) {
        client.produce("my-topic", ("Message " + i).getBytes());
    }
}
```

## Transaction-Based Produce

Atomic production across topics:

```java
try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
    try (Transaction txn = client.beginTransaction()) {
        txn.produce("orders", orderData);
        txn.produce("payments", paymentData);
        txn.commit();
    } catch (Exception e) {
        // Auto-rollback on exception
        System.err.println("Transaction failed: " + e.getMessage());
    }
}
```

## Error Handling

Handle producer errors gracefully:

```java
private static long produceWithRetry(FlyMQClient client, String topic, byte[] data, int maxRetries) throws Exception {
    int retries = 0;
    int backoff = 100;
    
    while (retries < maxRetries) {
        try {
            return client.produce(topic, data);
        } catch (Exception e) {
            retries++;
            if (retries >= maxRetries) {
                throw e;
            }
            System.out.println("Retry " + retries + " after " + backoff + "ms");
            Thread.sleep(backoff);
            backoff *= 2;
        }
    }
    throw new Exception("Failed to produce after " + maxRetries + " retries");
}
```

## JSON Serialization

Serialize objects to JSON before producing:

```java
import com.fasterxml.jackson.databind.ObjectMapper;

ObjectMapper mapper = new ObjectMapper();

class User {
    public String id;
    public String name;
}

User user = new User();
user.id = "123";
user.name = "Alice";

String json = mapper.writeValueAsString(user);
client.produce("users", json.getBytes());
```

## Configuration

Custom configuration for producers:

```java
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9092")
    .connectTimeoutMs(10000)
    .requestTimeoutMs(30000)
    .maxRetries(3)
    .retryDelayMs(1000)
    .build();

FlyMQClient client = new FlyMQClient(config);
```

## Performance Tips

1. **Reuse client instance**: Create once, use multiple times
2. **Batch operations**: Group multiple produces together
3. **Handle errors gracefully**: Implement retry logic
4. **Monitor latency**: Track offset differences
5. **Test with production volume**: Load test before deploying

## Best Practices

✅ DO:
- Reuse FlyMQClient instances
- Handle exceptions with retry logic
- Use transactions for multi-topic operations
- Close client properly (use try-with-resources)
- Serialize data before producing

❌ DON'T:
- Create new client for each operation
- Ignore production errors
- Send very large messages (>32MB)
- Leave connections open indefinitely
- Block on network operations without timeout
