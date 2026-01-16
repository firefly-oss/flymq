# Producer Guide - Java SDK

Best practices for producing messages with FlyMQ Java client.

## Basic Producer

```java
try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
    // Simple produce
    long offset = client.produce("my-topic", "Hello".getBytes());
    System.out.println("Produced at offset: " + offset);
}
```

## Key-Based Produce

Messages with the same key go to the same partition:

```java
// Produce with key
client.produceWithKey("orders", "user-123", orderJson.getBytes());
client.produceWithKey("orders", "user-123", anotherOrder.getBytes()); // Same partition

// Ensures ordering for the same key
```

## Batch Producing

Produce multiple messages efficiently:

```java
try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
    List<String> messages = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
        messages.add("Message " + i);
    }
    
    for (String msg : messages) {
        client.produce("my-topic", msg.getBytes());
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
