# Getting Started with FlyMQ Java SDK

Quick guide to get started with the FlyMQ Java client in 10 minutes.

## Installation

Add the dependency to your `pom.xml`:

```xml
<!-- Core client (standalone) -->
<dependency>
    <groupId>com.firefly.flymq</groupId>
    <artifactId>flymq-client-core</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- Spring Boot MVC -->
<dependency>
    <groupId>com.firefly.flymq</groupId>
    <artifactId>flymq-client-spring</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- Spring Boot WebFlux (reactive) -->
<dependency>
    <groupId>com.firefly.flymq</groupId>
    <artifactId>flymq-client-spring-webflux</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Prerequisites

- Java 21+
- FlyMQ server running (default: `localhost:9092`)

## Your First Program

### The Simplest Way: `connect()`

```java
import com.firefly.flymq.FlyMQClient;

public class HelloFlyMQ {
    public static void main(String[] args) throws Exception {
        // One-liner connection
        try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
            // Produce and consume
            long offset = client.produce("hello", "Hello, FlyMQ!".getBytes());
            byte[] data = client.consume("hello", offset);
            System.out.println("Received: " + new String(data));
        }
    }
}
```

### Full Example

```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.protocol.Records.ConsumedMessage;

public class HelloFlyMQ {
    public static void main(String[] args) throws Exception {
        // Connect to FlyMQ
        try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
            // Create topic
            client.createTopic("hello", 1);

            // Produce message
            long offset = client.produce("hello", "Hello, FlyMQ!".getBytes());
            System.out.println("Produced at offset: " + offset);

            // Consume message
            ConsumedMessage msg = client.consumeWithKey("hello", offset);
            System.out.println("Received: " + msg.dataAsString());
        }
    }
}
```

## Key Concepts

### Topics

Topics are named channels for messages:

```java
// Create a topic with 3 partitions
client.createTopic("my-topic", 3);

// List topics
Set<String> topics = client.listTopics();
```

### Producing Messages

Send messages:

```java
// Simple produce
long offset = client.produce("my-topic", "Hello".getBytes());

// Produce with key (Kafka-style partitioning)
// Same key = same partition (ordering guarantee)
long offset = client.produceWithKey("my-topic", "user-123", data.getBytes());
```

### Consuming Messages

Read messages:

```java
// Consume by offset
ConsumedMessage msg = client.consumeWithKey("my-topic", offset);
String data = msg.dataAsString();
String key = msg.keyAsString();

// Fetch batch
FetchResult result = client.fetch("my-topic", partition, offset, maxMessages);
for (ConsumedMessage m : result.messages()) {
    System.out.println(m.dataAsString());
}
```

### Consumer Groups (Low-Level)

Process messages as a coordinated group:

```java
Consumer consumer = new Consumer(client, "my-topic", "my-group");
consumer.subscribe(); // Start from committed offset

while (true) {
    List<ConsumedMessage> messages = consumer.poll(Duration.ofSeconds(1));
    for (ConsumedMessage msg : messages) {
        process(msg);
    }
    consumer.commitSync(); // Save position
}
```

## High-Level APIs (Kafka-like)

FlyMQ provides high-level APIs that feel familiar to Kafka users.

### High-Level Producer

The `HighLevelProducer` provides batching, callbacks, and automatic retries:

```java
import com.firefly.flymq.producer.HighLevelProducer;
import com.firefly.flymq.producer.ProducerConfig;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    ProducerConfig config = ProducerConfig.builder()
        .batchSize(100)
        .lingerMs(10)
        .build();

    try (HighLevelProducer producer = client.producer(config)) {
        // Send with callback
        producer.send("events", "data".getBytes())
            .whenComplete((metadata, error) -> {
                if (error == null) {
                    System.out.println("Sent @ offset " + metadata.offset());
                }
            });

        producer.flush();
    }
}
```

### High-Level Consumer

The `Consumer` provides auto-commit and poll-based consumption:

```java
import com.firefly.flymq.consumer.Consumer;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    try (Consumer consumer = client.consumer("my-topic", "my-group")) {
        consumer.subscribe();

        while (true) {
            var messages = consumer.poll(Duration.ofSeconds(1));
            for (var msg : messages) {
                System.out.println("Key: " + msg.keyAsString());
                System.out.println("Value: " + msg.dataAsString());
            }
            // Auto-commit enabled by default
        }
    }
}
```

## Spring Boot Integration

### Standalone Application

```java
@SpringBootApplication
public class MyApp {
    public static void main(String[] args) {
        SpringApplication.run(MyApp.class, args);
    }
}
```

Configuration in `application.properties`:

```properties
flymq.enabled=true
flymq.bootstrap-servers=localhost:9092
flymq.connect-timeout-ms=10000
```

### Using in a Service

```java
@Service
public class MyService {
    private final FlyMQClient client;
    
    public MyService(FlyMQClient client) {
        this.client = client;
    }
    
    public void sendMessage(String topic, String msg) throws Exception {
        client.produce(topic, msg.getBytes());
    }
}
```

### Spring Boot Controller

```java
@RestController
@RequestMapping("/api/messages")
public class MessageController {
    private final MessageService service;
    
    @PostMapping
    public Long sendMessage(@RequestParam String topic, @RequestBody String message) throws Exception {
        return service.sendMessage(topic, message);
    }
    
    @GetMapping("/{topic}/{offset}")
    public String getMessage(@PathVariable String topic, @PathVariable long offset) throws Exception {
        return service.getMessage(topic, offset);
    }
}
```

## Authentication

With credentials:

```java
FlyMQClient client = new FlyMQClient("localhost:9092");
client.authenticate("admin", "password");

// Or via configuration
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9092")
    .username("alice")
    .password("secret")
    .build();

FlyMQClient client = new FlyMQClient(config);
```

## TLS Configuration

Secure connections:

```java
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("flymq.example.com:9093")
    .tlsEnabled(true)
    .tlsCaFile("/path/to/ca.crt")
    .build();

FlyMQClient client = new FlyMQClient(config);
```

With client certificates (mTLS):

```java
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("flymq.example.com:9093")
    .tlsEnabled(true)
    .tlsCaFile("/path/to/ca.crt")
    .tlsCertFile("/path/to/client.crt")
    .tlsKeyFile("/path/to/client.key")
    .build();
```

## Error Handling

FlyMQ exceptions include helpful hints for quick debugging:

```java
import com.firefly.flymq.exception.FlyMQException;

try {
    client.produce("my-topic", data.getBytes());
} catch (FlyMQException e) {
    System.err.println("Error: " + e.getMessage());
    System.err.println("Hint: " + e.getHint());  // Helpful suggestion!

    // Or get full message with hint
    System.err.println(e.getFullMessage());
}
```

### Errors Include Hints

```java
try {
    client.produce("nonexistent-topic", data);
} catch (FlyMQException e) {
    System.err.println(e.getHint());
    // Output: Create the topic first with client.createTopic("nonexistent-topic", partitions)
    //         or use the CLI: flymq-cli topic create nonexistent-topic
}
```

### Exception Types

```java
import com.firefly.flymq.exception.FlyMQException;
import com.firefly.flymq.exception.ProtocolException;

try {
    client.produce("my-topic", data.getBytes());
} catch (ProtocolException e) {
    // Protocol-level errors
    log.error("Protocol error: " + e.getMessage());
} catch (FlyMQException e) {
    // All FlyMQ errors (includes hint)
    log.error("FlyMQ error: " + e.getFullMessage());
}
```

## Common Patterns

### Pattern 1: Request-Reply

```java
// Producer sends request
client.produceWithKey("requests", "req-123", requestData);

// Consumer processes and responds
Consumer consumer = new Consumer(client, "requests", "processor");
consumer.subscribe();
for (ConsumedMessage msg : consumer.poll(timeout).messages()) {
    byte[] response = processRequest(msg.data());
    client.produceWithKey("responses", msg.keyAsString(), response);
}
```

### Pattern 2: Load Balancing

```java
// Multiple consumers, same group
// Partitions automatically distributed
for (int i = 0; i < numWorkers; i++) {
    new Thread(() -> {
        Consumer consumer = new Consumer(client, "tasks", "workers");
        consumer.subscribe();
        // Process messages
    }).start();
}
```

## Next Steps

1. **Run examples**: Check `examples/` directory for complete working examples
2. **Read guides**: See `docs/` folder for detailed topics:
   - `PRODUCER_GUIDE.md` - Producer best practices
   - `CONSUMER_GUIDE.md` - Consumer best practices
   - `SECURITY.md` - TLS and authentication
3. **API reference**: Check main README.md for full API documentation

## Examples

See `examples/core/` for:
- `BasicProduceConsume.java` - Fundamental produce/consume operations
- `KeyBasedMessaging.java` - Key-based routing and partitioning
- `ConsumerGroupExample.java` - Consumer groups for parallel processing
- `TransactionExample.java` - Atomic multi-topic transactions
- `TLSAuthenticationExample.java` - Secure connections and authentication
- `ErrorHandlingExample.java` - Error handling and retry patterns
- `SchemaValidationExample.java` - JSON schema validation
- `DLQExample.java` - Dead letter queue operations
- `AdvancedPatternsExample.java` - Batch processing, connection pooling, TTL

See `examples/spring-mvc/` for:
- `MessageService.java` - Spring service integration
