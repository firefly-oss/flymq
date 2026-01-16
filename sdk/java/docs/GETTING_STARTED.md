# Getting Started with FlyMQ Java SDK

Quick guide to get started with the FlyMQ Java client.

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

```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.protocol.Records.ConsumedMessage;

public class HelloFlyMQ {
    public static void main(String[] args) throws Exception {
        // Connect to FlyMQ
        try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
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

### Consumer Groups

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

```java
try {
    client.produce("my-topic", data.getBytes());
} catch (ProtocolException e) {
    log.error("Protocol error: " + e.getMessage());
} catch (FlyMQException e) {
    log.error("FlyMQ error: " + e.getMessage());
} catch (Exception e) {
    log.error("Unexpected error: " + e.getMessage());
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

1. Run examples: Check `examples/` directory
2. Read guides: See `docs/` for detailed topics
3. API reference: Check main README.md

## Examples

See `examples/core/` for:
- BasicProduceConsume.java
- KeyBasedMessaging.java
- ConsumerGroupExample.java

See `examples/spring-mvc/` for:
- MessageService.java - Spring service integration
