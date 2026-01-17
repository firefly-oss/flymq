# FlyMQ Java Client SDK

A high-performance Java client library for FlyMQ message queue with Kafka-like APIs and Spring Boot support.

**Website:** [https://flymq.com](https://flymq.com)

---

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start) - Get running in 30 seconds
3. [Key Concepts](#key-concepts) - Understanding the fundamentals
4. [High-Level APIs](#high-level-apis) - Recommended for most applications
   - [Producer](#high-level-producer)
   - [Consumer](#high-level-consumer)
5. [Low-Level APIs](#low-level-apis) - For advanced control
   - [Direct Produce/Consume](#direct-produceconsume)
   - [Manual Offset Management](#manual-offset-management)
6. [Advanced Features](#advanced-features)
   - [Transactions](#transactions)
   - [Schema Validation](#schema-validation)
   - [Dead Letter Queues](#dead-letter-queues)
   - [Delayed Messages](#delayed-messages)
7. [Spring Boot Integration](#spring-boot-integration)
8. [Configuration Reference](#configuration-reference)
9. [Error Handling](#error-handling)
10. [Development](#development)

---

## Features

| Category | Features |
|----------|----------|
| **Core** | Kafka-like APIs, High-level Producer/Consumer, Full protocol support |
| **Reliability** | Automatic retries, Failover, Consumer groups with offset tracking |
| **Security** | TLS/mTLS, AES-256-GCM encryption, Authentication |
| **Advanced** | Transactions, Schema validation, Dead letter queues, Delayed messages, TTL |
| **Spring Boot** | MVC auto-configuration, WebFlux reactive support |

**Requirements:** Java 21+, Maven 3.8+

---

## Installation

### Maven - Core Only

```xml
<dependency>
    <groupId>com.firefly.flymq</groupId>
    <artifactId>flymq-client-core</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Maven - Spring Boot MVC

```xml
<dependency>
    <groupId>com.firefly.flymq</groupId>
    <artifactId>flymq-client-spring</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Maven - Spring Boot WebFlux (Reactive)

```xml
<dependency>
    <groupId>com.firefly.flymq</groupId>
    <artifactId>flymq-client-spring-webflux</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Build from Source

```bash
cd sdk/java
mvn clean install
```

---

## Quick Start

Get up and running in 30 seconds:

```java
import com.firefly.flymq.FlyMQClient;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    // Produce a message
    client.produce("my-topic", "Hello, FlyMQ!".getBytes());

    // Consume messages with a consumer group (tracks your position automatically)
    try (var consumer = client.consumer("my-topic", "my-app")) {
        consumer.subscribe();
        var messages = consumer.poll(Duration.ofSeconds(5));
        for (var msg : messages) {
            System.out.println("Received: " + msg.dataAsString());
        }
    }
}
```

That's it! The consumer group `"my-app"` tracks your position. If you restart, you'll continue from where you left off.

---

## Key Concepts

Before diving into the APIs, understand these core concepts:

### Topics and Partitions

A **topic** is a named stream of messages. Topics are divided into **partitions** for parallelism.

```
Topic: "orders"
├── Partition 0: [msg0] [msg1] [msg2] ...
├── Partition 1: [msg0] [msg1] ...
└── Partition 2: [msg0] [msg1] [msg2] [msg3] ...
```

### Message Keys

A **key** determines which partition a message goes to. Same key = same partition = guaranteed order.

```java
// All orders for user-123 go to the same partition (ordered)
client.produceWithKey("orders", "user-123", "{\"id\": 1}");
client.produceWithKey("orders", "user-123", "{\"id\": 2}");  // Same partition!
```

### Consumer Groups

A **consumer group** tracks your position in the topic. Key benefits:

- **Resume on restart**: If your app crashes, it continues from where it left off
- **Independent groups**: Multiple apps can each receive ALL messages
- **Load balancing**: Multiple consumers in the same group share partitions

```
Group "analytics" ──► Receives ALL messages (offset: 100)
Group "billing"   ──► Receives ALL messages (offset: 50, processing slower)
```

### High-Level vs Low-Level APIs

| API Level | When to Use | Features |
|-----------|-------------|----------|
| **High-Level** | Most applications | Auto-commit, batching, simple iteration |
| **Low-Level** | Advanced control | Manual offsets, direct partition access |

**Recommendation:** Start with High-Level APIs. They handle the complexity for you.

---

## High-Level APIs

These APIs are recommended for most applications. They handle offset tracking, batching, and error recovery automatically.

### High-Level Producer

The high-level producer provides batching, callbacks, and automatic retries.

#### Basic Usage

```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.producer.HighLevelProducer;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    try (HighLevelProducer producer = client.producer()) {
        // Send messages (batched automatically)
        producer.send("events", "{\"type\": \"click\"}".getBytes());
        producer.send("events", "{\"type\": \"view\"}".getBytes());

        // With a key (ensures ordering for same key)
        producer.send("orders", "{\"id\": 1}".getBytes(), "user-123".getBytes());

        producer.flush();  // Ensure all sent
    }
}
```

#### With Callbacks

```java
producer.send("events", data, key)
    .whenComplete((metadata, error) -> {
        if (error != null) {
            System.err.println("Failed: " + error.getMessage());
        } else {
            System.out.println("Sent to " + metadata.topic() +
                "[" + metadata.partition() + "] @ offset " + metadata.offset());
        }
    });

// Or wait synchronously
var metadata = producer.send("events", data).get();
```

#### Producer Configuration

```java
import com.firefly.flymq.producer.ProducerConfig;

ProducerConfig config = ProducerConfig.builder()
    .batchSize(1000)        // Max messages per batch
    .lingerMs(50)           // Wait time for batching
    .maxRetries(3)          // Retry attempts
    .retryBackoffMs(100)    // Backoff between retries
    .build();

try (HighLevelProducer producer = client.producer(config)) {
    // High-throughput pattern
    for (int i = 0; i < 100000; i++) {
        producer.send("events", ("event-" + i).getBytes());
    }
    producer.flush();
}
```

### High-Level Consumer

The high-level consumer provides automatic offset tracking and simple iteration.

#### Basic Usage

```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.consumer.Consumer;
import java.time.Duration;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    // Create consumer with a group ID (tracks your position automatically)
    try (Consumer consumer = client.consumer("my-topic", "my-app")) {
        consumer.subscribe();  // Resumes from last committed offset

        while (true) {
            var messages = consumer.poll(Duration.ofSeconds(1));
            for (var msg : messages) {
                System.out.println("Key: " + msg.keyAsString());
                System.out.println("Value: " + msg.dataAsString());
            }
            // Offset is committed automatically every 5 seconds
        }
    }
}
```

#### Starting Position

When a consumer group first connects (no committed offset):

```java
import com.firefly.flymq.protocol.Records.SubscribeMode;

// Start from the beginning (process all historical messages)
consumer.subscribe(SubscribeMode.EARLIEST);

// Start from the end (only new messages) - this is the default
consumer.subscribe(SubscribeMode.LATEST);

// Resume from committed offset (normal operation)
consumer.subscribe(SubscribeMode.COMMIT);
```

After the first run, the consumer always resumes from its committed offset.

#### Manual Commit (Exactly-Once Processing)

For critical workloads, disable auto-commit and commit after successful processing:

```java
import com.firefly.flymq.consumer.ConsumerConfig;

ConsumerConfig config = ConsumerConfig.builder()
    .enableAutoCommit(false)  // Disable auto-commit
    .build();

try (Consumer consumer = client.consumer("orders", "payment-processor", 0, config)) {
    consumer.subscribe();

    while (true) {
        var messages = consumer.poll(Duration.ofSeconds(1));
        for (var msg : messages) {
            try {
                processPayment(msg);
                consumer.commitSync();  // Only commit after successful processing
            } catch (Exception e) {
                // Message will be reprocessed on next run
                logError(e);
            }
        }
    }
}
```

#### Seek Operations

```java
try (Consumer consumer = client.consumer("my-topic", "my-group")) {
    consumer.subscribe();

    // Seek to specific offset
    consumer.seek(100);

    // Seek to beginning/end
    consumer.seekToBeginning();
    consumer.seekToEnd();

    // Get current position
    long position = consumer.position();
}
```

---

## Low-Level APIs

Use low-level APIs when you need fine-grained control over partitions and offsets.

### Direct Produce/Consume

The low-level API gives you direct access to partitions and offsets:

```java
import com.firefly.flymq.FlyMQClient;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    // Create a topic with specific partition count
    client.createTopic("my-topic", 3);

    // Produce to a specific partition - returns RecordMetadata
    var meta = client.produceToPartition("my-topic", 1, "Hello!".getBytes());
    System.out.println("Produced at offset " + meta.offset());

    // Consume from a specific partition and offset
    byte[] data = client.consume("my-topic", meta.offset());
    System.out.println("Message: " + new String(data));

    // Fetch multiple messages
    var result = client.fetch("my-topic", 0, 0, 100);
    for (var msg : result.messages()) {
        System.out.println("Offset " + msg.offset() + ": " + msg.dataAsString());
    }
}
```

### Manual Offset Management

For complete control over offset tracking:

```java
import com.firefly.flymq.protocol.Records.SubscribeMode;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    // Subscribe to a topic with a consumer group
    // Returns the starting offset (committed or based on reset policy)
    long offset = client.subscribe("my-topic", "my-group", 0, SubscribeMode.COMMIT);

    // Fetch messages starting from the offset
    var result = client.fetch("my-topic", 0, offset, 10);

    for (var msg : result.messages()) {
        // Process the message
        process(msg);

        // Commit the offset after processing
        // The offset to commit is the NEXT message to read
        client.commitOffset("my-topic", "my-group", 0, msg.offset() + 1);
    }
}
```

### Cluster Connection (High Availability)

```java
// Connect to multiple servers for automatic failover
FlyMQClient client = new FlyMQClient("node1:9092,node2:9092,node3:9092");

// If node1 fails, automatically connects to node2 or node3
var meta = client.produce("my-topic", "Hello!".getBytes());
System.out.println("Produced at offset " + meta.offset());
```

### TLS Configuration

```java
import com.firefly.flymq.config.ClientConfig;

// Basic TLS (server verification)
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9093")
    .tlsEnabled(true)
    .tlsCaFile("/path/to/ca.crt")
    .build();

// Mutual TLS (client certificate authentication)
ClientConfig mtlsConfig = ClientConfig.builder()
    .bootstrapServers("localhost:9093")
    .tlsEnabled(true)
    .tlsCaFile("/path/to/ca.crt")
    .tlsCertFile("/path/to/client.crt")
    .tlsKeyFile("/path/to/client-pkcs8.key")  // PKCS#8 format required
    .build();

> **Note:** Private keys must be in PKCS#8 format. Convert with:
> ```bash
> openssl pkcs8 -topk8 -nocrypt -in client.key -out client-pkcs8.key
> ```

---

## Advanced Features

### Transactions

Transactions ensure atomic writes across multiple topics:

```java
import com.firefly.flymq.transaction.Transaction;

// Using try-with-resources (auto-rollback on exception)
try (Transaction txn = client.beginTransaction()) {
    txn.produce("orders", "{\"id\": 1}".getBytes());
    txn.produce("inventory", "{\"sku\": \"ABC\", \"delta\": -1}".getBytes());
    txn.commit();
}

// Manual transaction control
Transaction txn = client.beginTransaction();
try {
    txn.produce("topic1", "message1".getBytes());
    txn.produce("topic2", "message2".getBytes());
    txn.commit();
} catch (Exception e) {
    txn.rollback();
    throw e;
}
```

### Schema Validation

Validate messages against JSON, Avro, or Protobuf schemas:

```java
// Register a JSON schema
String schema = """
    {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"}
        },
        "required": ["name"]
    }
    """;
client.registerSchema("user-schema", "json", schema);

// Produce with schema validation
String message = """
    {"name": "Alice", "age": 30}
    """;
var meta = client.produceWithSchema("users", message.getBytes(), "user-schema");
System.out.println("Produced at offset " + meta.offset());
```

### Dead Letter Queues

Handle failed messages with dead letter queues:

```java
// Fetch messages from DLQ
var dlqMessages = client.fetchDLQ("my-topic", 10);
for (var msg : dlqMessages) {
    System.out.println("Failed message: " + msg.get("id") + ", error: " + msg.get("error"));

    // Replay the message back to the original topic
    client.replayDLQ("my-topic", (String) msg.get("id"));
}

// Purge all DLQ messages
client.purgeDLQ("my-topic");
```

### Delayed Messages

Schedule messages for future delivery:

```java
// Delayed delivery (5 second delay)
client.produceDelayed("my-topic", "Delayed message".getBytes(), 5000);
```

### Message TTL

Set expiration time for messages:

```java
// Message expires in 60 seconds
client.produceWithTTL("my-topic", "Expiring message".getBytes(), 60000);
```

### Topic Filtering (MQTT-style)

FlyMQ supports powerful MQTT-style wildcards for topic subscriptions:

- `+`: Matches exactly one topic level.
- `#`: Matches zero or more topic levels at the end.

```java
// Subscribe to multiple topics using patterns
try (var consumer = client.consumerGroup(List.of("sensors/+/temp", "logs/#"), "monitor")) {
    consumer.subscribe();
    var messages = consumer.poll(Duration.ofSeconds(1));
    for (var msg : messages) {
        System.out.println("Topic: " + msg.topic() + ", Data: " + msg.dataAsString());
    }
}
```

### Server-Side Message Filtering

Reduce bandwidth by filtering messages on the server before they are sent to the client:

```java
// Only receive messages containing "ERROR" (regex supported)
ConsumerConfig config = ConsumerConfig.builder()
    .filter("ERROR")
    .build();

try (var consumer = client.consumer("app-logs", "error-mon", 0, config)) {
    consumer.subscribe();
    // Only matching messages will be returned by poll()
}
```

### Plug-and-Play SerDe System

Seamlessly handle structured data with built-in or custom Serializers/Deserializers:

```java
import com.firefly.flymq.serialization.Serdes;

// Use the built-in SerDe helper
var userSerde = Serdes.json(User.class);

// Produce object directly
client.produceObject("users", new User(1, "Alice"), userSerde.serializer());

// Consume and decode automatically
try (var consumer = client.consumer("users", "user-processor")) {
    consumer.subscribe();
    var messages = consumer.poll(Duration.ofSeconds(1));
    for (var msg : messages) {
        User user = msg.decode(userSerde.deserializer());
        System.out.println("User: " + user.getName());
    }
}
```

### Encryption (AES-256-GCM)

The SDK supports AES-256-GCM encryption:

```java
import com.firefly.flymq.crypto.Encryptor;

// Generate a new encryption key
String key = Encryptor.generateKey(); // 64-char hex string

// Create encryptor for manual encryption
Encryptor encryptor = Encryptor.fromHexKey(key);
byte[] encrypted = encryptor.encrypt("Hello, FlyMQ!".getBytes());
byte[] decrypted = encryptor.decrypt(encrypted);

// Or configure client-level encryption
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9092")
    .encryptionKey(key)
    .build();
```

---

## Spring Boot Integration

### application.properties

```properties
flymq.enabled=true
flymq.bootstrap-servers=localhost:9092,localhost:9192
flymq.connect-timeout-ms=10000
flymq.request-timeout-ms=30000
flymq.max-retries=3
flymq.tls-enabled=false
flymq.client-id=my-spring-app
```

### Auto-wired Client

```java
import com.firefly.flymq.FlyMQClient;
import org.springframework.stereotype.Service;

@Service
public class MessageService {
    
    private final FlyMQClient client;
    
    public MessageService(FlyMQClient client) {
        this.client = client;
    }
    
    public void sendMessage(String topic, String message) throws Exception {
        client.produce(topic, message.getBytes());
    }
}
```

### TLS Configuration

**Basic TLS (Server Certificate Verification):**
```properties
flymq.tls-enabled=true
flymq.tls-ca-file=/path/to/ca.crt
```

**Mutual TLS (mTLS - Client Certificate Authentication):**
```properties
flymq.tls-enabled=true
flymq.tls-ca-file=/path/to/ca.crt
flymq.tls-cert-file=/path/to/client.crt
flymq.tls-key-file=/path/to/client.key
```

> **Note:** Private keys must be in PKCS#8 format. Convert with:
> ```bash
> openssl pkcs8 -topk8 -nocrypt -in client.key -out client-pkcs8.key
> ```

**Skip Verification (Testing Only):**
```properties
flymq.tls-enabled=true
flymq.tls-insecure-skip-verify=true
```

### Encryption Configuration

```properties
flymq.encryption-key=your-64-character-hex-encryption-key-here
```

### WebFlux (Reactive)

For reactive applications, use `flymq-client-spring-webflux`:

```java
import com.firefly.flymq.spring.webflux.ReactiveFlyMQClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class ReactiveMessageService {

    private final ReactiveFlyMQClient client;

    public ReactiveMessageService(ReactiveFlyMQClient client) {
        this.client = client;
    }

    // Produce messages reactively
    public Mono<Long> sendMessage(String topic, String message) {
        return client.produce(topic, message);
    }

    // Consume messages as a stream
    public Flux<String> consumeMessages(String topic, String groupId) {
        return client.consumeStream(topic, groupId, 0)
            .map(msg -> msg.dataAsString());
    }
}
```

---

## Configuration Reference

### ClientConfig

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bootstrapServers` | String | localhost:9092 | Comma-separated server addresses |
| `connectTimeoutMs` | int | 10000 | Connection timeout |
| `requestTimeoutMs` | int | 30000 | Request timeout |
| `maxRetries` | int | 3 | Max retry attempts |
| `retryDelayMs` | int | 1000 | Delay between retries |
| `tlsEnabled` | boolean | false | Enable TLS |
| `tlsCaFile` | String | null | CA certificate path |
| `tlsCertFile` | String | null | Client certificate path |
| `tlsKeyFile` | String | null | Client key path |
| `tlsInsecureSkipVerify` | boolean | false | Skip cert verification |
| `clientId` | String | flymq-java-client | Client identifier |

### ProducerConfig

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `batchSize` | int | 100 | Max messages per batch |
| `lingerMs` | int | 0 | Time to wait for batching |
| `maxRetries` | int | 3 | Max retries |
| `retryBackoffMs` | int | 100 | Backoff between retries |

### ConsumerConfig

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxPollRecords` | int | 500 | Max records per poll |
| `enableAutoCommit` | boolean | true | Auto-commit offsets |
| `autoCommitIntervalMs` | int | 5000 | Auto-commit interval |

---

## Error Handling

FlyMQ Java SDK provides clear error messages with helpful hints.

### Exception Types

```java
import com.firefly.flymq.exception.FlyMQException;

try {
    client.produce("my-topic", data);
} catch (FlyMQException e) {
    System.err.println("Error: " + e.getMessage());
    System.err.println("Hint: " + e.getHint());  // Helpful suggestion!
}
```

### Errors with Hints

All exceptions include helpful hints:

```java
try {
    client.produce("nonexistent-topic", data);
} catch (FlyMQException e) {
    System.err.println(e.getHint());
    // Output: Create the topic first with client.createTopic("nonexistent-topic", partitions)
}
```

### Retry Pattern

```java
public long produceWithRetry(FlyMQClient client, String topic, byte[] data, int maxRetries)
        throws FlyMQException {
    int backoffMs = 100;
    for (int attempt = 0; attempt < maxRetries; attempt++) {
        try {
            return client.produce(topic, data);
        } catch (FlyMQException e) {
            if (attempt < maxRetries - 1) {
                System.err.println("Retry " + (attempt + 1) + ": " + e.getMessage());
                try { Thread.sleep(backoffMs); } catch (InterruptedException ie) { break; }
                backoffMs *= 2;
            } else {
                throw e;
            }
        }
    }
    throw new FlyMQException("Max retries exceeded");
}
```

---

## Development

### Running Tests

```bash
# Run unit tests
mvn test

# Run with integration tests (requires running FlyMQ server)
mvn verify -Pintegration-tests
```

### Building

```bash
# Build JAR
mvn clean package

# Build with sources and javadoc
mvn clean package -Prelease
```

---

## License

Copyright (c) 2026 Firefly Software Solutions Inc.

Licensed under the Apache License, Version 2.0.
