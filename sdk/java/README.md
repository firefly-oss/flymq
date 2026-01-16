# FlyMQ Java Client SDK

A high-performance Java client library for FlyMQ message queue with Kafka-like APIs and Spring Boot support.

**Website:** [https://getfirefly.io](https://getfirefly.io)

## Requirements

- Java 21+
- Maven 3.8+

## Features

- **Kafka-Like APIs** - Familiar `producer()` and `consumer()` patterns
- **High-Level Producer** - Batching, callbacks, automatic retries with `CompletableFuture`
- **High-Level Consumer** - Auto-commit, poll-based consumption, seek operations
- **Static `connect()` Method** - Simple one-liner connection
- **Full Protocol Support** - Implements the complete FlyMQ binary protocol
- **AES-256-GCM Encryption** - Data-in-motion and data-at-rest encryption
- **Automatic Failover** - Connects to multiple bootstrap servers with automatic failover
- **TLS/SSL Support** - Secure connections with certificate verification
- **Consumer Groups** - Coordinated consumption with offset management
- **Transactions** - Exactly-once semantics with atomic operations
- **Schema Validation** - JSON, Avro, and Protobuf schema support
- **Dead Letter Queues** - Failed message handling
- **Delayed Messages** - Scheduled message delivery
- **Message TTL** - Time-based message expiration
- **Helpful Error Messages** - Exceptions include hints for quick debugging
- **Spring Boot MVC Integration** - Auto-configuration for traditional applications
- **Spring Boot WebFlux Integration** - Reactive auto-configuration for non-blocking applications
- **Thread-Safe** - Safe for concurrent use

## Modules

This SDK is organized as a multi-module Maven project:

| Module | Description |
|--------|-------------|
| `flymq-client-core` | Core client library with protocol, crypto, and base client |
| `flymq-client-spring` | Spring Boot MVC auto-configuration |
| `flymq-client-spring-webflux` | Spring Boot WebFlux reactive auto-configuration |

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

## Quick Start

### The Simplest Way: `connect()`

```java
import com.firefly.flymq.FlyMQClient;

// One-liner connection
try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    client.produce("my-topic", "Hello, FlyMQ!".getBytes());
    byte[] data = client.consume("my-topic", 0);
    System.out.println("Received: " + new String(data));
}
```

### Basic Usage

```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.protocol.BinaryProtocol.RecordMetadata;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    // Create a topic
    client.createTopic("my-topic", 3);

    // Produce a message - returns RecordMetadata (Kafka-like)
    RecordMetadata meta = client.produce("my-topic", "Hello, FlyMQ!".getBytes());
    System.out.println("Produced to " + meta.topic() + "[" + meta.partition() + "] @ offset " + meta.offset());

    // Consume the message
    byte[] data = client.consume("my-topic", meta.offset());
    System.out.println("Received: " + new String(data));
}
```

### Key-Based Messaging (Kafka-style)

Messages with the same key are guaranteed to go to the same partition,
ensuring ordering for related messages.

```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.protocol.Records.ConsumedMessage;

try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
    // Produce messages with keys
    client.produceWithKey("orders", "user-123", "{\"id\": 1}");
    client.produceWithKey("orders", "user-123", "{\"id\": 2}");  // Same partition
    client.produceWithKey("orders", "user-456", "{\"id\": 3}");  // May differ

    // Consume and access key
    ConsumedMessage msg = client.consumeWithKey("orders", 0);
    System.out.println("Key: " + msg.keyAsString());   // "user-123"
    System.out.println("Value: " + msg.dataAsString()); // '{"id": 1}'

    // Fetch multiple messages with keys
    var result = client.fetch("orders", 0, 0, 10);
    for (var m : result.messages()) {
        System.out.printf("Offset %d: key=%s value=%s%n", 
            m.offset(), m.keyAsString(), m.dataAsString());
    }
}
```

### Cluster Connection (HA)

```java
// Connect to multiple servers for high availability
FlyMQClient client = new FlyMQClient("node1:9092,node2:9092,node3:9092");

// Automatic failover if a server becomes unavailable
long offset = client.produce("my-topic", "Hello!".getBytes());
```

### Custom Configuration

```java
import com.firefly.flymq.config.ClientConfig;

ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9092,localhost:9192")
    .connectTimeoutMs(5000)
    .requestTimeoutMs(30000)
    .maxRetries(5)
    .tlsEnabled(true)
    .tlsCaFile("/path/to/ca.crt")
    .build();

FlyMQClient client = new FlyMQClient(config);
```

## TLS/SSL Configuration

### Basic TLS (Server Verification)

```java
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9093")
    .tlsEnabled(true)
    .tlsCaFile("/path/to/ca.crt")  // CA certificate for server verification
    .build();

try (FlyMQClient client = new FlyMQClient(config)) {
    // Secure connection established
    client.produce("my-topic", "Hello, TLS!".getBytes());
}
```

### Mutual TLS (mTLS - Client Certificate Authentication)

```java
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9093")
    .tlsEnabled(true)
    .tlsCaFile("/path/to/ca.crt")           // CA certificate
    .tlsCertFile("/path/to/client.crt")     // Client certificate
    .tlsKeyFile("/path/to/client-pkcs8.key") // Client private key (PKCS#8 format)
    .build();

try (FlyMQClient client = new FlyMQClient(config)) {
    // mTLS connection with client authentication
    client.produce("secure-topic", "Hello, mTLS!".getBytes());
}
```

> **Important:** Private keys must be in PKCS#8 format. Convert from traditional format:
> ```bash
> openssl pkcs8 -topk8 -nocrypt -in client.key -out client-pkcs8.key
> ```

### Skip Certificate Verification (Testing Only)

```java
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9093")
    .tlsEnabled(true)
    .tlsInsecureSkipVerify(true)  // WARNING: Only for testing!
    .build();
```

## Understanding Consumer Groups

A **consumer group** is a named group of consumers that share message processing:

- **Offset Tracking**: FlyMQ remembers where each group left off
- **Resume on Restart**: If your app crashes, it resumes from the last committed offset
- **Load Balancing**: Multiple consumers in the same group share partitions
- **Independent Groups**: Different groups each receive ALL messages

```
// Two apps consuming the same topic independently:
App A (group="analytics")  ──► Receives ALL messages
App B (group="billing")    ──► Also receives ALL messages

// Two instances of the same app sharing work:
Instance 1 (group="processor") ──► Partition 0
Instance 2 (group="processor") ──► Partition 1
```

### Low-Level Consumer Group API

For fine-grained control over offset management:

```java
import com.firefly.flymq.protocol.Records.SubscribeMode;

// Subscribe with a consumer group
// SubscribeMode.COMMIT: Resume from last committed offset (recommended)
// SubscribeMode.EARLIEST: Start from beginning
// SubscribeMode.LATEST: Start from new messages only
long startOffset = client.subscribe("my-topic", "my-group", 0, SubscribeMode.COMMIT);

// Fetch messages in batches
var result = client.fetch("my-topic", 0, startOffset, 100);
for (var message : result.messages()) {
    System.out.println("Received: " + message.dataAsString());
}

// Commit offset after successful processing
client.commitOffset("my-topic", "my-group", 0, result.nextOffset());
```

## High-Level Producer (Kafka-like)

The `HighLevelProducer` provides batching, callbacks, and automatic retries for high-throughput scenarios.

```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.producer.HighLevelProducer;
import com.firefly.flymq.producer.ProducerConfig;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {

    // Create producer with batching
    ProducerConfig config = ProducerConfig.builder()
        .batchSize(100)
        .lingerMs(10)
        .maxRetries(3)
        .build();

    try (HighLevelProducer producer = client.producer(config)) {
        // Send with callback
        producer.send("events", "{\"event\": \"click\"}".getBytes(), "user-123".getBytes())
            .whenComplete((metadata, error) -> {
                if (error != null) {
                    System.err.println("Failed: " + error.getMessage());
                } else {
                    System.out.println("Sent to " + metadata.topic() +
                        "[" + metadata.partition() + "] @ " + metadata.offset());
                }
            });

        // Or wait for result
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

## High-Level Consumer (Kafka-like)

The `Consumer` provides a Kafka-like API with auto-commit and poll-based consumption.

### Basic Consumer

```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.consumer.Consumer;
import com.firefly.flymq.protocol.Records.ConsumedMessage;
import java.time.Duration;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {

    // Create consumer with a group ID
    // The group ID tracks your position - use a unique name per application
    try (Consumer consumer = client.consumer("my-topic", "my-group")) {
        consumer.subscribe();  // Resumes from last committed offset

        while (true) {
            List<ConsumedMessage> messages = consumer.poll(Duration.ofSeconds(1));
            for (ConsumedMessage msg : messages) {
                System.out.println("Key: " + msg.keyAsString());
                System.out.println("Value: " + msg.dataAsString());
                System.out.println("Offset: " + msg.offset());
            }
            // Offset is committed automatically every 5 seconds
        }
    }
}
```

### Starting Position

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

### Manual Commit (Exactly-Once Processing)

For critical workloads, disable auto-commit and commit after successful processing:

```java
ConsumerConfig config = ConsumerConfig.builder()
    .enableAutoCommit(false)  // Disable auto-commit
    .build();

try (Consumer consumer = client.consumer("orders", "payment-processor", 0, config)) {
    consumer.subscribe();

    while (true) {
        List<ConsumedMessage> messages = consumer.poll(Duration.ofSeconds(1));
        for (ConsumedMessage msg : messages) {
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

### Consumer Configuration

```java
import com.firefly.flymq.consumer.ConsumerConfig;

ConsumerConfig config = ConsumerConfig.builder()
    .maxPollRecords(100)           // Max messages per poll
    .enableAutoCommit(true)        // Auto-commit offsets (default: true)
    .autoCommitIntervalMs(5000)    // Commit interval (default: 5000ms)
    .build();

try (Consumer consumer = client.consumer("my-topic", "my-group", 0, config)) {
    consumer.subscribe();
    // ...
}
```

### Seek Operations

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

## Transactions

```java
import com.firefly.flymq.transaction.Transaction;

// Using try-with-resources (auto-rollback on exception)
try (Transaction txn = client.beginTransaction()) {
    txn.produce("topic1", "message1".getBytes());
    txn.produce("topic2", "message2".getBytes());
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

## Schema Validation

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
long offset = client.produceWithSchema("users", message.getBytes(), "user-schema");
```

## Dead Letter Queue

```java
// Fetch messages from DLQ
var dlqMessages = client.fetchDLQ("my-topic", 10);
for (var msg : dlqMessages) {
    System.out.println("Failed message: " + msg.get("id") + ", error: " + msg.get("error"));
    
    // Replay the message
    client.replayDLQ("my-topic", (String) msg.get("id"));
}

// Purge all DLQ messages
client.purgeDLQ("my-topic");
```

## Delayed Messages and TTL

```java
// Delayed delivery (5 second delay)
client.produceDelayed("my-topic", "Delayed message".getBytes(), 5000);

// Message with TTL (expires in 60 seconds)
client.produceWithTTL("my-topic", "Expiring message".getBytes(), 60000);
```

## Encryption (AES-256-GCM)

The SDK supports AES-256-GCM encryption for both data-in-motion and data-at-rest, compatible with FlyMQ server.

```java
import com.firefly.flymq.crypto.Encryptor;

// Generate a new encryption key
String key = Encryptor.generateKey(); // 64-char hex string

// Create encryptor
Encryptor encryptor = Encryptor.fromHexKey(key);

// Encrypt/decrypt data
byte[] encrypted = encryptor.encrypt("Hello, FlyMQ!".getBytes());
byte[] decrypted = encryptor.decrypt(encrypted);

// Enable encryption in client config
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9092")
    .encryptionKey(key)
    .build();
```

## Spring Boot MVC Integration

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

## Spring Boot WebFlux (Reactive)

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
    
    // Reactive transactions
    public Mono<Void> sendAtomicMessages(String topic1, String topic2) {
        return client.beginTransaction()
            .flatMap(txn -> 
                txn.produce(topic1, "msg1".getBytes())
                   .then(txn.produce(topic2, "msg2".getBytes()))
                   .then(txn.commit())
                   .onErrorResume(e -> txn.rollback().then(Mono.error(e)))
            );
    }
}
```

## Error Handling

FlyMQ Java SDK provides clear error messages with helpful hints to speed up debugging.

### Exception Types

```java
import com.firefly.flymq.exception.FlyMQException;
import com.firefly.flymq.exception.ProtocolException;

try {
    client.produce("my-topic", data);
} catch (FlyMQException e) {
    System.err.println("Error: " + e.getMessage());
    System.err.println("Hint: " + e.getHint());  // Helpful suggestion!
    System.err.println("Full: " + e.getFullMessage());
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
    //         or use the CLI: flymq-cli topic create nonexistent-topic
}
```

### Factory Methods for Common Errors

```java
// Create exceptions with helpful hints
throw FlyMQException.connectionFailed("localhost:9092", cause);
// Hint: Check that the FlyMQ server is running and accessible...

throw FlyMQException.topicNotFound("my-topic");
// Hint: Create the topic first with client.createTopic("my-topic", partitions)...

throw FlyMQException.authenticationFailed("alice");
// Hint: Check that the username and password are correct...

throw FlyMQException.timeout("produce");
// Hint: The server may be overloaded or unreachable...
```

### Graceful Error Recovery

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

## Configuration Options

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

## Records (DTOs)

The SDK uses Java Records for immutable DTOs:

```java
// ProduceResult - result of produce operation
record ProduceResult(String topic, long offset, int partition, Instant timestamp) {}

// ConsumedMessage - a consumed message with key support
record ConsumedMessage(String topic, int partition, long offset, 
                       byte[] key, byte[] data, 
                       Instant timestamp, Map<String, String> headers) {
    // Convenience methods
    String keyAsString() { return key != null ? new String(key, UTF_8) : null; }
    String dataAsString() { return new String(data, UTF_8); }
    boolean hasKey() { return key != null && key.length > 0; }
}

// FetchResult - result of fetch operation
record FetchResult(List<ConsumedMessage> messages, long nextOffset) {}
```

## License

Copyright (c) 2026 Firefly Software Solutions Inc.

Licensed under the Apache License, Version 2.0.
