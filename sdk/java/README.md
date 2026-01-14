# FlyMQ Java Client SDK

A high-performance Java client library for FlyMQ message queue with Spring Boot support.

**Website:** [https://getfirefly.io](https://getfirefly.io)

## Requirements

- Java 21+
- Maven 3.8+

## Features

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

### Basic Usage

```java
import com.firefly.flymq.FlyMQClient;

try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
    // Create a topic
    client.createTopic("my-topic", 3);
    
    // Produce a message
    long offset = client.produce("my-topic", "Hello, FlyMQ!".getBytes());
    System.out.println("Message written at offset: " + offset);
    
    // Consume the message
    byte[] data = client.consume("my-topic", offset);
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

## Consumer Groups

```java
import com.firefly.flymq.protocol.Records.SubscribeMode;

// Subscribe with a consumer group
long startOffset = client.subscribe("my-topic", "my-group", 0, SubscribeMode.EARLIEST);

// Fetch messages in batches
var result = client.fetch("my-topic", 0, startOffset, 100);
for (var message : result.messages()) {
    System.out.println("Received: " + message.dataAsString());
}

// Commit offset
client.commitOffset("my-topic", "my-group", 0, result.nextOffset());
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
