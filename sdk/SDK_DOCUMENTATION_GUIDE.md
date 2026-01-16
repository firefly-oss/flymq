# FlyMQ SDK Documentation Guide

## Overview

This document serves as a comprehensive reference for FlyMQ SDK documentation, examples, and tutorials. It provides detailed guidance on using FlyMQ client libraries in production environments, covering foundational concepts through advanced patterns. The guide includes API references, working examples in multiple languages, configuration guidance, and troubleshooting information.

All documentation is aligned with actual SDK implementations and has been tested for technical accuracy.

## Table of Contents

- [Available SDKs](#available-sdks)
- [Common Messaging Patterns](#common-messaging-patterns)
- [Configuration Reference](#configuration-reference)
- [Feature Comparison](#feature-comparison-matrix)
- [Troubleshooting Guide](#troubleshooting)
- [Best Practices](#best-practices)
- [Documentation Structure](#documentation-structure)

---

## Available SDKs

### Python SDK (PyFlyMQ)

The PyFlyMQ SDK provides native Python bindings to the FlyMQ message broker with **Kafka-like APIs** for familiar developer experience. The implementation is built on the FlyMQ binary protocol specification and provides comprehensive type hints using Pydantic models.

**Installation**:
```bash
pip install pyflymq
# or from source
pip install ./sdk/python
```

**Quick Start**:
```python
from pyflymq import connect

# One-liner connection
client = connect("localhost:9092")

# High-level producer with batching
with client.producer(batch_size=100) as producer:
    producer.send("events", b'{"event": "click"}', key="user-123")
    producer.flush()

# High-level consumer with auto-commit
with client.consumer("events", "my-group") as consumer:
    for msg in consumer:
        print(f"Key: {msg.key}, Value: {msg.decode()}")

client.close()
```

**System Requirements**:
- Python 3.7 or later
- 64-bit operating system (Linux, macOS, Windows)
- TCP/IP network connectivity to FlyMQ broker

**Primary Documentation**:
- [Getting Started Guide](python/docs/GETTING_STARTED.md) - Installation, architecture overview, quick start guide
- [Producer Patterns](python/docs/PRODUCER_PATTERNS.md) - HighLevelProducer, batching, callbacks
- [Consumer Patterns](python/docs/CONSUMER_PATTERNS.md) - HighLevelConsumer, poll-based consumption
- [Security Configuration Guide](python/docs/SECURITY.md) - Authentication, encryption, TLS/mTLS configuration
- [Examples Directory](python/examples/) - 10 comprehensive working examples covering all major features

**Key Features**:
- **`connect()` function** - One-liner connection for quick setup
- **`HighLevelProducer`** - Batching, callbacks, automatic retries
- **`HighLevelConsumer`** - Auto-commit, poll-based consumption, seek operations
- **Error hints** - All exceptions include actionable suggestions
- Full binary protocol support with efficient serialization
- AES-256-GCM message encryption for data confidentiality
- Pydantic validated models with runtime type checking
- Reactive stream processing via RxPY integration
- Consumer group management with persistent offset tracking
- ACID transactions with automatic rollback on error
- TLS and mTLS mutual authentication

**Available Examples**:

1. **01_basic_produce_consume.py** - Foundational producer and consumer operations with synchronous message flow
2. **02_key_based_messaging.py** - Key-based message partitioning following Kafka semantics for ordered delivery
3. **03_consumer_groups.py** - Consumer group management including offset tracking and rebalancing
4. **04_transactions.py** - Atomic multi-topic transactions with all-or-nothing semantics
5. **05_encryption.py** - End-to-end message encryption using AES-256-GCM
6. **06_reactive_streams.py** - Reactive processing patterns using RxPY for event-driven architectures
7. **07_schema_validation.py** - Schema registry integration and message validation
8. **08_error_handling.py** - Comprehensive error handling patterns and retry strategies
9. **09_tls_authentication.py** - TLS configuration with client certificates and CA validation
10. **10_advanced_patterns.py** - Batch processing, connection pooling, and dead letter queue handling

---

### Java SDK

The FlyMQ Java SDK provides type-safe, high-performance bindings for JVM-based applications with **Kafka-like APIs** for familiar developer experience. The implementation features thread-safe operations, automatic failover mechanisms, and seamless Spring Boot integration through auto-configuration.

**Quick Start**:
```java
import com.firefly.flymq.FlyMQClient;

// One-liner connection
try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {

    // High-level producer with batching
    try (var producer = client.producer()) {
        producer.send("events", "{\"event\": \"click\"}".getBytes(), "user-123".getBytes())
            .whenComplete((metadata, error) -> {
                if (error == null) {
                    System.out.println("Sent @ offset " + metadata.offset());
                }
            });
        producer.flush();
    }

    // High-level consumer with auto-commit
    try (var consumer = client.consumer("events", "my-group")) {
        consumer.subscribe();
        for (var msg : consumer.poll(Duration.ofSeconds(1))) {
            System.out.println("Key: " + msg.keyAsString() + ", Value: " + msg.dataAsString());
        }
    }
}
```

**Dependency Configuration** (Maven):
```xml
<!-- Core FlyMQ client library with binary protocol implementation -->
<dependency>
    <groupId>com.firefly.flymq</groupId>
    <artifactId>flymq-client-core</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- Spring Boot MVC auto-configuration and management -->
<dependency>
    <groupId>com.firefly.flymq</groupId>
    <artifactId>flymq-client-spring</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- Spring Boot WebFlux for reactive and non-blocking operations -->
<dependency>
    <groupId>com.firefly.flymq</groupId>
    <artifactId>flymq-client-spring-webflux</artifactId>
    <version>1.0.0</version>
</dependency>
```

**System Requirements**:
- Java 21 or later (for full feature support including virtual threads)
- Maven 3.8+ or Gradle 7.0+ for dependency management
- 64-bit JVM with sufficient heap memory (minimum 512MB recommended)

**Primary Documentation**:
- [Getting Started Guide](java/docs/GETTING_STARTED.md) - Maven/Gradle configuration, setup procedures, initial examples
- [Producer Design Guide](java/docs/PRODUCER_GUIDE.md) - HighLevelProducer, batching, callbacks
- [Consumer Guide](java/docs/CONSUMER_GUIDE.md) - Consumer, poll-based consumption, seek operations
- [Examples Directory](java/examples/) - Core Java and Spring Boot integration examples

**Key Features**:
- **`connect()` static method** - One-liner connection for quick setup
- **`HighLevelProducer`** - Batching, CompletableFuture callbacks, automatic retries
- **`Consumer`** - Auto-commit, poll-based consumption, seek operations
- **Error hints** - All exceptions include `getHint()` with actionable suggestions
- Full binary protocol support with efficient serialization
- Automatic failover and high availability with connection pooling
- Thread-safe concurrent operations for multi-threaded environments
- TLS and mTLS mutual authentication with certificate validation
- Consumer groups with offset management and rebalancing
- ACID transactions across multiple topics
- Spring Boot auto-configuration with lifecycle hooks

**Available Examples**:

Core Examples (standalone Java without framework dependencies):
- **BasicProduceConsume.java** - Foundational producer-consumer pattern with `connect()` and high-level APIs
- **KeyBasedMessaging.java** - Key-based partitioning with HighLevelProducer
- **ConsumerGroupExample.java** - Consumer group semantics with HighLevelConsumer
- **TransactionExample.java** - Atomic multi-topic transactions
- **ErrorHandlingExample.java** - Exception handling with hints

Spring Boot Examples (integrated with Spring lifecycle):
- **MessageService.java** - Service injection pattern demonstrating Spring component lifecycle integration

---

## Common Messaging Patterns

This section presents common messaging patterns implemented in both Python and Java using the **high-level Kafka-like APIs**.

### Pattern 1: Basic Producer/Consumer (High-Level)

**Purpose**: Fundamental message production and consumption using the recommended high-level APIs.

**Use Cases**:
- Production workloads with batching and callbacks
- Simple message queuing with automatic retries
- Learning FlyMQ with familiar Kafka-like patterns

**Python Implementation**:
```python
from pyflymq import connect

# One-liner connection
client = connect("localhost:9092")

# High-level producer with batching
with client.producer(batch_size=100) as producer:
    producer.send("my-topic", b"Hello, FlyMQ!", key="user-123",
                  on_success=lambda m: print(f"Sent @ offset {m.offset}"))
    producer.flush()

# High-level consumer with auto-commit
with client.consumer("my-topic", "my-group") as consumer:
    for msg in consumer:
        print(f"Key: {msg.key}, Value: {msg.decode()}")
        break  # Process one message for demo

client.close()
```

**Java Implementation**:
```java
import com.firefly.flymq.FlyMQClient;

// One-liner connection
try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {

    // High-level producer with batching
    try (var producer = client.producer()) {
        producer.send("my-topic", "Hello, FlyMQ!".getBytes(), "user-123".getBytes())
            .whenComplete((m, e) -> {
                if (e == null) System.out.println("Sent @ offset " + m.offset());
            });
        producer.flush();
    }

    // High-level consumer with auto-commit
    try (var consumer = client.consumer("my-topic", "my-group")) {
        consumer.subscribe();
        for (var msg : consumer.poll(Duration.ofSeconds(1))) {
            System.out.println("Key: " + msg.keyAsString() + ", Value: " + msg.dataAsString());
        }
    }
}
```

**Benefits**:
- Familiar Kafka-like API patterns
- Automatic batching for better throughput
- Callbacks for async result handling
- Auto-commit for simpler offset management

---

### Pattern 2: Consumer Groups (High-Level)

**Purpose**: Distributed message consumption using the high-level consumer API with automatic offset tracking.

**Use Cases**:
- Scalable message processing with multiple workers
- Load distribution across consumer instances
- Automatic failover when consumers disconnect
- Offset persistence for crash recovery

**Python Implementation**:
```python
from pyflymq import connect

client = connect("localhost:9092")

# High-level consumer with auto-commit (default)
with client.consumer("events-topic", "event-processors") as consumer:
    for message in consumer:
        try:
            process_event(message.decode())
            # Auto-commit handles offset management
        except Exception as e:
            print(f"Processing failed: {e}")

# Or with manual commit for at-least-once semantics
with client.consumer("events-topic", "event-processors", auto_commit=False) as consumer:
    for message in consumer:
        process_event(message.decode())
        consumer.commit()  # Manual commit after processing
```

**Java Implementation**:
```java
import com.firefly.flymq.FlyMQClient;
import java.time.Duration;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {

    // High-level consumer with auto-commit (default)
    try (var consumer = client.consumer("events-topic", "event-processors")) {
        consumer.subscribe();

        while (running) {
            var messages = consumer.poll(Duration.ofSeconds(1));
            for (var msg : messages) {
                try {
                    processEvent(msg);
                    // Auto-commit handles offset management
                } catch (Exception e) {
                    logger.error("Processing failed", e);
                }
            }
        }
    }
}
```

**Benefits**:
- Automatic load balancing across consumers
- Auto-commit simplifies offset management
- Poll-based consumption for batch processing
- Seek operations for replay scenarios

---

### Pattern 3: High-Throughput Producer

**Purpose**: Maximize message throughput using the HighLevelProducer with batching and async callbacks.

**Use Cases**:
- High-volume event streaming
- Log aggregation pipelines
- Real-time analytics ingestion
- IoT sensor data collection

**Python Implementation**:
```python
from pyflymq import connect

client = connect("localhost:9092")

# Configure for high throughput
with client.producer(batch_size=1000, linger_ms=50) as producer:
    # Send 100k messages efficiently
    for i in range(100000):
        producer.send("events", f"event-{i}".encode())

    producer.flush()  # Ensure all sent
    print("All messages sent!")
```

**Java Implementation**:
```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.producer.ProducerConfig;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {

    ProducerConfig config = ProducerConfig.builder()
        .batchSize(1000)
        .lingerMs(50)
        .build();

    try (var producer = client.producer(config)) {
        // Send 100k messages efficiently
        for (int i = 0; i < 100000; i++) {
            producer.send("events", ("event-" + i).getBytes());
        }
        producer.flush();
        System.out.println("All messages sent!");
    }
}
```

**Benefits**:
- Batching reduces network round-trips
- Linger time allows batch to fill
- Fire-and-forget for maximum speed
- Flush ensures delivery before exit

---

### Pattern 4: Transactions

**Purpose**: Atomic multi-topic operations ensuring all-or-nothing semantics across multiple message topics.

**Use Cases**:
- Atomic operations across multiple topics
- Ensuring data consistency in distributed systems
- Preventing partial message delivery
- Maintaining referential integrity

**Python Implementation**:
```python
from pyflymq import connect

client = connect("localhost:9092")

# Begin transaction context for atomic multi-topic operations
with client.transaction() as txn:
    txn.produce("orders-topic", order_data)
    txn.produce("audit-topic", audit_log)
    # Automatically commits on context exit if no exceptions
    # Automatically rolls back if exception occurs
```

**Java Implementation**:
```java
try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    try (Transaction txn = client.beginTransaction()) {
        txn.produce("orders-topic", orderData.getBytes());
        txn.produce("audit-topic", auditLog.getBytes());
        txn.commit();
    } catch (Exception e) {
        // Transaction automatically rolled back
        logger.error("Transaction failed", e);
    }
}
```

**Trade-offs**:
- Provides atomicity guarantees across topics
- Commits are atomic and durable
- Rollback has performance implications
- Not suitable for very large batches

---

### Pattern 5: Encryption

**Purpose**: End-to-end encryption of message content using AES-256-GCM, providing data confidentiality at rest and in transit.

**Use Cases**:
- Encrypting sensitive data before transmission
- Compliance with data protection regulations (GDPR, HIPAA)
- Protecting messages from unauthorized access
- Meeting security requirements for financial or healthcare data

**Python Implementation**:
```python
from pyflymq import connect, generate_key

# Generate cryptographically secure encryption key
encryption_key = generate_key()

# Connect with encryption enabled
client = connect(
    "localhost:9092",
    encryption_key=encryption_key,
    username="encrypted-user",
    password="secret"
)

# Messages are automatically encrypted on production
sensitive_data = b'{"credit_card": "1234-5678-9012-3456"}'
offset = client.produce("payment-topic", sensitive_data)

# Messages are automatically decrypted on consumption
decrypted_message = client.consume("payment-topic", offset)
client.close()
```

**Java Implementation**:
```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.ClientConfig;

// Build configuration with encryption
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9092")
    .encryptionKey(generateKey())  // Generate or load key
    .username("encrypted-user")
    .password("secret")
    .build();

try (FlyMQClient client = new FlyMQClient(config)) {
    // Messages automatically encrypted/decrypted
    byte[] sensitiveData = "{\"credit_card\": \"1234-5678-9012-3456\"}".getBytes();
    client.produce("payment-topic", sensitiveData);
}
```

**Trade-offs**:
- Transparent encryption/decryption for application code
- Slight performance overhead from cryptographic operations
- Key management responsibility shifts to application
- Encrypted messages not human-readable in logs

---

## Configuration Reference

This section provides detailed configuration options for both Python and Java SDKs.

### Python Configuration

**Basic Connection (Recommended)**:
```python
from pyflymq import connect

# Simple connection
client = connect("localhost:9092")

# With authentication
client = connect(
    "localhost:9092",
    username="alice",
    password="secret"
)
```

**Producer Configuration**:
```python
# Configure high-level producer
with client.producer(
    batch_size=100,        # Max messages per batch
    linger_ms=10,          # Wait time for batching (ms)
    max_retries=3,         # Retry attempts on failure
    retry_backoff_ms=100   # Backoff between retries
) as producer:
    producer.send("topic", b"data")
```

**Consumer Configuration**:
```python
# Configure high-level consumer
with client.consumer(
    topics="my-topic",
    group_id="my-group",
    auto_commit=True,              # Enable auto-commit
    auto_commit_interval_ms=5000,  # Commit every 5 seconds
    max_poll_records=500           # Max records per poll
) as consumer:
    for msg in consumer:
        process(msg)
```

**Security Configuration**:
```python
client = connect(
    "broker.example.com:9092",
    # TLS/SSL configuration
    tls_enabled=True,
    tls_ca_file="/path/to/ca.pem",              # CA certificate
    tls_cert_file="/path/to/client-cert.pem",   # Client certificate
    tls_key_file="/path/to/client-key.pem",     # Client private key
    # Encryption configuration
    encryption_key="...",                        # AES-256 key
    # Credentials
    username="authenticated-user",
    password="secure-password",
)
```

### Java Configuration

**Basic Connection (Recommended)**:
```java
// Simple connection
FlyMQClient client = FlyMQClient.connect("localhost:9092");

// With authentication
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9092")
    .username("alice")
    .password("secret")
    .build();

FlyMQClient client = new FlyMQClient(config);
```

**Producer Configuration**:
```java
ProducerConfig config = ProducerConfig.builder()
    .batchSize(100)        // Max messages per batch
    .lingerMs(10)          // Wait time for batching
    .maxRetries(3)         // Retry attempts
    .retryBackoffMs(100)   // Backoff between retries
    .build();

try (var producer = client.producer(config)) {
    producer.send("topic", data);
}
```

**Consumer Configuration**:
```java
ConsumerConfig config = ConsumerConfig.builder()
    .maxPollRecords(500)           // Max records per poll
    .enableAutoCommit(true)        // Enable auto-commit
    .autoCommitIntervalMs(5000)    // Commit every 5 seconds
    .build();

try (var consumer = client.consumer("topic", "group", 0, config)) {
    consumer.subscribe();
    // ...
}
```

**Spring Boot Configuration** (application.properties):
```properties
# Broker connectivity
flymq.bootstrap-servers=localhost:9092

# Authentication
flymq.username=alice
flymq.password=secret

# Security
flymq.tls-enabled=true
flymq.tls-ca-file=/path/to/ca.pem
flymq.tls-cert-file=/path/to/client-cert.pem
flymq.tls-key-file=/path/to/client-key.pem

# Encryption
flymq.encryption-key=...

# Timeouts
flymq.connection-timeout-ms=5000
flymq.request-timeout-ms=30000

# Consumer group
flymq.consumer-group-id=my-group
flymq.auto-commit-enabled=true
flymq.auto-commit-interval-ms=5000
```

---

## RecordMetadata

All produce operations return `RecordMetadata`, similar to Kafka's RecordMetadata.
This provides complete information about where and when the message was stored.

### RecordMetadata Fields

| Field | Type | Description |
|-------|------|-------------|
| `topic` | string | Topic name the message was produced to |
| `partition` | int32 | Partition the message was assigned to |
| `offset` | uint64 | Offset of the message in the partition |
| `timestamp` | int64 | Server timestamp when message was stored (ms) |
| `key_size` | int32 | Size of key in bytes (-1 if no key) |
| `value_size` | int32 | Size of value in bytes |

### Python Example

```python
from pyflymq import connect

client = connect("localhost:9092")

# Get full RecordMetadata
meta = client.produce_with_metadata("events", b"data", key="user-123")

print(f"Topic: {meta.topic}")
print(f"Partition: {meta.partition}")
print(f"Offset: {meta.offset}")
print(f"Timestamp: {meta.timestamp_datetime}")  # datetime object
print(f"Key size: {meta.key_size}")
print(f"Value size: {meta.value_size}")

client.close()
```

### Java Example

```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.protocol.BinaryProtocol.RecordMetadata;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {

    // Get full RecordMetadata
    RecordMetadata meta = client.produceWithMetadata("events", data);

    System.out.println("Topic: " + meta.topic());
    System.out.println("Partition: " + meta.partition());
    System.out.println("Offset: " + meta.offset());
    System.out.println("Timestamp: " + meta.timestampAsInstant());
    System.out.println("Has key: " + meta.hasKey());
    System.out.println("Value size: " + meta.valueSize());
}
```

---

## Feature Comparison Matrix

| Feature | Python | Java |
|---------|--------|------|
| Basic Producer/Consumer | Yes | Yes |
| RecordMetadata | Yes | Yes |
| Consumer Groups | Yes | Yes |
| Key-Based Partitioning | Yes | Yes |
| Transactions | Yes | Yes |
| Message Encryption (AES-256-GCM) | Yes | Yes |
| TLS/mTLS Security | Yes | Yes |
| SASL Authentication | Yes | Yes |
| Reactive Streams | Yes (RxPY) | Yes (WebFlux) |
| Spring Boot Integration | No | Yes |
| Pydantic Models | Yes | No |
| JSON Serialization | Yes | Yes |
| Schema Registry Integration | Yes | Yes |
| Dead Letter Queues | Yes | Yes |
| Delayed Messages | Yes | Yes |
| Message TTL | Yes | Yes |
| Connection Pooling | Yes | Yes |
| Automatic Failover | Yes | Yes |

---

## Troubleshooting

### Connection Issues

**Symptom: Connection Refused**

Root causes:
- FlyMQ broker is not running or accessible
- Incorrect broker host or port
- Firewall blocking connection
- Network routing issues

Resolution steps:
1. Verify broker is running: `nc -zv localhost 9092`
2. Check broker configuration and startup logs
3. Verify firewall rules allow port 9092
4. Test with `telnet` or similar tools
5. Check network connectivity: `ping broker-host`

**Symptom: Connection Timeout**

Root causes:
- Broker is unreachable or slow to respond
- Network latency or congestion
- Firewall dropping packets
- Configuration timeouts too short

Resolution steps:
1. Check broker latency: `ping broker-host`
2. Increase timeout values if network is slow
3. Monitor broker CPU and memory
4. Check for network packet loss

---

### Authentication Issues

**Symptom: Authentication Failed**

Root causes:
- Incorrect username or password
- User does not exist on broker
- User lacks required permissions
- Authentication is disabled on broker

Resolution steps:
1. Verify username and password match broker configuration
2. Check user exists in broker user database
3. Verify user has "produce" and "consume" permissions
4. Check broker authentication is enabled
5. Review broker audit logs for failed attempts

---

### Message Size Issues

**Symptom: Message Too Large Error**

Root causes:
- Message exceeds 32MB size limit
- Broker configuration restricts message size
- Memory pressure on broker

Resolution steps:
1. Reduce message size or split into multiple messages
2. Compress message content before sending
3. Check broker configuration for message size limits
4. Monitor broker memory and disk space

---

### Encryption/Security Issues

**Symptom: TLS Certificate Validation Failed**

Root causes:
- CA certificate path is incorrect
- Certificate is expired or invalid
- Certificate hostname doesn't match broker host
- Certificate is self-signed without proper configuration

Resolution steps:
1. Verify CA certificate file path and content
2. Check certificate expiration: `openssl x509 -in cert.pem -noout -dates`
3. Verify certificate hostname matches broker
4. For self-signed testing only: disable verification (never in production)
5. Check certificate chain completeness

**Symptom: Encryption Key Mismatch**

Root causes:
- Consumer using different key than producer
- Key lost or changed
- Key corrupted during storage or transmission

Resolution steps:
1. Ensure all consumers use same encryption key as producer
2. Implement key versioning if keys are rotated
3. Store keys securely (environment variables, key management systems)
4. Verify key integrity and format

---

## Best Practices

### General Practices

**Client Instance Management**:
- Create FlyMQClient instances once and reuse for the lifetime of the application
- Close clients properly when application terminates
- Use context managers (Python `with` statement) or try-with-resources (Java)
- Avoid creating new client per operation (connection overhead)

**Error Handling**:
- Implement comprehensive exception handling around all broker operations
- Distinguish between transient errors (retry) and permanent errors (fail)
- Implement exponential backoff for retries
- Log all errors with sufficient context for debugging
- Monitor error rates and alert on anomalies

**Performance**:
- Batch multiple messages together before sending for throughput optimization
- Use connection pooling for multi-threaded applications
- Monitor message latencies and throughput
- Profile memory usage under load
- Load test before production deployment

---

### Security Practices

**Credential Management**:
- Never hardcode usernames or passwords in source code
- Load credentials from environment variables or secure vaults
- Use separate credentials for different applications
- Rotate credentials regularly
- Audit credential access and usage

**Encryption**:
- Always enable TLS in production environments
- Use TLS 1.2 or later (configure broker accordingly)
- Validate server certificates for authenticity
- Implement certificate rotation and renewal
- Use mutual TLS (mTLS) for bidirectional authentication
- Manage encryption keys securely with key management systems
- Rotate encryption keys at regular intervals (e.g., annually)

**Access Control**:
- Create users with minimal required permissions (principle of least privilege)
- Segregate credentials by application and environment
- Implement role-based access control at broker level
- Audit all authentication attempts and operations
- Monitor for suspicious access patterns

---

### Performance Tuning

**Connection Management**:
- Reuse client connections across operations
- Configure appropriate pool sizes for concurrent usage
- Monitor connection pool utilization
- Set reasonable timeout values (not too aggressive)

**Message Processing**:
- Batch produce operations to reduce round-trips
- Use transactions for multi-topic atomicity when needed
- Scale consumers horizontally with consumer groups
- Implement proper backpressure handling
- Monitor processing latencies and adjust batch sizes

**Consumer Optimization**:
- Configure appropriate poll timeout values
- Use multiple consumer instances for parallel processing
- Monitor consumer lag and rebalance events
- Implement graceful shutdown procedures
- Test behavior under high message volume

---

### Production Deployment

**Monitoring**:
- Monitor broker health and connectivity
- Track message production and consumption rates
- Monitor consumer lag for all groups
- Alert on connection failures or timeouts
- Log all operational events

**High Availability**:
- Configure client with multiple broker addresses for failover
- Implement circuit breakers for resilience
- Test failover scenarios before production
- Monitor and alert on failover events

**Testing**:
- Unit test all message handling logic
- Integration test with real FlyMQ instance
- Load test with expected production volume
- Chaos engineering test failover scenarios
- Test security and encryption configuration

---

## Documentation Structure

```
flymq/
├── README.md                               (project overview)
├── INSTALL.md                              (installation instructions)
├── docs/                                   (server documentation)
│   ├── getting-started.md
│   ├── architecture.md
│   ├── sdk_development.md
│   └── ... (other server docs)
│
└── sdk/
    ├── START_HERE.md                       (SDK navigation guide)
    ├── SDK_DOCUMENTATION_GUIDE.md          (this file - comprehensive SDK reference)
    ├── python/
    │   ├── README.md                       (Python SDK overview)
    │   ├── docs/
    │   │   ├── GETTING_STARTED.md          (installation, architecture, setup)
    │   │   └── SECURITY.md                 (auth, encryption, TLS configuration)
    │   └── examples/
    │       ├── 01_basic_produce_consume.py
    │       ├── 02_key_based_messaging.py
    │       ├── 03_consumer_groups.py
    │       ├── 04_transactions.py
    │       ├── 05_encryption.py
    │       ├── 06_reactive_streams.py
    │       ├── 07_schema_validation.py
    │       ├── 08_error_handling.py
    │       ├── 09_tls_authentication.py
    │       └── 10_advanced_patterns.py
    │
    └── java/
        ├── README.md                       (Java SDK overview)
        ├── docs/
        │   ├── GETTING_STARTED.md          (Maven, Spring Boot setup)
        │   └── PRODUCER_GUIDE.md           (patterns, transactions)
        └── examples/
            ├── core/
            │   ├── BasicProduceConsume.java
            │   ├── KeyBasedMessaging.java
            │   └── ConsumerGroupExample.java
            └── spring-mvc/
                └── MessageService.java
```

---

## Next Steps

1. **Choose your language**: Python or Java
2. **Read the Getting Started guide** for your language
3. **Execute the first example** to verify installation
4. **Study additional examples** covering your use cases
5. **Implement security configuration** for production
6. **Deploy with confidence** using best practices from this guide

---

**Documentation Version**: 1.0.0  
**FlyMQ SDK Version**: 1.0.0+  
**Last Updated**: January 2026  
**Status**: Production Ready
