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

The PyFlyMQ SDK provides native Python bindings to the FlyMQ message broker, supporting both synchronous and asynchronous message processing patterns. The implementation is built on the FlyMQ binary protocol specification and provides comprehensive type hints using Pydantic models.

**Installation**:
```bash
pip install pyflymq
# or from source
pip install ./sdk/python
```

**System Requirements**:
- Python 3.7 or later
- 64-bit operating system (Linux, macOS, Windows)
- TCP/IP network connectivity to FlyMQ broker

**Primary Documentation**:
- [Getting Started Guide](sdk/python/docs/GETTING_STARTED.md) - Installation, architecture overview, quick start guide
- [Security Configuration Guide](sdk/python/docs/SECURITY.md) - Authentication, encryption, TLS/mTLS configuration
- [Examples Directory](sdk/python/examples/) - 10 comprehensive working examples covering all major features

**Key Features**:
- Full binary protocol support with efficient serialization
- AES-256-GCM message encryption for data confidentiality
- Pydantic validated models with runtime type checking
- Reactive stream processing via RxPY integration
- Consumer group management with persistent offset tracking
- ACID transactions with automatic rollback on error
- TLS and mTLS mutual authentication
- Comprehensive error handling and retry mechanisms

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

The FlyMQ Java SDK provides type-safe, high-performance bindings for JVM-based applications. The implementation features thread-safe operations, automatic failover mechanisms, and seamless Spring Boot integration through auto-configuration.

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
- [Getting Started Guide](sdk/java/docs/GETTING_STARTED.md) - Maven/Gradle configuration, setup procedures, initial examples
- [Producer Design Guide](sdk/java/docs/PRODUCER_GUIDE.md) - Production patterns, transaction handling, and optimization
- [Examples Directory](sdk/java/examples/) - Core Java and Spring Boot integration examples

**Key Features**:
- Full binary protocol support with efficient serialization
- Automatic failover and high availability with connection pooling
- Thread-safe concurrent operations for multi-threaded environments
- TLS and mTLS mutual authentication with certificate validation
- Consumer groups with offset management and rebalancing
- ACID transactions across multiple topics
- Spring Boot auto-configuration with lifecycle hooks
- Comprehensive metrics and monitoring integration

**Available Examples**:

Core Examples (standalone Java without framework dependencies):
- **BasicProduceConsume.java** - Foundational producer-consumer pattern demonstrating core operations
- **KeyBasedMessaging.java** - Key-based partitioning with JSON payload serialization
- **ConsumerGroupExample.java** - Consumer group semantics with offset management and concurrent consumption

Spring Boot Examples (integrated with Spring lifecycle):
- **MessageService.java** - Service injection pattern demonstrating Spring component lifecycle integration

---

## Common Messaging Patterns

This section presents common messaging patterns implemented in both Python and Java, with detailed explanations of use cases and trade-offs.

### Pattern 1: Basic Producer/Consumer

**Purpose**: Fundamental message production and consumption, suitable for simple point-to-point communication or learning FlyMQ basics.

**Use Cases**:
- Simple message queuing without ordering requirements
- Testing and development environments
- Learning FlyMQ fundamentals
- Fire-and-forget messaging

**Python Implementation**:
```python
from pyflymq import FlyMQClient

# Create a client instance for communicating with FlyMQ broker
client = FlyMQClient("localhost:9092")

# Produce a message and receive offset confirmation
message_bytes = b"Hello, FlyMQ!"
offset = client.produce("my-topic", message_bytes)
print(f"Message published at offset: {offset}")

# Consume the message using its offset
consumed_message = client.consume("my-topic", offset)
print(f"Received: {consumed_message.decode()}")
```

**Java Implementation**:
```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.ConsumedMessage;

try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
    // Produce message and capture offset for later retrieval
    byte[] messageBytes = "Hello, FlyMQ!".getBytes(StandardCharsets.UTF_8);
    long offset = client.produce("my-topic", messageBytes);
    System.out.println("Message published at offset: " + offset);
    
    // Consume using explicit offset
    ConsumedMessage msg = client.consumeWithKey("my-topic", offset);
    System.out.println("Received: " + msg.dataAsString());
}
```

**Trade-offs**:
- Simple and straightforward implementation
- No message ordering guarantees (unless using key-based partitioning)
- Requires manual offset management
- Not suitable for distributed consumer groups

---

### Pattern 2: Consumer Groups

**Purpose**: Distributed message consumption across multiple consumers with automatic offset tracking and rebalancing.

**Use Cases**:
- Scalable message processing with multiple workers
- Load distribution across consumer instances
- Automatic failover when consumers disconnect
- Offset persistence for crash recovery

**Python Implementation**:
```python
from pyflymq import Consumer

# Create consumer group for coordinated consumption
consumer = Consumer(
    client,
    topic="events-topic",
    group_id="event-processors",
    auto_commit=False  # Manual offset management
)

# Iterate through messages in consumer group
for message in consumer:
    # Process message with error handling
    try:
        process_event(message.decode())
        # Commit offset only after successful processing
        consumer.commit()
    except Exception as e:
        print(f"Processing failed: {e}")
        # Leave offset uncommitted for retry
```

**Java Implementation**:
```java
import com.firefly.flymq.Consumer;
import com.firefly.flymq.ConsumedMessage;
import java.time.Duration;
import java.util.List;

Consumer consumer = new Consumer(client, "events-topic", "event-processors");
consumer.subscribe();

// Poll for messages with configurable timeout
while (running) {
    List<ConsumedMessage> messages = consumer.poll(Duration.ofSeconds(1));
    for (ConsumedMessage msg : messages) {
        try {
            processEvent(msg);
            consumer.commitSync();  // Synchronous offset commit
        } catch (Exception e) {
            logger.error("Processing failed", e);
            // Offset not committed; message will be reprocessed
        }
    }
}
```

**Trade-offs**:
- Automatic load balancing across consumers
- Offset persistence enables crash recovery
- Rebalancing can cause brief processing delays
- Requires careful management of processing vs. commit timing

---

### Pattern 3: Transactions

**Purpose**: Atomic multi-topic operations ensuring all-or-nothing semantics across multiple message topics.

**Use Cases**:
- Atomic operations across multiple topics
- Ensuring data consistency in distributed systems
- Preventing partial message delivery
- Maintaining referential integrity

**Python Implementation**:
```python
# Begin transaction context for atomic multi-topic operations
with client.transaction() as txn:
    # All operations within transaction
    txn.produce("orders-topic", order_data)
    txn.produce("audit-topic", audit_log)
    # Automatically commits on context exit if no exceptions
    # Automatically rolls back if exception occurs
```

**Java Implementation**:
```java
try (Transaction txn = client.beginTransaction()) {
    // Produce messages in transaction scope
    txn.produce("orders-topic", orderData.getBytes());
    txn.produce("audit-topic", auditLog.getBytes());
    // Explicit commit required
    txn.commit();
    // If commit fails or exception occurs, automatic rollback
} catch (Exception e) {
    // Transaction automatically rolled back in catch or finally
    logger.error("Transaction failed", e);
}
```

**Trade-offs**:
- Provides atomicity guarantees across topics
- Commits are atomic and durable
- Rollback has performance implications
- Not suitable for very large batches

---

### Pattern 4: Encryption

**Purpose**: End-to-end encryption of message content using AES-256-GCM, providing data confidentiality at rest and in transit.

**Use Cases**:
- Encrypting sensitive data before transmission
- Compliance with data protection regulations (GDPR, HIPAA)
- Protecting messages from unauthorized access
- Meeting security requirements for financial or healthcare data

**Python Implementation**:
```python
from pyflymq import FlyMQClient, generate_key

# Generate cryptographically secure encryption key
encryption_key = generate_key()

# Initialize client with encryption enabled
secure_client = FlyMQClient(
    "localhost:9092",
    encryption_key=encryption_key,
    username="encrypted-user",
    password="secret"
)

# Messages are automatically encrypted on production
sensitive_data = b"{"credit_card": "1234-5678-9012-3456"}"
offset = secure_client.produce("payment-topic", sensitive_data)

# Messages are automatically decrypted on consumption
decrypted_message = secure_client.consume("payment-topic", offset)
```

**Java Implementation**:
```java
import com.firefly.flymq.ClientConfig;

// Build configuration with encryption
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9092")
    .encryptionKey(generateKey())  // Generate or load key
    .username("encrypted-user")
    .password("secret")
    .build();

FlyMQClient client = new FlyMQClient(config);

// Messages automatically encrypted/decrypted
byte[] sensitiveData = "{\"credit_card\": \"1234-5678-9012-3456\"}".getBytes();
client.produce("payment-topic", sensitiveData);
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

**Basic Connection**:
```python
from pyflymq import FlyMQClient

client = FlyMQClient(
    bootstrap_servers="localhost:9092",  # Broker address
    username="alice",                     # Authentication
    password="secret",
    connection_timeout_ms=5000,          # Connection timeout
    request_timeout_ms=30000,            # Request timeout
)
```

**Security Configuration**:
```python
client = FlyMQClient(
    bootstrap_servers="broker.example.com:9092",
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

**Basic Connection**:
```java
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9092")
    .username("alice")
    .password("secret")
    .connectionTimeoutMs(5000)
    .requestTimeoutMs(30000)
    .build();

FlyMQClient client = new FlyMQClient(config);
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

## Feature Comparison Matrix

| Feature | Python | Java |
|---------|--------|------|
| Basic Producer/Consumer | Yes | Yes |
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
