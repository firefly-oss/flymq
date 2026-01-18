# Security Guide - Java SDK

Comprehensive guide to implementing security in your FlyMQ Java applications.

## Table of Contents

- [Authentication](#authentication)
- [TLS/SSL](#tlsssl)
- [Data-at-Rest Encryption](#data-at-rest-encryption)
- [Best Practices](#best-practices)

## Authentication

### Basic Authentication

FlyMQ supports username/password authentication:

```java
try (FlyMQClient client = FlyMQClient.builder()
        .address("localhost:9092")
        .username("admin")
        .password("password")
        .build()) {
    
    // Verify identity
    FlyMQClient.WhoAmIResponse whoami = client.whoami();
    System.out.println("Logged in as: " + whoami.username);
}
```

### Loading Credentials from Environment

Never hardcode credentials:

```java
String username = System.getenv("FLYMQ_USERNAME");
String password = System.getenv("FLYMQ_PASSWORD");

if (username == null || password == null) {
    throw new IllegalStateException("Credentials not configured");
}

try (FlyMQClient client = FlyMQClient.builder()
        .address("localhost:9092")
        .username(username)
        .password(password)
        .build()) {
    // Authenticated connection
}
```

## TLS/SSL

### Security Levels

FlyMQ supports multiple TLS security levels:

| Level | Description | Use Case |
|-------|-------------|----------|
| **No TLS** | Plain text communication | Development only |
| **TLS (Server Auth)** | Server certificate validation | Production (basic) |
| **TLS + CA** | Custom CA certificate | Private PKI |
| **Mutual TLS** | Client + server certificates | High security |
| **Insecure TLS** | Skip certificate verification | Testing/debugging |

### Basic TLS Connection

```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.config.TLSConfig;

// Method 1: Using TLSConfig (recommended)
TLSConfig tls = TLSConfig.builder()
    .enabled(true)
    .caFile("/path/to/ca.crt")
    .build();

try (FlyMQClient client = FlyMQClient.builder()
        .address("localhost:9093")
        .tls(tls)
        .build()) {
    // Secure connection established
}
```

### System CA Certificates

Use the operating system's certificate store:

```java
// Uses system CA store automatically
TLSConfig tls = TLSConfig.builder()
    .enabled(true)
    .build();

try (FlyMQClient client = FlyMQClient.builder()
        .address("flymq.example.com:9093")
        .tls(tls)
        .build()) {
    // Connection uses system CA certificates
}
```

### Mutual TLS (mTLS)

Use client certificates for bidirectional authentication:

```java
TLSConfig tls = TLSConfig.builder()
    .enabled(true)
    .certFile("/path/to/client.crt")     // Client certificate
    .keyFile("/path/to/client.key")      // Client private key
    .caFile("/path/to/ca.crt")           // CA certificate
    .serverName("flymq-server")          // Optional: override hostname
    .build();

try (FlyMQClient client = FlyMQClient.builder()
        .address("localhost:9093")
        .tls(tls)
        .build()) {
    // mTLS connection established
}
```

### Skip Verification (Testing Only)

```java
// WARNING: Never use in production!
TLSConfig tls = TLSConfig.builder()
    .enabled(true)
    .insecureSkipVerify(true)  // Dangerous!
    .build();

try (FlyMQClient client = FlyMQClient.builder()
        .address("localhost:9093")
        .tls(tls)
        .build()) {
    // Insecure connection (testing only)
}
```

## Data-at-Rest Encryption

### Server-Side Encryption

Data-at-rest encryption is configured on the **FlyMQ server**, not in the client SDK. The server encrypts all stored messages using AES-256-GCM.

**Server Configuration:**

```bash
# Generate encryption key
openssl rand -hex 32 > /etc/flymq/encryption.key
chmod 600 /etc/flymq/encryption.key

# Set environment variable
export FLYMQ_ENCRYPTION_KEY=$(cat /etc/flymq/encryption.key)

# Start server with encryption enabled
flymq --config /etc/flymq/flymq.json
```

**Client Usage:**

Clients don't need any special configuration. Messages are automatically encrypted when stored and decrypted when retrieved:

```java
import com.firefly.flymq.FlyMQClient;

try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
    // Messages are automatically encrypted at rest by the server
    var meta = client.produce("my-topic", "Sensitive data".getBytes());
    
    // Messages are automatically decrypted when consumed
    byte[] data = client.consume("my-topic", meta.offset);
    System.out.println(new String(data));  // "Sensitive data"
}
```

**Security Layers:**

1. **TLS** - Encrypts data in transit (client ↔ server)
2. **Server-Side Encryption** - Encrypts data at rest (on disk)
3. **Authentication** - Controls who can access data

```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.config.TLSConfig;

// Complete security stack
TLSConfig tls = TLSConfig.builder()
    .enabled(true)
    .caFile("/path/to/ca.crt")
    .build();

try (FlyMQClient client = FlyMQClient.builder()
        .address("flymq.example.com:9093")
        .tls(tls)                    // Encrypts data in transit
        .username("admin")
        .password("secret")           // Authenticates access
        .build()) {
    // Server handles data-at-rest encryption automatically
}
```

## Combined Security

Use TLS + Authentication for complete security:

```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.config.TLSConfig;

TLSConfig tls = TLSConfig.builder()
    .enabled(true)
    .certFile("/path/to/client.crt")
    .keyFile("/path/to/client.key")
    .caFile("/path/to/ca.crt")
    .build();

try (FlyMQClient client = FlyMQClient.builder()
        .address("flymq.example.com:9093")
        .tls(tls)                                    // TLS encryption
        .username(System.getenv("FLYMQ_USERNAME"))  // Authentication
        .password(System.getenv("FLYMQ_PASSWORD"))
        .build()) {
    
    // All communication is now:
    // 1. Encrypted via TLS (channel security)
    // 2. Authenticated via mTLS certificates
    // 3. Authenticated via username/password
    // 4. Messages encrypted at rest by server (if configured)
}
```

## Best Practices

### Credential Management

✅ DO:
- Store credentials in environment variables
- Use secrets management (Vault, AWS Secrets Manager)
- Rotate credentials regularly
- Use strong passwords

❌ DON'T:
- Hardcode credentials in source code
- Commit credentials to version control
- Use weak or default passwords
- Share credentials between applications

### TLS/SSL Certificates

✅ DO:
- Use certificates from trusted CA in production
- Enable certificate verification
- Use TLS 1.2 or higher
- Monitor certificate expiry

❌ DON'T:
- Use self-signed certificates in production
- Disable certificate verification in production
- Use expired certificates

### Environment-Based Configuration

```java
import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.config.TLSConfig;

public FlyMQClient createSecureClient() {
    String env = System.getenv("ENV");
    
    if ("production".equals(env)) {
        TLSConfig tls = TLSConfig.builder()
            .enabled(true)
            .caFile(System.getenv("FLYMQ_CA_FILE"))
            .build();
            
        return FlyMQClient.builder()
            .address(System.getenv("FLYMQ_SERVERS"))
            .tls(tls)
            .build();
    } else {
        return FlyMQClient.connect("localhost:9092");
    }
}
```

## Security Checklist

Before deploying to production:

- [ ] Credentials loaded from environment variables
- [ ] TLS enabled with trusted CA certificates
- [ ] Client certificate authentication (mTLS) enabled
- [ ] Application-level authentication configured
- [ ] Server-side encryption enabled (FLYMQ_ENCRYPTION_KEY)
- [ ] Network access restricted to authorized sources
- [ ] Audit logging enabled
- [ ] Regular security audits scheduled
