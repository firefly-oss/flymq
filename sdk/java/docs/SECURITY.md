# Security Guide - Java SDK

Comprehensive guide to implementing security in your FlyMQ Java applications.

## Table of Contents

- [Authentication](#authentication)
- [TLS/SSL](#tlsssl)
- [Best Practices](#best-practices)

## Authentication

### Basic Authentication

FlyMQ supports username/password authentication:

```java
try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
    String token = client.authenticate("username", "password");
    System.out.println("Authenticated, token: " + token);
    
    // Verify identity
    String identity = client.whoAmI();
    System.out.println("Logged in as: " + identity);
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

try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
    client.authenticate(username, password);
}
```

### Token-Based Authentication

Use pre-existing tokens:

```java
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9092")
    .authToken(System.getenv("FLYMQ_TOKEN"))
    .build();

try (FlyMQClient client = new FlyMQClient(config)) {
    // Client is authenticated via token
}
```

## TLS/SSL

### Basic TLS Connection

```java
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9093")
    .tlsEnabled(true)
    .tlsCaFile("/path/to/ca.crt")
    .build();

try (FlyMQClient client = new FlyMQClient(config)) {
    // Secure connection established
}
```

### Mutual TLS (mTLS)

Use client certificates for bidirectional authentication:

```java
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9093")
    .tlsEnabled(true)
    .tlsCaFile("/path/to/ca.crt")
    .tlsCertFile("/path/to/client.crt")
    .tlsKeyFile("/path/to/client.key")
    .tlsVerifyHostname(true)
    .build();

try (FlyMQClient client = new FlyMQClient(config)) {
    // mTLS connection established
}
```

### Skip Verification (Testing Only)

```java
// WARNING: Never use in production!
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("localhost:9093")
    .tlsEnabled(true)
    .tlsInsecureSkipVerify(true)  // Dangerous!
    .build();
```

## Combined Security

Use TLS + Authentication for complete security:

```java
ClientConfig config = ClientConfig.builder()
    .bootstrapServers("flymq.example.com:9093")
    // TLS settings
    .tlsEnabled(true)
    .tlsCaFile("/path/to/ca.crt")
    .tlsCertFile("/path/to/client.crt")
    .tlsKeyFile("/path/to/client.key")
    // Timeouts
    .connectTimeoutMs(10000)
    .requestTimeoutMs(30000)
    .build();

try (FlyMQClient client = new FlyMQClient(config)) {
    // Authenticate after TLS handshake
    client.authenticate(
        System.getenv("FLYMQ_USERNAME"),
        System.getenv("FLYMQ_PASSWORD")
    );
    
    // Fully secure connection ready
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
public FlyMQClient createSecureClient() {
    String env = System.getenv("ENV");
    
    if ("production".equals(env)) {
        return new FlyMQClient(ClientConfig.builder()
            .bootstrapServers(System.getenv("FLYMQ_SERVERS"))
            .tlsEnabled(true)
            .tlsCaFile(System.getenv("FLYMQ_CA_FILE"))
            .build());
    } else {
        return new FlyMQClient("localhost:9092");
    }
}
```

## Security Checklist

Before deploying to production:

- [ ] Credentials loaded from environment variables
- [ ] TLS enabled with trusted CA certificates
- [ ] Client certificate authentication (mTLS) enabled
- [ ] Application-level authentication configured
- [ ] Network access restricted to authorized sources
- [ ] Audit logging enabled
- [ ] Regular security audits scheduled

