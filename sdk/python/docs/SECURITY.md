# Security Guide - PyFlyMQ

Comprehensive guide to implementing security in your FlyMQ applications.

## Table of Contents

- [Authentication](#authentication)
- [TLS/SSL](#tlsssl)
- [Data-at-Rest Encryption](#data-at-rest-encryption)
- [Best Practices](#best-practices)

## Authentication

### Basic Authentication

FlyMQ supports username/password authentication:

```python
from pyflymq import FlyMQClient

client = FlyMQClient(
    "localhost:9092",
    username="alice",
    password="secure_password"
)
```

### Loading Credentials from Environment

Never hardcode credentials in your code:

```python
import os
from pyflymq import FlyMQClient

client = FlyMQClient(
    "localhost:9092",
    username=os.getenv("FLYMQ_USERNAME"),
    password=os.getenv("FLYMQ_PASSWORD")
)
```

Set environment variables before running:

```bash
export FLYMQ_USERNAME=alice
export FLYMQ_PASSWORD=my_secure_password
python myapp.py
```

### User Management

Create and manage users on the server (requires admin privileges):

```python
# These methods are admin-only
client.create_user("bob", "password123", roles=["consumer", "producer"])
client.update_user("bob", roles=["admin"])
client.delete_user("bob")
client.list_users()
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

```python
from pyflymq import connect

# No encryption_key parameter needed!
client = connect("localhost:9092")

# Messages are automatically encrypted at rest by the server
meta = client.produce("my-topic", b"Sensitive data")

# Messages are automatically decrypted when consumed
msg = client.consume("my-topic", meta.offset)
print(msg.decode())  # "Sensitive data"
```

**Security Layers:**

1. **TLS** - Encrypts data in transit (client ↔ server)
2. **Server-Side Encryption** - Encrypts data at rest (on disk)
3. **Authentication** - Controls who can access data

```python
from pyflymq import connect, TLSConfig

# Complete security stack
tls = TLSConfig(
    enabled=True,
    ca_file="/path/to/ca.crt"
)

client = connect(
    "flymq.example.com:9093",
    tls=tls,                    # Encrypts data in transit
    username="admin",
    password="secret"            # Authenticates access
)
# Server handles data-at-rest encryption automatically
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

### Basic TLS (Server Verification)

Connect securely to FlyMQ using TLS:

```python
from pyflymq import connect, TLSConfig

# Method 1: Using TLSConfig (recommended)
tls = TLSConfig(
    enabled=True,
    ca_file="/path/to/ca.crt"  # CA certificate for verification
)
client = connect("flymq.example.com:9093", tls=tls)

# Method 2: Using parameters directly
client = connect(
    "flymq.example.com:9093",
    tls_enabled=True,
    tls_ca_file="/path/to/ca.crt"
)
```

### System CA Certificates

Use the operating system's certificate store:

```python
from pyflymq import connect, TLSConfig

# Uses system CA store automatically
tls = TLSConfig(enabled=True)
client = connect("flymq.example.com:9093", tls=tls)
```

### Mutual TLS (mTLS)

Use client certificates for bidirectional authentication:

```python
from pyflymq import connect, TLSConfig

tls = TLSConfig(
    enabled=True,
    cert_file="/path/to/client.crt",     # Client certificate
    key_file="/path/to/client.key",      # Client private key
    ca_file="/path/to/ca.crt",           # CA certificate
    server_name="flymq-server"           # Optional: override hostname
)
client = connect("flymq.example.com:9093", tls=tls)
```

### Skip Certificate Verification (Testing Only)

For development/testing only:

```python
from pyflymq import connect, TLSConfig

# WARNING: This disables all certificate verification
# NEVER use in production
tls = TLSConfig(
    enabled=True,
    insecure_skip_verify=True  # Dangerous!
)
client = connect("localhost:9093", tls=tls)
```

### Generating Test Certificates

For development and testing:

```bash
# Generate CA
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 365 -key ca.key -out ca.crt

# Generate server certificate
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr
openssl x509 -req -days 365 -in server.csr \
  -CA ca.crt -CAkey ca.key -out server.crt \
  -CAcreateserial

# Generate client certificate
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr
openssl x509 -req -days 365 -in client.csr \
  -CA ca.crt -CAkey ca.key -out client.crt \
  -CAcreateserial
```

## Combined Security

Use TLS + Authentication for complete security:

```python
import os
from pyflymq import connect, TLSConfig

tls = TLSConfig(
    enabled=True,
    cert_file="/path/to/client.crt",
    key_file="/path/to/client.key",
    ca_file="/path/to/ca.crt"
)

client = connect(
    "flymq.example.com:9093",
    tls=tls,                                    # TLS encryption
    username=os.getenv("FLYMQ_USERNAME"),      # Authentication
    password=os.getenv("FLYMQ_PASSWORD")
)

# All communication is now:
# 1. Encrypted via TLS (channel security)
# 2. Authenticated via mTLS certificates
# 3. Authenticated via username/password
# 4. Messages encrypted at rest by server (if configured)
```

## Best Practices

### 1. Credential Management

✅ DO:
- Store credentials in environment variables
- Use secrets management systems (Vault, AWS Secrets Manager, etc.)
- Rotate credentials regularly
- Use strong passwords (min 12 chars, mix of case/numbers/symbols)

❌ DON'T:
- Hardcode credentials in source code
- Commit credentials to version control
- Use weak or default passwords
- Share credentials between applications

### 2. Encryption

✅ DO:
- Enable server-side encryption with FLYMQ_ENCRYPTION_KEY
- Store encryption keys in secure key management systems
- Rotate keys periodically
- Keep separate keys for different environments
- Archive old keys for decrypting historical data

❌ DON'T:
- Store encryption keys in configuration files
- Use the same key for multiple clusters
- Transmit keys in plain text
- Forget to back up encryption keys securely

### 3. TLS/SSL Certificates

✅ DO:
- Use certificates from a trusted CA in production
- Enable certificate verification
- Use TLS 1.2 or higher
- Rotate certificates before expiry
- Monitor certificate expiry dates

❌ DON'T:
- Use self-signed certificates in production
- Disable certificate verification in production
- Use expired certificates
- Ignore certificate warnings

### 4. Transport Security

```python
import os
from pyflymq import connect, TLSConfig

# Always use TLS in production
if os.getenv("ENV") == "production":
    tls = TLSConfig(
        enabled=True,
        ca_file="/path/to/ca.crt"
    )
    client = connect("flymq.example.com:9093", tls=tls)
else:
    # Can use unencrypted connection for development
    client = connect("localhost:9092")
```

### 5. Access Control

```python
# Implement least privilege principle
# Create users with only needed permissions

# Producer-only user
client.create_user("producer-app", "password", 
                  roles=["producer"])

# Consumer-only user  
client.create_user("consumer-app", "password",
                  roles=["consumer"])

# Admin user (if needed)
client.create_user("admin-user", "password",
                  roles=["admin"])
```

### 6. Audit and Monitoring

```python
import logging
from pyflymq import connect

# Set up logging for security events
logging.basicConfig(level=logging.INFO)

try:
    client = connect(
        "localhost:9092",
        username="alice",
        password="password"
    )
    logging.info("Authenticated as alice")
except Exception as e:
    logging.error(f"Authentication failed: {e}")
```

### 7. Network Security

- Use VPN or private networks when possible
- Restrict FlyMQ ports to authorized IP ranges
- Use firewalls to control access
- Monitor network traffic for anomalies

## Security Checklist

Before deploying to production:

- [ ] All credentials loaded from environment variables
- [ ] TLS enabled with certificates from trusted CA
- [ ] Client certificate authentication enabled (mTLS)
- [ ] Application-level authentication enabled
- [ ] Server-side encryption enabled (FLYMQ_ENCRYPTION_KEY)
- [ ] Encryption keys securely stored and rotated
- [ ] User access control configured
- [ ] Audit logging enabled
- [ ] Network access restricted to authorized sources
- [ ] Regular security audits scheduled
- [ ] Incident response plan in place

## Security Resources

- [NIST Cybersecurity Framework](https://www.nist.gov/cyberframework)
- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [TLS Best Practices](https://wiki.mozilla.org/Security/Server_Side_TLS)
- [Encryption Key Management](https://en.wikipedia.org/wiki/Key_management)
