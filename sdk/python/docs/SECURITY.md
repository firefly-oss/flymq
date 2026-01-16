# Security Guide - PyFlyMQ

Comprehensive guide to implementing security in your FlyMQ applications.

## Table of Contents

- [Authentication](#authentication)
- [Encryption](#encryption)
- [TLS/SSL](#tlsssl)
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

## Encryption

### AES-256-GCM Encryption

FlyMQ supports AES-256-GCM encryption for protecting message content:

```python
from pyflymq import FlyMQClient, generate_key

# Generate a new encryption key
key = generate_key()  # Returns 64-char hex string

# Connect with encryption enabled
client = FlyMQClient(
    "localhost:9092",
    encryption_key=key
)

# Messages are automatically encrypted before sending
# and decrypted after receiving
offset = client.produce("secure-topic", b"Secret message")
msg = client.consume("secure-topic", offset)
print(msg.decode())  # Automatically decrypted
```

### Key Management

Proper key management is critical:

```python
import os
from pyflymq import FlyMQClient, generate_key

# Option 1: Load existing key from environment
encryption_key = os.getenv("FLYMQ_ENCRYPTION_KEY")
if not encryption_key:
    raise ValueError("FLYMQ_ENCRYPTION_KEY not set")

client = FlyMQClient("localhost:9092", encryption_key=encryption_key)

# Option 2: Use key management service (AWS KMS example)
import boto3

kms = boto3.client("kms")
response = kms.decrypt(CiphertextBlob=encrypted_key)
encryption_key = response["Plaintext"].decode()

client = FlyMQClient("localhost:9092", encryption_key=encryption_key)
```

### Encryption Workflow

```python
from pyflymq import FlyMQClient, Encryptor

# Get encryption key
key = "your_encryption_key_here"

# Create encryptor
encryptor = Encryptor.from_hex_key(key)

# Manual encryption/decryption
plaintext = b"Sensitive data"
encrypted = encryptor.encrypt(plaintext)
decrypted = encryptor.decrypt(encrypted)

# Or use client-level encryption
client = FlyMQClient("localhost:9092", encryption_key=key)
# All messages automatically encrypted/decrypted
```

## TLS/SSL

### Basic TLS (Server Verification)

Connect securely to FlyMQ using TLS:

```python
from pyflymq import FlyMQClient

client = FlyMQClient(
    "flymq.example.com:9093",
    tls_enabled=True,
    tls_ca_file="/path/to/ca.crt"  # CA certificate for verification
)
```

### Mutual TLS (mTLS)

Use client certificates for bidirectional authentication:

```python
from pyflymq import FlyMQClient

client = FlyMQClient(
    "flymq.example.com:9093",
    tls_enabled=True,
    tls_ca_file="/path/to/ca.crt",           # CA certificate
    tls_cert_file="/path/to/client.crt",     # Client certificate
    tls_key_file="/path/to/client.key"       # Client private key
)
```

### Skip Certificate Verification (Testing Only)

For development/testing only:

```python
# WARNING: This disables all certificate verification
# NEVER use in production
client = FlyMQClient(
    "localhost:9093",
    tls_enabled=True,
    tls_insecure_skip_verify=True  # Dangerous!
)
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

Use TLS + Authentication + Encryption for complete security:

```python
import os
from pyflymq import FlyMQClient

client = FlyMQClient(
    "flymq.example.com:9093",
    # TLS/SSL
    tls_enabled=True,
    tls_ca_file="/path/to/ca.crt",
    tls_cert_file="/path/to/client.crt",
    tls_key_file="/path/to/client.key",
    # Authentication
    username=os.getenv("FLYMQ_USERNAME"),
    password=os.getenv("FLYMQ_PASSWORD"),
    # Encryption
    encryption_key=os.getenv("FLYMQ_ENCRYPTION_KEY")
)

# All communication is now:
# 1. Encrypted via TLS (channel security)
# 2. Authenticated via mTLS certificates
# 3. Authenticated via username/password
# 4. Messages encrypted via AES-256-GCM
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

### 2. Encryption Keys

✅ DO:
- Generate keys using `generate_key()`
- Store keys in secure key management systems
- Rotate keys periodically
- Keep separate keys for different purposes
- Archive old keys for decrypting historical data

❌ DON'T:
- Hardcode encryption keys
- Use the same key for multiple applications
- Transmit keys in plain text
- Use weak random number generators

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
# Always use TLS in production
if os.getenv("ENV") == "production":
    client = FlyMQClient(
        "flymq.example.com:9093",
        tls_enabled=True,
        tls_ca_file="/path/to/ca.crt"
    )
else:
    # Can use unencrypted connection for development
    client = FlyMQClient("localhost:9092")
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

# Set up logging for security events
logging.basicConfig(level=logging.INFO)

try:
    client = FlyMQClient(
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
- [ ] TLS enabled and certificates from trusted CA
- [ ] Client certificate authentication enabled (mTLS)
- [ ] Application-level authentication enabled
- [ ] Message encryption enabled (AES-256-GCM)
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
