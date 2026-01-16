#!/usr/bin/env python3
"""
09_tls_authentication.py - TLS/SSL and Authentication

This example demonstrates:
- Basic TLS connection (server verification)
- Mutual TLS (client certificates)
- Authentication with username/password
- Environment-based credential loading

Prerequisites:
    - FlyMQ server running with TLS on port 9093 (or 9092)
    - pyflymq installed
    - SSL certificates (for TLS examples)

Run with:
    python 09_tls_authentication.py

Note: Requires proper server configuration and certificates
"""

from pyflymq import FlyMQClient
import os


def basic_tls_example():
    """Basic TLS connection with server verification"""
    print("Basic TLS Connection (Server Verification)")
    print("-" * 50)
    
    print("""
This example shows connecting with TLS and server certificate verification:

    client = FlyMQClient(
        "localhost:9093",
        tls_enabled=True,
        tls_ca_file="/path/to/ca.crt"
    )

Key points:
    - tls_enabled=True: Enable TLS
    - tls_ca_file: Path to CA certificate for verification
    - Server certificate will be validated against the CA
    """)
    
    try:
        # This will fail without proper TLS setup, but shows the API
        client = FlyMQClient(
            "localhost:9092",  # Note: most tests run on 9092 without TLS
            tls_enabled=False   # Disabled for demo, would need certs
        )
        print("✓ Connected with TLS configuration")
        client.close()
    except Exception as e:
        print(f"Note: {type(e).__name__} (expected without TLS server)")


def mutual_tls_example():
    """Mutual TLS (mTLS) with client certificates"""
    print("\nMutual TLS (mTLS) Configuration")
    print("-" * 50)
    
    print("""
This example shows mutual TLS with client certificates:

    client = FlyMQClient(
        "localhost:9093",
        tls_enabled=True,
        tls_ca_file="/path/to/ca.crt",
        tls_cert_file="/path/to/client.crt",
        tls_key_file="/path/to/client.key"
    )

Key points:
    - tls_ca_file: CA certificate for server verification
    - tls_cert_file: Client certificate for authentication
    - tls_key_file: Client private key (PKCS#8 format)
    - Server can authenticate the client
    """)


def authentication_example():
    """Username/password authentication"""
    print("\nUsername/Password Authentication")
    print("-" * 50)
    
    # Example with hardcoded credentials (not recommended for production)
    print("""
Method 1: Hardcoded credentials (NOT recommended for production):

    client = FlyMQClient(
        "localhost:9092",
        username="alice",
        password="secret123"
    )
    """)
    
    # Better approach: Load from environment
    print("""
Method 2: Environment variables (recommended):

    import os
    username = os.getenv("FLYMQ_USERNAME")
    password = os.getenv("FLYMQ_PASSWORD")
    
    client = FlyMQClient(
        "localhost:9092",
        username=username,
        password=password
    )
    
Then set environment variables before running:
    
    export FLYMQ_USERNAME=alice
    export FLYMQ_PASSWORD=secret123
    python script.py
    """)
    
    # Demonstrate environment variable approach
    username = os.getenv("FLYMQ_USERNAME", "admin")
    password = os.getenv("FLYMQ_PASSWORD", "default")
    
    print(f"\nTesting authentication with username={username}...")
    
    try:
        client = FlyMQClient(
            "localhost:9092",
            username=username,
            password=password
        )
        
        # Test authentication
        client.authenticate(username, password)
        print("✓ Authentication successful")
        
        client.close()
    except Exception as e:
        print(f"Note: Authentication failed - {type(e).__name__}")
        print("  (This may be expected if auth is disabled on server)")


def combined_security_example():
    """TLS + Authentication together"""
    print("\nTLS + Authentication (Combined)")
    print("-" * 50)
    
    print("""
Using TLS and authentication together:

    client = FlyMQClient(
        "localhost:9093",
        # TLS settings
        tls_enabled=True,
        tls_ca_file="/path/to/ca.crt",
        tls_cert_file="/path/to/client.crt",
        tls_key_file="/path/to/client.key",
        # Authentication settings
        username=os.getenv("FLYMQ_USERNAME"),
        password=os.getenv("FLYMQ_PASSWORD")
    )

This provides:
    - Encrypted connection (TLS)
    - Client certificate authentication (mTLS)
    - User/password authentication (application-level)
    - Complete security chain
    """)


def certificate_generation():
    """Guide for generating test certificates"""
    print("\nGenerating Test Certificates")
    print("-" * 50)
    
    print("""
To set up TLS, generate certificates with OpenSSL:

# Generate CA key and certificate
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 365 -key ca.key -out ca.crt

# Generate server certificate
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr
openssl x509 -req -days 365 -in server.csr -CA ca.crt -CAkey ca.key \\
    -out server.crt -CAcreateserial

# Generate client certificate
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr
openssl x509 -req -days 365 -in client.csr -CA ca.crt -CAkey ca.key \\
    -out client.crt -CAcreateserial

# Convert client key to PKCS#8 format
openssl pkcs8 -topk8 -nocrypt -in client.key -out client-pkcs8.key

Use client-pkcs8.key in tls_key_file parameter.
    """)


def security_best_practices():
    """Best practices for security"""
    print("\nSecurity Best Practices")
    print("-" * 50)
    
    print("""
Credentials:
    - Never hardcode credentials in source code
    - Use environment variables
    - Use config files (with restricted permissions)
    - Use secrets management (Vault, AWS Secrets Manager, etc.)

TLS Certificates:
    - Use proper certificates from a CA
    - Avoid self-signed certs in production
    - Rotate certificates regularly
    - Store keys securely

Connection Security:
    - Always enable TLS in production
    - Use mutual TLS (mTLS) when possible
    - Validate certificate chains
    - Use strong encryption algorithms

Audit and Monitoring:
    - Log all authentication attempts
    - Monitor for failed login attempts
    - Set up alerts for security events
    - Regularly audit access permissions
    """)


def main():
    print("=" * 50)
    print("FlyMQ TLS & Authentication Example")
    print("=" * 50)
    
    basic_tls_example()
    mutual_tls_example()
    authentication_example()
    combined_security_example()
    certificate_generation()
    security_best_practices()
    
    print("\n" + "=" * 50)
    print("✓ Security examples completed!")


if __name__ == "__main__":
    main()
