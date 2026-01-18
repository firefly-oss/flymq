#!/usr/bin/env python3
# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0

"""
TLS and Authentication Example for PyFlyMQ.

This example demonstrates all TLS security levels:
1. No TLS (plain text) - development only
2. TLS with system CA - basic production
3. TLS with custom CA - private PKI
4. Mutual TLS - high security
5. Insecure TLS - testing/debugging only
"""

from pyflymq import connect, TLSConfig


def example_1_no_tls():
    """Example 1: Plain text connection (development only)."""
    print("=" * 60)
    print("Example 1: No TLS (Plain Text)")
    print("=" * 60)
    
    client = connect("localhost:9092")
    
    # Create topic and produce message
    client.create_topic("tls-test")
    meta = client.produce("tls-test", b"Hello without TLS!")
    print(f"✓ Produced message at offset {meta.offset}")
    
    # Consume message
    msg = client.consume("tls-test", meta.offset)
    print(f"✓ Consumed: {msg.data.decode()}")
    
    client.close()
    print()


def example_2_tls_system_ca():
    """Example 2: TLS with system CA certificates."""
    print("=" * 60)
    print("Example 2: TLS with System CA")
    print("=" * 60)
    
    # Method 1: Using TLSConfig (recommended)
    tls = TLSConfig(enabled=True)
    client = connect("localhost:9093", tls=tls)
    
    meta = client.produce("tls-test", b"Hello with TLS!")
    print(f"✓ Produced message at offset {meta.offset}")
    
    client.close()
    
    # Method 2: Using parameters directly
    client = connect("localhost:9093", tls_enabled=True)
    
    msg = client.consume("tls-test", meta.offset)
    print(f"✓ Consumed: {msg.data.decode()}")
    
    client.close()
    print()


def example_3_tls_custom_ca():
    """Example 3: TLS with custom CA certificate."""
    print("=" * 60)
    print("Example 3: TLS with Custom CA")
    print("=" * 60)
    
    # Method 1: Using TLSConfig (recommended)
    tls = TLSConfig(
        enabled=True,
        ca_file="/etc/flymq/ca.crt"
    )
    client = connect("localhost:9093", tls=tls)
    
    meta = client.produce("tls-test", b"Hello with custom CA!")
    print(f"✓ Produced message at offset {meta.offset}")
    
    client.close()
    
    # Method 2: Using parameters directly
    client = connect(
        "localhost:9093",
        tls_enabled=True,
        tls_ca_file="/etc/flymq/ca.crt"
    )
    
    msg = client.consume("tls-test", meta.offset)
    print(f"✓ Consumed: {msg.data.decode()}")
    
    client.close()
    print()


def example_4_mutual_tls():
    """Example 4: Mutual TLS (client + server certificates)."""
    print("=" * 60)
    print("Example 4: Mutual TLS (High Security)")
    print("=" * 60)
    
    # Method 1: Using TLSConfig (recommended)
    tls = TLSConfig(
        enabled=True,
        cert_file="/etc/flymq/client.crt",
        key_file="/etc/flymq/client.key",
        ca_file="/etc/flymq/ca.crt",
        server_name="flymq-server"  # Optional: override hostname verification
    )
    client = connect("localhost:9093", tls=tls)
    
    meta = client.produce("tls-test", b"Hello with mutual TLS!")
    print(f"✓ Produced message at offset {meta.offset}")
    
    client.close()
    
    # Method 2: Using parameters directly
    client = connect(
        "localhost:9093",
        tls_enabled=True,
        tls_cert_file="/etc/flymq/client.crt",
        tls_key_file="/etc/flymq/client.key",
        tls_ca_file="/etc/flymq/ca.crt",
        tls_server_name="flymq-server"
    )
    
    msg = client.consume("tls-test", meta.offset)
    print(f"✓ Consumed: {msg.data.decode()}")
    
    client.close()
    print()


def example_5_insecure_tls():
    """Example 5: Insecure TLS (skip certificate verification)."""
    print("=" * 60)
    print("Example 5: Insecure TLS (Testing Only)")
    print("=" * 60)
    print("⚠️  WARNING: This skips certificate verification!")
    print("⚠️  Use only for testing/debugging, NOT for production!")
    print()
    
    # Method 1: Using TLSConfig (recommended)
    tls = TLSConfig(
        enabled=True,
        insecure_skip_verify=True
    )
    client = connect("localhost:9093", tls=tls)
    
    meta = client.produce("tls-test", b"Hello with insecure TLS!")
    print(f"✓ Produced message at offset {meta.offset}")
    
    client.close()
    
    # Method 2: Using parameters directly
    client = connect(
        "localhost:9093",
        tls_enabled=True,
        tls_insecure_skip_verify=True
    )
    
    msg = client.consume("tls-test", meta.offset)
    print(f"✓ Consumed: {msg.data.decode()}")
    
    client.close()
    print()


def example_6_tls_with_authentication():
    """Example 6: TLS + Authentication."""
    print("=" * 60)
    print("Example 6: TLS + Authentication")
    print("=" * 60)
    
    tls = TLSConfig(
        enabled=True,
        ca_file="/etc/flymq/ca.crt"
    )
    
    client = connect(
        "localhost:9093",
        tls=tls,
        username="admin",
        password="secret"
    )
    
    # Check authentication status
    whoami = client.whoami()
    print(f"✓ Authenticated as: {whoami.username}")
    print(f"✓ Roles: {', '.join(whoami.roles)}")
    
    meta = client.produce("tls-test", b"Secure and authenticated!")
    print(f"✓ Produced message at offset {meta.offset}")
    
    client.close()
    print()


def example_7_full_security_stack():
    """Example 7: Complete security stack (TLS + Authentication)."""
    print("=" * 60)
    print("Example 7: Complete Security Stack")
    print("=" * 60)
    
    tls = TLSConfig(
        enabled=True,
        ca_file="/etc/flymq/ca.crt"
    )
    
    # TLS encrypts data in transit
    # Authentication provides access control
    # Server handles data-at-rest encryption with FLYMQ_ENCRYPTION_KEY
    client = connect(
        "localhost:9093",
        tls=tls,
        username="admin",
        password="secret"
    )
    
    # Check authentication status
    whoami = client.whoami()
    print(f"✓ Authenticated as: {whoami.username}")
    print(f"✓ Roles: {', '.join(whoami.roles)}")
    
    meta = client.produce("tls-test", b"Fully secured message!")
    print(f"✓ Produced message at offset {meta.offset}")
    print("✓ Data encrypted in transit (TLS)")
    print("✓ Data encrypted at rest (server-side)")
    
    msg = client.consume("tls-test", meta.offset)
    print(f"✓ Consumed: {msg.data.decode()}")
    
    client.close()
    print()


def example_8_context_manager():
    """Example 8: TLS with context manager (recommended)."""
    print("=" * 60)
    print("Example 8: TLS with Context Manager")
    print("=" * 60)
    
    tls = TLSConfig(
        enabled=True,
        ca_file="/etc/flymq/ca.crt"
    )
    
    with connect("localhost:9093", tls=tls) as client:
        meta = client.produce("tls-test", b"Hello from context manager!")
        print(f"✓ Produced message at offset {meta.offset}")
        
        msg = client.consume("tls-test", meta.offset)
        print(f"✓ Consumed: {msg.data.decode()}")
    # Connection automatically closed
    
    print("✓ Connection automatically closed")
    print()


def example_9_high_availability_with_tls():
    """Example 9: High availability with TLS."""
    print("=" * 60)
    print("Example 9: High Availability with TLS")
    print("=" * 60)
    
    tls = TLSConfig(
        enabled=True,
        ca_file="/etc/flymq/ca.crt"
    )
    
    # Multiple servers for automatic failover
    servers = ["server1:9093", "server2:9093", "server3:9093"]
    
    client = connect(servers, tls=tls)
    
    meta = client.produce("tls-test", b"HA with TLS!")
    print(f"✓ Produced message at offset {meta.offset}")
    print("✓ Client will automatically failover if a server goes down")
    
    client.close()
    print()


def main():
    """Run all TLS examples."""
    print("\n" + "=" * 60)
    print("PyFlyMQ TLS Examples")
    print("=" * 60 + "\n")
    
    try:
        # Run examples (comment out those that require specific setup)
        example_1_no_tls()
        
        # Uncomment these when you have TLS configured:
        # example_2_tls_system_ca()
        # example_3_tls_custom_ca()
        # example_4_mutual_tls()
        # example_5_insecure_tls()
        # example_6_tls_with_authentication()
        # example_7_full_security_stack()
        # example_8_context_manager()
        # example_9_high_availability_with_tls()
        
        print("=" * 60)
        print("✓ All examples completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nNote: Some examples require TLS to be configured on the server.")
        print("See docs/SECURITY.md for setup instructions.")


if __name__ == "__main__":
    main()
