# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0

"""
TLS configuration for FlyMQ client connections.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class TLSConfig:
    """
    TLS/SSL configuration for secure connections.
    
    Security Levels:
    - No TLS: Plain text (development only)
    - TLS (Server Auth): Server certificate validation (production basic)
    - TLS + CA: Custom CA certificate (private PKI)
    - Mutual TLS: Client + server certificates (high security)
    - Insecure TLS: Skip certificate verification (testing/debugging only)
    
    Examples:
        # Secure TLS with certificate verification
        >>> tls = TLSConfig(
        ...     enabled=True,
        ...     cert_file="/path/to/client.crt",
        ...     key_file="/path/to/client.key",
        ...     ca_file="/path/to/ca.crt",
        ...     server_name="flymq-server"
        ... )
        
        # Insecure TLS (skip certificate verification)
        >>> tls = TLSConfig(enabled=True, insecure_skip_verify=True)
        
        # System CA certificates
        >>> tls = TLSConfig(enabled=True)
    """
    
    enabled: bool = False
    """Enable TLS encryption."""
    
    cert_file: Optional[str] = None
    """Path to client certificate file (for mutual TLS)."""
    
    key_file: Optional[str] = None
    """Path to client private key file (for mutual TLS)."""
    
    ca_file: Optional[str] = None
    """Path to CA certificate file for server verification."""
    
    server_name: Optional[str] = None
    """Expected server name in certificate (for SNI and verification)."""
    
    insecure_skip_verify: bool = False
    """Skip certificate verification (INSECURE - use only for testing)."""
