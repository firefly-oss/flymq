# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0

"""
Cryptographic utilities for FlyMQ Python SDK.

Implements AES-256-GCM encryption for both data-at-rest and data-in-motion.
Compatible with the Go server implementation.
"""

from __future__ import annotations

import os
import secrets
from typing import ClassVar

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .exceptions import CryptoError


class Encryptor:
    """
    AES-256-GCM Encryptor for FlyMQ message encryption.
    
    Provides authenticated encryption ensuring both confidentiality and integrity.
    Compatible with the Go FlyMQ server implementation.
    
    Data format:
        nonce (12 bytes) || ciphertext || auth tag (16 bytes)
    
    Example:
        >>> encryptor = Encryptor.from_hex_key("0" * 64)  # 32-byte key in hex
        >>> encrypted = encryptor.encrypt(b"Hello, FlyMQ!")
        >>> decrypted = encryptor.decrypt(encrypted)
        >>> assert decrypted == b"Hello, FlyMQ!"
    """
    
    KEY_SIZE: ClassVar[int] = 32  # AES-256 requires 32-byte key
    NONCE_SIZE: ClassVar[int] = 12  # GCM standard nonce size
    TAG_SIZE: ClassVar[int] = 16  # GCM authentication tag size
    
    def __init__(self, key: bytes) -> None:
        """
        Initialize encryptor with a raw key.
        
        Args:
            key: 32-byte (256-bit) encryption key.
            
        Raises:
            CryptoError: If key is not 32 bytes.
        """
        if len(key) != self.KEY_SIZE:
            raise CryptoError(f"Key must be {self.KEY_SIZE} bytes (256 bits), got {len(key)}")
        self._key = key
        self._aesgcm = AESGCM(key)
    
    @classmethod
    def from_hex_key(cls, hex_key: str) -> Encryptor:
        """
        Create encryptor from a hex-encoded key.
        
        Args:
            hex_key: 64 character hex string (32 bytes).
            
        Returns:
            Configured Encryptor instance.
            
        Raises:
            CryptoError: If hex_key is invalid.
        """
        try:
            key = bytes.fromhex(hex_key)
        except ValueError as e:
            raise CryptoError(f"Invalid hex key: {e}") from e
        return cls(key)
    
    def encrypt(self, plaintext: bytes) -> bytes:
        """
        Encrypt data using AES-256-GCM.
        
        Args:
            plaintext: Data to encrypt.
            
        Returns:
            nonce || ciphertext || auth_tag
            
        Raises:
            CryptoError: If encryption fails.
        """
        try:
            nonce = os.urandom(self.NONCE_SIZE)
            ciphertext = self._aesgcm.encrypt(nonce, plaintext, None)
            return nonce + ciphertext
        except Exception as e:
            raise CryptoError(f"Encryption failed: {e}") from e
    
    def decrypt(self, ciphertext: bytes) -> bytes:
        """
        Decrypt data that was encrypted with encrypt().
        
        Args:
            ciphertext: Data from encrypt() (nonce || ciphertext || tag).
            
        Returns:
            Decrypted plaintext.
            
        Raises:
            CryptoError: If decryption fails or data is tampered.
        """
        min_length = self.NONCE_SIZE + self.TAG_SIZE
        if len(ciphertext) < min_length:
            raise CryptoError(f"Ciphertext too short (min {min_length} bytes)")
        
        try:
            nonce = ciphertext[:self.NONCE_SIZE]
            encrypted_data = ciphertext[self.NONCE_SIZE:]
            return self._aesgcm.decrypt(nonce, encrypted_data, None)
        except Exception as e:
            raise CryptoError(f"Decryption failed - data may be corrupted or tampered: {e}") from e


def generate_key() -> str:
    """
    Generate a cryptographically secure 256-bit key.
    
    Returns:
        64-character hex string representing the key.
    """
    return secrets.token_hex(Encryptor.KEY_SIZE)


def validate_key(hex_key: str) -> bool:
    """
    Validate a hex-encoded encryption key.
    
    Args:
        hex_key: Key to validate.
        
    Returns:
        True if valid, False otherwise.
    """
    try:
        key = bytes.fromhex(hex_key)
        return len(key) == Encryptor.KEY_SIZE
    except ValueError:
        return False
