#!/usr/bin/env python3
"""
05_encryption.py - AES-256-GCM Encryption Example

This example demonstrates:
- Generating encryption keys
- Encrypting and decrypting data
- Using encryption with FlyMQ client
- Server-side encrypted storage compatibility

FlyMQ encryption features:
- AES-256-GCM: Industry-standard encryption
- Data-in-motion: Secure client-server communication
- Data-at-rest: Compatible with server-side encryption
- Per-message encryption: Full message isolation

Prerequisites:
    - FlyMQ server running on localhost:9092
    - pyflymq installed

Run with:
    python 05_encryption.py
"""

from pyflymq import FlyMQClient, Encryptor, generate_key
import json


def encryption_basics():
    """Basic encryption and decryption"""
    print("Encryption Basics")
    print("-" * 50)
    
    # Generate a new encryption key
    key = generate_key()
    print(f"Generated encryption key: {key[:16]}...{key[-8:]}")
    
    # Create encryptor
    encryptor = Encryptor.from_hex_key(key)
    
    # Encrypt data
    plaintext = b"This is a secret message!"
    encrypted = encryptor.encrypt(plaintext)
    print(f"\nOriginal:  {plaintext.decode()}")
    print(f"Encrypted: {encrypted.hex()[:32]}... ({len(encrypted)} bytes)")
    
    # Decrypt data
    decrypted = encryptor.decrypt(encrypted)
    print(f"Decrypted: {decrypted.decode()}")
    
    assert plaintext == decrypted, "Encryption/decryption failed!"
    print("✓ Encryption/decryption verified!")


def client_encryption():
    """Using encryption with FlyMQ client"""
    print("\nEncrypted Client Connection")
    print("-" * 50)
    
    # Generate encryption key for this session
    key = generate_key()
    
    # Connect with encryption enabled
    client = FlyMQClient(
        "localhost:9092",
        encryption_key=key
    )
    
    try:
        topic = "encrypted-messages"
        
        # Create topic
        try:
            client.create_topic(topic, partitions=1)
        except:
            pass
        
        # Produce encrypted message
        # The SDK automatically encrypts the message before sending
        data = json.dumps({
            "user_id": "user-123",
            "password_hash": "abc123def456",
            "credit_card": "****-****-****-7890",
            "ssn": "***-**-1234"
        }).encode()
        
        print(f"Sending encrypted message (plaintext size: {len(data)} bytes)...")
        offset = client.produce(topic, data)
        print(f"Message produced at offset {offset}")
        
        # Consume encrypted message
        # The SDK automatically decrypts the message after receiving
        print(f"Consuming encrypted message...")
        msg = client.consume(topic, offset)
        
        # Verify decryption
        decrypted_data = json.loads(msg.decode())
        print(f"Decrypted message:")
        for key, value in decrypted_data.items():
            print(f"  {key}: {value}")
        
        print("✓ Encryption/decryption with client completed!")
    
    finally:
        client.close()


def multiple_recipients():
    """Different recipients can decrypt with the same key"""
    print("\nMultiple Recipients (Same Key)")
    print("-" * 50)
    
    # Shared encryption key (same for all producers and consumers)
    shared_key = generate_key()
    
    # Producer uses the shared key
    producer = FlyMQClient("localhost:9092", encryption_key=shared_key)
    try:
        topic = "shared-encrypted"
        try:
            producer.create_topic(topic, partitions=1)
        except:
            pass
        
        # Produce encrypted message
        message1 = b"Message from Producer 1"
        print(f"Producer 1: Sending encrypted message...")
        offset1 = producer.produce(topic, message1)
        
        message2 = b"Message from Producer 2"
        print(f"Producer 2: Sending encrypted message...")
        offset2 = producer.produce(topic, message2)
    finally:
        producer.close()
    
    # Consumer uses the same shared key to decrypt
    consumer = FlyMQClient("localhost:9092", encryption_key=shared_key)
    try:
        print(f"\nConsumer: Consuming with shared key...")
        msg1 = consumer.consume(topic, offset1)
        msg2 = consumer.consume(topic, offset2)
        
        print(f"  Decrypted message 1: {msg1.decode()}")
        print(f"  Decrypted message 2: {msg2.decode()}")
        print("✓ Shared key decryption verified!")
    finally:
        consumer.close()


def key_management():
    """Best practices for key management"""
    print("\nKey Management Best Practices")
    print("-" * 50)
    
    print("""
Best practices for encryption keys:

1. Key Generation:
   - Use generate_key() for cryptographically secure keys
   - Never hardcode keys in source code
   
2. Key Storage:
   - Store keys in environment variables
   - Use key management services (AWS KMS, HashiCorp Vault, etc.)
   - Rotate keys periodically
   
3. Key Distribution:
   - Use secure channels to distribute keys
   - Never transmit keys in plain text
   - Use TLS for all connections containing encrypted data
   
4. Key Lifecycle:
   - Archive old keys for decryption of historical messages
   - Maintain audit logs of key usage
   - Revoke compromised keys immediately

Example: Loading key from environment:
    import os
    encryption_key = os.getenv('FLYMQ_ENCRYPTION_KEY')
    client = FlyMQClient('localhost:9092', encryption_key=encryption_key)
    """)


def main():
    print("=" * 50)
    print("FlyMQ Encryption Example")
    print("=" * 50)
    
    encryption_basics()
    client_encryption()
    multiple_recipients()
    key_management()
    
    print("\n" + "=" * 50)
    print("✓ All encryption examples completed!")


if __name__ == "__main__":
    main()
