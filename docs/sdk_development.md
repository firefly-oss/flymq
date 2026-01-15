# FlyMQ SDK Development Guide

This guide explains how to develop client SDKs for FlyMQ in any programming language. It covers the binary wire protocol, encryption, and all supported operations.

## Overview

FlyMQ uses a custom **binary-only** protocol over TCP for client-server communication. The protocol is designed to be:

- **Simple** - Easy to implement in any language
- **Efficient** - Minimal overhead with compact binary encoding (~30% less than JSON)
- **Extensible** - OpCodes and flags allow adding new features
- **Safe** - Magic bytes and version checking prevent protocol mismatches
- **Fast** - No JSON parsing overhead

> **IMPORTANT**: FlyMQ uses binary encoding exclusively. There is NO JSON support in the wire protocol.

## Connection

### Transport
- Default port: `9092`
- Protocol: TCP (optionally with TLS)
- Connections are persistent and can handle multiple requests

### TLS Support
When TLS is enabled on the server, clients should:
1. Load the CA certificate
2. Establish a TLS connection instead of plain TCP
3. Optionally provide client certificates for mutual TLS

## Wire Protocol

### Message Format

Every message consists of a fixed 8-byte header followed by a **binary payload**:

```
+-------+-------+-------+-------+-------+-------+-------+-------+
| Magic | Ver   | Op    | Flags | Length (4 bytes, big-endian) |
+-------+-------+-------+-------+-------+-------+-------+-------+
|                  Binary Payload (Length bytes)                |
+---------------------------------------------------------------+
```

### Header Fields

| Offset | Size | Field   | Description |
|--------|------|---------|-------------|
| 0      | 1    | Magic   | `0xAF` - Identifies FlyMQ protocol |
| 1      | 1    | Version | `0x01` - Protocol version |
| 2      | 1    | OpCode  | Operation code (see below) |
| 3      | 1    | Flags   | `0x01`=Binary (always set), `0x02`=Compressed |
| 4-7    | 4    | Length  | Payload length (big-endian uint32) |

### Constants

```
MAGIC_BYTE       = 0xAF
PROTOCOL_VERSION = 0x01
MAX_MESSAGE_SIZE = 33554432  (32 MB)
HEADER_SIZE      = 8
FLAG_BINARY      = 0x01  (always set)
```

## Binary Encoding Rules

All payloads use length-prefixed binary encoding:

| Type | Encoding |
|------|----------|
| `bool` | Single byte: `0x00`=false, `0x01`=true |
| `int32` | Big-endian signed 32-bit (4 bytes) |
| `uint32` | Big-endian unsigned 32-bit (4 bytes) |
| `int64` | Big-endian signed 64-bit (8 bytes) |
| `uint64` | Big-endian unsigned 64-bit (8 bytes) |
| `string` | `[uint16 length][UTF-8 bytes]` |
| `bytes` | `[uint32 length][raw bytes]` |
| `[]string` | `[uint32 count][string, string, ...]` |

### Encoding Examples

**String "hello":**
```
00 05 68 65 6C 6C 6F
│───│ │─────────────│
  │        └─────────── UTF-8 bytes
  └──────────────────── Length: 5 (uint16, big-endian)
```

**Bytes [0xDE, 0xAD, 0xBE, 0xEF]:**
```
00 00 00 04 DE AD BE EF
│─────────│ │─────────│
      │          └────── Raw bytes
      └───────────────── Length: 4 (uint32, big-endian)
```

### Sending a Message

```python
# Binary protocol - NO JSON
def send_message(socket, opcode, payload_bytes):
    header = bytes([
        0xAF,                           # Magic
        0x01,                           # Version
        opcode,                         # Operation
        0x01,                           # Flags (binary)
    ])
    header += len(payload_bytes).to_bytes(4, 'big')
    socket.sendall(header + payload_bytes)
```

### Receiving a Message

```python
def receive_message(socket):
    header = socket.recv(8)
    magic, version, opcode, flags = header[0], header[1], header[2], header[3]
    length = int.from_bytes(header[4:8], 'big')
    
    if magic != 0xAF:
        raise ProtocolError("Invalid magic byte")
    if length > 33554432:
        raise ProtocolError("Message too large")
    
    # Read payload in chunks
    payload = b""
    while len(payload) < length:
        chunk = socket.recv(min(length - len(payload), 65536))
        if not chunk:
            raise ConnectionError("Connection closed")
        payload += chunk
    
    return opcode, payload
```

## Operation Codes (OpCodes)

See [protocol.md](protocol.md) for the complete OpCode reference.

### Core Operations (0x01-0x0F)

| OpCode | Name | Description |
|--------|------|-------------|
| `0x01` | PRODUCE | Write a message to a topic |
| `0x02` | CONSUME | Read a single message by offset |
| `0x03` | CREATE_TOPIC | Create a new topic |
| `0x05` | SUBSCRIBE | Subscribe with consumer group |
| `0x06` | COMMIT | Commit consumer offset |
| `0x07` | FETCH | Fetch batch of messages |
| `0x08` | LIST_TOPICS | List all topics |
| `0x09` | DELETE_TOPIC | Delete a topic |

### Authentication Operations (0x70-0x7F)

| OpCode | Name | Description |
|--------|------|-------------|
| `0x70` | AUTH | Authenticate with username/password |
| `0x71` | AUTH_RESPONSE | Authentication response |
| `0x72` | WHOAMI | Get current user info |

### Error Response

| OpCode | Name | Description |
|--------|------|-------------|
| `0xFF` | ERROR | Error response from server |

## Request/Response Payloads (Binary Format)

### PRODUCE (0x01)

**Request:**
```
[uint16] topic length
[bytes]  topic name (UTF-8)
[uint32] key length (0 = no key)
[bytes]  key data
[uint32] value length
[bytes]  value data
[int32]  partition (-1 = auto-assign)
```

**Response:**
```
[uint64] offset
[int32]  partition
```

**Encoding Example:**
```python
def encode_produce_request(topic, key, value, partition=-1):
    buf = bytearray()
    
    # Topic (string with uint16 length)
    topic_bytes = topic.encode('utf-8')
    buf += len(topic_bytes).to_bytes(2, 'big')
    buf += topic_bytes
    
    # Key (bytes with uint32 length)
    key_bytes = key if key else b""
    buf += len(key_bytes).to_bytes(4, 'big')
    buf += key_bytes
    
    # Value (bytes with uint32 length)
    buf += len(value).to_bytes(4, 'big')
    buf += value
    
    # Partition (int32)
    buf += partition.to_bytes(4, 'big', signed=True)
    
    return bytes(buf)

def decode_produce_response(payload):
    offset = int.from_bytes(payload[0:8], 'big')
    partition = int.from_bytes(payload[8:12], 'big', signed=True)
    return {"offset": offset, "partition": partition}
```

### CONSUME (0x02)

**Request:**
```
[uint16] topic length
[bytes]  topic name
[int32]  partition
[uint64] offset
```

**Response:**
```
[uint32] key length
[bytes]  key data
[uint32] value length
[bytes]  value data
```

### FETCH (0x07)

**Request:**
```
[uint16] topic length
[bytes]  topic name
[int32]  partition
[uint64] offset
[int32]  max_messages
```

**Response:**
```
[uint32] message_count
For each message:
  [uint32] key length
  [bytes]  key data
  [uint32] value length
  [bytes]  value data
  [uint64] offset
[uint64] next_offset
```

### CREATE_TOPIC (0x03)

**Request:**
```
[uint16] topic length
[bytes]  topic name
[int32]  partitions
```

**Response:**
```
[bool]   success
[uint16] message length
[bytes]  message
```

### SUBSCRIBE (0x05)

**Request:**
```
[uint16] topic length
[bytes]  topic name
[uint16] group_id length
[bytes]  group_id
[int32]  partition
[uint16] mode length
[bytes]  mode ("earliest", "latest", "commit")
```

**Response:**
```
[uint64] offset
```

### COMMIT (0x06)

**Request:**
```
[uint16] topic length
[bytes]  topic name
[uint16] group_id length
[bytes]  group_id
[int32]  partition
[uint64] offset
```

**Response:**
```
[bool]   success
[uint16] message length
[bytes]  message
```

### LIST_TOPICS (0x08)

**Request:** Empty payload (length = 0)

**Response:**
```
[uint32] topic_count
For each topic:
  [uint16] topic length
  [bytes]  topic name
```

### DELETE_TOPIC (0x09)

**Request:**
```
[uint16] topic length
[bytes]  topic name
```

**Response:**
```
[bool]   success
[uint16] message length
[bytes]  message
```

### AUTH (0x70)

**Request:**
```
[uint16] username length
[bytes]  username
[uint16] password length
[bytes]  password
```

**Response:**
```
[bool]   success
[uint16] error length (0 if success)
[bytes]  error message
[uint16] username length
[bytes]  username
[uint32] roles count
For each role:
  [uint16] role length
  [bytes]  role name
```

### ERROR (0xFF)

**Response:**
```
[bool]   success (always false)
[uint16] message length
[bytes]  error message
```

## Encryption

FlyMQ supports AES-256-GCM encryption for data-in-motion (client-side) that is compatible with server-side data-at-rest encryption.

### Key Format
- **Size:** 32 bytes (256 bits)
- **Encoding:** 64-character hexadecimal string
- **Generation:** `openssl rand -hex 32`

### Encryption Format

Encrypted data is structured as:

```
+-------------+----------------+----------+
| Nonce (12B) | Ciphertext     | Tag (16B)|
+-------------+----------------+----------+
```

- **Nonce:** 12 random bytes (unique per encryption)
- **Ciphertext:** AES-256-GCM encrypted plaintext
- **Tag:** 16-byte authentication tag

### Implementation

**Encrypt:**
```python
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import os

def encrypt(key_hex: str, plaintext: bytes) -> bytes:
    key = bytes.fromhex(key_hex)
    nonce = os.urandom(12)
    aesgcm = AESGCM(key)
    ciphertext_with_tag = aesgcm.encrypt(nonce, plaintext, None)
    return nonce + ciphertext_with_tag  # nonce || ciphertext || tag
```

**Decrypt:**
```python
def decrypt(key_hex: str, encrypted: bytes) -> bytes:
    key = bytes.fromhex(key_hex)
    nonce = encrypted[:12]
    ciphertext_with_tag = encrypted[12:]
    aesgcm = AESGCM(key)
    return aesgcm.decrypt(nonce, ciphertext_with_tag, None)
```

### SDK Integration

When encryption is enabled:
1. **Produce:** Encrypt the message value before sending
2. **Consume:** Decrypt the received value after decoding

```python
# Produce with encryption
plaintext = b"Hello World"
encrypted = encrypt(key, plaintext)
payload = encode_produce_request("my-topic", None, encrypted)
send_message(socket, 0x01, payload)

# Consume with decryption
opcode, response = receive_message(socket)
key_data, value_data = decode_consume_response(response)
plaintext = decrypt(key, value_data)
```

## Error Handling

### Protocol Errors

The server returns `ERROR` (0xFF) for errors:
- Payload is binary encoded with success=false and error message
- Always check the opcode before parsing the response

```python
opcode, payload = receive_message(socket)
if opcode == 0xFF:
    # Decode error response
    success = payload[0] != 0
    msg_len = int.from_bytes(payload[1:3], 'big')
    message = payload[3:3+msg_len].decode('utf-8')
    raise FlyMQError(message)
```

### Connection Handling

- Reconnect on connection loss
- Set appropriate socket timeouts
- Handle partial reads (ensure full header/payload received)

## Best Practices for SDK Development

### 1. Thread Safety
- Use connection pools for concurrent access
- Protect shared state with locks/mutexes

### 2. Resource Management
- Implement proper connection cleanup
- Use context managers or try-finally patterns

### 3. Type Safety
- Use typed models for requests/responses
- Validate inputs before sending

### 4. Binary Encoding
- Test encoding/decoding thoroughly
- Handle endianness correctly (always big-endian)

### 5. Configuration
- Support environment variables and config files
- Provide sensible defaults

### 6. Logging
- Log connection events and errors
- Provide debug logging for protocol messages (hex dumps)

### 7. Testing
- Unit test binary encoding/decoding
- Integration test against running FlyMQ server

## Example SDK Structure

```
flymq-client-{language}/
├── src/
│   ├── client.{ext}        # Main client class
│   ├── protocol.{ext}      # Binary wire protocol implementation
│   ├── encoding.{ext}      # Binary encoding utilities
│   ├── crypto.{ext}        # Encryption utilities
│   ├── models.{ext}        # Request/response types
│   ├── errors.{ext}        # Exception/error types
│   └── reactive.{ext}      # Reactive streams (optional)
├── tests/
│   ├── test_encoding.{ext} # Binary encoding unit tests
│   ├── test_crypto.{ext}   # Encryption unit tests
│   └── test_integration.{ext}  # Integration tests
├── examples/
│   └── basic_usage.{ext}
└── README.md
```

## Official SDK References

For implementation examples, see the official SDKs:

- **Go:** `pkg/client/client.go`, `internal/protocol/binary.go` - Reference implementation
- **Python:** `sdk/python/pyflymq/protocol.py` - Binary protocol, Pydantic models
- **Java:** `sdk/java/flymq-client-core/` - Binary protocol, Spring Boot integration

## SDK Compliance Checklist

- [ ] Correctly encode/decode all primitive types (big-endian)
- [ ] Handle strings with uint16 length prefix
- [ ] Handle byte arrays with uint32 length prefix
- [ ] Validate magic byte (0xAF) on all responses
- [ ] Reject messages exceeding 32MB
- [ ] Implement all core OpCodes (0x01-0x09)
- [ ] Support TLS connections
- [ ] Support authentication (0x70-0x72)
- [ ] Handle ERROR (0xFF) responses gracefully
- [ ] Implement AES-256-GCM encryption
- [ ] Implement connection retry logic

## Protocol Version Compatibility

| Protocol Version | FlyMQ Server | Features |
|-----------------|--------------|----------|
| 0x01 | 1.0.0+ | Core operations, schemas, DLQ, transactions, TTL, delayed delivery, authentication |

Future protocol versions will maintain backward compatibility where possible.

## Contributing

When contributing a new SDK:

1. Follow the existing SDK structure
2. Implement all core operations with **binary encoding**
3. Add encryption support
4. Include comprehensive tests
5. Provide usage examples
6. Document language-specific considerations

See [CONTRIBUTING.md](../CONTRIBUTING.md) for contribution guidelines.
