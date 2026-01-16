# FlyMQ Binary Protocol Specification

This document provides the complete binary protocol specification for implementing FlyMQ clients in any programming language.

## Overview

FlyMQ uses a custom **binary-only** protocol designed for high performance:

- **No JSON** - All payloads are binary encoded
- **~30% less overhead** than JSON-based protocols
- **Simple framing** - Fixed 8-byte header + variable payload
- **Extensible** - OpCodes and flags support future features

## Wire Format

### Message Structure

Every message consists of a fixed 8-byte header followed by a binary payload:

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
| 0      | 1    | Magic   | `0xAF` - FlyMQ protocol identifier |
| 1      | 1    | Version | `0x01` - Protocol version |
| 2      | 1    | OpCode  | Operation code (see tables below) |
| 3      | 1    | Flags   | `0x01`=Binary, `0x02`=Compressed |
| 4-7    | 4    | Length  | Payload size in bytes (big-endian uint32) |

### Constants

```
MAGIC_BYTE       = 0xAF
PROTOCOL_VERSION = 0x01
HEADER_SIZE      = 8
MAX_MESSAGE_SIZE = 33554432  (32 MB)

FLAG_BINARY      = 0x01  (always set)
FLAG_COMPRESSED  = 0x02  (reserved for future use)
```

## Binary Encoding Rules

All payloads use consistent binary encoding:

### Primitive Types

| Type | Encoding | Size |
|------|----------|------|
| `bool` | Single byte: `0x00`=false, `0x01`=true | 1 byte |
| `int32` | Big-endian signed 32-bit | 4 bytes |
| `uint32` | Big-endian unsigned 32-bit | 4 bytes |
| `int64` | Big-endian signed 64-bit | 8 bytes |
| `uint64` | Big-endian unsigned 64-bit | 8 bytes |

### Variable-Length Types

| Type | Encoding |
|------|----------|
| `string` | `[uint16 length][UTF-8 bytes]` |
| `bytes` | `[uint32 length][raw bytes]` |
| `[]string` | `[uint32 count][string, string, ...]` |

### Encoding Examples

**String "hello":**
```
00 05 68 65 6C 6C 6F
└─┬─┘ └──────┬──────┘
  │          └─────── UTF-8 bytes: "hello"
  └────────────────── Length: 5 (uint16)
```

**Bytes [0xDE, 0xAD, 0xBE, 0xEF]:**
```
00 00 00 04 DE AD BE EF
└────┬────┘ └────┬────┘
     │           └─────── Raw bytes
     └─────────────────── Length: 4 (uint32)
```

**Array ["a", "bc"]:**
```
00 00 00 02 00 01 61 00 02 62 63
└────┬────┘ └──┬───┘ └────┬────┘
     │         │          └───── String "bc"
     │         └──────────────── String "a"
     └────────────────────────── Count: 2 (uint32)
```

## Operation Codes (OpCodes)

### Core Operations (0x01-0x0F)

| OpCode | Name | Description |
|--------|------|-------------|
| `0x01` | PRODUCE | Write a message to a topic |
| `0x02` | CONSUME | Read a single message by offset |
| `0x03` | CREATE_TOPIC | Create a new topic |
| `0x04` | METADATA | Get topic metadata |
| `0x05` | SUBSCRIBE | Subscribe with consumer group |
| `0x06` | COMMIT | Commit consumer offset |
| `0x07` | FETCH | Fetch batch of messages |
| `0x08` | LIST_TOPICS | List all topics |
| `0x09` | DELETE_TOPIC | Delete a topic |
| `0x0A` | CONSUME_ZERO_COPY | Zero-copy consume (large messages) |
| `0x0B` | FETCH_BINARY | Binary batch fetch (high performance) |

### Schema Operations (0x10-0x1F)

| OpCode | Name | Description |
|--------|------|-------------|
| `0x10` | REGISTER_SCHEMA | Register a new schema |
| `0x11` | GET_SCHEMA | Get schema by name |
| `0x12` | LIST_SCHEMAS | List all schemas |
| `0x13` | VALIDATE_SCHEMA | Validate message against schema |
| `0x14` | PRODUCE_WITH_SCHEMA | Produce with schema validation |
| `0x15` | DELETE_SCHEMA | Delete a schema |

### Dead Letter Queue Operations (0x20-0x2F)

| OpCode | Name | Description |
|--------|------|-------------|
| `0x20` | FETCH_DLQ | Get messages from DLQ |
| `0x21` | REPLAY_DLQ | Replay a DLQ message |
| `0x22` | PURGE_DLQ | Delete all DLQ messages |

### Delayed/TTL Operations (0x30-0x3F)

| OpCode | Name | Description |
|--------|------|-------------|
| `0x30` | PRODUCE_DELAYED | Produce with delay |
| `0x31` | CANCEL_DELAYED | Cancel delayed message |
| `0x35` | PRODUCE_WITH_TTL | Produce with TTL |

### Transaction Operations (0x40-0x4F)

| OpCode | Name | Description |
|--------|------|-------------|
| `0x40` | TXN_BEGIN | Begin a transaction |
| `0x41` | TXN_COMMIT | Commit transaction |
| `0x42` | TXN_ROLLBACK | Abort/rollback transaction |
| `0x43` | TXN_PRODUCE | Produce within transaction |

### Cluster Operations (0x50-0x5F)

| OpCode | Name | Description |
|--------|------|-------------|
| `0x50` | CLUSTER_JOIN | Join a cluster |
| `0x51` | CLUSTER_LEAVE | Leave cluster gracefully |
| `0x52` | CLUSTER_STATUS | Get cluster status |

### Consumer Group Operations (0x60-0x6F)

| OpCode | Name | Description |
|--------|------|-------------|
| `0x60` | GET_OFFSET | Get committed offset |
| `0x61` | RESET_OFFSET | Reset consumer offset |
| `0x62` | LIST_GROUPS | List consumer groups |
| `0x63` | DESCRIBE_GROUP | Get group details |
| `0x64` | GET_LAG | Get consumer lag |
| `0x65` | DELETE_GROUP | Delete consumer group |
| `0x66` | SEEK_TO_TIMESTAMP | Seek to offset by timestamp |

### Authentication Operations (0x70-0x7F)

| OpCode | Name | Description |
|--------|------|-------------|
| `0x70` | AUTH | Authenticate with credentials |
| `0x71` | AUTH_RESPONSE | Authentication response |
| `0x72` | WHOAMI | Get current user info |
| `0x73` | USER_CREATE | Create a user |
| `0x74` | USER_DELETE | Delete a user |
| `0x75` | USER_UPDATE | Update user roles |
| `0x76` | USER_LIST | List all users |
| `0x77` | USER_GET | Get user details |
| `0x78` | ACL_SET | Set topic ACL |
| `0x79` | ACL_GET | Get topic ACL |
| `0x7A` | ACL_DELETE | Delete topic ACL |
| `0x7B` | ACL_LIST | List all ACLs |
| `0x7C` | PASSWORD_CHANGE | Change password |
| `0x7D` | ROLE_LIST | List available roles |

### Error Response

| OpCode | Name | Description |
|--------|------|-------------|
| `0xFF` | ERROR | Error response from server |

## Request/Response Formats

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

**Response (RecordMetadata):**

The produce response returns complete `RecordMetadata`, similar to Kafka's RecordMetadata.
This provides all information about where and when the message was stored.

```
[uint16] topic length
[bytes]  topic name (UTF-8)
[int32]  partition (assigned partition)
[uint64] offset (assigned offset)
[int64]  timestamp (milliseconds since Unix epoch)
[int32]  key_size (-1 if no key, otherwise key length in bytes)
[int32]  value_size (value length in bytes)
```

| Field | Type | Description |
|-------|------|-------------|
| topic | string | Topic name the message was produced to |
| partition | int32 | Partition the message was assigned to |
| offset | uint64 | Offset of the message in the partition |
| timestamp | int64 | Server timestamp when message was stored (ms) |
| key_size | int32 | Size of key in bytes (-1 if no key) |
| value_size | int32 | Size of value in bytes |

**Wire Example - Produce "hello" to "test":**
```
Request:
  Header: AF 01 01 01 00 00 00 13
  Payload:
    00 04                       Topic length: 4
    74 65 73 74                 Topic: "test"
    00 00 00 00                 Key length: 0 (no key)
    00 00 00 05                 Value length: 5
    68 65 6C 6C 6F              Value: "hello"
    FF FF FF FF                 Partition: -1 (auto)

Response (RecordMetadata):
  Header: AF 01 01 01 00 00 00 22
  Payload:
    00 04                       Topic length: 4
    74 65 73 74                 Topic: "test"
    00 00 00 00                 Partition: 0
    00 00 00 00 00 00 00 2A     Offset: 42
    00 00 01 8D 5A 3B 2C 00     Timestamp: 1705123456000 (ms)
    FF FF FF FF                 Key size: -1 (no key)
    00 00 00 05                 Value size: 5
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
[bool] success
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
[uint64] offset (starting offset)
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

## Implementing a Client

### Connection Setup

1. Open TCP connection to server (default port 9092)
2. Optionally upgrade to TLS
3. Optionally authenticate using AUTH (0x70)

### Request/Response Flow

```
Client                              Server
  │                                    │
  │── [Header][Payload] ──────────────>│
  │                                    │── Process
  │<────────────────── [Header][Payload]│
  │                                    │
```

### Pseudocode

**Sending a Request:**
```python
def send_request(socket, opcode, payload):
    header = bytes([
        0xAF,                           # Magic
        0x01,                           # Version
        opcode,                         # Operation
        0x01,                           # Flags (binary)
    ])
    header += len(payload).to_bytes(4, 'big')
    socket.sendall(header + payload)
```

**Receiving a Response:**
```python
def receive_response(socket):
    header = socket.recv(8)
    magic, version, opcode, flags = header[0], header[1], header[2], header[3]
    length = int.from_bytes(header[4:8], 'big')
    
    if magic != 0xAF:
        raise ProtocolError("Invalid magic byte")
    if length > 33554432:
        raise ProtocolError("Message too large")
    
    payload = b""
    while len(payload) < length:
        chunk = socket.recv(min(length - len(payload), 65536))
        if not chunk:
            raise ConnectionError("Connection closed")
        payload += chunk
    
    return opcode, payload
```

**Encoding a Produce Request:**
```python
def encode_produce_request(topic, key, value, partition=-1):
    buf = bytearray()
    
    # Topic (string)
    topic_bytes = topic.encode('utf-8')
    buf += len(topic_bytes).to_bytes(2, 'big')
    buf += topic_bytes
    
    # Key (bytes)
    key_bytes = key if key else b""
    buf += len(key_bytes).to_bytes(4, 'big')
    buf += key_bytes
    
    # Value (bytes)
    buf += len(value).to_bytes(4, 'big')
    buf += value
    
    # Partition (int32)
    buf += partition.to_bytes(4, 'big', signed=True)
    
    return bytes(buf)
```

## Error Handling

- **Invalid Magic (0xAF)**: Connection should be closed immediately
- **Unknown Version**: Reject message, optionally close connection
- **Unknown OpCode**: Server returns ERROR (0xFF) response
- **Payload Too Large**: Reject before reading payload
- **Network Errors**: Implement retry with exponential backoff

## SDK Compliance Checklist

- [ ] Correctly encode/decode all primitive types (big-endian)
- [ ] Handle strings with uint16 length prefix
- [ ] Handle byte arrays with uint32 length prefix
- [ ] Validate magic byte on all responses
- [ ] Reject messages exceeding 32MB
- [ ] Implement all core OpCodes (0x01-0x09)
- [ ] Support TLS connections
- [ ] Support authentication (0x70-0x72)
- [ ] Handle ERROR (0xFF) responses gracefully
- [ ] Implement connection retry logic

## Reference Implementations

- **Go**: `pkg/client/client.go`, `internal/protocol/binary.go`
- **Python**: `sdk/python/pyflymq/protocol.py`
- **Java**: `sdk/java/flymq-client-core/src/main/java/com/firefly/flymq/protocol/`
