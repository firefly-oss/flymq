# FlyMQ API Reference

This document provides a comprehensive reference for the FlyMQ Go client API.

## Client API

### Creating a Client

```go
import "github.com/firefly-oss/flymq/pkg/client"

// Create with default options (plain TCP)
c, err := client.NewClient("localhost:9092")
if err != nil {
    log.Fatal(err)
}
defer c.Close()

// Create with TLS
c, err := client.NewClientWithOptions("localhost:9093", client.ClientOptions{
    TLSEnabled: true,
    TLSCAFile:  "/path/to/ca.crt",
})

// Create cluster-aware client with multiple bootstrap servers
c, err := client.NewClusterClient("node1:9092,node2:9092,node3:9092", client.ClientOptions{
    MaxRetries:     3,
    RetryDelayMs:   1000,
    ConnectTimeout: 10,
})
```

### Client Options

```go
type ClientOptions struct {
    // Bootstrap servers for HA connections
    BootstrapServers []string

    // Authentication
    Username string // Username for authentication
    Password string // Password for authentication

    // TLS configuration
    TLSEnabled            bool   // Enable TLS connection
    TLSCertFile           string // Client certificate file (for mTLS)
    TLSKeyFile            string // Client key file (for mTLS)
    TLSCAFile             string // CA certificate file
    TLSInsecureSkipVerify bool   // Skip verification (testing only)

    // Encryption
    EncryptionKey string // AES-256 key for data encryption (64-char hex)

    // Connection behavior
    MaxRetries     int // Maximum connection retries per server (default: 3)
    RetryDelayMs   int // Delay between retries in milliseconds (default: 1000)
    ConnectTimeout int // Connection timeout in seconds (default: 10)
}
```

### High Availability Features

The client provides automatic failover capabilities:

- **Bootstrap Servers**: Connect to multiple servers for redundancy
- **Automatic Reconnection**: On connection failure, tries next server in rotation
- **Leader Detection**: Detects "not leader" errors and redirects to current leader
- **Retry Logic**: Configurable retries with exponential backoff between servers

### TLS/SSL Configuration

FlyMQ supports TLS 1.2+ for secure client-server communication with the following options:

**Basic TLS (Server Verification):**
```go
c, err := client.NewClientWithOptions("localhost:9093", client.ClientOptions{
    TLSEnabled: true,
    TLSCAFile:  "/path/to/ca.crt",  // CA certificate for server verification
})
```

**Mutual TLS (mTLS - Client Certificate Authentication):**
```go
c, err := client.NewClientWithOptions("localhost:9093", client.ClientOptions{
    TLSEnabled:  true,
    TLSCAFile:   "/path/to/ca.crt",      // CA certificate
    TLSCertFile: "/path/to/client.crt",  // Client certificate
    TLSKeyFile:  "/path/to/client.key",  // Client private key
})
```

**Skip Verification (Testing Only):**
```go
c, err := client.NewClientWithOptions("localhost:9093", client.ClientOptions{
    TLSEnabled:            true,
    TLSInsecureSkipVerify: true,  // WARNING: Only for testing!
})
```

**Security Features:**
- Minimum TLS version: 1.2
- Strong cipher suites: ECDHE + AES-GCM or ChaCha20-Poly1305
- Server certificate verification by default
- Optional client certificate authentication (mTLS)

---

## Authentication

### Authenticating

```go
// Option 1: Auto-authenticate via ClientOptions
c, err := client.NewClientWithOptions("localhost:9092", client.ClientOptions{
    Username: "admin",
    Password: "password",
})

// Option 2: Explicit authentication
c, err := client.NewClient("localhost:9092")
err = c.Authenticate("admin", "password")
if err != nil {
    log.Fatal("authentication failed:", err)
}

// Check authentication status
if c.IsAuthenticated() {
    log.Printf("Authenticated as: %s", c.Username())
}

// Get detailed auth info
resp, err := c.WhoAmI()
if resp.Authenticated {
    log.Printf("User: %s, Roles: %v", resp.Username, resp.Roles)
}
```

### User Management (Admin Only)

```go
// List all users
users, err := c.ListUsers()
for _, u := range users {
    log.Printf("User: %s, Roles: %v, Enabled: %v", u.Username, u.Roles, u.Enabled)
}

// Create a new user
err = c.CreateUser("alice", "password123", []string{"producer", "consumer"})

// Update user roles
err = c.UpdateUser("alice", []string{"admin"}, nil)

// Disable a user
enabled := false
err = c.UpdateUser("alice", nil, &enabled)

// Delete a user
err = c.DeleteUser("alice")

// Change password
err = c.ChangePassword("alice", "", "newpassword")
```

### ACL Management (Admin Only)

```go
// List all ACLs
acls, defaultPublic, err := c.ListACLs()
log.Printf("Default public: %v", defaultPublic)
for _, acl := range acls {
    log.Printf("Topic: %s, Public: %v, Users: %v, Roles: %v",
        acl.Topic, acl.Public, acl.AllowedUsers, acl.AllowedRoles)
}

// Get ACL for a specific topic
acl, err := c.GetACL("orders")

// Set topic ACL
err = c.SetACL("orders", false, []string{"alice", "bob"}, []string{"admin"})

// Make topic public
err = c.SetACL("public-events", true, nil, nil)

// Delete topic ACL (reverts to default)
err = c.DeleteACL("orders")
```

### Role Information

```go
// List available roles
roles, err := c.ListRoles()
for _, r := range roles {
    log.Printf("Role: %s, Permissions: %v, Description: %s",
        r.Name, r.Permissions, r.Description)
}
```

---

## Core Operations

### Creating Topics

```go
// Create a topic with specified partitions
err := c.CreateTopic("events", 6)
```

### Producing Messages

```go
// Produce a message (auto-creates topic if needed)
offset, err := c.Produce("events", []byte("key"), []byte("value"))
if err != nil {
    log.Printf("produce failed: %v", err)
    return
}
log.Printf("produced at offset %d", offset)
```

### Consuming Messages

```go
// Consume a single message by offset
value, err := c.Consume("events", 0, offset)

// Fetch multiple messages
messages, nextOffset, err := c.Fetch("events", 0, startOffset, 100)
for _, msg := range messages {
    process(msg)
}
```

### Subscribing to Topics

```go
// Subscribe with consumer group (modes: "earliest", "latest", "commit")
msgChan, err := c.Subscribe("events", "my-group", "latest")
for msg := range msgChan {
    process(msg)
}
```

### Committing Offsets

```go
// Commit consumer offset
err := c.CommitOffset("events", "my-group", partition, offset)
```

### Topic Management

```go
// List all topics
topics, err := c.ListTopics()

// Delete a topic
err := c.DeleteTopic("events")

// Get topic metadata
meta, err := c.GetMetadata("events")
// meta.Topic, meta.Partitions, meta.MessageCount
```

---

## Advanced Features

### Transactions

```go
// Begin a transaction
txn, err := c.BeginTransaction()
if err != nil {
    log.Fatal(err)
}

// Produce within transaction
err = txn.Produce("events", []byte("key"), []byte("value"))

// Commit or abort
err = txn.Commit()
// or: err = txn.Abort()
```

### Schema Validation

```go
// Register a JSON schema
schemaID, err := c.RegisterSchema("events", `{
    "type": "object",
    "properties": {
        "event": {"type": "string"},
        "timestamp": {"type": "integer"}
    },
    "required": ["event"]
}`)

// Produce with schema validation
offset, err := c.ProduceWithSchema("events", schemaID, key, value)
```

### Delayed Messages

```go
// Produce with delay (delivered after 5 minutes)
msgID, err := c.ProduceDelayed("events", key, value, 5*time.Minute)

// Cancel a delayed message
err := c.CancelDelayed("events", msgID)
```

### Message TTL

```go
// Produce with TTL (expires after 1 hour)
offset, err := c.ProduceWithTTL("events", key, value, time.Hour)
```

### Dead Letter Queue

```go
// Get DLQ messages
messages, err := c.GetDLQMessages("events", 100)

// Replay a message from DLQ
err := c.ReplayDLQMessage("events", messageID)

// Purge DLQ
err := c.PurgeDLQ("events")
```

---

## Wire Protocol

FlyMQ uses a custom binary protocol for efficient communication.

### Message Format

```
┌─────────┬─────────┬────────┬───────┬────────┬─────────────┐
│ Magic   │ Version │ OpCode │ Flags │ Length │ Payload     │
│ 1 byte  │ 1 byte  │ 1 byte │ 1 byte│ 4 bytes│ Variable    │
└─────────┴─────────┴────────┴───────┴────────┴─────────────┘
```

- **Magic**: `0xAF` (identifies FlyMQ protocol)
- **Version**: Protocol version (currently `0x01`)
- **OpCode**: Operation type (see below)
- **Flags**: Reserved for future use
- **Length**: Payload length in bytes (big-endian)
- **Payload**: Binary-encoded request/response data (see docs/protocol.md)

### Operation Codes

| OpCode | Name | Description |
|--------|------|-------------|
| `0x01` | Produce | Publish messages to a topic |
| `0x02` | Consume | Consume a single message by offset |
| `0x03` | CreateTopic | Create a new topic |
| `0x04` | Metadata | Get topic metadata |
| `0x05` | Subscribe | Subscribe to a topic with consumer group |
| `0x06` | Commit | Commit consumer offset |
| `0x07` | Fetch | Batch fetch messages |
| `0x08` | ListTopics | List all topics |
| `0x09` | DeleteTopic | Delete a topic |
| `0x10` | RegisterSchema | Register a message schema |
| `0x11` | GetSchema | Get schema by ID |
| `0x12` | ListSchemas | List schemas for a topic |
| `0x13` | ValidateMessage | Validate message against schema |
| `0x14` | ProduceWithSchema | Produce with schema validation |
| `0x20` | GetDLQMessages | Get dead letter queue messages |
| `0x21` | ReplayDLQ | Replay message from DLQ |
| `0x22` | PurgeDLQ | Purge DLQ messages |
| `0x30` | ProduceDelayed | Produce with delay |
| `0x31` | CancelDelayed | Cancel delayed message |
| `0x35` | ProduceWithTTL | Produce with TTL |
| `0x40` | TxnBegin | Begin transaction |
| `0x41` | TxnCommit | Commit transaction |
| `0x42` | TxnAbort | Abort transaction |
| `0x43` | TxnProduce | Produce within transaction |
| `0xFF` | Error | Error response |

---

## Error Handling

Errors are returned as `OpError` (0xFF) responses with a binary-encoded payload:

```
┌──────────────────────────┬────────────────────────────┐
│ Error Length (2 bytes)   │ Error Message (UTF-8)      │
└──────────────────────────┴────────────────────────────┘
```

Example error: `topic not found: events`

### Common Errors

| Error | Description |
|-------|-------------|
| `topic not found` | The specified topic does not exist |
| `partition not found` | Invalid partition number |
| `invalid offset` | Offset is out of range |
| `schema validation failed` | Message doesn't match schema |
| `transaction not found` | Invalid transaction ID |

---

## Subscribe Modes

When subscribing to a topic, specify the starting position:

| Mode | Description |
|------|-------------|
| `earliest` | Start from the first message |
| `latest` | Start from new messages only |
| `commit` | Resume from last committed offset |

---

## Compression Types

Supported compression algorithms for batch operations:

| Type | Description |
|------|-------------|
| `none` | No compression |
| `gzip` | Gzip compression |
| `snappy` | Snappy compression (fast) |
| `lz4` | LZ4 compression (very fast) |
| `zstd` | Zstandard compression (high ratio) |

---

## Consumer Group Management

### Joining a Consumer Group

```go
// Join a consumer group with partition assignment
assignment, err := c.JoinGroup("my-group", "events", "range")
// assignment contains: map[partition]offset
```

### Leaving a Consumer Group

```go
// Leave the consumer group (triggers rebalance)
err := c.LeaveGroup("my-group")
```

### Heartbeat

```go
// Send heartbeat to maintain group membership
err := c.Heartbeat("my-group")
```

### List Consumer Groups

```go
// List all consumer groups
groups, err := c.ListGroups()
for _, g := range groups {
    fmt.Printf("Group: %s, Members: %d, State: %s\n",
        g.GroupID, g.MemberCount, g.State)
}
```

### Describe Consumer Group

```go
// Get detailed group information
info, err := c.DescribeGroup("my-group")
// info.Members, info.Assignments, info.Offsets
```

---

## Partition Assignment Strategies

When joining a consumer group, specify the assignment strategy:

| Strategy | Description |
|----------|-------------|
| `range` | Assigns contiguous partition ranges to each consumer |
| `roundrobin` | Distributes partitions evenly in round-robin fashion |
| `sticky` | Minimizes partition movement during rebalances |

---

## Key-Based Partitioning

Messages with keys are consistently routed to the same partition using FNV-1a hashing:

```go
// Messages with the same key go to the same partition
c.Produce("events", []byte("user-123"), []byte("event1"))
c.Produce("events", []byte("user-123"), []byte("event2"))
// Both messages are in the same partition, maintaining order

// Messages without keys are round-robin distributed
c.Produce("events", nil, []byte("event3"))
```

---

## Cluster Operations

### Get Cluster Metadata

```go
// Get cluster information
cluster, err := c.GetClusterInfo()
// cluster.Leader, cluster.Nodes, cluster.Topics
```

### Get Partition Leaders

```go
// Get leader for each partition
leaders, err := c.GetPartitionLeaders("events")
// leaders[partition] = nodeAddress
```

---

## SDK Availability

FlyMQ provides official SDKs for multiple languages:

| Language | Package | Features |
|----------|---------|----------|
| Go | `github.com/firefly-oss/flymq/pkg/client` | Full feature support |
| Python | `flymq-python` | Core operations, TLS, encryption |
| Java | `flymq-java` | Core operations, TLS, encryption |

All SDKs support:
- TLS/SSL connections
- Message encryption (AES-256-GCM)
- Key-based partitioning (FNV-1a)
- Consumer groups
- Automatic reconnection

