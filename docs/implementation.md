# FlyMQ Implementation Details

This document provides technical deep-dives into FlyMQ's implementation.

## Storage Engine

FlyMQ uses a custom segmented log storage engine inspired by Apache Kafka's design. The storage layer is implemented in `internal/storage/` and provides efficient append-only writes with memory-mapped indexes for fast reads.

### Storage Architecture

The storage system consists of four main components:

1. **Store** (`store.go`) - Low-level file wrapper for append/read operations
2. **Index** (`index.go`) - Memory-mapped index for offset-to-position mapping
3. **Segment** (`segment.go`) - Combines store and index for a log segment
4. **Log** (`log.go`) - Manages multiple segments for a complete partition log

### Message Storage Format

Messages are stored as length-prefixed records:

```
[Length(4 bytes)][Message Data(variable)]
```

Each message is serialized using the protocol package's binary encoding.

### Partition Log Structure

Each partition maintains a directory with multiple segments:

```
data/
└── topics/
    └── events/
        └── partition-0/
            ├── 00000000000000000000.store  # Message log segment
            ├── 00000000000000000000.index  # Memory-mapped offset index
            ├── 00000000000000001000.store  # Next segment (after 1000 messages)
            └── 00000000000000001000.index  # Index for second segment
```

Segment files are named by their base offset (zero-padded to 20 digits).

### Write Path

The Log's Append method handles writing messages to the active segment:

```go
// Append adds a record to the log
func (l *Log) Append(record *protocol.Record) (uint64, error) {
    l.mu.Lock()
    defer l.mu.Unlock()

    // Check if we need a new segment
    if l.activeSegment.IsFull() {
        if err := l.newSegment(l.activeSegment.nextOffset); err != nil {
            return 0, err
        }
    }

    // Append to active segment
    offset, err := l.activeSegment.Append(record)
    if err != nil {
        return 0, err
    }

    return offset, nil
}
```

The Segment writes to both the store and index:

```go
// Append writes a record to the segment
func (s *Segment) Append(record *protocol.Record) (uint64, error) {
    // Serialize the record
    data, err := protocol.MarshalRecord(record)
    if err != nil {
        return 0, err
    }

    // Write to store (length-prefixed)
    pos, err := s.store.Append(data)
    if err != nil {
        return 0, err
    }

    // Update index: offset -> position
    offset := s.nextOffset
    if err := s.index.Write(offset, pos); err != nil {
        return 0, err
    }

    s.nextOffset++
    return offset, nil
}
```

### Read Path

Reading uses the index for O(1) offset lookup:

```go
// Read retrieves a record by offset
func (s *Segment) Read(offset uint64) (*protocol.Record, error) {
    // Look up position in index
    _, pos, err := s.index.Read(int64(offset - s.baseOffset))
    if err != nil {
        return nil, err
    }

    // Read from store at position
    data, err := s.store.Read(pos)
    if err != nil {
        return nil, err
    }

    // Deserialize record
    return protocol.UnmarshalRecord(data)
}
```

## Topic Filtering Implementation

FlyMQ implements a high-performance topic filtering engine in the `internal/topic` package. It uses a Trie (prefix tree) data structure to support efficient MQTT-style wildcard matching.

### Trie-based Matcher

The Trie structure allows matching a topic against thousands of patterns in near-constant time. Each node in the Trie represents a topic level (separated by `/`).

```go
type Node struct {
    children map[string]*Node
    patterns []string // Patterns that end at this node
}
```

**Matching Logic:**
1. Split the incoming topic into levels (e.g., `sensors/room1/temp` -> `["sensors", "room1", "temp"]`).
2. Traverse the Trie recursively, following matching level names, `+` (single-level wildcard), and `#` (multi-level wildcard).
3. Collect all patterns encountered during the traversal.

### Server-Side Message Filtering

To reduce network bandwidth and client-side load, FlyMQ performs message filtering directly on the broker during `Fetch` requests.

**Implementation in Broker:**
When a `Fetch` request includes a `Filter` field, the broker applies the filter to each message before including it in the response batch.

```go
func (b *Broker) Fetch(req *protocol.BinaryFetchRequest) (*protocol.BinaryFetchResponse, error) {
    // ... get messages from storage ...
    
    var filteredMessages []*protocol.Message
    for _, msg := range messages {
        if req.Filter != "" {
            // Case-insensitive substring or regex match
            matched, _ := regexp.MatchString("(?i)"+req.Filter, string(msg.Value))
            if !matched {
                continue
            }
        }
        filteredMessages = append(filteredMessages, msg)
    }
    // ...
}
```

*Note: The actual implementation is optimized to avoid redundant allocations and uses a pre-compiled regex where possible.*

## Plug-and-Play SerDe System

The SerDe (Serializer/Deserializer) system provides a unified way to handle typed data across all SDKs.

### Architecture

All SDKs follow a common pattern for SerDe implementation:

1. **Interface Definition**:
    - `Serializer<T>`: Converts an object to bytes.
    - `Deserializer<T>`: Converts bytes back to an object.

2. **Registry**: A central registry in the client allows switching between encoders/decoders by name (e.g., `json`, `string`, `binary`).

3. **Pipeline Integration**:
    - **Producer**: Automatically calls the selected Serializer before sending the payload.
    - **Consumer**: Provides a `Decode()` or `Value()` method that uses the selected Deserializer to return the typed object.

### Implementation Details (Go)

In the Go SDK, the SerDe system is implemented in `pkg/client/serde.go`.

```go
type Serializer interface {
    Encode(v interface{}) ([]byte, error)
}

type Deserializer interface {
    Decode(data []byte, v interface{}) error
}

var registry = map[string]SerDe{
    "json":   {JSONSerializer{}, JSONDeserializer{}},
    "string": {StringSerializer{}, StringDeserializer{}},
    "binary": {BinarySerializer{}, BinaryDeserializer{}},
}
```

Clients can register custom SerDes to support proprietary formats (e.g., Protobuf, Avro) while maintaining a clean API.

## Consumer Group Coordination

### Group State Machine

```
                    ┌─────────────┐
                    │    Empty    │
                    └──────┬──────┘
                           │ member joins
                           ▼
                    ┌─────────────┐
         timeout ──>│ Preparing   │<── member joins/leaves
                    │  Rebalance  │
                    └──────┬──────┘
                           │ all members joined
                           ▼
                    ┌─────────────┐
                    │ Completing  │
                    │  Rebalance  │
                    └──────┬──────┘
                           │ all members synced
                           ▼
                    ┌─────────────┐
         stable ───>│   Stable    │<── heartbeats
                    └──────┬──────┘
                           │ member leaves/timeout
                           ▼
                    ┌─────────────┐
                    │    Dead     │
                    └─────────────┘
```

### Partition Assignment

FlyMQ implements two partition assignment strategies:

**Range Assignment:**
```go
func RangeAssign(members []string, partitions []int32) map[string][]int32 {
    sort.Strings(members)
    sort.Slice(partitions, func(i, j int) bool { 
        return partitions[i] < partitions[j] 
    })
    
    assignment := make(map[string][]int32)
    numPartitions := len(partitions)
    numMembers := len(members)
    
    partitionsPerMember := numPartitions / numMembers
    extra := numPartitions % numMembers
    
    idx := 0
    for i, member := range members {
        count := partitionsPerMember
        if i < extra {
            count++
        }
        assignment[member] = partitions[idx : idx+count]
        idx += count
    }
    
    return assignment
}
```

**Round-Robin Assignment:**
```go
func RoundRobinAssign(members []string, partitions []int32) map[string][]int32 {
    sort.Strings(members)
    
    assignment := make(map[string][]int32)
    for i, p := range partitions {
        member := members[i % len(members)]
        assignment[member] = append(assignment[member], p)
    }
    
    return assignment
}
```

### Offset Management

Consumer offsets are stored in a special internal topic `__consumer_offsets`:

```
Key:   {group_id}/{topic}/{partition}
Value: {offset, metadata, timestamp}
```

```go
type OffsetCommit struct {
    GroupID   string
    Topic     string
    Partition int32
    Offset    int64
    Metadata  string
    Timestamp time.Time
}

func (g *Group) CommitOffset(commit *OffsetCommit) error {
    key := fmt.Sprintf("%s/%s/%d", commit.GroupID, commit.Topic, commit.Partition)
    data, _ := json.Marshal(commit)
    return g.offsetStore.Put(key, data)
}
```

## Protocol Implementation

### Wire Format

FlyMQ uses a binary protocol for efficient network communication:

```
┌────────────────────────────────────────────────────────┐
│                    Request/Response                     │
├──────────┬──────────┬──────────┬───────────────────────┤
│  Length  │ API Key  │ Version  │       Payload         │
│  4 bytes │ 2 bytes  │ 2 bytes  │     Variable          │
└──────────┴──────────┴──────────┴───────────────────────┘
```

### API Keys

| API Key | Name | Description |
|---------|------|-------------|
| 0 | Produce | Publish messages |
| 1 | Fetch | Consume messages |
| 2 | ListOffsets | Get partition offsets |
| 3 | Metadata | Get topic/partition info |
| 8 | OffsetCommit | Commit consumer offsets |
| 9 | OffsetFetch | Fetch committed offsets |
| 10 | FindCoordinator | Find group coordinator |
| 11 | JoinGroup | Join consumer group |
| 12 | Heartbeat | Consumer heartbeat |
| 13 | LeaveGroup | Leave consumer group |
| 14 | SyncGroup | Sync group assignment |

### Message Encoding

```go
func (e *Encoder) WriteMessage(msg *Message) error {
    // Write key
    if err := e.WriteBytes(msg.Key); err != nil {
        return err
    }

    // Write value
    if err := e.WriteBytes(msg.Value); err != nil {
        return err
    }

    // Write headers
    if err := e.WriteInt32(int32(len(msg.Headers))); err != nil {
        return err
    }
    for k, v := range msg.Headers {
        if err := e.WriteString(k); err != nil {
            return err
        }
        if err := e.WriteString(v); err != nil {
            return err
        }
    }

    // Write timestamp
    return e.WriteInt64(msg.Timestamp.UnixMilli())
}
```

## Concurrency Model

### Server Threading

```
┌─────────────────────────────────────────────────────────┐
│                      Main Goroutine                     │
│  - Accepts connections                                  │
│  - Spawns connection handlers                           │
└─────────────────────────────────────────────────────────┘
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
    ┌────────────┐  ┌────────────┐  ┌────────────┐
    │ Connection │  │ Connection │  │ Connection │
    │  Handler   │  │  Handler   │  │  Handler   │
    │ Goroutine  │  │ Goroutine  │  │ Goroutine  │
    └────────────┘  └────────────┘  └────────────┘
           │               │               │
           └───────────────┼───────────────┘
                           ▼
                  ┌─────────────────┐
                  │  Request Router │
                  │   (per topic)   │
                  └─────────────────┘
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
    ┌────────────┐  ┌────────────┐  ┌────────────┐
    │ Partition  │  │ Partition  │  │ Partition  │
    │  Handler   │  │  Handler   │  │  Handler   │
    └────────────┘  └────────────┘  └────────────┘
```

### Lock Hierarchy

To prevent deadlocks, locks are acquired in this order:

1. Server lock (for topic creation/deletion)
2. Topic lock (for partition access)
3. Partition lock (for message operations)
4. Consumer group lock (for membership changes)

```go
// Correct lock ordering
func (s *Server) PublishToTopic(topic string, msg *Message) error {
    s.mu.RLock()
    t, ok := s.topics[topic]
    s.mu.RUnlock()

    if !ok {
        return ErrTopicNotFound
    }

    return t.Publish(msg)  // Topic handles its own locking
}
```

## Performance Optimizations

### Zero-Copy Reads

FlyMQ uses memory-mapped files for zero-copy reads when possible:

```go
func (s *Segment) ReadAt(offset int64) ([]byte, error) {
    // Direct memory access without copying
    return s.mmap[offset : offset+msgSize], nil
}
```

### Batch Processing

Messages are batched for network efficiency:

```go
type RecordBatch struct {
    FirstOffset      int64
    LastOffsetDelta  int32
    Records          []*Record
    CompressionType  Compression
}
```

### Connection Pooling

Clients maintain connection pools to reduce connection overhead:

```go
type ConnectionPool struct {
    addr     string
    maxConns int
    conns    chan *Conn
}

func (p *ConnectionPool) Get() (*Conn, error) {
    select {
    case conn := <-p.conns:
        return conn, nil
    default:
        return p.dial()
    }
}
```

## Cluster Implementation

### Raft Transport

The Raft transport creates a new TCP connection for each RPC request to avoid stale connection issues:

```go
// SendAppendEntries sends an append entries request to a peer.
// Creates a new connection for each request to avoid stale connection issues.
func (t *TCPTransport) SendAppendEntries(peer string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
    // Create a new connection for this request
    conn, err := net.DialTimeout("tcp", peer, t.timeout)
    if err != nil {
        return nil, err
    }
    defer conn.Close()

    // Send request and read response
    if err := t.sendRPC(conn, RPCAppendEntries, data); err != nil {
        return nil, err
    }
    return t.readResponse(conn, resp)
}
```

This approach prevents issues where cached connections become stale due to server-side read timeouts.

### Stats Exchange via Raft

Node statistics are exchanged through Raft AppendEntries responses:

```go
type AppendEntriesResponse struct {
    Term      uint64     `json:"term"`
    Success   bool       `json:"success"`
    NodeStats *NodeStats `json:"node_stats,omitempty"`
}

type NodeStats struct {
    NodeID           string  `json:"node_id"`
    Address          string  `json:"address"`
    RaftState        string  `json:"raft_state"`
    MemoryUsedMB     float64 `json:"memory_used_mb"`
    Goroutines       int     `json:"goroutines"`
    TopicCount       int     `json:"topic_count"`
    MessagesReceived int64   `json:"messages_received"`
    MessagesSent     int64   `json:"messages_sent"`
    Uptime           string  `json:"uptime"`
}
```

The leader collects stats from followers during heartbeats, enabling cluster-wide monitoring.

### Partition-Level Leadership

FlyMQ implements partition-level leadership for horizontal scaling, similar to Apache Kafka. Each partition has its own leader, allowing write load to be distributed across all cluster nodes.

**Key Concepts:**

1. **Message Partitioning** (client-side): Determines which partition a message goes to
   - Key-based: `partition = FNV1a(key) % numPartitions`
   - Round-robin: When no key is provided

2. **Leader Distribution** (cluster-side): Determines which node leads each partition
   - Configurable via `partition.distribution_strategy`

**Leader Distribution Strategies:**

```go
// internal/cluster/distributor.go

type PartitionDistributor interface {
    AssignLeader(topic string, partition int, nodes []string) string
    RebalanceLeaders(assignments map[string][]PartitionInfo) map[string][]PartitionMove
}

// Round-robin: Distribute evenly in order
type RoundRobinDistributor struct{}

func (d *RoundRobinDistributor) AssignLeader(topic string, partition int, nodes []string) string {
    return nodes[partition % len(nodes)]
}

// Least-loaded: Assign to node with fewest leaders
type LeastLoadedDistributor struct {
    leaderCounts map[string]int
}

func (d *LeastLoadedDistributor) AssignLeader(topic string, partition int, nodes []string) string {
    minCount := math.MaxInt
    var minNode string
    for _, node := range nodes {
        if d.leaderCounts[node] < minCount {
            minCount = d.leaderCounts[node]
            minNode = node
        }
    }
    d.leaderCounts[minNode]++
    return minNode
}
```

**Partition Metadata:**

```go
// internal/cluster/partition.go

type PartitionInfo struct {
    Topic      string
    Partition  int
    LeaderID   string
    LeaderAddr string  // Client-facing address for direct routing
    Replicas   []string
    ISR        []string
    Epoch      uint64
}

type PartitionManager struct {
    mu          sync.RWMutex
    assignments map[string][]PartitionInfo  // topic -> partitions
    distributor PartitionDistributor
}

func (pm *PartitionManager) GetLeaderAddr(topic string, partition int) (string, error) {
    pm.mu.RLock()
    defer pm.mu.RUnlock()

    partitions, ok := pm.assignments[topic]
    if !ok || partition >= len(partitions) {
        return "", ErrPartitionNotFound
    }
    return partitions[partition].LeaderAddr, nil
}
```

### Partition Replication

Partitions are replicated across nodes for fault tolerance:

```
┌─────────────────────────────────────────────────────────────┐
│                    Topic: events (3 partitions)             │
├─────────────────────────────────────────────────────────────┤
│  Partition 0    │  Partition 1    │  Partition 2            │
│  Leader: node-1 │  Leader: node-2 │  Leader: node-3         │
│  Replicas:      │  Replicas:      │  Replicas:              │
│   - node-2      │   - node-3      │   - node-1              │
│   - node-3      │   - node-1      │   - node-2              │
└─────────────────────────────────────────────────────────────┘
```

**In-Sync Replicas (ISR):**
- Replicas that are caught up with the leader
- Only ISR members can become leader on failover
- Configurable lag threshold for ISR membership

### Deadlock Prevention

The Raft implementation carefully manages lock ordering to prevent deadlocks:

```go
func (n *RaftNode) handleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
    n.mu.Lock()
    // ... process request ...
    resp.Success = true

    // Release lock BEFORE collecting stats to avoid deadlock
    // (stats collector may call back into raft methods that need the lock)
    n.mu.Unlock()

    // Collect local stats for the response (outside lock)
    resp.NodeStats = n.getLocalStatsUnlocked()
    return resp
}
```

### Client High Availability

The client supports automatic failover across multiple bootstrap servers:

```go
func (c *Client) produceWithRetry(topic string, data []byte) (uint64, error) {
    for attempt := 0; attempt < c.opts.MaxRetries; attempt++ {
        offset, err := c.doProduceRequest(topic, data)
        if err == nil {
            return offset, nil
        }

        // Don't retry server errors (application-level)
        if strings.Contains(err.Error(), "server error:") {
            return 0, err
        }

        // Handle "not leader" errors by finding the leader
        if strings.Contains(err.Error(), "not leader") {
            // Extract and connect to leader...
        }

        // Try next server in rotation
        c.currentServer = (c.currentServer + 1) % len(c.servers)
        c.connectToServer(c.servers[c.currentServer])
    }
    return 0, fmt.Errorf("produce failed after retries")
}
```

## Performance Optimizations

### Zero-Copy I/O

FlyMQ uses platform-specific system calls for zero-copy network transfers, eliminating unnecessary data copies between kernel and user space:

**Platform Support:**

| Platform | File → Socket | Socket → File | Implementation |
|----------|---------------|---------------|----------------|
| **Linux** | `sendfile()` | `splice()` | Full zero-copy both directions |
| **macOS/Darwin** | `sendfile()` | Regular copy | Zero-copy for reads only |
| **Windows/Others** | Regular copy | Regular copy | Graceful fallback |

```go
// ZeroCopyReader wraps a file for zero-copy reads
type ZeroCopyReader struct {
    file   *os.File
    offset int64
    length int64
}

// SendTo sends data directly to a network connection using sendfile.
// Platform-specific implementations in zerocopy_linux.go, zerocopy_darwin.go
func (z *ZeroCopyReader) SendTo(conn net.Conn) (int64, error) {
    tcpConn, ok := conn.(*net.TCPConn)
    if !ok {
        return z.regularCopy(conn) // Fallback for non-TCP
    }
    // Use platform-specific sendfile syscall
    return z.sendfile(tcpConn)
}

// IsZeroCopySupported returns true if zero-copy is available on this platform
func IsZeroCopySupported() bool {
    return zeroCopySupported() // true on Linux and macOS
}
```

**Linux-specific optimizations:**
- Uses `unix.Sendfile()` for file-to-socket transfers
- Uses `unix.Splice()` with pipes for socket-to-file transfers (WriteFrom)
- Handles `EAGAIN` and `EINTR` for non-blocking I/O

**macOS-specific optimizations:**
- Uses Darwin's `sendfile()` syscall (different signature than Linux)
- Falls back to regular copy for socket-to-file (no splice on Darwin)

### Compression

FlyMQ supports multiple compression algorithms:

| Algorithm | Speed | Ratio | Use Case |
|-----------|-------|-------|----------|
| LZ4 | Fastest | Lower | High-throughput, low-latency |
| Snappy | Fast | Medium | General purpose |
| Zstd | Medium | Highest | Storage efficiency |
| Gzip | Slow | High | Compatibility |

### Async Disk I/O

Background workers handle disk operations:

```go
type AsyncWriter struct {
    queue   chan writeRequest
    workers int
}

func (a *AsyncWriter) Write(data []byte) <-chan error {
    result := make(chan error, 1)
    a.queue <- writeRequest{data: data, result: result}
    return result
}
```

### Connection Multiplexing

Multiple logical streams share a single TCP connection:

```go
type MultiplexedConn struct {
    conn    net.Conn
    streams map[uint32]*Stream
    mu      sync.RWMutex
}

// Each stream has its own read/write buffers
type Stream struct {
    id     uint32
    readCh chan []byte
    writeCh chan []byte
}
```

## Message Encryption

FlyMQ supports AES-256-GCM encryption for messages at rest and in transit:

```go
type Encryptor struct {
    cipher cipher.AEAD
}

func (e *Encryptor) Encrypt(plaintext []byte) ([]byte, error) {
    nonce := make([]byte, e.cipher.NonceSize())
    if _, err := rand.Read(nonce); err != nil {
        return nil, err
    }
    return e.cipher.Seal(nonce, nonce, plaintext, nil), nil
}

func (e *Encryptor) Decrypt(ciphertext []byte) ([]byte, error) {
    nonceSize := e.cipher.NonceSize()
    nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
    return e.cipher.Open(nil, nonce, ciphertext, nil)
}
```

**Key Features:**
- 32-byte AES-256 keys
- Random nonce per message
- Authenticated encryption (prevents tampering)
- Compatible across all SDKs (Go, Python, Java)

## Binary Protocol Implementation

FlyMQ uses a **binary-only** protocol for all client-server communication. The implementation is in `internal/protocol/binary.go`.

### Encoding Architecture

All request/response types follow a consistent pattern:

```go
// Request struct with binary encoding
type BinaryProduceRequest struct {
    Topic     string
    Key       []byte
    Value     []byte
    Partition int32
}

// Encoder function
func EncodeBinaryProduceRequest(req *BinaryProduceRequest) []byte {
    buf := make([]byte, calculatedSize)
    offset := 0
    
    // String: [uint16 length][UTF-8 bytes]
    binary.BigEndian.PutUint16(buf[offset:], uint16(len(req.Topic)))
    offset += 2
    copy(buf[offset:], req.Topic)
    offset += len(req.Topic)
    
    // Bytes: [uint32 length][raw bytes]
    binary.BigEndian.PutUint32(buf[offset:], uint32(len(req.Key)))
    offset += 4
    copy(buf[offset:], req.Key)
    // ...
    return buf
}

// Decoder function
func DecodeBinaryProduceRequest(data []byte) (*BinaryProduceRequest, error) {
    // Validate minimum size
    // Decode fields in order
    // Return struct or error
}
```

### Adding New Operations

To add a new protocol operation:

1. **Define OpCode** in `protocol.go`:
   ```go
   OpMyNewOp OpCode = 0xXX
   ```

2. **Create Request/Response structs** in `binary.go`:
   ```go
   type BinaryMyNewOpRequest struct { ... }
   type BinaryMyNewOpResponse struct { ... }
   ```

3. **Implement Encode/Decode functions**:
   ```go
   func EncodeBinaryMyNewOpRequest(req *BinaryMyNewOpRequest) []byte
   func DecodeBinaryMyNewOpRequest(data []byte) (*BinaryMyNewOpRequest, error)
   ```

4. **Add handler** in `server.go`:
   ```go
   case protocol.OpMyNewOp:
       return s.handleMyNewOp(w, msg.Payload)
   ```

### Protocol Extension Guidelines

- **Backward compatibility**: New fields should be appended, not inserted
- **Version checking**: Use protocol version for breaking changes
- **Size validation**: Always validate payload size before decoding
- **Error handling**: Return descriptive errors for malformed data

## Storage Performance Optimizations

### BatchedStore

The `BatchedStore` (`internal/storage/batched_store.go`) provides high-performance writes with configurable durability.

**Acks Modes:**

| Mode | Behavior | Throughput | Durability |
|------|----------|------------|------------|
| `all` | fsync every write | ~300 msg/s | Highest |
| `leader` | batch fsync on interval | ~3,000 msg/s | Medium |
| `none` | async, fsync on close | ~10,000 msg/s | Lowest |

```go
// BatchedStore wraps Store with batched write support
type BatchedStore struct {
    *Store
    config        PerformanceConfig
    pendingWrites int32
    lastSync      time.Time
    syncTicker    *time.Ticker
}

// Append with configurable durability
func (bs *BatchedStore) Append(p []byte) (n uint64, pos uint64, err error) {
    switch bs.config.Acks {
    case "all":
        // Synchronous fsync
        bs.buf.Flush()
        bs.File.Sync()
    case "leader":
        // Background goroutine handles periodic fsync
        atomic.AddInt32(&bs.pendingWrites, 1)
    case "none":
        // No sync - data synced on close only
    }
}
```

### StoreWriter Interface

The `StoreWriter` interface abstracts over `Store` and `BatchedStore`:

```go
type StoreWriter interface {
    Append([]byte) (uint64, uint64, error)
    Read(uint64) ([]byte, error)
    Close() error
    Sync() error
    GetZeroCopyInfo(offset, size uint64) (filename string, fileOffset int64, err error)
}
```

**Segment Selection Logic:**
```go
func NewSegmentWithConfig(baseOffset uint64, dir string, cfg SegmentConfig, perf PerformanceConfig) *Segment {
    // Use BatchedStore when acks != "all"
    if perf.Acks != "all" {
        store = NewBatchedStore(file, perf)
    } else {
        store = NewStore(file)
    }
}
```

### Zero-Copy Support

For large messages (≥64KB), the server uses platform-specific zero-copy transfers:

```go
func (s *Segment) GetZeroCopyInfo(offset, size uint64) (string, int64, error) {
    return s.store.Name(), int64(offset), nil
}

func (s *Server) handleConsumeZeroCopy(w io.Writer, payload []byte) error {
    file, offset, length, _ := segment.GetZeroCopyInfo(relOffset, msgSize)

    // Use platform-specific zero-copy (sendfile on Linux/macOS)
    reader := performance.NewZeroCopyReader(file, offset, length)
    return reader.SendTo(conn) // Uses sendfile() internally
}
```

**Platform behavior:**
- **Linux**: Uses `sendfile()` syscall for direct kernel-to-network transfer
- **macOS**: Uses Darwin `sendfile()` with different syscall signature
- **Windows/Others**: Gracefully falls back to buffered I/O

## Audit Trail

FlyMQ includes a comprehensive audit trail system for tracking security-relevant operations. The audit system is implemented in `internal/audit/`.

### Audit Event Types

The audit system tracks the following event categories:

| Category | Event Types | Description |
|----------|-------------|-------------|
| Authentication | `auth.success`, `auth.failure`, `auth.logout` | User authentication events |
| Authorization | `access.granted`, `access.denied` | Resource access decisions |
| Topics | `topic.create`, `topic.delete`, `topic.modify` | Topic management operations |
| Users | `user.create`, `user.delete`, `user.modify` | User management operations |
| ACLs | `acl.change` | Access control list changes |
| Configuration | `config.change` | Configuration modifications |
| Cluster | `cluster.join`, `cluster.leave` | Cluster membership changes |

### Audit Event Structure

Each audit event contains:

```go
type AuditEvent struct {
    ID        string            // Unique event identifier
    Timestamp time.Time         // When the event occurred
    Type      AuditEventType    // Event type (e.g., auth.success)
    User      string            // Username who performed the action
    ClientIP  string            // Client IP address
    Resource  string            // Resource affected (topic, user, etc.)
    Action    string            // Action performed
    Result    string            // Result (success, failure, denied)
    Details   map[string]string // Additional context
    NodeID    string            // Cluster node that recorded the event
}
```

### Storage Format

Audit events are stored in JSON Lines format for easy parsing:

```json
{"id":"evt_abc123","timestamp":"2026-01-17T10:30:00Z","type":"auth.success","user":"admin",...}
{"id":"evt_abc124","timestamp":"2026-01-17T10:30:01Z","type":"topic.create","user":"admin",...}
```

### Query API

The audit system supports flexible querying:

```go
filter := &AuditQueryFilter{
    StartTime:  time.Now().Add(-24 * time.Hour),
    EndTime:    time.Now(),
    EventTypes: []string{"auth.success", "auth.failure"},
    User:       "admin",
    Limit:      100,
}
result, err := auditStore.Query(filter)
```

### Integration Points

Audit events are recorded at key points in the server:

1. **Authentication** - `handleAuth()` records success/failure
2. **Authorization** - ACL checks record access decisions
3. **Topic Operations** - Create/delete/modify operations
4. **User Management** - User CRUD operations
5. **Cluster Events** - Node join/leave events

---

## Multi-Protocol Bridges (gRPC, WS, MQTT)

FlyMQ supports multiple access protocols via lightweight bridges that map protocol-specific requests to core broker operations. Each bridge is designed with security, performance, and operational best practices in mind.

### 1. gRPC Server Implementation

The gRPC server is implemented in `internal/server/grpc/server.go` using the `google.golang.org/grpc` package.

**Security:**
- **TLS/mTLS**: Uses the project's standard `crypto` package to load server certificates and optionally CA certificates for mutual TLS authentication.
- **Authentication**: Implements both `UnaryInterceptor` and `StreamInterceptor`. Credentials are expected in gRPC metadata under the `username` and `password` keys.
- **Authorization**: Per-operation authorization checks via the `Authorizer` interface.

**Performance Features:**
- **Connection Keepalive**: Configured with server-side keepalive parameters to detect dead connections and manage resources:
  - `MaxConnectionIdle`: 5 minutes (closes idle connections)
  - `MaxConnectionAge`: 30 minutes (graceful connection recycling)
  - `KeepaliveTime`: 30 seconds (ping interval)
  - `KeepaliveTimeout`: 10 seconds (ping acknowledgment timeout)
- **Configurable Poll Interval**: The `Consume` stream uses a configurable poll interval (default 100ms) to balance latency vs CPU usage.

**Health Checks:**
- Implements the standard gRPC health check protocol (`grpc.health.v1.Health`)
- Enables integration with load balancers and orchestrators (Kubernetes, Consul, etc.)
- Health status is set to `NOT_SERVING` during graceful shutdown

**Cluster Awareness:**
- The `GetMetadata` RPC returns partition-to-node mappings by calling `Broker.GetClusterMetadata`.
- Write operations return "not leader" errors if the client connects to the wrong node, including the address of the correct leader.

**Debugging:**
- gRPC reflection is enabled for debugging with tools like `grpcurl`

### 2. WebSocket Gateway Implementation

The WebSocket gateway (`internal/server/ws/gateway.go`) provides a JSON-based command protocol over persistent WebSocket connections, enabling browser-based and other WebSocket clients.

**Command Protocol:**
Requests follow a standard JSON format:
```json
{"id": "req_1", "command": "produce", "params": {"topic": "orders", "value": "..."}}
```

Responses:
```json
{"id": "req_1", "success": true, "data": {"offset": 123, "partition": 0}}
```

Push messages for subscriptions:
```json
{"command": "message", "data": {"topic": "orders", "partition": 0, "offset": 123, "key": "...", "value": "..."}}
```

**Supported Commands:**
- `login` - Authenticate with username/password
- `produce` - Send a message to a topic
- `consume` - Fetch messages from a topic/partition
- `subscribe` - Start a push-based subscription
- `unsubscribe` - Stop a subscription
- `list_topics` - List available topics
- `get_cluster_metadata` - Get cluster topology
- `commit` - Commit consumer group offset

**Security:**
- **Origin Checking**: Configurable allowed origins for CORS security (defaults to allow all for development)
- **Authentication**: Clients must send a `login` command before performing other operations (unless `allow_anonymous` is enabled)
- **Authorization**: Per-command authorization checks

**Connection Health:**
- **Ping/Pong Heartbeat**: Automatic ping frames every 30 seconds to detect dead connections
- **Pong Timeout**: Connections are closed if no pong is received within 10 seconds
- **Write Timeouts**: 10-second timeout on write operations to prevent blocking on slow clients

**Performance:**
- **Compression**: WebSocket compression is enabled for better performance over slow networks
- **Configurable Buffer Sizes**: 4KB read/write buffers (configurable)
- **Exponential Backoff**: Subscription loops use exponential backoff on errors (capped at 30 seconds)

### 3. MQTT Bridge Implementation

The MQTT bridge (`internal/server/bridge/mqtt.go`) supports MQTT v3.1.1 clients by adapting MQTT packets to FlyMQ operations. This enables existing IoT devices and MQTT-based applications to integrate with FlyMQ.

**Supported MQTT Features:**
- **CONNECT**: Client authentication with username/password
- **CONNACK**: Connection acknowledgment with return codes
- **PUBLISH**: Message publishing (QoS 0 only)
- **SUBSCRIBE**: Topic subscription (QoS 0 only)
- **SUBACK**: Subscription acknowledgment
- **PINGREQ/PINGRESP**: Keep-alive mechanism
- **DISCONNECT**: Clean session termination

**Limitations (by design):**
- **QoS 0 only**: Messages are delivered at-most-once. QoS 1/2 are not supported.
- **No retained messages**: Retained message flag is parsed but not implemented.
- **No will messages**: Will topic/message are parsed but not implemented.
- **No wildcard subscriptions**: MQTT wildcards (+, #) are not supported.
- **No UNSUBSCRIBE**: Subscriptions are cleaned up on disconnect only.
- **No session persistence**: Clean session is always assumed.

**Topic Mapping:**
MQTT topics are mapped directly to FlyMQ topics. All messages are published to partition 0.

**Security:**
- **TLS**: Full TLS support for encrypted connections
- **Authentication**: Username/password from CONNECT packet
- **Authorization**: Per-operation authorization checks for PUBLISH and SUBSCRIBE

**Connection Management:**
- **Read Timeout**: 60-second idle timeout for detecting dead connections
- **Write Timeout**: 10-second timeout on write operations
- **Exponential Backoff**: Subscription loops use exponential backoff on errors (capped at 30 seconds)

### Cluster Design

All bridges share common design principles:

1. **Broker Interface**: All bridges use the `Broker` interface, ensuring that operations are replicated via Raft in cluster mode.

2. **Stateless Design**: Bridges are designed to be stateless (with the exception of authenticated connection state), allowing them to scale horizontally behind a load balancer.

3. **Graceful Shutdown**: All bridges support graceful shutdown, allowing in-flight requests to complete.

4. **Consistent Security Model**: All bridges use the same `Authorizer` interface for authentication and authorization.

### Configuration Summary

| Protocol | Default Port | TLS Config | Auth Config |
|----------|-------------|------------|-------------|
| gRPC | 9094 | `grpc.tls.*` | `auth.enabled` |
| WebSocket | 9095 | `ws.tls.*` | `auth.enabled` |
| MQTT | 1883 | `mqtt.tls.*` | `auth.enabled` |

