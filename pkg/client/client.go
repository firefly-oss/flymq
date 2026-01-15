/*
 * Copyright (c) 2026 Firefly Software Solutions Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
Package client provides the FlyMQ Go client library.

QUICK START:
============

	// Connect to FlyMQ
	client, err := client.NewClient("localhost:9092")
	defer client.Close()

	// Produce a message
	err = client.Produce("my-topic", []byte("key"), []byte("value"))

	// Consume messages
	consumer, err := client.NewConsumer("my-topic", "my-group")
	for msg := range consumer.Messages() {
	    fmt.Printf("Received: %s\n", msg.Value)
	    msg.Ack()
	}

TLS CONNECTION:
===============

	client, err := client.NewClientWithOptions("localhost:9093", client.ClientOptions{
	    TLSEnabled: true,
	    TLSCAFile:  "/path/to/ca.crt",
	})

THREAD SAFETY:
==============
The client is safe for concurrent use by multiple goroutines.
*/
package client

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"flymq/internal/crypto"
	"flymq/internal/protocol"
)

// ClientOptions configures the client connection.
type ClientOptions struct {
	// Bootstrap servers for HA connections (e.g., "host1:9092,host2:9092,host3:9092")
	BootstrapServers []string

	// TLS configuration
	TLSEnabled            bool   // Enable TLS connection
	TLSCertFile           string // Client certificate file (for mTLS)
	TLSKeyFile            string // Client key file (for mTLS)
	TLSCAFile             string // CA certificate file for server verification
	TLSInsecureSkipVerify bool   // Skip server certificate verification (testing only)

	// Authentication configuration
	Username string // Username for authentication (optional)
	Password string // Password for authentication (optional)

	// Connection behavior
	MaxRetries     int // Maximum connection retries per server (default: 3)
	RetryDelayMs   int // Delay between retries in milliseconds (default: 1000)
	ConnectTimeout int // Connection timeout in seconds (default: 10)
}

// Client represents a FlyMQ client connection with HA support.
type Client struct {
	servers       []string      // List of bootstrap servers
	currentServer int           // Index of currently connected server
	conn          net.Conn      // Current connection
	tlsConfig     *tls.Config   // TLS configuration
	opts          ClientOptions // Client options
	mu            sync.Mutex    // Protects connection state
	leaderAddr    string        // Cached leader address (if known)
	authenticated bool          // Whether the client is authenticated
	username      string        // Authenticated username
}

// NewClient creates a new client connected to the specified address.
func NewClient(addr string) (*Client, error) {
	return NewClientWithOptions(addr, ClientOptions{})
}

// NewClusterClient creates a new client with multiple bootstrap servers for HA.
// Servers should be comma-separated: "host1:9092,host2:9092,host3:9092"
func NewClusterClient(bootstrapServers string, opts ClientOptions) (*Client, error) {
	servers := parseBootstrapServers(bootstrapServers)
	if len(servers) == 0 {
		return nil, fmt.Errorf("no bootstrap servers provided")
	}
	opts.BootstrapServers = servers
	return NewClientWithOptions(servers[0], opts)
}

// parseBootstrapServers parses a comma-separated list of servers.
func parseBootstrapServers(servers string) []string {
	var result []string
	for _, s := range strings.Split(servers, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			result = append(result, s)
		}
	}
	return result
}

// NewClientWithOptions creates a new client with custom options.
func NewClientWithOptions(addr string, opts ClientOptions) (*Client, error) {
	// Set defaults
	if opts.MaxRetries == 0 {
		opts.MaxRetries = 3
	}
	if opts.RetryDelayMs == 0 {
		opts.RetryDelayMs = 1000
	}
	if opts.ConnectTimeout == 0 {
		opts.ConnectTimeout = 10
	}

	// Build server list
	servers := opts.BootstrapServers
	if len(servers) == 0 {
		servers = []string{addr}
	}

	client := &Client{
		servers:       servers,
		currentServer: 0,
		opts:          opts,
	}

	// Configure TLS if enabled
	if opts.TLSEnabled {
		tlsCfg, err := crypto.NewClientTLSConfig(crypto.TLSConfig{
			CertFile:           opts.TLSCertFile,
			KeyFile:            opts.TLSKeyFile,
			CAFile:             opts.TLSCAFile,
			InsecureSkipVerify: opts.TLSInsecureSkipVerify,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to configure TLS: %w", err)
		}
		client.tlsConfig = tlsCfg
	}

	if err := client.connectWithRetry(); err != nil {
		return nil, err
	}

	// Auto-authenticate if credentials are provided
	if opts.Username != "" {
		if err := client.Authenticate(opts.Username, opts.Password); err != nil {
			client.Close()
			return nil, fmt.Errorf("authentication failed: %w", err)
		}
	}

	return client, nil
}

// NewTLSClient creates a new TLS-enabled client.
func NewTLSClient(addr string, insecureSkipVerify bool) (*Client, error) {
	return NewClientWithOptions(addr, ClientOptions{
		TLSEnabled:            true,
		TLSInsecureSkipVerify: insecureSkipVerify,
	})
}

// connectWithRetry attempts to connect to any available server.
func (c *Client) connectWithRetry() error {
	var lastErr error

	// Try each server in order
	for attempt := 0; attempt < c.opts.MaxRetries; attempt++ {
		for i := 0; i < len(c.servers); i++ {
			serverIdx := (c.currentServer + i) % len(c.servers)
			server := c.servers[serverIdx]

			if err := c.connectToServer(server); err != nil {
				lastErr = err
				continue
			}

			c.currentServer = serverIdx
			return nil
		}

		// Wait before retry
		if attempt < c.opts.MaxRetries-1 {
			time.Sleep(time.Duration(c.opts.RetryDelayMs) * time.Millisecond)
		}
	}

	return fmt.Errorf("failed to connect to any server after %d attempts: %w", c.opts.MaxRetries, lastErr)
}

// connectToServer connects to a specific server.
func (c *Client) connectToServer(addr string) error {
	var conn net.Conn
	var err error

	dialer := &net.Dialer{
		Timeout: time.Duration(c.opts.ConnectTimeout) * time.Second,
	}

	if c.tlsConfig != nil {
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, c.tlsConfig)
	} else {
		conn, err = dialer.Dial("tcp", addr)
	}

	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	c.conn = conn
	return nil
}

// Close closes the client connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// IsTLS returns true if the client is using TLS.
func (c *Client) IsTLS() bool {
	return c.tlsConfig != nil
}

// Reconnect attempts to reconnect to any available server.
func (c *Client) Reconnect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}

	// Try next server in rotation
	c.currentServer = (c.currentServer + 1) % len(c.servers)
	return c.connectWithRetry()
}

// CurrentServer returns the address of the currently connected server.
func (c *Client) CurrentServer() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.currentServer < len(c.servers) {
		return c.servers[c.currentServer]
	}
	return ""
}

// Servers returns the list of bootstrap servers.
func (c *Client) Servers() []string {
	return c.servers
}

// IsAuthenticated returns true if the client has successfully authenticated.
func (c *Client) IsAuthenticated() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.authenticated
}

// Username returns the authenticated username, or empty string if not authenticated.
func (c *Client) Username() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.username
}

// AuthResponse represents the server's response to an authentication request.
type AuthResponse struct {
	Success     bool
	Error       string
	Username    string
	Roles       []string
	Permissions []string
}

// Authenticate sends authentication credentials to the server.
// This is called automatically if Username/Password are set in ClientOptions.
func (c *Client) Authenticate(username, password string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Build binary auth request
	req := &protocol.BinaryAuthRequest{
		Username: username,
		Password: password,
	}
	payload := protocol.EncodeBinaryAuthRequest(req)

	// Send auth request using binary protocol
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpAuth, payload); err != nil {
		return fmt.Errorf("failed to send auth request: %w", err)
	}

	// Read response
	msg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return fmt.Errorf("failed to read auth response: %w", err)
	}

	// Handle error response
	if msg.Header.Op == protocol.OpError {
		return fmt.Errorf("authentication failed: %s", string(msg.Payload))
	}

	if msg.Header.Op != protocol.OpAuthResponse {
		return fmt.Errorf("unexpected response opcode: %d", msg.Header.Op)
	}

	// Parse binary response
	resp, err := protocol.DecodeBinaryAuthResponse(msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to parse auth response: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("authentication failed: %s", resp.Error)
	}

	c.authenticated = true
	c.username = username
	return nil
}

// WhoAmIResponse represents the server's response to a WhoAmI request.
type WhoAmIResponse struct {
	Authenticated bool
	Username      string
	Roles         []string
	Permissions   []string
}

// WhoAmI queries the server for the current authentication status.
func (c *Client) WhoAmI() (*WhoAmIResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Send WhoAmI request using binary protocol
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpWhoAmI, nil); err != nil {
		return nil, fmt.Errorf("failed to send whoami request: %w", err)
	}

	// Read response
	msg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read whoami response: %w", err)
	}

	if msg.Header.Op != protocol.OpWhoAmI {
		return nil, fmt.Errorf("unexpected response opcode: %d", msg.Header.Op)
	}

	// Parse binary response (uses same format as auth response)
	resp, err := protocol.DecodeBinaryAuthResponse(msg.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to parse whoami response: %w", err)
	}

	return &WhoAmIResponse{
		Authenticated: resp.Success,
		Username:      resp.Username,
		Roles:         resp.Roles,
	}, nil
}

// parseLeaderAddr extracts the leader address from a "not leader" error message.
// Error format: "not leader: leader_id=<id> leader_addr=<addr>"
// Returns the leader address if found, empty string otherwise.
func parseLeaderAddr(errMsg string) string {
	if !strings.Contains(errMsg, "not leader") {
		return ""
	}
	// Look for leader_addr=<addr> pattern
	if idx := strings.Index(errMsg, "leader_addr="); idx != -1 {
		addr := errMsg[idx+len("leader_addr="):]
		// Address ends at space or end of string
		if spaceIdx := strings.Index(addr, " "); spaceIdx != -1 {
			addr = addr[:spaceIdx]
		}
		return strings.TrimSpace(addr)
	}
	return ""
}

// isNotLeaderError checks if an error is a "not leader" error.
func isNotLeaderError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "not leader")
}

// tryFailoverToLeader attempts to connect to the leader if the error indicates
// we're not connected to the leader. Returns true if failover was successful.
func (c *Client) tryFailoverToLeader(err error) bool {
	if !isNotLeaderError(err) {
		return false
	}
	leaderAddr := parseLeaderAddr(err.Error())
	if leaderAddr == "" {
		return false
	}
	// Close current connection and connect to leader
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.leaderAddr = leaderAddr
	return c.connectToServer(leaderAddr) == nil
}

// executeWithLeaderFailover executes a function with automatic leader failover.
// The function should return an error if the operation fails.
// If the error is a "not leader" error, it will try to connect to the leader and retry.
func (c *Client) executeWithLeaderFailover(fn func() error) error {
	for attempt := 0; attempt < c.opts.MaxRetries; attempt++ {
		// Ensure we have a valid connection before attempting
		if c.conn == nil {
			if err := c.connectToServer(c.servers[c.currentServer]); err != nil {
				c.currentServer = (c.currentServer + 1) % len(c.servers)
				time.Sleep(time.Duration(c.opts.RetryDelayMs/4) * time.Millisecond)
				continue
			}
		}

		err := fn()
		if err == nil {
			return nil
		}

		// Check if it's a "not leader" error
		if isNotLeaderError(err) {
			// Try to failover to leader
			if c.tryFailoverToLeader(err) {
				continue // Retry with new leader connection
			}
			// Leader address unknown - election may be in progress
			time.Sleep(time.Duration(c.opts.RetryDelayMs/2) * time.Millisecond)
			// Fall through to try other servers
		}

		// If it's a non-retryable error, return immediately
		if strings.Contains(err.Error(), "server error:") && !isNotLeaderError(err) {
			return err
		}

		// Connection error - try next server
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		c.currentServer = (c.currentServer + 1) % len(c.servers)
		if connErr := c.connectToServer(c.servers[c.currentServer]); connErr != nil {
			time.Sleep(time.Duration(c.opts.RetryDelayMs/4) * time.Millisecond)
			continue
		}
	}
	return fmt.Errorf("operation failed after %d attempts", c.opts.MaxRetries)
}

// Produce sends a message to a topic with automatic failover.
// If the current server is unavailable, it will try other bootstrap servers.
func (c *Client) Produce(topic string, data []byte) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.produceWithRetry(topic, data)
}

func (c *Client) produceWithRetry(topic string, data []byte) (uint64, error) {
	var lastErr error

	for attempt := 0; attempt < c.opts.MaxRetries; attempt++ {
		// Ensure we have a valid connection before attempting
		if c.conn == nil {
			if err := c.connectToServer(c.servers[c.currentServer]); err != nil {
				lastErr = err
				c.currentServer = (c.currentServer + 1) % len(c.servers)
				time.Sleep(time.Duration(c.opts.RetryDelayMs/4) * time.Millisecond)
				continue
			}
		}

		offset, err := c.doProduceRequest(topic, data)
		if err == nil {
			return offset, nil
		}

		lastErr = err

		// Check if it's a "not leader" error - try to failover to leader
		if isNotLeaderError(err) {
			leaderAddr := parseLeaderAddr(err.Error())
			if leaderAddr != "" {
				// Close current connection and connect to leader
				if c.conn != nil {
					c.conn.Close()
					c.conn = nil
				}
				c.leaderAddr = leaderAddr
				if connErr := c.connectToServer(leaderAddr); connErr == nil {
					// Successfully connected to leader, retry immediately
					continue
				}
			} else {
				// Leader address unknown - election may be in progress
				// Add a small delay before trying another server
				time.Sleep(time.Duration(c.opts.RetryDelayMs/2) * time.Millisecond)
			}
			// Fall through to try other servers
		}

		// If it's a non-retryable server error, don't retry
		if strings.Contains(err.Error(), "server error:") && !isNotLeaderError(err) {
			return 0, err
		}

		// Connection error or leader not found - try next server in rotation
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}

		c.currentServer = (c.currentServer + 1) % len(c.servers)
		if err := c.connectToServer(c.servers[c.currentServer]); err != nil {
			lastErr = err
			// Small delay before trying next server
			time.Sleep(time.Duration(c.opts.RetryDelayMs/4) * time.Millisecond)
			continue
		}
	}

	return 0, fmt.Errorf("produce failed after %d attempts: %w", c.opts.MaxRetries, lastErr)
}

func (c *Client) doProduceRequest(topic string, data []byte) (uint64, error) {
	return c.doProduceRequestFull(topic, nil, data, -1)
}

func (c *Client) doProduceRequestWithKey(topic string, key, data []byte) (uint64, error) {
	return c.doProduceRequestFull(topic, key, data, -1)
}

func (c *Client) doProduceRequestFull(topic string, key, data []byte, partition int) (uint64, error) {
	// Use binary protocol for better performance on all message sizes
	// Binary protocol has ~30% less overhead than JSON
	part := int32(partition)
	if partition < 0 {
		part = -1 // Sentinel for auto-partition
	}
	req := &protocol.BinaryProduceRequest{
		Topic:     topic,
		Key:       key,
		Value:     data,
		Partition: part,
	}
	payload := protocol.EncodeBinaryProduceRequest(req)
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpProduce, payload); err != nil {
		return 0, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return 0, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return 0, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	// Parse binary response (binary-only protocol)
	resp, err := protocol.DecodeBinaryProduceResponse(respMsg.Payload)
	if err != nil {
		return 0, err
	}
	return resp.Offset, nil
}

// ProduceWithKey produces a message with a key for key-based partitioning.
// Messages with the same key will be sent to the same partition.
func (c *Client) ProduceWithKey(topic string, key, data []byte) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.doProduceRequestWithKey(topic, key, data)
}

// ProduceToPartition produces a message to a specific partition.
// Use this when you need explicit control over partition assignment.
func (c *Client) ProduceToPartition(topic string, partition int, data []byte) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.doProduceRequestFull(topic, nil, data, partition)
}

// ProduceWithKeyToPartition produces a message with a key to a specific partition.
// Note: When partition is specified, it overrides key-based partition selection.
func (c *Client) ProduceWithKeyToPartition(topic string, partition int, key, data []byte) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.doProduceRequestFull(topic, key, data, partition)
}

// ConsumedMessage represents a consumed message with its key and value.
type ConsumedMessage struct {
	Key   []byte
	Value []byte
}

// Consume consumes a single message from partition 0.
func (c *Client) Consume(topic string, offset uint64) ([]byte, error) {
	msg, err := c.ConsumeFromPartition(topic, 0, offset)
	if err != nil {
		return nil, err
	}
	return msg.Value, nil
}

// ConsumeWithKey consumes a single message from partition 0 and returns both key and value.
func (c *Client) ConsumeWithKey(topic string, offset uint64) (*ConsumedMessage, error) {
	return c.ConsumeFromPartition(topic, 0, offset)
}

// ConsumeFromPartition consumes a single message from a specific partition.
// Returns the message with its key and value.
// Uses binary protocol for better performance.
func (c *Client) ConsumeFromPartition(topic string, partition int, offset uint64) (*ConsumedMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Use binary protocol for better performance
	req := &protocol.BinaryConsumeRequest{
		Topic:     topic,
		Partition: int32(partition),
		Offset:    offset,
	}
	payload := protocol.EncodeBinaryConsumeRequest(req)
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpConsume, payload); err != nil {
		return nil, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return nil, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return nil, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	// Parse binary response (binary-only protocol)
	resp, err := protocol.DecodeBinaryConsumeResponse(respMsg.Payload)
	if err != nil {
		return nil, err
	}
	return &ConsumedMessage{Key: resp.Key, Value: resp.Value}, nil
}

func (c *Client) CreateTopic(topic string, partitions int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.executeWithLeaderFailover(func() error {
		// Use binary protocol for performance
		req := &protocol.BinaryCreateTopicRequest{
			Topic:      topic,
			Partitions: int32(partitions),
		}
		payload := protocol.EncodeBinaryCreateTopicRequest(req)
		if err := protocol.WriteBinaryMessage(c.conn, protocol.OpCreateTopic, payload); err != nil {
			return err
		}

		respMsg, err := protocol.ReadMessage(c.conn)
		if err != nil {
			return err
		}
		if respMsg.Header.Op == protocol.OpError {
			return fmt.Errorf("server error: %s", string(respMsg.Payload))
		}
		return nil
	})
}

// Subscribe subscribes to a topic and returns the starting offset.
// mode can be "earliest", "latest", or "commit".
func (c *Client) Subscribe(topic, groupID string, partition int, mode string) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var resultOffset uint64
	err := c.executeWithLeaderFailover(func() error {
		// Use binary protocol for performance
		req := &protocol.BinarySubscribeRequest{
			Topic:     topic,
			GroupID:   groupID,
			Partition: int32(partition),
			Mode:      mode,
		}
		payload := protocol.EncodeBinarySubscribeRequest(req)
		if err := protocol.WriteBinaryMessage(c.conn, protocol.OpSubscribe, payload); err != nil {
			return err
		}

		respMsg, err := protocol.ReadMessage(c.conn)
		if err != nil {
			return err
		}
		if respMsg.Header.Op == protocol.OpError {
			return fmt.Errorf("server error: %s", string(respMsg.Payload))
		}

		// Parse binary response
		resp, err := protocol.DecodeBinarySubscribeResponse(respMsg.Payload)
		if err != nil {
			return err
		}
		resultOffset = resp.Offset
		return nil
	})
	return resultOffset, err
}

// CommitOffset commits the consumer offset.
func (c *Client) CommitOffset(topic, groupID string, partition int, offset uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.executeWithLeaderFailover(func() error {
		// Use binary protocol for performance
		req := &protocol.BinaryCommitRequest{
			Topic:     topic,
			GroupID:   groupID,
			Partition: int32(partition),
			Offset:    offset,
		}
		payload := protocol.EncodeBinaryCommitRequest(req)
		if err := protocol.WriteBinaryMessage(c.conn, protocol.OpCommit, payload); err != nil {
			return err
		}

		respMsg, err := protocol.ReadMessage(c.conn)
		if err != nil {
			return err
		}
		if respMsg.Header.Op == protocol.OpError {
			return fmt.Errorf("server error: %s", string(respMsg.Payload))
		}
		return nil
	})
}

// FetchedMessage represents a message with its key, value, and offset.
type FetchedMessage struct {
	Key    []byte `json:"key,omitempty"`
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

// Fetch retrieves multiple messages from a topic (values only for backward compatibility).
func (c *Client) Fetch(topic string, partition int, offset uint64, maxMessages int) ([][]byte, uint64, error) {
	messages, nextOffset, err := c.FetchWithKeys(topic, partition, offset, maxMessages)
	if err != nil {
		return nil, 0, err
	}
	// Extract just the values for backward compatibility
	values := make([][]byte, len(messages))
	for i, msg := range messages {
		values[i] = msg.Value
	}
	return values, nextOffset, nil
}

// FetchWithKeys retrieves multiple messages from a topic with their keys.
func (c *Client) FetchWithKeys(topic string, partition int, offset uint64, maxMessages int) ([]FetchedMessage, uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Use binary protocol for performance
	req := &protocol.BinaryFetchRequest{
		Topic:       topic,
		Partition:   int32(partition),
		Offset:      offset,
		MaxMessages: int32(maxMessages),
	}
	payload := protocol.EncodeBinaryFetchRequest(req)
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpFetch, payload); err != nil {
		return nil, 0, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return nil, 0, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return nil, 0, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	// Parse binary response
	resp, err := protocol.DecodeBinaryFetchResponse(respMsg.Payload)
	if err != nil {
		return nil, 0, err
	}

	// Convert to client FetchedMessage type
	messages := make([]FetchedMessage, len(resp.Messages))
	for i, m := range resp.Messages {
		messages[i] = FetchedMessage{
			Key:    m.Key,
			Value:  m.Value,
			Offset: m.Offset,
		}
	}
	return messages, resp.NextOffset, nil
}

// ListTopics returns all available topics.
func (c *Client) ListTopics() ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Use binary protocol for performance
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpListTopics, nil); err != nil {
		return nil, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return nil, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return nil, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	// Parse binary response
	resp, err := protocol.DecodeBinaryListTopicsResponse(respMsg.Payload)
	if err != nil {
		return nil, err
	}
	return resp.Topics, nil
}

// DeleteTopic deletes a topic.
func (c *Client) DeleteTopic(topic string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.executeWithLeaderFailover(func() error {
		// Use binary protocol for performance
		req := &protocol.BinaryDeleteTopicRequest{Topic: topic}
		payload := protocol.EncodeBinaryDeleteTopicRequest(req)
		if err := protocol.WriteBinaryMessage(c.conn, protocol.OpDeleteTopic, payload); err != nil {
			return err
		}

		respMsg, err := protocol.ReadMessage(c.conn)
		if err != nil {
			return err
		}
		if respMsg.Header.Op == protocol.OpError {
			return fmt.Errorf("server error: %s", string(respMsg.Payload))
		}
		return nil
	})
}

// ============================================================================
// Consumer Group Management
// ============================================================================

// ConsumerGroupInfo represents information about a consumer group.
type ConsumerGroupInfo struct {
	GroupID     string                `json:"group_id"`
	State       string                `json:"state"`
	Members     []string              `json:"members"`
	Topics      []string              `json:"topics"`
	Coordinator string                `json:"coordinator,omitempty"`
	Offsets     []ConsumerGroupOffset `json:"offsets,omitempty"`
}

// ConsumerGroupOffset represents offset information for a consumer group.
type ConsumerGroupOffset struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
	Lag       uint64 `json:"lag,omitempty"`
}

// ConsumerLag represents lag information for a consumer group.
type ConsumerLag struct {
	Topic           string `json:"topic"`
	Partition       int    `json:"partition"`
	CurrentOffset   uint64 `json:"current_offset"`
	CommittedOffset uint64 `json:"committed_offset"`
	LatestOffset    uint64 `json:"latest_offset"`
	Lag             uint64 `json:"lag"`
}

// GetCommittedOffset retrieves the committed offset for a consumer group.
func (c *Client) GetCommittedOffset(topic, groupID string, partition int) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := &protocol.BinaryGetOffsetRequest{
		Topic:     topic,
		GroupID:   groupID,
		Partition: int32(partition),
	}
	payload := protocol.EncodeBinaryGetOffsetRequest(req)
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpGetOffset, payload); err != nil {
		return 0, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return 0, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return 0, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryGetOffsetResponse(respMsg.Payload)
	if err != nil {
		return 0, err
	}
	return resp.Offset, nil
}

// ResetOffset resets the consumer group offset to a specific position.
// mode can be "earliest", "latest", or "offset" (with specific offset value).
func (c *Client) ResetOffset(topic, groupID string, partition int, mode string, offset uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.executeWithLeaderFailover(func() error {
		req := &protocol.BinaryResetOffsetRequest{
			Topic:     topic,
			GroupID:   groupID,
			Partition: int32(partition),
			Mode:      mode,
			Offset:    offset,
		}
		payload := protocol.EncodeBinaryResetOffsetRequest(req)
		if err := protocol.WriteBinaryMessage(c.conn, protocol.OpResetOffset, payload); err != nil {
			return err
		}

		respMsg, err := protocol.ReadMessage(c.conn)
		if err != nil {
			return err
		}
		if respMsg.Header.Op == protocol.OpError {
			return fmt.Errorf("server error: %s", string(respMsg.Payload))
		}
		return nil
	})
}

// ResetOffsetToEarliest resets the consumer group offset to the earliest position.
func (c *Client) ResetOffsetToEarliest(topic, groupID string, partition int) error {
	return c.ResetOffset(topic, groupID, partition, "earliest", 0)
}

// ResetOffsetToLatest resets the consumer group offset to the latest position.
func (c *Client) ResetOffsetToLatest(topic, groupID string, partition int) error {
	return c.ResetOffset(topic, groupID, partition, "latest", 0)
}

// GetLag retrieves the consumer lag for a consumer group.
func (c *Client) GetLag(topic, groupID string, partition int) (*ConsumerLag, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := &protocol.BinaryGetLagRequest{
		Topic:     topic,
		GroupID:   groupID,
		Partition: int32(partition),
	}
	payload := protocol.EncodeBinaryGetLagRequest(req)
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpGetLag, payload); err != nil {
		return nil, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return nil, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return nil, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryGetLagResponse(respMsg.Payload)
	if err != nil {
		return nil, err
	}
	return &ConsumerLag{
		Topic:           topic,
		Partition:       partition,
		CurrentOffset:   resp.CurrentOffset,
		CommittedOffset: resp.CommittedOffset,
		LatestOffset:    resp.LatestOffset,
		Lag:             resp.Lag,
	}, nil
}

// ListConsumerGroups returns all consumer groups.
func (c *Client) ListConsumerGroups() ([]ConsumerGroupInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Binary protocol: send empty payload for list request
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpListGroups, nil); err != nil {
		return nil, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return nil, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return nil, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryListGroupsResponse(respMsg.Payload)
	if err != nil {
		return nil, err
	}

	// Convert binary response to ConsumerGroupInfo slice
	groups := make([]ConsumerGroupInfo, len(resp.Groups))
	for i, g := range resp.Groups {
		offsets := make([]ConsumerGroupOffset, len(g.Offsets))
		for j, o := range g.Offsets {
			offsets[j] = ConsumerGroupOffset{
				Partition: int(o.Partition),
				Offset:    o.Offset,
			}
		}
		groups[i] = ConsumerGroupInfo{
			GroupID: g.GroupID,
			Topics:  []string{g.Topic},
			Members: make([]string, g.Members),
			Offsets: offsets,
		}
	}
	return groups, nil
}

// DescribeConsumerGroup returns detailed information about a consumer group.
// Note: topic parameter is required for binary protocol.
func (c *Client) DescribeConsumerGroup(groupID string) (*ConsumerGroupInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// For backwards compatibility, we'll use an empty topic and let server handle it
	// In the future, callers should use DescribeConsumerGroupForTopic
	req := &protocol.BinaryDescribeGroupRequest{
		Topic:   "", // Empty topic - server may return error or aggregate
		GroupID: groupID,
	}
	payload := protocol.EncodeBinaryDescribeGroupRequest(req)
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpDescribeGroup, payload); err != nil {
		return nil, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return nil, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return nil, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryDescribeGroupResponse(respMsg.Payload)
	if err != nil {
		return nil, err
	}

	offsets := make([]ConsumerGroupOffset, len(resp.Offsets))
	for i, o := range resp.Offsets {
		offsets[i] = ConsumerGroupOffset{
			Partition: int(o.Partition),
			Offset:    o.Offset,
		}
	}

	return &ConsumerGroupInfo{
		GroupID: resp.GroupID,
		Topics:  []string{resp.Topic},
		Members: make([]string, resp.Members),
		Offsets: offsets,
	}, nil
}

// DeleteConsumerGroup deletes a consumer group.
// Note: topic parameter is required for binary protocol.
func (c *Client) DeleteConsumerGroup(groupID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.executeWithLeaderFailover(func() error {
		// For backwards compatibility, use empty topic
		req := &protocol.BinaryDeleteGroupRequest{
			Topic:   "", // Empty topic - server may handle differently
			GroupID: groupID,
		}
		payload := protocol.EncodeBinaryDeleteGroupRequest(req)
		if err := protocol.WriteBinaryMessage(c.conn, protocol.OpDeleteGroup, payload); err != nil {
			return err
		}

		respMsg, err := protocol.ReadMessage(c.conn)
		if err != nil {
			return err
		}
		if respMsg.Header.Op == protocol.OpError {
			return fmt.Errorf("server error: %s", string(respMsg.Payload))
		}
		return nil
	})
}

// ============================================================================
// Advanced Features
// ============================================================================

// Transaction represents a client-side transaction.
type Transaction struct {
	client *Client
	txnID  string
	active bool
}

// BeginTransaction starts a new transaction.
func (c *Client) BeginTransaction() (*Transaction, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpTxnBegin, nil); err != nil {
		return nil, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return nil, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return nil, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryTxnResponse(respMsg.Payload)
	if err != nil {
		return nil, err
	}

	return &Transaction{
		client: c,
		txnID:  resp.TxnID,
		active: true,
	}, nil
}

// ProduceInTransaction produces a message within a transaction.
func (t *Transaction) Produce(topic string, data []byte) (uint64, error) {
	if !t.active {
		return 0, fmt.Errorf("transaction is not active")
	}

	t.client.mu.Lock()
	defer t.client.mu.Unlock()

	req := &protocol.BinaryTxnProduceRequest{
		TxnID: t.txnID,
		Topic: topic,
		Data:  data,
	}
	payload := protocol.EncodeBinaryTxnProduceRequest(req)
	if err := protocol.WriteBinaryMessage(t.client.conn, protocol.OpTxnProduce, payload); err != nil {
		return 0, err
	}

	respMsg, err := protocol.ReadMessage(t.client.conn)
	if err != nil {
		return 0, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return 0, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryProduceResponse(respMsg.Payload)
	if err != nil {
		return 0, err
	}
	return resp.Offset, nil
}

// Commit commits the transaction.
func (t *Transaction) Commit() error {
	if !t.active {
		return fmt.Errorf("transaction is not active")
	}

	t.client.mu.Lock()
	defer t.client.mu.Unlock()

	req := &protocol.BinaryTxnRequest{TxnID: t.txnID}
	payload := protocol.EncodeBinaryTxnRequest(req)
	if err := protocol.WriteBinaryMessage(t.client.conn, protocol.OpTxnCommit, payload); err != nil {
		return err
	}

	respMsg, err := protocol.ReadMessage(t.client.conn)
	if err != nil {
		return err
	}
	if respMsg.Header.Op == protocol.OpError {
		return fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	t.active = false
	return nil
}

// Rollback aborts the transaction.
func (t *Transaction) Rollback() error {
	if !t.active {
		return fmt.Errorf("transaction is not active")
	}

	t.client.mu.Lock()
	defer t.client.mu.Unlock()

	req := &protocol.BinaryTxnRequest{TxnID: t.txnID}
	payload := protocol.EncodeBinaryTxnRequest(req)
	if err := protocol.WriteBinaryMessage(t.client.conn, protocol.OpTxnRollback, payload); err != nil {
		return err
	}

	respMsg, err := protocol.ReadMessage(t.client.conn)
	if err != nil {
		return err
	}
	if respMsg.Header.Op == protocol.OpError {
		return fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	t.active = false
	return nil
}

// ProduceDelayed produces a message with a delay.
func (c *Client) ProduceDelayed(topic string, data []byte, delayMs int64) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := &protocol.BinaryProduceDelayedRequest{
		Topic:   topic,
		Data:    data,
		DelayMs: delayMs,
	}
	payload := protocol.EncodeBinaryProduceDelayedRequest(req)
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpProduceDelayed, payload); err != nil {
		return 0, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return 0, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return 0, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryProduceResponse(respMsg.Payload)
	if err != nil {
		return 0, err
	}
	return resp.Offset, nil
}

// ProduceWithTTL produces a message with a time-to-live.
func (c *Client) ProduceWithTTL(topic string, data []byte, ttlMs int64) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := &protocol.BinaryProduceWithTTLRequest{
		Topic: topic,
		Data:  data,
		TTLMs: ttlMs,
	}
	payload := protocol.EncodeBinaryProduceWithTTLRequest(req)
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpProduceWithTTL, payload); err != nil {
		return 0, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return 0, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return 0, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryProduceResponse(respMsg.Payload)
	if err != nil {
		return 0, err
	}
	return resp.Offset, nil
}

// ProduceWithSchema produces a message with schema validation.
func (c *Client) ProduceWithSchema(topic string, data []byte, schemaName string) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := &protocol.BinaryProduceWithSchemaRequest{
		Topic:      topic,
		SchemaName: schemaName,
		Data:       data,
	}
	payload := protocol.EncodeBinaryProduceWithSchemaRequest(req)
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpProduceWithSchema, payload); err != nil {
		return 0, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return 0, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return 0, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryProduceResponse(respMsg.Payload)
	if err != nil {
		return 0, err
	}
	return resp.Offset, nil
}

// DLQMessage represents a message in the dead letter queue.
type DLQMessage struct {
	ID        string `json:"id"`
	Topic     string `json:"topic"`
	Data      []byte `json:"data"`
	Error     string `json:"error"`
	Retries   int    `json:"retries"`
	Timestamp int64  `json:"timestamp"`
}

// FetchDLQ retrieves messages from the dead letter queue.
func (c *Client) FetchDLQ(topic string, maxMessages int) ([]DLQMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := &protocol.BinaryDLQRequest{
		Topic:       topic,
		MaxMessages: int32(maxMessages),
	}
	payload := protocol.EncodeBinaryDLQRequest(req)
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpFetchDLQ, payload); err != nil {
		return nil, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return nil, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return nil, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryDLQResponse(respMsg.Payload)
	if err != nil {
		return nil, err
	}
	messages := make([]DLQMessage, len(resp.Messages))
	for i, m := range resp.Messages {
		messages[i] = DLQMessage{
			ID:      m.ID,
			Topic:   topic,
			Data:    m.Data,
			Error:   m.Error,
			Retries: int(m.Retries),
		}
	}
	return messages, nil
}

// ReplayDLQ replays a message from the dead letter queue.
func (c *Client) ReplayDLQ(topic, messageID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := &protocol.BinaryReplayDLQRequest{
		Topic:     topic,
		MessageID: messageID,
	}
	payload := protocol.EncodeBinaryReplayDLQRequest(req)
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpReplayDLQ, payload); err != nil {
		return err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return err
	}
	if respMsg.Header.Op == protocol.OpError {
		return fmt.Errorf("server error: %s", string(respMsg.Payload))
	}
	return nil
}

// PurgeDLQ purges messages from the dead letter queue.
func (c *Client) PurgeDLQ(topic string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := &protocol.BinaryPurgeDLQRequest{Topic: topic}
	payload := protocol.EncodeBinaryPurgeDLQRequest(req)
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpPurgeDLQ, payload); err != nil {
		return err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return err
	}
	if respMsg.Header.Op == protocol.OpError {
		return fmt.Errorf("server error: %s", string(respMsg.Payload))
	}
	return nil
}

// RegisterSchema registers a new schema.
func (c *Client) RegisterSchema(name string, schemaType string, schema []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.executeWithLeaderFailover(func() error {
		req := &protocol.BinaryRegisterSchemaRequest{
			Name:   name,
			Type:   schemaType,
			Schema: schema,
		}
		payload := protocol.EncodeBinaryRegisterSchemaRequest(req)
		if err := protocol.WriteBinaryMessage(c.conn, protocol.OpRegisterSchema, payload); err != nil {
			return err
		}

		respMsg, err := protocol.ReadMessage(c.conn)
		if err != nil {
			return err
		}
		if respMsg.Header.Op == protocol.OpError {
			return fmt.Errorf("server error: %s", string(respMsg.Payload))
		}
		return nil
	})
}

// SchemaInfo represents schema metadata.
type SchemaInfo struct {
	Name    string
	Type    string
	Version int
}

// ListSchemas returns all registered schemas.
func (c *Client) ListSchemas() ([]SchemaInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := &protocol.BinaryListSchemasRequest{Topic: ""}
	payload := protocol.EncodeBinaryListSchemasRequest(req)
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpListSchemas, payload); err != nil {
		return nil, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return nil, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return nil, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryListSchemasResponse(respMsg.Payload)
	if err != nil {
		return nil, err
	}
	schemas := make([]SchemaInfo, len(resp.Schemas))
	for i, s := range resp.Schemas {
		schemas[i] = SchemaInfo{
			Name:    s.Name,
			Type:    s.Type,
			Version: int(s.Version),
		}
	}
	return schemas, nil
}

// ValidateSchema validates a message against a schema.
func (c *Client) ValidateSchema(name string, message []byte) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := &protocol.BinaryValidateSchemaRequest{
		Name:    name,
		Message: message,
	}
	payload := protocol.EncodeBinaryValidateSchemaRequest(req)
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpValidateSchema, payload); err != nil {
		return false, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return false, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return false, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryValidateSchemaResponse(respMsg.Payload)
	if err != nil {
		return false, err
	}
	return resp.Valid, nil
}

// DeleteSchema deletes a schema by name.
func (c *Client) DeleteSchema(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.executeWithLeaderFailover(func() error {
		req := &protocol.BinaryDeleteSchemaRequest{
			Topic:   name,
			Version: 0, // 0 means delete all versions
		}
		payload := protocol.EncodeBinaryDeleteSchemaRequest(req)
		if err := protocol.WriteBinaryMessage(c.conn, protocol.OpDeleteSchema, payload); err != nil {
			return err
		}

		respMsg, err := protocol.ReadMessage(c.conn)
		if err != nil {
			return err
		}
		if respMsg.Header.Op == protocol.OpError {
			return fmt.Errorf("server error: %s", string(respMsg.Payload))
		}
		return nil
	})
}

// ClusterJoin requests the server to join a cluster via the specified peer.
func (c *Client) ClusterJoin(peerAddr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := &protocol.BinaryClusterJoinRequest{Peer: peerAddr}
	payload := protocol.EncodeBinaryClusterJoinRequest(req)
	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpClusterJoin, payload); err != nil {
		return err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return err
	}
	if respMsg.Header.Op == protocol.OpError {
		return fmt.Errorf("server error: %s", string(respMsg.Payload))
	}
	return nil
}

// ClusterLeave requests the server to leave the cluster gracefully.
func (c *Client) ClusterLeave() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpClusterLeave, nil); err != nil {
		return err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return err
	}
	if respMsg.Header.Op == protocol.OpError {
		return fmt.Errorf("server error: %s", string(respMsg.Payload))
	}
	return nil
}

// ============================================================================
// User Management Methods
// ============================================================================

// UserInfo represents user information returned by the server.
type UserInfo struct {
	Username    string
	Roles       []string
	Permissions []string
	Enabled     bool
	CreatedAt   int64
	UpdatedAt   int64
}

// ListUsers returns a list of all users.
func (c *Client) ListUsers() ([]UserInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpUserList, nil); err != nil {
		return nil, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return nil, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return nil, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryUserListResponse(respMsg.Payload)
	if err != nil {
		return nil, err
	}
	users := make([]UserInfo, len(resp.Users))
	for i, u := range resp.Users {
		users[i] = UserInfo{
			Username:    u.Username,
			Roles:       u.Roles,
			Permissions: u.Permissions,
			Enabled:     u.Enabled,
			CreatedAt:   u.CreatedAt,
			UpdatedAt:   u.UpdatedAt,
		}
	}
	return users, nil
}

// CreateUser creates a new user with the given password and roles.
func (c *Client) CreateUser(username, password string, roles []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.executeWithLeaderFailover(func() error {
		req := &protocol.BinaryUserRequest{
			Username: username,
			Password: password,
			Roles:    roles,
			Enabled:  true,
		}
		payload := protocol.EncodeBinaryUserRequest(req)

		if err := protocol.WriteBinaryMessage(c.conn, protocol.OpUserCreate, payload); err != nil {
			return err
		}

		respMsg, err := protocol.ReadMessage(c.conn)
		if err != nil {
			return err
		}
		if respMsg.Header.Op == protocol.OpError {
			return fmt.Errorf("server error: %s", string(respMsg.Payload))
		}
		return nil
	})
}

// DeleteUser deletes a user.
func (c *Client) DeleteUser(username string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.executeWithLeaderFailover(func() error {
		payload := protocol.EncodeBinaryStringRequest(username)

		if err := protocol.WriteBinaryMessage(c.conn, protocol.OpUserDelete, payload); err != nil {
			return err
		}

		respMsg, err := protocol.ReadMessage(c.conn)
		if err != nil {
			return err
		}
		if respMsg.Header.Op == protocol.OpError {
			return fmt.Errorf("server error: %s", string(respMsg.Payload))
		}
		return nil
	})
}

// UpdateUser updates a user's roles and/or enabled status.
func (c *Client) UpdateUser(username string, roles []string, enabled *bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.executeWithLeaderFailover(func() error {
		enb := true
		if enabled != nil {
			enb = *enabled
		}
		if roles == nil {
			roles = []string{}
		}
		req := &protocol.BinaryUserRequest{
			Username: username,
			Roles:    roles,
			Enabled:  enb,
		}
		payload := protocol.EncodeBinaryUserRequest(req)

		if err := protocol.WriteBinaryMessage(c.conn, protocol.OpUserUpdate, payload); err != nil {
			return err
		}

		respMsg, err := protocol.ReadMessage(c.conn)
		if err != nil {
			return err
		}
		if respMsg.Header.Op == protocol.OpError {
			return fmt.Errorf("server error: %s", string(respMsg.Payload))
		}
		return nil
	})
}

// GetUser returns details for a specific user.
func (c *Client) GetUser(username string) (*UserInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	payload := protocol.EncodeBinaryStringRequest(username)

	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpUserGet, payload); err != nil {
		return nil, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return nil, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return nil, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryUserInfo(respMsg.Payload)
	if err != nil {
		return nil, err
	}
	return &UserInfo{
		Username:    resp.Username,
		Roles:       resp.Roles,
		Permissions: resp.Permissions,
		Enabled:     resp.Enabled,
		CreatedAt:   resp.CreatedAt,
		UpdatedAt:   resp.UpdatedAt,
	}, nil
}

// ChangePassword changes a user's password.
func (c *Client) ChangePassword(username, oldPassword, newPassword string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := &protocol.BinaryUserRequest{
		Username:    username,
		OldPassword: oldPassword,
		Password:    newPassword,
	}
	payload := protocol.EncodeBinaryUserRequest(req)

	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpPasswordChange, payload); err != nil {
		return err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return err
	}
	if respMsg.Header.Op == protocol.OpError {
		return fmt.Errorf("server error: %s", string(respMsg.Payload))
	}
	return nil
}

// ============================================================================
// ACL Management Methods
// ============================================================================

// ACLInfo represents ACL information for a topic.
type ACLInfo struct {
	Topic         string
	Exists        bool
	Public        bool
	AllowedUsers  []string
	AllowedRoles  []string
	DefaultPublic bool
}

// ListACLs returns all topic ACLs and the default public setting.
func (c *Client) ListACLs() ([]ACLInfo, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpACLList, nil); err != nil {
		return nil, false, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return nil, false, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return nil, false, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryACLListResponse(respMsg.Payload)
	if err != nil {
		return nil, false, err
	}
	acls := make([]ACLInfo, len(resp.ACLs))
	for i, a := range resp.ACLs {
		acls[i] = ACLInfo{
			Topic:        a.Topic,
			Exists:       a.Exists,
			Public:       a.Public,
			AllowedUsers: a.AllowedUsers,
			AllowedRoles: a.AllowedRoles,
		}
	}
	return acls, resp.DefaultPublic, nil
}

// GetACL returns the ACL for a specific topic.
func (c *Client) GetACL(topic string) (*ACLInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	payload := protocol.EncodeBinaryStringRequest(topic)

	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpACLGet, payload); err != nil {
		return nil, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return nil, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return nil, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryACLInfo(respMsg.Payload)
	if err != nil {
		return nil, err
	}
	return &ACLInfo{
		Topic:         resp.Topic,
		Exists:        resp.Exists,
		Public:        resp.Public,
		AllowedUsers:  resp.AllowedUsers,
		AllowedRoles:  resp.AllowedRoles,
		DefaultPublic: resp.DefaultPublic,
	}, nil
}

// SetACL sets the ACL for a topic.
func (c *Client) SetACL(topic string, public bool, allowedUsers, allowedRoles []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.executeWithLeaderFailover(func() error {
		if allowedUsers == nil {
			allowedUsers = []string{}
		}
		if allowedRoles == nil {
			allowedRoles = []string{}
		}
		req := &protocol.BinaryACLRequest{
			Topic:        topic,
			Public:       public,
			AllowedUsers: allowedUsers,
			AllowedRoles: allowedRoles,
		}
		payload := protocol.EncodeBinaryACLRequest(req)

		if err := protocol.WriteBinaryMessage(c.conn, protocol.OpACLSet, payload); err != nil {
			return err
		}

		respMsg, err := protocol.ReadMessage(c.conn)
		if err != nil {
			return err
		}
		if respMsg.Header.Op == protocol.OpError {
			return fmt.Errorf("server error: %s", string(respMsg.Payload))
		}
		return nil
	})
}

// DeleteACL deletes the ACL for a topic.
func (c *Client) DeleteACL(topic string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.executeWithLeaderFailover(func() error {
		payload := protocol.EncodeBinaryStringRequest(topic)

		if err := protocol.WriteBinaryMessage(c.conn, protocol.OpACLDelete, payload); err != nil {
			return err
		}

		respMsg, err := protocol.ReadMessage(c.conn)
		if err != nil {
			return err
		}
		if respMsg.Header.Op == protocol.OpError {
			return fmt.Errorf("server error: %s", string(respMsg.Payload))
		}
		return nil
	})
}

// RoleInfo contains information about a role.
type RoleInfo struct {
	Name        string
	Permissions []string
	Description string
}

// ListRoles returns all available roles with their permissions and descriptions.
func (c *Client) ListRoles() ([]RoleInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := protocol.WriteBinaryMessage(c.conn, protocol.OpRoleList, nil); err != nil {
		return nil, err
	}

	respMsg, err := protocol.ReadMessage(c.conn)
	if err != nil {
		return nil, err
	}
	if respMsg.Header.Op == protocol.OpError {
		return nil, fmt.Errorf("server error: %s", string(respMsg.Payload))
	}

	resp, err := protocol.DecodeBinaryRoleListResponse(respMsg.Payload)
	if err != nil {
		return nil, err
	}
	roles := make([]RoleInfo, len(resp.Roles))
	for i, r := range resp.Roles {
		roles[i] = RoleInfo{
			Name:        r.Name,
			Permissions: r.Permissions,
			Description: r.Description,
		}
	}
	return roles, nil
}
