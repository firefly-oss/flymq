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
Cluster Transport Layer for FlyMQ.

OVERVIEW:
=========
Provides reliable RPC communication between cluster nodes.
Used for Raft consensus, replication, and membership protocol.

RPC TYPES:
==========
- VoteRequest/Response: Raft leader election
- AppendEntries/Response: Raft log replication
- InstallSnapshot/Response: Raft state transfer
- FetchRequest/Response: Data replication
- GossipMessage: Membership protocol

CONNECTION POOLING:
===================
Maintains persistent connections to peer nodes with:
- Automatic reconnection on failure
- Connection health monitoring
- Request pipelining for throughput

WIRE FORMAT:
============
[1 byte: RPC Type][4 bytes: Length][N bytes: JSON Payload]
*/
package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"flymq/internal/logging"
)

// RPCType identifies the type of RPC message.
type RPCType byte

const (
	RPCVoteRequest RPCType = iota + 1
	RPCVoteResponse
	RPCAppendEntries
	RPCAppendEntriesResponse
	RPCInstallSnapshot
	RPCInstallSnapshotResponse
)

// Buffer pools for reducing allocations
var (
	// Pool for small read buffers (header, length)
	smallBufPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 5) // 1 byte type + 4 bytes length
		},
	}
	// Pool for medium-sized payloads (typical RPC)
	mediumBufPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 4096)
		},
	}
	// Pool for bytes.Buffer used in JSON encoding
	bufferPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
)

// TCPTransport implements the Transport interface using TCP.
type TCPTransport struct {
	bindAddr string
	listener net.Listener
	logger   *logging.Logger

	mu          sync.RWMutex
	connections map[string]*connectionPool
	timeout     time.Duration

	// Handlers
	voteHandler   func(*VoteRequest) *VoteResponse
	appendHandler func(*AppendEntriesRequest) *AppendEntriesResponse

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// connectionPool manages a pool of connections to a peer
type connectionPool struct {
	peer     string
	mu       sync.Mutex
	conns    chan net.Conn
	maxConns int
	timeout  time.Duration
	logger   *logging.Logger
}

// newConnectionPool creates a new connection pool for a peer
func newConnectionPool(peer string, maxConns int, timeout time.Duration, logger *logging.Logger) *connectionPool {
	return &connectionPool{
		peer:     peer,
		conns:    make(chan net.Conn, maxConns),
		maxConns: maxConns,
		timeout:  timeout,
		logger:   logger,
	}
}

// get returns a connection from the pool or creates a new one
func (p *connectionPool) get() (net.Conn, error) {
	// Try to get an existing connection
	select {
	case conn := <-p.conns:
		// Test if connection is still alive
		conn.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
		one := make([]byte, 1)
		if _, err := conn.Read(one); err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Timeout is expected, connection is alive
				conn.SetReadDeadline(time.Time{})
				return conn, nil
			}
			// Connection is dead, close and create new
			conn.Close()
		} else {
			// Got data unexpectedly, connection might be in bad state
			conn.Close()
		}
	default:
		// No connection available
	}

	// Create new connection
	conn, err := net.DialTimeout("tcp", p.peer, p.timeout)
	if err != nil {
		return nil, err
	}

	// Apply TCP optimizations for cluster communication
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
		tcpConn.SetNoDelay(true)
		// Increase buffer sizes for higher throughput (4MB each)
		tcpConn.SetReadBuffer(4 * 1024 * 1024)
		tcpConn.SetWriteBuffer(4 * 1024 * 1024)
	}

	return conn, nil
}

// put returns a connection to the pool
func (p *connectionPool) put(conn net.Conn) {
	select {
	case p.conns <- conn:
		// Connection returned to pool
	default:
		// Pool is full, close connection
		conn.Close()
	}
}

// close closes all connections in the pool
func (p *connectionPool) close() {
	close(p.conns)
	for conn := range p.conns {
		conn.Close()
	}
}

// NewTCPTransport creates a new TCP transport.
func NewTCPTransport(bindAddr string, timeout time.Duration) *TCPTransport {
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	return &TCPTransport{
		bindAddr:    bindAddr,
		connections: make(map[string]*connectionPool),
		timeout:     timeout,
		stopCh:      make(chan struct{}),
		logger:      logging.NewLogger("transport"),
	}
}

// SetVoteHandler sets the handler for vote requests.
func (t *TCPTransport) SetVoteHandler(handler func(*VoteRequest) *VoteResponse) {
	t.voteHandler = handler
}

// SetAppendHandler sets the handler for append entries requests.
func (t *TCPTransport) SetAppendHandler(handler func(*AppendEntriesRequest) *AppendEntriesResponse) {
	t.appendHandler = handler
}

// Start begins listening for incoming connections.
func (t *TCPTransport) Start() error {
	ln, err := net.Listen("tcp", t.bindAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", t.bindAddr, err)
	}
	t.listener = ln
	t.logger.Info("Transport listening", "addr", t.bindAddr)

	t.wg.Add(1)
	go t.acceptLoop()
	return nil
}

// Stop closes the transport.
func (t *TCPTransport) Stop() error {
	close(t.stopCh)
	if t.listener != nil {
		t.listener.Close()
	}

	t.mu.Lock()
	for _, pool := range t.connections {
		pool.close()
	}
	t.connections = make(map[string]*connectionPool)
	t.mu.Unlock()

	t.wg.Wait()
	return nil
}

func (t *TCPTransport) acceptLoop() {
	defer t.wg.Done()

	for {
		select {
		case <-t.stopCh:
			return
		default:
		}

		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.stopCh:
				return
			default:
				t.logger.Error("Accept error", "error", err)
				continue
			}
		}

		t.wg.Add(1)
		go t.handleConn(conn)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn) {
	defer t.wg.Done()
	defer conn.Close()

	// Apply TCP optimizations for incoming cluster connections
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
		tcpConn.SetReadBuffer(4 * 1024 * 1024)
		tcpConn.SetWriteBuffer(4 * 1024 * 1024)
	}

	for {
		select {
		case <-t.stopCh:
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(t.timeout))

		// Read RPC type
		typeBuf := make([]byte, 1)
		if _, err := io.ReadFull(conn, typeBuf); err != nil {
			return
		}

		// Read length
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return
		}
		length := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])

		// Read payload
		payload := make([]byte, length)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return
		}

		// Handle based on type
		switch RPCType(typeBuf[0]) {
		case RPCVoteRequest:
			var req VoteRequest
			if err := json.Unmarshal(payload, &req); err != nil {
				t.logger.Error("Failed to unmarshal VoteRequest", "error", err, "payload_len", len(payload))
				continue
			}
			t.logger.Info("Calling voteHandler", "req_term", req.Term)
			if t.voteHandler != nil {
				resp := t.voteHandler(&req)
				t.logger.Info("voteHandler returned", "resp_term", resp.Term, "granted", resp.VoteGranted)
				t.sendResponse(conn, RPCVoteResponse, resp)
			}
		case RPCAppendEntries:
			var req AppendEntriesRequest
			if err := json.Unmarshal(payload, &req); err != nil {
				continue
			}
			if t.appendHandler != nil {
				resp := t.appendHandler(&req)
				t.sendResponse(conn, RPCAppendEntriesResponse, resp)
			}
		}
	}
}

func (t *TCPTransport) sendResponse(conn net.Conn, rpcType RPCType, resp interface{}) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}

	buf := make([]byte, 5+len(data))
	buf[0] = byte(rpcType)
	buf[1] = byte(len(data) >> 24)
	buf[2] = byte(len(data) >> 16)
	buf[3] = byte(len(data) >> 8)
	buf[4] = byte(len(data))
	copy(buf[5:], data)

	_, err = conn.Write(buf)
	if err != nil {
		t.logger.Error("Failed to write response", "error", err)
	} else {
		t.logger.Info("Sent response", "type", rpcType)
	}
	return err
}

// SendVoteRequest sends a vote request to a peer.
// Uses connection pooling for better performance.
func (t *TCPTransport) SendVoteRequest(peer string, req *VoteRequest) (*VoteResponse, error) {
	// Normalize peer address
	peer = t.normalizePeerAddress(peer)

	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	pool := t.getOrCreatePool(peer)
	conn, err := pool.get()
	if err != nil {
		return nil, err
	}

	if err := t.sendRPC(conn, RPCVoteRequest, data); err != nil {
		conn.Close()
		return nil, err
	}

	resp := &VoteResponse{}
	if err := t.readResponse(conn, resp); err != nil {
		conn.Close()
		return nil, err
	}

	pool.put(conn)
	return resp, nil
}

// SendAppendEntries sends an append entries request to a peer.
// Uses connection pooling for better performance.
func (t *TCPTransport) SendAppendEntries(peer string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	// Normalize peer address
	peer = t.normalizePeerAddress(peer)

	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	pool := t.getOrCreatePool(peer)
	conn, err := pool.get()
	if err != nil {
		return nil, err
	}

	if err := t.sendRPC(conn, RPCAppendEntries, data); err != nil {
		conn.Close()
		return nil, err
	}

	resp := &AppendEntriesResponse{}
	if err := t.readResponse(conn, resp); err != nil {
		conn.Close()
		return nil, err
	}

	pool.put(conn)
	return resp, nil
}

// getOrCreatePool returns the connection pool for a peer, creating one if needed
func (t *TCPTransport) getOrCreatePool(peer string) *connectionPool {
	t.mu.RLock()
	pool, ok := t.connections[peer]
	t.mu.RUnlock()

	if ok {
		return pool
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Double-check
	if pool, ok := t.connections[peer]; ok {
		return pool
	}

	// Create new pool with 10 connections max per peer
	pool = newConnectionPool(peer, 10, t.timeout, t.logger)
	t.connections[peer] = pool
	return pool
}

// normalizePeerAddress converts localhost to 127.0.0.1 for consistent addressing
func (t *TCPTransport) normalizePeerAddress(peer string) string {
	// Replace localhost with 127.0.0.1 for consistency
	if len(peer) > 9 && peer[:9] == "localhost" {
		return "127.0.0.1" + peer[9:]
	}
	return peer
}

func (t *TCPTransport) sendRPC(conn net.Conn, rpcType RPCType, data []byte) error {
	buf := make([]byte, 5+len(data))
	buf[0] = byte(rpcType)
	buf[1] = byte(len(data) >> 24)
	buf[2] = byte(len(data) >> 16)
	buf[3] = byte(len(data) >> 8)
	buf[4] = byte(len(data))
	copy(buf[5:], data)

	conn.SetWriteDeadline(time.Now().Add(t.timeout))
	_, err := conn.Write(buf)
	return err
}

func (t *TCPTransport) readResponse(conn net.Conn, resp interface{}) error {
	conn.SetReadDeadline(time.Now().Add(t.timeout))

	// Read type
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, typeBuf); err != nil {
		return err
	}

	// Read length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return err
	}
	length := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])

	// Read payload
	payload := make([]byte, length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return err
	}

	return json.NewDecoder(bytes.NewReader(payload)).Decode(resp)
}
