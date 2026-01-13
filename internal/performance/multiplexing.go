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
Connection Multiplexing for FlyMQ.

OVERVIEW:
=========
Multiplexes multiple logical streams over a single TCP connection.
Reduces connection overhead and improves resource utilization.

BENEFITS:
=========
- Fewer TCP connections (reduced memory, file descriptors)
- Head-of-line blocking mitigation
- Flow control per stream
- Efficient use of network bandwidth

FRAME FORMAT:
=============

	+--------+--------+--------+--------+
	| Stream ID (4 bytes)               |
	+--------+--------+--------+--------+
	| Type   | Flags  | Length (2 bytes)|
	+--------+--------+--------+--------+
	| Payload (variable length)         |
	+--------+--------+--------+--------+

FRAME TYPES:
============
- DATA: Application data
- WINDOW_UPDATE: Flow control credit
- RESET: Stream termination
- PING: Keep-alive
*/
package performance

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"flymq/internal/logging"
)

// StreamID represents a unique stream identifier within a multiplexed connection.
type StreamID uint32

// FrameType represents the type of frame in the multiplexed protocol.
type FrameType byte

const (
	FrameData FrameType = iota
	FrameWindowUpdate
	FrameReset
	FramePing
	FramePong
	FrameGoAway
)

// Frame represents a multiplexed frame.
type Frame struct {
	Type     FrameType
	StreamID StreamID
	Flags    byte
	Length   uint32
	Payload  []byte
}

// FrameHeader size in bytes
const FrameHeaderSize = 10 // 1 (type) + 4 (stream ID) + 1 (flags) + 4 (length)

// MultiplexedConn represents a multiplexed connection.
type MultiplexedConn struct {
	conn       net.Conn
	streams    map[StreamID]*Stream
	mu         sync.RWMutex
	nextStream uint32
	logger     *logging.Logger

	// Flow control
	windowSize   uint32
	maxFrameSize uint32

	// Channels
	acceptCh chan *Stream
	closeCh  chan struct{}
	wg       sync.WaitGroup
}

// Stream represents a single stream within a multiplexed connection.
type Stream struct {
	id         StreamID
	conn       *MultiplexedConn
	readBuf    []byte
	readMu     sync.Mutex
	readCond   *sync.Cond
	writeMu    sync.Mutex
	closed     int32
	windowSize uint32
}

// MultiplexConfig holds configuration for multiplexed connections.
type MultiplexConfig struct {
	MaxStreams   int
	WindowSize   uint32
	MaxFrameSize uint32
	KeepAlive    time.Duration
}

// DefaultMultiplexConfig returns default multiplexing configuration.
func DefaultMultiplexConfig() MultiplexConfig {
	return MultiplexConfig{
		MaxStreams:   1000,
		WindowSize:   65535,
		MaxFrameSize: 16384,
		KeepAlive:    30 * time.Second,
	}
}

// NewMultiplexedConn creates a new multiplexed connection.
func NewMultiplexedConn(conn net.Conn, config MultiplexConfig) *MultiplexedConn {
	mc := &MultiplexedConn{
		conn:         conn,
		streams:      make(map[StreamID]*Stream),
		nextStream:   1,
		windowSize:   config.WindowSize,
		maxFrameSize: config.MaxFrameSize,
		acceptCh:     make(chan *Stream, config.MaxStreams),
		closeCh:      make(chan struct{}),
		logger:       logging.NewLogger("multiplex"),
	}

	mc.wg.Add(1)
	go mc.readLoop()

	return mc
}

// OpenStream opens a new stream on the connection.
func (mc *MultiplexedConn) OpenStream() (*Stream, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	select {
	case <-mc.closeCh:
		return nil, fmt.Errorf("connection closed")
	default:
	}

	streamID := StreamID(atomic.AddUint32(&mc.nextStream, 2) - 2)
	stream := &Stream{
		id:         streamID,
		conn:       mc,
		windowSize: mc.windowSize,
	}
	stream.readCond = sync.NewCond(&stream.readMu)

	mc.streams[streamID] = stream
	return stream, nil
}

// AcceptStream accepts an incoming stream.
func (mc *MultiplexedConn) AcceptStream() (*Stream, error) {
	select {
	case stream := <-mc.acceptCh:
		return stream, nil
	case <-mc.closeCh:
		return nil, fmt.Errorf("connection closed")
	}
}

// Close closes the multiplexed connection.
func (mc *MultiplexedConn) Close() error {
	mc.mu.Lock()
	select {
	case <-mc.closeCh:
		mc.mu.Unlock()
		return nil
	default:
		close(mc.closeCh)
	}
	mc.mu.Unlock()

	mc.wg.Wait()
	return mc.conn.Close()
}

func (mc *MultiplexedConn) readLoop() {
	defer mc.wg.Done()

	for {
		select {
		case <-mc.closeCh:
			return
		default:
		}

		frame, err := mc.readFrame()
		if err != nil {
			if err != io.EOF {
				mc.logger.Error("Read frame error", "error", err)
			}
			return
		}

		mc.handleFrame(frame)
	}
}

func (mc *MultiplexedConn) readFrame() (*Frame, error) {
	header := make([]byte, FrameHeaderSize)
	if _, err := io.ReadFull(mc.conn, header); err != nil {
		return nil, err
	}

	frame := &Frame{
		Type:     FrameType(header[0]),
		StreamID: StreamID(binary.BigEndian.Uint32(header[1:5])),
		Flags:    header[5],
		Length:   binary.BigEndian.Uint32(header[6:10]),
	}

	if frame.Length > 0 {
		frame.Payload = make([]byte, frame.Length)
		if _, err := io.ReadFull(mc.conn, frame.Payload); err != nil {
			return nil, err
		}
	}

	return frame, nil
}

func (mc *MultiplexedConn) handleFrame(frame *Frame) {
	switch frame.Type {
	case FrameData:
		mc.handleDataFrame(frame)
	case FrameWindowUpdate:
		mc.handleWindowUpdate(frame)
	case FrameReset:
		mc.handleReset(frame)
	case FramePing:
		mc.handlePing(frame)
	case FrameGoAway:
		mc.handleGoAway(frame)
	}
}

func (mc *MultiplexedConn) handleDataFrame(frame *Frame) {
	mc.mu.RLock()
	stream, ok := mc.streams[frame.StreamID]
	mc.mu.RUnlock()

	if !ok {
		// New stream from remote
		stream = &Stream{
			id:         frame.StreamID,
			conn:       mc,
			windowSize: mc.windowSize,
		}
		stream.readCond = sync.NewCond(&stream.readMu)

		mc.mu.Lock()
		mc.streams[frame.StreamID] = stream
		mc.mu.Unlock()

		select {
		case mc.acceptCh <- stream:
		default:
			mc.logger.Warn("Accept channel full, dropping stream")
		}
	}

	stream.readMu.Lock()
	stream.readBuf = append(stream.readBuf, frame.Payload...)
	stream.readCond.Signal()
	stream.readMu.Unlock()
}

func (mc *MultiplexedConn) handleWindowUpdate(frame *Frame) {
	mc.mu.RLock()
	stream, ok := mc.streams[frame.StreamID]
	mc.mu.RUnlock()

	if ok && len(frame.Payload) >= 4 {
		delta := binary.BigEndian.Uint32(frame.Payload)
		atomic.AddUint32(&stream.windowSize, delta)
	}
}

func (mc *MultiplexedConn) handleReset(frame *Frame) {
	mc.mu.Lock()
	stream, ok := mc.streams[frame.StreamID]
	if ok {
		delete(mc.streams, frame.StreamID)
	}
	mc.mu.Unlock()

	if ok {
		atomic.StoreInt32(&stream.closed, 1)
		stream.readCond.Broadcast()
	}
}

func (mc *MultiplexedConn) handlePing(frame *Frame) {
	// Send pong
	pong := &Frame{
		Type:     FramePong,
		StreamID: 0,
		Payload:  frame.Payload,
	}
	mc.writeFrame(pong)
}

func (mc *MultiplexedConn) handleGoAway(frame *Frame) {
	mc.Close()
}

func (mc *MultiplexedConn) writeFrame(frame *Frame) error {
	header := make([]byte, FrameHeaderSize)
	header[0] = byte(frame.Type)
	binary.BigEndian.PutUint32(header[1:5], uint32(frame.StreamID))
	header[5] = frame.Flags
	binary.BigEndian.PutUint32(header[6:10], uint32(len(frame.Payload)))

	mc.mu.Lock()
	defer mc.mu.Unlock()

	if _, err := mc.conn.Write(header); err != nil {
		return err
	}
	if len(frame.Payload) > 0 {
		if _, err := mc.conn.Write(frame.Payload); err != nil {
			return err
		}
	}
	return nil
}

// Stream methods

// Read reads data from the stream.
func (s *Stream) Read(p []byte) (int, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()

	for len(s.readBuf) == 0 {
		if atomic.LoadInt32(&s.closed) == 1 {
			return 0, io.EOF
		}
		s.readCond.Wait()
	}

	n := copy(p, s.readBuf)
	s.readBuf = s.readBuf[n:]

	// Send window update
	if n > 0 {
		s.sendWindowUpdate(uint32(n))
	}

	return n, nil
}

// Write writes data to the stream.
func (s *Stream) Write(p []byte) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if atomic.LoadInt32(&s.closed) == 1 {
		return 0, fmt.Errorf("stream closed")
	}

	frame := &Frame{
		Type:     FrameData,
		StreamID: s.id,
		Payload:  p,
	}

	if err := s.conn.writeFrame(frame); err != nil {
		return 0, err
	}

	return len(p), nil
}

// Close closes the stream.
func (s *Stream) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}

	frame := &Frame{
		Type:     FrameReset,
		StreamID: s.id,
	}
	s.conn.writeFrame(frame)

	s.conn.mu.Lock()
	delete(s.conn.streams, s.id)
	s.conn.mu.Unlock()

	s.readCond.Broadcast()
	return nil
}

func (s *Stream) sendWindowUpdate(delta uint32) {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, delta)

	frame := &Frame{
		Type:     FrameWindowUpdate,
		StreamID: s.id,
		Payload:  payload,
	}
	s.conn.writeFrame(frame)
}

// ConnectionPool manages a pool of multiplexed connections.
type ConnectionPool struct {
	mu     sync.RWMutex
	conns  map[string]*MultiplexedConn
	config MultiplexConfig
	dialer func(addr string) (net.Conn, error)
	logger *logging.Logger
}

// NewConnectionPool creates a new connection pool.
func NewConnectionPool(config MultiplexConfig) *ConnectionPool {
	return &ConnectionPool{
		conns:  make(map[string]*MultiplexedConn),
		config: config,
		dialer: func(addr string) (net.Conn, error) {
			return net.DialTimeout("tcp", addr, 10*time.Second)
		},
		logger: logging.NewLogger("connpool"),
	}
}

// GetConnection gets or creates a multiplexed connection to the address.
func (cp *ConnectionPool) GetConnection(addr string) (*MultiplexedConn, error) {
	cp.mu.RLock()
	conn, ok := cp.conns[addr]
	cp.mu.RUnlock()

	if ok {
		return conn, nil
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()

	// Double-check
	if conn, ok := cp.conns[addr]; ok {
		return conn, nil
	}

	rawConn, err := cp.dialer(addr)
	if err != nil {
		return nil, err
	}

	conn = NewMultiplexedConn(rawConn, cp.config)
	cp.conns[addr] = conn
	return conn, nil
}

// Close closes all connections in the pool.
func (cp *ConnectionPool) Close() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	for addr, conn := range cp.conns {
		if err := conn.Close(); err != nil {
			cp.logger.Error("Failed to close connection", "addr", addr, "error", err)
		}
	}
	cp.conns = make(map[string]*MultiplexedConn)
	return nil
}
