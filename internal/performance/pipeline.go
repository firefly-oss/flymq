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
High-Performance Message Pipeline for FlyMQ.

This package provides an integrated high-performance message pipeline that:
- Uses ring buffers for lock-free message queuing
- Applies adaptive batching based on throughput
- Integrates compression for large messages
- Uses zero-copy I/O for network transfers
- Pools buffers to reduce GC pressure
*/
package performance

import (
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// PipelineConfig holds configuration for the message pipeline.
type PipelineConfig struct {
	// Compression settings
	CompressionType    CompressionType
	CompressionLevel   int
	CompressionMinSize int

	// Buffer settings
	RingBufferSize  int
	WriteBufferSize int

	// Batching settings
	MinBatchSize    int
	MaxBatchSize    int
	TargetLatencyMs float64

	// Zero-copy settings
	ZeroCopyEnabled       bool
	LargeMessageThreshold int
}

// DefaultPipelineConfig returns sensible defaults for the pipeline.
func DefaultPipelineConfig() PipelineConfig {
	return PipelineConfig{
		CompressionType:       CompressionLZ4,
		CompressionLevel:      3,
		CompressionMinSize:    1024,
		RingBufferSize:        65536,
		WriteBufferSize:       16 * 1024 * 1024,
		MinBatchSize:          10,
		MaxBatchSize:          1000,
		TargetLatencyMs:       1.0,
		ZeroCopyEnabled:       true,
		LargeMessageThreshold: 64 * 1024,
	}
}

// MessagePipeline provides high-performance message processing.
type MessagePipeline struct {
	config     PipelineConfig
	compressor Compressor
	batcher    *AdaptiveBatcher
	bufferPool *FastMessagePool
	ringBuffer *RingBuffer

	// Metrics
	messagesProcessed uint64
	bytesProcessed    uint64
	compressionSaved  uint64

	mu     sync.RWMutex
	closed int32
}

// NewMessagePipeline creates a new high-performance message pipeline.
func NewMessagePipeline(config PipelineConfig) *MessagePipeline {
	var compressor Compressor
	switch config.CompressionType {
	case CompressionLZ4:
		compressor = NewLZ4CompressorLevel(config.CompressionLevel)
	case CompressionSnappy:
		compressor = NewSnappyCompressor()
	case CompressionZstd:
		compressor = NewZstdCompressor(config.CompressionLevel)
	case CompressionGzip:
		compressor = NewGzipCompressor(config.CompressionLevel)
	default:
		compressor = &NoopCompressor{}
	}

	return &MessagePipeline{
		config:     config,
		compressor: compressor,
		batcher:    NewAdaptiveBatcher(config.MinBatchSize, config.MaxBatchSize, config.TargetLatencyMs),
		bufferPool: NewFastMessagePool(),
		ringBuffer: NewRingBuffer(config.RingBufferSize),
	}
}

// ProcessMessage processes a message through the pipeline.
// Returns the processed message (possibly compressed) and whether compression was applied.
func (p *MessagePipeline) ProcessMessage(data []byte) ([]byte, bool) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return data, false
	}

	start := time.Now()
	defer func() {
		p.batcher.RecordLatency(time.Since(start).Nanoseconds())
	}()

	atomic.AddUint64(&p.messagesProcessed, 1)
	atomic.AddUint64(&p.bytesProcessed, uint64(len(data)))

	// Only compress if above minimum size
	if len(data) < p.config.CompressionMinSize {
		return data, false
	}

	compressed, err := p.compressor.Compress(data)
	if err != nil || len(compressed) >= len(data) {
		return data, false
	}

	saved := uint64(len(data) - len(compressed))
	atomic.AddUint64(&p.compressionSaved, saved)

	return compressed, true
}

// DecompressMessage decompresses a message if needed.
func (p *MessagePipeline) DecompressMessage(data []byte, compressed bool) ([]byte, error) {
	if !compressed {
		return data, nil
	}
	return p.compressor.Decompress(data)
}

// GetBuffer gets a buffer from the pool.
func (p *MessagePipeline) GetBuffer(size int) []byte {
	return p.bufferPool.Get(size)
}

// PutBuffer returns a buffer to the pool.
func (p *MessagePipeline) PutBuffer(buf []byte) {
	p.bufferPool.Put(buf)
}

// GetBatchSize returns the current optimal batch size.
func (p *MessagePipeline) GetBatchSize() int {
	return p.batcher.GetBatchSize()
}

// SendZeroCopy sends data using zero-copy if possible.
func (p *MessagePipeline) SendZeroCopy(conn net.Conn, file *os.File, offset, length int64) (int64, error) {
	if !p.config.ZeroCopyEnabled || length < int64(p.config.LargeMessageThreshold) {
		// Fall back to regular copy for small messages
		return p.regularSend(conn, file, offset, length)
	}

	reader := NewZeroCopyReader(file, offset, length)
	return reader.SendTo(conn)
}

func (p *MessagePipeline) regularSend(conn net.Conn, file *os.File, offset, length int64) (int64, error) {
	buf := p.GetBuffer(int(length))
	defer p.PutBuffer(buf)

	if _, err := file.ReadAt(buf[:length], offset); err != nil {
		return 0, err
	}

	n, err := conn.Write(buf[:length])
	return int64(n), err
}

// Stats returns pipeline statistics.
func (p *MessagePipeline) Stats() PipelineStats {
	return PipelineStats{
		MessagesProcessed: atomic.LoadUint64(&p.messagesProcessed),
		BytesProcessed:    atomic.LoadUint64(&p.bytesProcessed),
		CompressionSaved:  atomic.LoadUint64(&p.compressionSaved),
		CurrentBatchSize:  p.batcher.GetBatchSize(),
	}
}

// PipelineStats holds pipeline statistics.
type PipelineStats struct {
	MessagesProcessed uint64
	BytesProcessed    uint64
	CompressionSaved  uint64
	CurrentBatchSize  int
}

// Close closes the pipeline.
func (p *MessagePipeline) Close() error {
	atomic.StoreInt32(&p.closed, 1)
	return nil
}

// IsLargeMessage returns true if the message should use zero-copy.
func (p *MessagePipeline) IsLargeMessage(size int) bool {
	return size >= p.config.LargeMessageThreshold
}

// CompressionRatio returns the compression ratio achieved.
func (p *MessagePipeline) CompressionRatio() float64 {
	processed := atomic.LoadUint64(&p.bytesProcessed)
	saved := atomic.LoadUint64(&p.compressionSaved)
	if processed == 0 {
		return 1.0
	}
	return float64(processed-saved) / float64(processed)
}
