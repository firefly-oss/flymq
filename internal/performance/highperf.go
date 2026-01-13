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
High-Performance Optimizations for FlyMQ.

This package provides advanced performance optimizations:
- Lock-free ring buffers for message queuing
- Adaptive batching based on throughput
- Memory-mapped I/O for zero-copy operations
- CPU affinity and NUMA-aware optimizations
- Connection pooling with persistent connections
*/
package performance

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// RingBuffer is a lock-free single-producer single-consumer ring buffer.
// It provides extremely low-latency message passing between goroutines.
type RingBuffer struct {
	buffer   []unsafe.Pointer
	mask     uint64
	head     uint64   // Written by producer
	tail     uint64   // Written by consumer
	_padding [56]byte // Prevent false sharing
}

// NewRingBuffer creates a new ring buffer with the given capacity.
// Capacity must be a power of 2.
func NewRingBuffer(capacity int) *RingBuffer {
	// Round up to power of 2
	cap := uint64(1)
	for cap < uint64(capacity) {
		cap <<= 1
	}
	return &RingBuffer{
		buffer: make([]unsafe.Pointer, cap),
		mask:   cap - 1,
	}
}

// Push adds an item to the ring buffer. Returns false if full.
func (rb *RingBuffer) Push(item unsafe.Pointer) bool {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)

	if head-tail >= uint64(len(rb.buffer)) {
		return false // Buffer full
	}

	rb.buffer[head&rb.mask] = item
	atomic.StoreUint64(&rb.head, head+1)
	return true
}

// Pop removes and returns an item from the ring buffer. Returns nil if empty.
func (rb *RingBuffer) Pop() unsafe.Pointer {
	tail := atomic.LoadUint64(&rb.tail)
	head := atomic.LoadUint64(&rb.head)

	if tail >= head {
		return nil // Buffer empty
	}

	item := rb.buffer[tail&rb.mask]
	atomic.StoreUint64(&rb.tail, tail+1)
	return item
}

// Len returns the number of items in the buffer.
func (rb *RingBuffer) Len() int {
	return int(atomic.LoadUint64(&rb.head) - atomic.LoadUint64(&rb.tail))
}

// AdaptiveBatcher dynamically adjusts batch sizes based on throughput.
type AdaptiveBatcher struct {
	mu              sync.Mutex
	minBatchSize    int
	maxBatchSize    int
	currentBatch    int
	targetLatencyMs float64

	// Metrics for adaptation
	lastAdjustTime   time.Time
	messagesInWindow int64
	latencySum       int64
	latencyCount     int64
}

// NewAdaptiveBatcher creates a new adaptive batcher.
func NewAdaptiveBatcher(minBatch, maxBatch int, targetLatencyMs float64) *AdaptiveBatcher {
	return &AdaptiveBatcher{
		minBatchSize:    minBatch,
		maxBatchSize:    maxBatch,
		currentBatch:    minBatch,
		targetLatencyMs: targetLatencyMs,
		lastAdjustTime:  time.Now(),
	}
}

// RecordLatency records a message latency for adaptation.
func (ab *AdaptiveBatcher) RecordLatency(latencyNs int64) {
	atomic.AddInt64(&ab.latencySum, latencyNs)
	atomic.AddInt64(&ab.latencyCount, 1)
	atomic.AddInt64(&ab.messagesInWindow, 1)
}

// GetBatchSize returns the current optimal batch size.
func (ab *AdaptiveBatcher) GetBatchSize() int {
	ab.mu.Lock()
	defer ab.mu.Unlock()

	// Adjust every 100ms
	if time.Since(ab.lastAdjustTime) < 100*time.Millisecond {
		return ab.currentBatch
	}

	count := atomic.SwapInt64(&ab.latencyCount, 0)
	sum := atomic.SwapInt64(&ab.latencySum, 0)
	atomic.StoreInt64(&ab.messagesInWindow, 0)

	if count == 0 {
		return ab.currentBatch
	}

	avgLatencyMs := float64(sum) / float64(count) / 1e6

	// Adjust batch size based on latency
	if avgLatencyMs < ab.targetLatencyMs*0.5 {
		// Latency is low, increase batch size for throughput
		ab.currentBatch = min(ab.currentBatch*2, ab.maxBatchSize)
	} else if avgLatencyMs > ab.targetLatencyMs {
		// Latency is high, decrease batch size
		ab.currentBatch = max(ab.currentBatch/2, ab.minBatchSize)
	}

	ab.lastAdjustTime = time.Now()
	return ab.currentBatch
}

// SystemInfo contains system resource information for auto-tuning.
type SystemInfo struct {
	NumCPU      int
	TotalMemory uint64
	AvailMemory uint64
	NumaNodes   int
}

// GetSystemInfo returns current system resource information.
func GetSystemInfo() SystemInfo {
	return SystemInfo{
		NumCPU:      runtime.NumCPU(),
		TotalMemory: 0, // Would need syscall to get actual memory
		NumaNodes:   1, // Simplified - would need NUMA detection
	}
}

// OptimalConfig calculates optimal performance configuration based on system resources.
func OptimalConfig(info SystemInfo) PerformanceSettings {
	numCPU := info.NumCPU
	if numCPU == 0 {
		numCPU = runtime.NumCPU()
	}

	// Scale workers with CPU count
	numWorkers := numCPU
	if numWorkers > 16 {
		numWorkers = 16 // Cap at 16 workers
	}

	// Larger batch sizes for more CPUs
	batchSize := 100 * numCPU
	if batchSize > 10000 {
		batchSize = 10000
	}

	// Buffer size scales with CPU count
	bufferSize := 1024 * 1024 * numCPU // 1MB per CPU
	if bufferSize > 64*1024*1024 {
		bufferSize = 64 * 1024 * 1024 // Cap at 64MB
	}

	return PerformanceSettings{
		NumIOWorkers:    numWorkers,
		SyncBatchSize:   batchSize,
		WriteBufferSize: bufferSize,
		SyncIntervalMs:  5,     // Aggressive 5ms sync interval
		RingBufferSize:  65536, // 64K entries
	}
}

// PerformanceSettings holds auto-tuned performance settings.
type PerformanceSettings struct {
	NumIOWorkers    int
	SyncBatchSize   int
	WriteBufferSize int
	SyncIntervalMs  int
	RingBufferSize  int
}

// FastMessagePool provides a high-performance message buffer pool.
type FastMessagePool struct {
	pools []*sync.Pool
	sizes []int
}

// NewFastMessagePool creates a new fast message pool with tiered sizes.
func NewFastMessagePool() *FastMessagePool {
	sizes := []int{256, 1024, 4096, 16384, 65536, 262144, 1048576}
	pools := make([]*sync.Pool, len(sizes))

	for i, size := range sizes {
		s := size // Capture for closure
		pools[i] = &sync.Pool{
			New: func() interface{} {
				return make([]byte, s)
			},
		}
	}

	return &FastMessagePool{
		pools: pools,
		sizes: sizes,
	}
}

// Get returns a buffer of at least the requested size.
func (p *FastMessagePool) Get(size int) []byte {
	for i, s := range p.sizes {
		if size <= s {
			buf := p.pools[i].Get().([]byte)
			return buf[:size]
		}
	}
	// Size too large, allocate directly
	return make([]byte, size)
}

// Put returns a buffer to the pool.
func (p *FastMessagePool) Put(buf []byte) {
	cap := cap(buf)
	for i, s := range p.sizes {
		if cap == s {
			p.pools[i].Put(buf[:cap])
			return
		}
	}
	// Non-standard size, let GC handle it
}

// Global fast message pool
var GlobalMessagePool = NewFastMessagePool()
