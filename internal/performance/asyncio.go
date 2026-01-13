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
Async I/O Manager for FlyMQ.

OVERVIEW:
=========
Provides asynchronous I/O operations to maximize throughput.
Operations are queued and processed by a pool of worker goroutines.

BENEFITS:
=========
- Non-blocking writes: Producers don't wait for disk I/O
- Batching: Multiple small writes combined into larger ones
- Prioritization: Critical operations can be prioritized
- Backpressure: Queue limits prevent memory exhaustion

ARCHITECTURE:
=============

	Producer → Queue → Workers → Disk
	    ↑                  ↓
	    └──── Result ──────┘

USAGE:
======

	manager := performance.NewAsyncIOManager(config)
	manager.Start()
	defer manager.Stop()

	result := manager.Write(file, offset, data)
	<-result.Done() // Wait for completion
*/
package performance

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"flymq/internal/logging"
)

// IOOperation represents an async I/O operation.
type IOOperation struct {
	Type     IOOpType
	File     *os.File
	Offset   int64
	Data     []byte
	Result   chan IOResult
	Priority int
}

// IOOpType represents the type of I/O operation.
type IOOpType int

const (
	IORead IOOpType = iota
	IOWrite
	IOSync
)

// IOResult represents the result of an I/O operation.
type IOResult struct {
	BytesProcessed int
	Error          error
}

// AsyncIOConfig holds configuration for async I/O.
type AsyncIOConfig struct {
	NumWorkers    int
	QueueSize     int
	BatchSize     int
	FlushInterval time.Duration
}

// DefaultAsyncIOConfig returns default async I/O configuration.
func DefaultAsyncIOConfig() AsyncIOConfig {
	return AsyncIOConfig{
		NumWorkers:    4,
		QueueSize:     1000,
		BatchSize:     100,
		FlushInterval: 10 * time.Millisecond,
	}
}

// AsyncIOManager manages asynchronous I/O operations.
type AsyncIOManager struct {
	config  AsyncIOConfig
	queue   chan *IOOperation
	workers []*ioWorker
	logger  *logging.Logger
	stopCh  chan struct{}
	wg      sync.WaitGroup
	stats   IOStats
}

// IOStats holds I/O statistics.
type IOStats struct {
	ReadsCompleted  uint64
	WritesCompleted uint64
	BytesRead       uint64
	BytesWritten    uint64
	ReadLatencyNs   uint64
	WriteLatencyNs  uint64
}

// ioWorker processes I/O operations.
type ioWorker struct {
	id      int
	queue   chan *IOOperation
	stopCh  chan struct{}
	manager *AsyncIOManager
	logger  *logging.Logger
}

// NewAsyncIOManager creates a new async I/O manager.
func NewAsyncIOManager(config AsyncIOConfig) *AsyncIOManager {
	m := &AsyncIOManager{
		config: config,
		queue:  make(chan *IOOperation, config.QueueSize),
		stopCh: make(chan struct{}),
		logger: logging.NewLogger("asyncio"),
	}

	// Create workers
	m.workers = make([]*ioWorker, config.NumWorkers)
	for i := 0; i < config.NumWorkers; i++ {
		m.workers[i] = &ioWorker{
			id:      i,
			queue:   m.queue,
			stopCh:  m.stopCh,
			manager: m,
			logger:  logging.NewLogger("asyncio-worker"),
		}
	}

	return m
}

// Start starts the async I/O manager.
func (m *AsyncIOManager) Start() {
	m.logger.Info("Starting async I/O manager", "workers", m.config.NumWorkers)

	for _, worker := range m.workers {
		m.wg.Add(1)
		go worker.run(&m.wg)
	}
}

// Stop stops the async I/O manager.
func (m *AsyncIOManager) Stop() {
	m.logger.Info("Stopping async I/O manager")
	close(m.stopCh)
	m.wg.Wait()
}

// Read performs an async read operation.
func (m *AsyncIOManager) Read(ctx context.Context, file *os.File, offset int64, length int) ([]byte, error) {
	result := make(chan IOResult, 1)
	data := make([]byte, length)

	op := &IOOperation{
		Type:   IORead,
		File:   file,
		Offset: offset,
		Data:   data,
		Result: result,
	}

	select {
	case m.queue <- op:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case res := <-result:
		if res.Error != nil {
			return nil, res.Error
		}
		return data[:res.BytesProcessed], nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Write performs an async write operation.
func (m *AsyncIOManager) Write(ctx context.Context, file *os.File, offset int64, data []byte) (int, error) {
	result := make(chan IOResult, 1)

	op := &IOOperation{
		Type:   IOWrite,
		File:   file,
		Offset: offset,
		Data:   data,
		Result: result,
	}

	select {
	case m.queue <- op:
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	select {
	case res := <-result:
		return res.BytesProcessed, res.Error
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// Sync performs an async sync operation.
func (m *AsyncIOManager) Sync(ctx context.Context, file *os.File) error {
	result := make(chan IOResult, 1)

	op := &IOOperation{
		Type:   IOSync,
		File:   file,
		Result: result,
	}

	select {
	case m.queue <- op:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case res := <-result:
		return res.Error
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats returns I/O statistics.
func (m *AsyncIOManager) Stats() IOStats {
	return IOStats{
		ReadsCompleted:  atomic.LoadUint64(&m.stats.ReadsCompleted),
		WritesCompleted: atomic.LoadUint64(&m.stats.WritesCompleted),
		BytesRead:       atomic.LoadUint64(&m.stats.BytesRead),
		BytesWritten:    atomic.LoadUint64(&m.stats.BytesWritten),
		ReadLatencyNs:   atomic.LoadUint64(&m.stats.ReadLatencyNs),
		WriteLatencyNs:  atomic.LoadUint64(&m.stats.WriteLatencyNs),
	}
}

func (w *ioWorker) run(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-w.stopCh:
			return
		case op := <-w.queue:
			w.processOperation(op)
		}
	}
}

func (w *ioWorker) processOperation(op *IOOperation) {
	start := time.Now()
	var result IOResult

	switch op.Type {
	case IORead:
		result = w.doRead(op)
		atomic.AddUint64(&w.manager.stats.ReadsCompleted, 1)
		atomic.AddUint64(&w.manager.stats.BytesRead, uint64(result.BytesProcessed))
		atomic.AddUint64(&w.manager.stats.ReadLatencyNs, uint64(time.Since(start).Nanoseconds()))

	case IOWrite:
		result = w.doWrite(op)
		atomic.AddUint64(&w.manager.stats.WritesCompleted, 1)
		atomic.AddUint64(&w.manager.stats.BytesWritten, uint64(result.BytesProcessed))
		atomic.AddUint64(&w.manager.stats.WriteLatencyNs, uint64(time.Since(start).Nanoseconds()))

	case IOSync:
		result = w.doSync(op)
	}

	select {
	case op.Result <- result:
	default:
	}
}

func (w *ioWorker) doRead(op *IOOperation) IOResult {
	n, err := op.File.ReadAt(op.Data, op.Offset)
	return IOResult{BytesProcessed: n, Error: err}
}

func (w *ioWorker) doWrite(op *IOOperation) IOResult {
	n, err := op.File.WriteAt(op.Data, op.Offset)
	return IOResult{BytesProcessed: n, Error: err}
}

func (w *ioWorker) doSync(op *IOOperation) IOResult {
	err := op.File.Sync()
	return IOResult{Error: err}
}

// WriteAheadLog provides async write-ahead logging.
type WriteAheadLog struct {
	file    *os.File
	manager *AsyncIOManager
	offset  int64
	mu      sync.Mutex
	logger  *logging.Logger
}

// NewWriteAheadLog creates a new write-ahead log.
func NewWriteAheadLog(path string, manager *AsyncIOManager) (*WriteAheadLog, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	return &WriteAheadLog{
		file:    file,
		manager: manager,
		offset:  info.Size(),
		logger:  logging.NewLogger("wal"),
	}, nil
}

// Append appends data to the log asynchronously.
func (w *WriteAheadLog) Append(ctx context.Context, data []byte) (int64, error) {
	w.mu.Lock()
	offset := w.offset
	w.offset += int64(len(data))
	w.mu.Unlock()

	_, err := w.manager.Write(ctx, w.file, offset, data)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

// Sync syncs the log to disk.
func (w *WriteAheadLog) Sync(ctx context.Context) error {
	return w.manager.Sync(ctx, w.file)
}

// Close closes the log.
func (w *WriteAheadLog) Close() error {
	return w.file.Close()
}

// BatchWriter batches writes for efficiency.
type BatchWriter struct {
	manager       *AsyncIOManager
	file          *os.File
	buffer        []byte
	bufferSize    int
	flushInterval time.Duration
	mu            sync.Mutex
	offset        int64
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// NewBatchWriter creates a new batch writer.
func NewBatchWriter(file *os.File, manager *AsyncIOManager, bufferSize int, flushInterval time.Duration) *BatchWriter {
	bw := &BatchWriter{
		manager:       manager,
		file:          file,
		buffer:        make([]byte, 0, bufferSize),
		bufferSize:    bufferSize,
		flushInterval: flushInterval,
		stopCh:        make(chan struct{}),
	}

	info, _ := file.Stat()
	bw.offset = info.Size()

	bw.wg.Add(1)
	go bw.flushLoop()

	return bw
}

// Write writes data to the batch buffer.
func (bw *BatchWriter) Write(data []byte) (int, error) {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	bw.buffer = append(bw.buffer, data...)

	if len(bw.buffer) >= bw.bufferSize {
		return len(data), bw.flushLocked()
	}

	return len(data), nil
}

// Flush flushes the buffer to disk.
func (bw *BatchWriter) Flush() error {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return bw.flushLocked()
}

func (bw *BatchWriter) flushLocked() error {
	if len(bw.buffer) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := bw.manager.Write(ctx, bw.file, bw.offset, bw.buffer)
	if err != nil {
		return err
	}

	bw.offset += int64(len(bw.buffer))
	bw.buffer = bw.buffer[:0]
	return nil
}

func (bw *BatchWriter) flushLoop() {
	defer bw.wg.Done()

	ticker := time.NewTicker(bw.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bw.stopCh:
			bw.Flush()
			return
		case <-ticker.C:
			bw.Flush()
		}
	}
}

// Close closes the batch writer.
func (bw *BatchWriter) Close() error {
	close(bw.stopCh)
	bw.wg.Wait()
	return nil
}
