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
Package storage implements the persistent storage layer for FlyMQ.

ARCHITECTURE OVERVIEW:
======================
The storage package provides a durable, append-only log storage system inspired
by Apache Kafka's log-structured storage. The key components are:

1. Store - Low-level file wrapper with buffered writes and length-prefixed records
2. Index - Memory-mapped index for O(1) offset-to-position lookups
3. Segment - Combines a Store and Index for a portion of a topic's log
4. Log - Manages multiple segments for a complete topic log

STORAGE FORMAT:
===============
Each record in the store is written as:

	+----------------+------------------+
	| Length (8 bytes) | Data (N bytes) |
	+----------------+------------------+

The 8-byte length prefix (big-endian uint64) allows reading records without
knowing their size in advance. This is a common pattern in log-structured storage.

DURABILITY GUARANTEES:
======================
- Every write is flushed to the OS buffer (buf.Flush())
- Every write is synced to disk (File.Sync())
- This provides strong durability but impacts write throughput
- For higher throughput, consider batching writes or async sync

CONCURRENCY MODEL:
==================
- All operations are protected by a mutex for thread safety
- Reads and writes are serialized (could be optimized with RWMutex)
- The mutex is held during disk I/O, which may cause contention

DESIGN DECISIONS:
=================
- Big-endian encoding for cross-platform compatibility
- Buffered writes for better performance
- Length-prefixed records for variable-size messages
- Position-based addressing for direct access

PERFORMANCE CONSIDERATIONS:
===========================
- Buffered writes reduce syscall overhead
- Memory-mapped index (in index.go) provides fast lookups
- Sequential writes are optimal for spinning disks and SSDs
- Sync on every write trades throughput for durability
*/
package storage

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// enc is the byte order used for encoding integers.
// Big-endian is used for network/cross-platform compatibility.
// This is consistent with network byte order (RFC 1700).
var (
	enc = binary.BigEndian
)

// lenWidth is the size in bytes of the length prefix for each record.
// Using 8 bytes (uint64) allows records up to 18 exabytes in size.
// In practice, messages are much smaller, but this provides headroom.
const (
	lenWidth = 8
)

// Store wraps a file with buffered writes and provides append-only storage.
// It implements a simple log-structured storage format where each record
// is prefixed with its length.
//
// THREAD SAFETY:
// All methods are protected by a RWMutex, making Store safe for concurrent use.
// Reads can proceed concurrently, writes are serialized.
//
// FILE FORMAT:
// The file contains a sequence of length-prefixed records:
//
//	[len1][data1][len2][data2][len3][data3]...
//
// Each length is an 8-byte big-endian uint64.
type Store struct {
	// File is the underlying file handle.
	// Exported for direct access when needed (e.g., memory mapping).
	File *os.File

	// mu protects all fields and operations for thread safety
	// RWMutex allows concurrent reads while serializing writes
	mu sync.RWMutex

	// buf provides buffered writes for better performance.
	// Writes go to the buffer first, then are flushed to the file.
	buf *bufio.Writer

	// size tracks the current file size (next write position).
	// This is maintained in memory to avoid stat() calls.
	size uint64
}

// NewStore creates a new Store wrapping the given file.
// The file should be opened with appropriate flags (O_RDWR, O_CREATE, O_APPEND).
//
// INITIALIZATION:
// - Stats the file to get current size
// - Creates a buffered writer for efficient writes
// - Size is used to track the append position
//
// PARAMETERS:
// - f: An open file handle (caller is responsible for opening)
//
// RETURNS:
// - A configured Store ready for use
// - Error if the file cannot be stat'd
func NewStore(f *os.File) (*Store, error) {
	// Get current file size to know where to append
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &Store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append writes a record to the store and returns the number of bytes written
// and the position where the record was written.
//
// WRITE FORMAT:
// The record is written as: [8-byte length][data]
// This allows reading without knowing the record size in advance.
//
// DURABILITY:
// After writing, the buffer is flushed and the file is synced to disk.
// This ensures the data survives a crash but impacts performance.
// For higher throughput, consider batching or async sync.
//
// PARAMETERS:
// - p: The data to write (can be any byte slice)
//
// RETURNS:
// - n: Total bytes written (length prefix + data)
// - pos: File position where the record starts (for indexing)
// - err: Any error that occurred
//
// THREAD SAFETY:
// This method is protected by a mutex and safe for concurrent use.
func (s *Store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Record the current position (where this record will start)
	pos = s.size

	// Write the length prefix (8 bytes, big-endian)
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// Write the actual data
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// Update size to include length prefix + data
	w += lenWidth
	s.size += uint64(w)

	// DURABILITY: Flush buffer to OS and sync to disk.
	// buf.Flush() moves data from user-space buffer to OS buffer.
	// File.Sync() forces OS to write to physical disk.
	// This two-step process ensures data survives crashes.
	if err := s.buf.Flush(); err != nil {
		return 0, 0, err
	}
	if err := s.File.Sync(); err != nil {
		return 0, 0, err
	}

	return uint64(w), pos, nil
}

// Read reads a record from the store at the given position.
// The position should be the start of a record (as returned by Append).
//
// READ PROCESS:
// 1. Flush any buffered writes to ensure we read the latest data
// 2. Read the 8-byte length prefix at the position
// 3. Allocate a buffer of that size
// 4. Read the data into the buffer
//
// PARAMETERS:
// - pos: File position of the record (from Append or index lookup)
//
// RETURNS:
// - The record data (without the length prefix)
// - Error if the position is invalid or I/O fails
//
// NOTE: This method allocates a new buffer for each read.
// For high-performance scenarios, consider using ReadAt with a pooled buffer.
func (s *Store) Read(pos uint64) ([]byte, error) {
	// First, flush any buffered writes (requires write lock)
	s.mu.Lock()
	if s.buf.Buffered() > 0 {
		if err := s.buf.Flush(); err != nil {
			s.mu.Unlock()
			return nil, err
		}
	}
	s.mu.Unlock()

	// Now read with read lock (allows concurrent reads)
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Read the length prefix
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// Allocate buffer and read the data
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadAt reads len(p) bytes from the store starting at offset off.
// This implements the io.ReaderAt interface for direct file access.
//
// USE CASES:
// - Reading raw bytes without length-prefix parsing
// - Memory mapping support
// - Custom read patterns
//
// NOTE: Unlike Read(), this does not interpret the length prefix.
// The caller is responsible for knowing how many bytes to read.
func (s *Store) ReadAt(p []byte, off int64) (int, error) {
	// Flush any buffered writes first (requires write lock)
	s.mu.Lock()
	if s.buf.Buffered() > 0 {
		if err := s.buf.Flush(); err != nil {
			s.mu.Unlock()
			return 0, err
		}
	}
	s.mu.Unlock()

	// Read with read lock (allows concurrent reads)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.File.ReadAt(p, off)
}

// Close flushes any buffered data and closes the underlying file.
// After Close, the Store should not be used.
//
// CLEANUP:
// - Flushes the write buffer to ensure all data is written
// - Closes the file handle
//
// NOTE: This does not call Sync(). If durability is required,
// call Sync() before Close() or ensure Append() was used for all writes.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush any remaining buffered data
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}

// Name returns the name of the underlying file.
// This is useful for logging and debugging.
func (s *Store) Name() string {
	return s.File.Name()
}

// GetFileAndPosition returns the file and position info for zero-copy reads.
// This allows using sendfile() to transfer data directly to network.
// Returns the file, offset, and length of the record at the given position.
func (s *Store) GetFileAndPosition(pos uint64) (*os.File, int64, int64, error) {
	// Flush any buffered writes first
	s.mu.Lock()
	if s.buf.Buffered() > 0 {
		if err := s.buf.Flush(); err != nil {
			s.mu.Unlock()
			return nil, 0, 0, err
		}
	}
	s.mu.Unlock()

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Read the length prefix
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, 0, 0, err
	}

	length := int64(enc.Uint64(size))
	dataOffset := int64(pos) + lenWidth

	return s.File, dataOffset, length, nil
}
