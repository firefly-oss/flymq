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
Segment combines a Store and Index to manage a portion of a topic's log.

CONCEPT:
========
A segment is a self-contained unit of storage that holds a range of messages.
Each segment has:
- A store file (.store) containing the actual message data
- An index file (.index) mapping offsets to positions in the store

WHY SEGMENTS?
=============
Splitting a log into segments provides several benefits:

 1. BOUNDED FILE SIZE: Each segment has a maximum size, preventing any single
    file from growing too large. Large files are harder to manage, backup,
    and can cause issues with some filesystems.

 2. EFFICIENT DELETION: Old messages can be deleted by removing entire segments
    rather than rewriting files. This is much faster and doesn't fragment the log.

 3. PARALLEL OPERATIONS: Different segments can be read/written concurrently
    (though this implementation uses a single-threaded model).

 4. RECOVERY: If a segment is corrupted, only that segment is lost, not the
    entire log.

SEGMENT LIFECYCLE:
==================
1. Created when the previous segment is full (or for a new topic)
2. Receives appends until it reaches MaxStoreBytes or MaxIndexBytes
3. Becomes read-only when a new segment is created
4. Eventually deleted when retention policy expires

FILE NAMING:
============
Files are named with the base offset: {baseOffset}.store and {baseOffset}.index
For example: 0.store, 0.index, 1000.store, 1000.index, etc.
This makes it easy to identify which messages are in which segment.

OFFSET CALCULATION:
===================
- baseOffset: The first offset in this segment
- nextOffset: The offset that will be assigned to the next message
- Relative offset = absolute offset - baseOffset (stored in index)
*/
package storage

import (
	"fmt"
	"os"
	"path"
)

// StoreWriter is the interface for store implementations.
// Both Store and BatchedStore implement this interface.
type StoreWriter interface {
	Append(p []byte) (n uint64, pos uint64, err error)
	Read(pos uint64) ([]byte, error)
	Close() error
	Name() string
}

// Segment manages a store and index pair for a range of messages.
// It provides the primary interface for reading and writing messages
// within a bounded portion of the log.
type Segment struct {
	// store holds the actual message data (can be Store or BatchedStore)
	store StoreWriter

	// storeSize tracks the store size for IsMaxed check
	storeSize *uint64

	// index maps offsets to positions in the store
	index *Index

	// baseOffset is the first offset in this segment.
	// All offsets in this segment are >= baseOffset.
	baseOffset uint64

	// nextOffset is the offset that will be assigned to the next appended message.
	// After append, this is incremented.
	nextOffset uint64

	// config contains size limits for the segment
	config Config
}

// NewSegment creates or opens a segment starting at the given base offset.
//
// INITIALIZATION:
// 1. Create/open the store file ({baseOffset}.store)
// 2. Create/open the index file ({baseOffset}.index)
// 3. Recover nextOffset from the index (for existing segments)
//
// RECOVERY:
// If the segment already exists (recovery after restart), the nextOffset
// is recovered by reading the last entry in the index. This ensures we
// continue from where we left off without duplicating offsets.
//
// FILE FLAGS:
// - O_RDWR: Read and write access
// - O_CREATE: Create if doesn't exist
// - O_APPEND: All writes go to end (for store only)
//
// PARAMETERS:
// - dir: Directory to store segment files
// - baseOffset: First offset in this segment
// - c: Configuration with size limits
//
// RETURNS:
// - Configured Segment ready for use
// - Error if file operations fail
func NewSegment(dir string, baseOffset uint64, c Config) (*Segment, error) {
	s := &Segment{
		baseOffset: baseOffset,
		config:     c,
	}

	// Open/create the store file
	// O_APPEND ensures all writes go to the end, maintaining append-only semantics
	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.store", baseOffset)),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644, // rw-r--r--
	)
	if err != nil {
		return nil, err
	}

	// Use BatchedStore if performance config specifies non-"all" acks mode
	// This provides significant throughput improvement by batching fsyncs
	if c.Performance.Acks != "" && c.Performance.Acks != "all" {
		batchedStore, err := NewBatchedStore(storeFile, c.Performance)
		if err != nil {
			return nil, err
		}
		s.store = batchedStore
		s.storeSize = &batchedStore.size
		// Debug: log that we're using batched store
		// fmt.Printf("DEBUG: Using BatchedStore for segment %d (acks=%s)\n", baseOffset, c.Performance.Acks)
	} else {
		store, err := NewStore(storeFile)
		if err != nil {
			return nil, err
		}
		s.store = store
		s.storeSize = &store.size
		// Debug: log that we're using regular store
		// fmt.Printf("DEBUG: Using regular Store for segment %d (acks=%s)\n", baseOffset, c.Performance.Acks)
	}

	// Open/create the index file
	// No O_APPEND because index uses mmap for random access writes
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.index", baseOffset)),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = NewIndex(indexFile, c); err != nil {
		return nil, err
	}

	// Recover nextOffset from existing index (if any)
	// Read(-1) gets the last entry
	if off, _, err := s.index.Read(-1); err != nil {
		// Empty index - start from baseOffset
		s.nextOffset = baseOffset
	} else {
		// Continue from last offset + 1
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// Append writes a message to the segment and returns the assigned offset.
//
// WRITE PROCESS:
// 1. Record the current nextOffset as the message's offset
// 2. Append data to the store, getting the byte position
// 3. Write the offset->position mapping to the index
// 4. Increment nextOffset for the next message
//
// ATOMICITY:
// This operation is NOT atomic. If the store write succeeds but the index
// write fails, the segment will be in an inconsistent state. In production,
// you might want to add recovery logic or use write-ahead logging.
//
// INDEX ENTRY:
// The index stores the RELATIVE offset (nextOffset - baseOffset) to save space.
// This allows using uint32 instead of uint64 for the offset field.
//
// PARAMETERS:
// - p: The message data to append
//
// RETURNS:
// - cur: The offset assigned to this message
// - err: Any error that occurred
func (s *Segment) Append(p []byte) (cur uint64, err error) {
	cur = s.nextOffset

	// Write data to store and get the byte position
	// Store format: [8-byte length][data]
	if _, pos, err := s.store.Append(p); err != nil {
		return 0, err
	} else {
		// Write offset->position mapping to index
		// Use relative offset (4 bytes) to save space
		if err = s.index.Write(uint32(s.nextOffset-s.baseOffset), pos); err != nil {
			return 0, err
		}
	}

	// Increment for next message
	s.nextOffset++
	return cur, nil
}

// Read retrieves a message by its absolute offset.
//
// READ PROCESS:
// 1. Convert absolute offset to relative offset
// 2. Look up the byte position in the index
// 3. Read the message data from the store
//
// PERFORMANCE:
// - Index lookup is O(1) via memory-mapped access
// - Store read is a single disk seek + read
//
// PARAMETERS:
// - off: The absolute offset of the message to read
//
// RETURNS:
// - The message data
// - Error if offset doesn't exist or I/O fails
func (s *Segment) Read(off uint64) ([]byte, error) {
	// Convert to relative offset and look up position
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	// Read message data from store
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// GetZeroCopyInfo returns file and position info for zero-copy reads.
// This allows using sendfile() to transfer data directly to network.
func (s *Segment) GetZeroCopyInfo(off uint64) (*os.File, int64, int64, error) {
	// Convert to relative offset and look up position
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, 0, 0, err
	}

	// Get file and position from store
	// Type assert to check if store supports GetFileAndPosition
	type zeroCopyStore interface {
		GetFileAndPosition(pos uint64) (*os.File, int64, int64, error)
	}

	if zcs, ok := s.store.(zeroCopyStore); ok {
		return zcs.GetFileAndPosition(pos)
	}

	return nil, 0, 0, fmt.Errorf("store does not support zero-copy")
}

// IsMaxed returns true if the segment has reached its size limit.
// When a segment is maxed, a new segment should be created for new messages.
//
// SIZE LIMITS:
// A segment is considered full if EITHER:
// - The store file reaches MaxStoreBytes
// - The index file reaches MaxIndexBytes
//
// This ensures neither file grows beyond configured limits.
func (s *Segment) IsMaxed() bool {
	var storeSize uint64
	if s.storeSize != nil {
		storeSize = *s.storeSize
	}
	return storeSize >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Close closes the segment's store and index files.
// After Close, the segment should not be used.
//
// CLOSE ORDER:
// Index is closed first, then store. This order doesn't matter much
// for correctness, but closing index first ensures any pending index
// writes are flushed before the store is closed.
func (s *Segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// Remove closes the segment and deletes its files from disk.
// This is used when implementing retention policies to delete old segments.
//
// CLEANUP:
// 1. Close the segment (flushes and closes files)
// 2. Delete the index file
// 3. Delete the store file
//
// WARNING: This permanently deletes the segment's data!
func (s *Segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}
