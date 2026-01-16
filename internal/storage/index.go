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
Index provides a memory-mapped index for fast offset-to-position lookups.

PURPOSE:
========
The index maps logical message offsets to physical file positions in the store.
This enables O(1) lookups: given an offset, we can immediately find where the
message is stored on disk without scanning the entire file.

MEMORY MAPPING:
===============
The index uses memory-mapped I/O (mmap) for performance:
- The file is mapped directly into the process's address space
- Reads/writes go directly to memory, with the OS handling disk I/O
- No system calls needed for individual reads/writes
- The OS manages caching and page faults transparently

This is significantly faster than traditional file I/O for random access patterns.

INDEX ENTRY FORMAT:
===================
Each entry is 12 bytes:

	+------------------+------------------+
	| Offset (4 bytes) | Position (8 bytes) |
	+------------------+------------------+

- Offset: The logical message offset (uint32, max ~4 billion messages per segment)
- Position: The byte position in the store file (uint64)

WHY 4 BYTES FOR OFFSET?
=======================
Using 4 bytes limits each segment to ~4 billion messages. This is intentional:
- Keeps index entries small (12 bytes vs 16 bytes with uint64)
- Segments are typically rotated before reaching this limit
- The Log manages multiple segments for unlimited total messages

PRE-ALLOCATION:
===============
The index file is pre-allocated to MaxIndexBytes on creation. This:
- Ensures contiguous disk space for the memory map
- Avoids fragmentation and resize operations
- The actual used size is tracked separately

DURABILITY:
===========
- mmap.Sync() flushes changes to disk
- On close, the file is truncated to actual size
- This ensures clean recovery after crashes
*/
package storage

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// Index entry dimensions.
// These define the binary format of each index entry.
var (
	// offWidth is the size of the offset field (4 bytes = uint32)
	// This limits each segment to ~4 billion messages
	offWidth uint64 = 4

	// posWidth is the size of the position field (8 bytes = uint64)
	// This supports files up to 18 exabytes
	posWidth uint64 = 8

	// entWidth is the total size of one index entry
	entWidth = offWidth + posWidth // 12 bytes
)

// Index is a memory-mapped index file that maps offsets to store positions.
// It provides O(1) lookup performance by using mmap for direct memory access.
//
// LIFECYCLE:
// 1. NewIndex() - Creates/opens index, sets up memory mapping
// 2. Write() - Adds new entries as messages are appended
// 3. Read() - Looks up positions for consumption
// 4. Close() - Syncs to disk and cleans up
type Index struct {
	// file is the underlying file handle
	file *os.File

	// mmap is the memory-mapped region of the file.
	// Reading/writing to this slice directly accesses the file.
	mmap gommap.MMap

	// size is the number of bytes currently used in the index.
	// This may be less than the file size due to pre-allocation.
	size uint64
}

// NewIndex creates a new Index from the given file.
// The file is pre-allocated to MaxIndexBytes and memory-mapped.
//
// INITIALIZATION STEPS:
// 1. Get current file size (for recovery of existing index)
// 2. Pre-allocate file to MaxIndexBytes
// 3. Memory-map the file for direct access
// 4. Scan for actual data size (recovery after crash)
//
// PRE-ALLOCATION:
// The file is truncated to MaxIndexBytes even if empty. This:
// - Reserves contiguous disk space
// - Allows the mmap to cover the full potential size
// - Avoids costly resize operations during writes
//
// CRASH RECOVERY:
// If the server crashes without calling Close(), the file will have the
// pre-allocated size (MaxIndexBytes) but only contain valid data up to
// the last written entry. We scan the file to find the actual data size
// by looking for the last non-zero entry.
//
// PARAMETERS:
// - f: Open file handle for the index file
// - config: Configuration with MaxIndexBytes setting
//
// RETURNS:
// - Configured Index ready for use
// - Error if file operations or mmap fails
func NewIndex(f *os.File, config Config) (*Index, error) {
	idx := &Index{
		file: f,
	}

	// Get current file size (non-zero if recovering existing index)
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	savedSize := uint64(fi.Size())

	// Pre-allocate file to maximum size for memory mapping.
	// This ensures the mmap covers the full potential index size.
	if err := os.Truncate(f.Name(), int64(config.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	// Memory-map the file for direct access.
	// PROT_READ|PROT_WRITE: Allow both reading and writing
	// MAP_SHARED: Changes are visible to other processes and persisted to disk
	if idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}

	// Recover actual data size after crash
	// If the file was properly closed, savedSize reflects the actual data size.
	// If the server crashed, savedSize might be MaxIndexBytes (pre-allocated).
	// We need to scan to find the actual last valid entry.
	idx.size = idx.recoverActualSize(savedSize, config.Segment.MaxIndexBytes)

	return idx, nil
}

// recoverActualSize finds the actual data size in the index after a crash.
// If the file was properly closed, savedSize is the actual data size.
// If the server crashed, we need to scan for the last non-zero entry.
//
// ALGORITHM:
// 1. If savedSize < MaxIndexBytes, the file was properly closed - use savedSize
// 2. If savedSize == MaxIndexBytes, the file might have crashed while pre-allocated
//    - Scan backwards from the end to find the last non-zero entry
//    - This is O(n) in the worst case but only happens on crash recovery
//
// ZERO ENTRIES:
// A valid entry has a non-zero position field (8 bytes after offset).
// The first entry at offset 0 has position 0, but subsequent entries have
// increasing positions. We check if the entire entry is zero to detect
// unused space.
func (i *Index) recoverActualSize(savedSize, maxSize uint64) uint64 {
	// If file was properly closed (truncated to actual size), use that
	if savedSize < maxSize && savedSize%entWidth == 0 {
		return savedSize
	}

	// File might have crashed while pre-allocated - scan for actual data
	// Start from the end and work backwards to find the last valid entry
	numEntries := uint64(len(i.mmap)) / entWidth

	// Binary search for the first zero entry
	// This is more efficient than linear scan for large indexes
	low := uint64(0)
	high := numEntries

	for low < high {
		mid := (low + high) / 2
		pos := mid * entWidth

		// Check if this entry is zero (unused)
		if i.isZeroEntry(pos) {
			high = mid
		} else {
			low = mid + 1
		}
	}

	return low * entWidth
}

// isZeroEntry checks if the entry at the given position is all zeros.
// This indicates unused space in the pre-allocated index.
func (i *Index) isZeroEntry(pos uint64) bool {
	if pos+entWidth > uint64(len(i.mmap)) {
		return true
	}
	for j := pos; j < pos+entWidth; j++ {
		if i.mmap[j] != 0 {
			return false
		}
	}
	return true
}

// Close syncs the index to disk and closes the file.
//
// CLEANUP SEQUENCE:
// 1. Sync mmap to disk (MS_SYNC = synchronous, blocks until complete)
// 2. Sync file metadata
// 3. Truncate file to actual used size (removes pre-allocated space)
// 4. Close file handle
//
// TRUNCATION:
// The file was pre-allocated to MaxIndexBytes, but we only used 'size' bytes.
// Truncating removes the unused space, saving disk space and making the
// file size accurately reflect the number of entries.
//
// DURABILITY:
// The MS_SYNC flag ensures all data is written to disk before returning.
// This is slower than MS_ASYNC but guarantees durability.
func (i *Index) Close() error {
	// Sync memory-mapped region to disk
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// Sync file metadata
	if err := i.file.Sync(); err != nil {
		return err
	}

	// Truncate to actual size (remove pre-allocated unused space)
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

// Read looks up an index entry and returns the offset and store position.
//
// LOOKUP MODES:
// - in == -1: Read the last entry (useful for finding the latest offset)
// - in >= 0: Read the entry at index 'in'
//
// CALCULATION:
// The entry position is calculated as: in * entWidth
// This gives O(1) lookup - no scanning required.
//
// PARAMETERS:
// - in: The index of the entry to read (-1 for last entry)
//
// RETURNS:
// - out: The message offset stored in this entry
// - pos: The byte position in the store file
// - err: io.EOF if index is empty or entry doesn't exist
//
// EXAMPLE:
//
//	offset, pos, err := index.Read(5)  // Read 6th entry (0-indexed)
//	offset, pos, err := index.Read(-1) // Read last entry
func (i *Index) Read(in int64) (out uint32, pos uint64, err error) {
	// Empty index
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	// Handle -1 (last entry) case
	if in == -1 {
		out = uint32(i.size/entWidth) - 1
	} else {
		out = uint32(in)
	}

	// Calculate byte position of the entry
	pos = uint64(out) * entWidth

	// Check if entry exists
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	// Read offset and position from mmap
	// Direct memory access - no system calls!
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// Write appends a new entry to the index.
//
// ENTRY FORMAT:
// [4 bytes: offset][8 bytes: position]
//
// The entry is written at the current size position, then size is incremented.
// This is an append-only operation - entries cannot be modified or deleted.
//
// PARAMETERS:
// - off: The logical message offset
// - pos: The byte position in the store file
//
// RETURNS:
// - io.EOF if the index is full (no more space in mmap)
// - nil on success
//
// PERFORMANCE:
// Writing to mmap is extremely fast - it's just a memory write.
// The OS handles flushing to disk asynchronously (or on Close/Sync).
func (i *Index) Write(off uint32, pos uint64) error {
	// Check if there's space for another entry
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	// Write offset (4 bytes)
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)

	// Write position (8 bytes)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	// Advance size to next entry position
	i.size += entWidth
	return nil
}

// Name returns the name of the underlying file.
// Useful for logging and debugging.
func (i *Index) Name() string {
	return i.file.Name()
}
