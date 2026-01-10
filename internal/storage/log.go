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
Log manages multiple segments to provide an unbounded append-only log.

CONCEPT:
========
The Log is the top-level storage abstraction for a topic. It manages a
collection of segments, routing reads and writes to the appropriate segment.

ARCHITECTURE:
=============

	Log
	 ├── Segment 0 (offsets 0-999)
	 │    ├── 0.store
	 │    └── 0.index
	 ├── Segment 1000 (offsets 1000-1999)
	 │    ├── 1000.store
	 │    └── 1000.index
	 └── Segment 2000 (offsets 2000-...) [active]
	      ├── 2000.store
	      └── 2000.index

SEGMENT MANAGEMENT:
===================
- Only the "active" segment receives new writes
- When the active segment is full, a new segment is created
- Old segments are read-only
- Segments can be deleted for retention (Truncate)

OFFSET SPACE:
=============
Offsets are globally unique across all segments:
- Segment 0: offsets 0-999
- Segment 1000: offsets 1000-1999
- etc.

The Log routes reads to the correct segment based on offset.

CONCURRENCY:
============
- Uses RWMutex for concurrent read access
- Writes are serialized (single writer)
- Multiple readers can read different segments concurrently

RECOVERY:
=========
On startup, the Log scans the directory for existing segment files
and reconstructs the segment list. This enables crash recovery.
*/
package storage

import (
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Log manages a collection of segments for a topic.
// It provides the primary interface for reading and writing messages.
type Log struct {
	// mu protects all fields. RWMutex allows concurrent reads.
	mu sync.RWMutex

	// Dir is the directory containing segment files
	Dir string

	// Config contains segment size limits
	Config Config

	// activeSegment is the segment currently receiving writes.
	// This is always the last segment in the segments slice.
	activeSegment *Segment

	// segments is the ordered list of all segments.
	// Sorted by baseOffset (ascending).
	segments []*Segment
}

// NewLog creates or opens a Log in the given directory.
//
// INITIALIZATION:
// 1. Apply default configuration if not specified
// 2. Scan directory for existing segments (recovery)
// 3. Create initial segment if none exist
//
// DEFAULTS:
// - MaxStoreBytes: 1024 bytes (very small, for testing)
// - MaxIndexBytes: 1024 bytes (very small, for testing)
// In production, these should be much larger (e.g., 1GB).
//
// PARAMETERS:
// - dir: Directory to store segment files (must exist)
// - c: Configuration with segment size limits
//
// RETURNS:
// - Configured Log ready for use
// - Error if directory scan or segment creation fails
func NewLog(dir string, c Config) (*Log, error) {
	// Apply defaults for unset configuration
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

// setup scans the directory for existing segments and initializes the Log.
// This is called during NewLog to recover state after a restart.
//
// RECOVERY PROCESS:
// 1. List all files in the directory
// 2. Extract base offsets from filenames (e.g., "1000.store" -> 1000)
// 3. Sort offsets to ensure correct segment order
// 4. Open each segment
// 5. If no segments exist, create the initial segment
//
// FILE NAMING:
// Segment files are named {baseOffset}.store and {baseOffset}.index.
// We extract the base offset by removing the extension.
//
// DUPLICATE HANDLING:
// Each segment has two files (.store and .index), so we see each
// base offset twice. We skip every other entry to avoid duplicates.
func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	// Extract base offsets from filenames
	var baseOffsets []uint64
	for _, file := range files {
		// Remove extension to get base offset
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	// Sort to ensure segments are in order
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// Open each segment (skip duplicates from .store/.index pairs)
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// Skip the duplicate (each segment has 2 files)
		i++
	}

	// Create initial segment if none exist
	if len(l.segments) == 0 {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

// Append writes a message to the log and returns the assigned offset.
//
// WRITE PROCESS:
// 1. Acquire write lock (exclusive access)
// 2. Append to the active segment
// 3. If segment is full, create a new segment
// 4. Return the assigned offset
//
// SEGMENT ROTATION:
// When the active segment reaches its size limit (IsMaxed), a new
// segment is created starting at offset+1. The old segment becomes
// read-only.
//
// THREAD SAFETY:
// Uses exclusive lock - only one writer at a time.
func (l *Log) Append(p []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Append to active segment
	off, err := l.activeSegment.Append(p)
	if err != nil {
		return 0, err
	}

	// Rotate segment if full
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

// Read retrieves a message by its offset.
//
// READ PROCESS:
// 1. Acquire read lock (allows concurrent readers)
// 2. Find the segment containing the offset
// 3. Read from that segment
//
// SEGMENT LOOKUP:
// We iterate through segments to find one where:
//
//	baseOffset <= off < nextOffset
//
// This is O(n) in the number of segments. For many segments,
// consider using binary search.
//
// RETURNS:
// - The message data
// - io.EOF if offset doesn't exist
func (l *Log) Read(off uint64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Find the segment containing this offset
	var s *Segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}

	// Check if offset exists
	if s == nil || s.nextOffset <= off {
		return nil, io.EOF
	}
	return s.Read(off)
}

// GetZeroCopyInfo returns file and position info for zero-copy reads.
// This allows using sendfile() to transfer data directly to network.
func (l *Log) GetZeroCopyInfo(off uint64) (*os.File, int64, int64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Find the segment containing this offset
	var s *Segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}

	if s == nil || s.nextOffset <= off {
		return nil, 0, 0, io.EOF
	}

	return s.GetZeroCopyInfo(off)
}

// Close closes all segments in the log.
// After Close, the Log should not be used.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Remove closes the log and deletes all segment files.
// WARNING: This permanently deletes all data!
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// Reset removes all data and recreates the directory.
// This is useful for testing or clearing a topic.
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return os.MkdirAll(l.Dir, 0755)
}

// LowestOffset returns the lowest available offset in the log.
// This is the base offset of the first segment.
// Messages before this offset have been deleted (truncated).
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

// HighestOffset returns the highest written offset in the log.
// This is the last message that was successfully appended.
// Returns 0 if the log is empty.
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

// Truncate removes segments containing only offsets <= lowest.
// This is used to implement retention policies (delete old messages).
//
// RETENTION EXAMPLE:
// If lowest=1000, segments with nextOffset <= 1001 are deleted.
// This means all messages with offset <= 1000 are removed.
//
// NOTE: This only removes complete segments. Messages within
// a segment cannot be individually deleted.
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*Segment
	for _, s := range l.segments {
		// Remove segments that are entirely before the cutoff
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

// newSegment creates a new segment and makes it the active segment.
// This is called during setup (recovery) and when rotating segments.
func (l *Log) newSegment(off uint64) error {
	s, err := NewSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
