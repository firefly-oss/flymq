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
Storage Configuration for FlyMQ.

CONFIGURATION OPTIONS:
======================
- MaxStoreBytes: Maximum size of a segment file before rolling
- MaxIndexBytes: Maximum size of the index file
- InitialOffset: Starting offset for new logs
*/
package storage

// Config holds the storage engine configuration.
type Config struct {
	Segment     SegmentConfig
	Performance PerformanceConfig
}

// SegmentConfig holds configuration for log segments.
type SegmentConfig struct {
	// MaxStoreBytes is the maximum size of a segment data file in bytes.
	// When exceeded, a new segment is created. Default: 1GB.
	MaxStoreBytes uint64

	// MaxIndexBytes is the maximum size of the segment index file.
	// Should be sized to hold enough entries for MaxStoreBytes. Default: 10MB.
	MaxIndexBytes uint64

	// InitialOffset is the starting offset for new logs.
	// Typically 0 for new topics.
	InitialOffset uint64
}

// PerformanceConfig holds performance tuning for storage.
type PerformanceConfig struct {
	// Acks controls durability mode:
	// "all" - fsync every write (safest, slowest)
	// "leader" - batch fsync on interval (balanced)
	// "none" - async writes, fsync on segment rotation only (fastest)
	Acks string

	// SyncIntervalMs is the interval between batch fsyncs when Acks="leader"
	SyncIntervalMs int

	// SyncBatchSize is the max messages before forcing fsync when Acks="leader"
	SyncBatchSize int

	// WriteBufferSize is the per-partition write buffer size in bytes
	WriteBufferSize int

	// Compression settings
	Compression      string // "none", "lz4", "snappy", "zstd", "gzip"
	CompressionLevel int    // Compression level (algorithm-specific)
	CompressionMin   int    // Minimum size to compress
}
