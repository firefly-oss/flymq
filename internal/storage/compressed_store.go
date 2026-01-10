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
CompressedStore provides transparent compression for message storage.

COMPRESSION STRATEGY:
=====================
- Messages below minSize are stored uncompressed (overhead not worth it)
- Messages above minSize are compressed with the configured algorithm
- A 1-byte header indicates compression type for each message

SUPPORTED ALGORITHMS:
=====================
- None (0x00): No compression
- LZ4 (0x01): Fast compression, good for real-time
- Snappy (0x02): Very fast, lower ratio
- Zstd (0x03): Best ratio, configurable speed
- Gzip (0x04): Good ratio, moderate speed
*/
package storage

import (
	"encoding/binary"
	"fmt"
	"sync"

	"flymq/internal/performance"
)

// CompressionConfig holds compression settings.
type CompressionConfig struct {
	Algorithm string // "none", "lz4", "snappy", "zstd", "gzip"
	Level     int    // Compression level (algorithm-specific)
	MinSize   int    // Minimum size to compress
}

// CompressedStore wraps a StoreWriter with transparent compression.
type CompressedStore struct {
	store      StoreWriter
	compressor performance.Compressor
	minSize    int
	mu         sync.RWMutex
}

// NewCompressedStore creates a new compressed store wrapper.
func NewCompressedStore(store StoreWriter, config CompressionConfig) *CompressedStore {
	var compressor performance.Compressor
	switch config.Algorithm {
	case "lz4":
		compressor = performance.NewLZ4CompressorLevel(config.Level)
	case "snappy":
		compressor = performance.NewSnappyCompressor()
	case "zstd":
		compressor = performance.NewZstdCompressor(config.Level)
	case "gzip":
		compressor = performance.NewGzipCompressor(config.Level)
	default:
		compressor = &performance.NoopCompressor{}
	}

	minSize := config.MinSize
	if minSize <= 0 {
		minSize = 1024 // Default 1KB minimum
	}

	return &CompressedStore{
		store:      store,
		compressor: compressor,
		minSize:    minSize,
	}
}

// Append compresses and writes data to the store.
// Format: [1 byte compression type][4 bytes original size][compressed data]
func (cs *CompressedStore) Append(p []byte) (n uint64, pos uint64, err error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	var data []byte
	if len(p) >= cs.minSize && cs.compressor.Type() != performance.CompressionNone {
		// Compress the data
		compressed, err := cs.compressor.Compress(p)
		if err != nil {
			return 0, 0, fmt.Errorf("compression failed: %w", err)
		}

		// Only use compressed if it's actually smaller
		if len(compressed) < len(p) {
			// Header: [type(1)][original_size(4)][compressed_data]
			data = make([]byte, 5+len(compressed))
			data[0] = byte(cs.compressor.Type())
			binary.BigEndian.PutUint32(data[1:5], uint32(len(p)))
			copy(data[5:], compressed)
		} else {
			// Compression didn't help, store uncompressed
			data = make([]byte, 5+len(p))
			data[0] = byte(performance.CompressionNone)
			binary.BigEndian.PutUint32(data[1:5], uint32(len(p)))
			copy(data[5:], p)
		}
	} else {
		// Below minimum size, store uncompressed
		data = make([]byte, 5+len(p))
		data[0] = byte(performance.CompressionNone)
		binary.BigEndian.PutUint32(data[1:5], uint32(len(p)))
		copy(data[5:], p)
	}

	return cs.store.Append(data)
}

// Read reads and decompresses data from the store.
func (cs *CompressedStore) Read(pos uint64) ([]byte, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	data, err := cs.store.Read(pos)
	if err != nil {
		return nil, err
	}

	if len(data) < 5 {
		return nil, fmt.Errorf("invalid compressed data: too short")
	}

	compType := performance.CompressionType(data[0])
	originalSize := binary.BigEndian.Uint32(data[1:5])
	payload := data[5:]

	if compType == performance.CompressionNone {
		return payload, nil
	}

	// Decompress based on type
	var decompressor performance.Compressor
	switch compType {
	case performance.CompressionLZ4:
		decompressor = performance.NewLZ4Compressor()
	case performance.CompressionSnappy:
		decompressor = performance.NewSnappyCompressor()
	case performance.CompressionZstd:
		decompressor = performance.NewZstdCompressor(3)
	case performance.CompressionGzip:
		decompressor = performance.NewGzipCompressor(-1)
	default:
		return nil, fmt.Errorf("unknown compression type: %d", compType)
	}

	decompressed, err := decompressor.Decompress(payload)
	if err != nil {
		return nil, fmt.Errorf("decompression failed: %w", err)
	}

	if uint32(len(decompressed)) != originalSize {
		return nil, fmt.Errorf("decompressed size mismatch: got %d, expected %d", len(decompressed), originalSize)
	}

	return decompressed, nil
}

// Close closes the underlying store.
func (cs *CompressedStore) Close() error {
	return cs.store.Close()
}

// Name returns the name of the underlying store.
func (cs *CompressedStore) Name() string {
	return cs.store.Name()
}
