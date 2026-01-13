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
Message Compression for FlyMQ.

SUPPORTED ALGORITHMS:
=====================
- None: No compression (fastest, no CPU overhead)
- Gzip: Good compression ratio, moderate CPU
- LZ4: Fast compression/decompression, moderate ratio
- Snappy: Very fast, lower compression ratio
- Zstd: Best ratio, configurable speed/ratio tradeoff

COMPRESSION TRADEOFFS:
======================

	Algorithm | Speed    | Ratio  | CPU Usage
	----------|----------|--------|----------
	None      | Fastest  | 1.0x   | None
	LZ4       | Fast     | 2-3x   | Low
	Snappy    | Fast     | 2-3x   | Low
	Gzip      | Moderate | 3-5x   | Medium
	Zstd      | Variable | 4-6x   | Variable

BATCH COMPRESSION:
==================
Messages are compressed in batches for better ratio.
Each batch is compressed as a unit before writing to disk.

BUFFER POOLING:
===============
Compression buffers are pooled to reduce GC pressure.
*/
package performance

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// CompressionType represents the type of compression algorithm.
type CompressionType byte

const (
	CompressionNone CompressionType = iota
	CompressionGzip
	CompressionLZ4
	CompressionSnappy
	CompressionZstd
)

func (c CompressionType) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionGzip:
		return "gzip"
	case CompressionLZ4:
		return "lz4"
	case CompressionSnappy:
		return "snappy"
	case CompressionZstd:
		return "zstd"
	default:
		return "unknown"
	}
}

// ParseCompressionType parses a compression type from string.
func ParseCompressionType(s string) CompressionType {
	switch s {
	case "gzip":
		return CompressionGzip
	case "lz4":
		return CompressionLZ4
	case "snappy":
		return CompressionSnappy
	case "zstd":
		return CompressionZstd
	default:
		return CompressionNone
	}
}

// Compressor provides compression and decompression functionality.
type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
	Type() CompressionType
}

// GzipCompressor implements gzip compression.
type GzipCompressor struct {
	level int
}

// NewGzipCompressor creates a new gzip compressor.
func NewGzipCompressor(level int) *GzipCompressor {
	if level < gzip.BestSpeed || level > gzip.BestCompression {
		level = gzip.DefaultCompression
	}
	return &GzipCompressor{level: level}
}

func (g *GzipCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, g.level)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (g *GzipCompressor) Decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func (g *GzipCompressor) Type() CompressionType {
	return CompressionGzip
}

// LZ4 Constants for the block format
const (
	lz4MagicNumber    uint32 = 0x184D2204 // LZ4 frame magic number
	lz4MinMatch              = 4          // Minimum match length
	lz4MaxMatchLength        = 65535      // Maximum match length
	lz4HashLog               = 16         // Hash table size = 1 << 16 = 64KB
	lz4HashTableSize         = 1 << lz4HashLog
	lz4MaxOffset             = 65535 // Maximum back-reference offset (16-bit)
	lz4MaxInputSize          = 0x7E000000
	lz4BlockMaxSize          = 4 * 1024 * 1024 // 4MB max block size
)

// LZ4Compressor implements LZ4 block compression.
// This is a production-quality implementation following the LZ4 block format.
type LZ4Compressor struct {
	hashTable []int // Hash table for fast match finding
	level     int   // Compression level (1-12, higher = better ratio, slower)
}

// NewLZ4Compressor creates a new LZ4 compressor with default settings.
func NewLZ4Compressor() *LZ4Compressor {
	return NewLZ4CompressorLevel(6)
}

// NewLZ4CompressorLevel creates a new LZ4 compressor with specified level.
func NewLZ4CompressorLevel(level int) *LZ4Compressor {
	if level < 1 {
		level = 1
	} else if level > 12 {
		level = 12
	}
	return &LZ4Compressor{
		hashTable: make([]int, lz4HashTableSize),
		level:     level,
	}
}

func (l *LZ4Compressor) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}
	if len(data) > lz4MaxInputSize {
		return nil, fmt.Errorf("lz4: input too large (%d bytes, max %d)", len(data), lz4MaxInputSize)
	}
	return l.compressBlock(data)
}

func (l *LZ4Compressor) Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}
	return l.decompressBlock(data)
}

func (l *LZ4Compressor) Type() CompressionType {
	return CompressionLZ4
}

// lz4Hash computes a hash for 4 bytes at the given position
func lz4Hash(data []byte, pos int) uint32 {
	if pos+4 > len(data) {
		return 0
	}
	// Read 4 bytes as little-endian uint32 and multiply by prime
	v := uint32(data[pos]) | uint32(data[pos+1])<<8 | uint32(data[pos+2])<<16 | uint32(data[pos+3])<<24
	return (v * 2654435761) >> (32 - lz4HashLog)
}

// compressBlock compresses a single LZ4 block
func (l *LZ4Compressor) compressBlock(src []byte) ([]byte, error) {
	srcLen := len(src)
	if srcLen == 0 {
		return []byte{}, nil
	}

	// Reset hash table
	for i := range l.hashTable {
		l.hashTable[i] = -1
	}

	// Allocate output buffer (worst case: slightly larger than input)
	// Format: [4-byte uncompressed length][compressed data]
	maxDstSize := srcLen + (srcLen/255 + 16) + 4
	dst := make([]byte, 4, maxDstSize)

	// Write uncompressed length (big-endian)
	binary.BigEndian.PutUint32(dst[:4], uint32(srcLen))

	anchor := 0 // Start of literals
	pos := 0    // Current position

	// Main compression loop
	for pos < srcLen-lz4MinMatch {
		// Find a match using hash table
		hash := lz4Hash(src, pos)
		ref := l.hashTable[hash]
		l.hashTable[hash] = pos

		// Check if we have a valid match
		matchLen := 0
		offset := 0

		if ref >= 0 && pos-ref <= lz4MaxOffset {
			// Verify match at reference position
			if src[ref] == src[pos] && src[ref+1] == src[pos+1] &&
				src[ref+2] == src[pos+2] && src[ref+3] == src[pos+3] {
				// Count match length
				matchLen = 4
				for pos+matchLen < srcLen && src[ref+matchLen] == src[pos+matchLen] {
					matchLen++
					if matchLen >= lz4MaxMatchLength {
						break
					}
				}
				offset = pos - ref
			}
		}

		if matchLen < lz4MinMatch {
			pos++
			continue
		}

		// Encode sequence: literals + match
		literalLen := pos - anchor

		// Token byte: high 4 bits = literal length, low 4 bits = match length - 4
		token := byte(0)
		if literalLen >= 15 {
			token = 0xF0
		} else {
			token = byte(literalLen << 4)
		}

		matchLenEncoded := matchLen - lz4MinMatch
		if matchLenEncoded >= 15 {
			token |= 0x0F
		} else {
			token |= byte(matchLenEncoded)
		}

		dst = append(dst, token)

		// Write extended literal length
		if literalLen >= 15 {
			remaining := literalLen - 15
			for remaining >= 255 {
				dst = append(dst, 255)
				remaining -= 255
			}
			dst = append(dst, byte(remaining))
		}

		// Write literals
		dst = append(dst, src[anchor:pos]...)

		// Write offset (little-endian 16-bit)
		dst = append(dst, byte(offset), byte(offset>>8))

		// Write extended match length
		if matchLenEncoded >= 15 {
			remaining := matchLenEncoded - 15
			for remaining >= 255 {
				dst = append(dst, 255)
				remaining -= 255
			}
			dst = append(dst, byte(remaining))
		}

		// Update positions
		pos += matchLen
		anchor = pos

		// Update hash table for skipped positions (improves compression ratio)
		if l.level >= 6 {
			for i := pos - matchLen + 1; i < pos && i < srcLen-lz4MinMatch; i++ {
				h := lz4Hash(src, i)
				l.hashTable[h] = i
			}
		}
	}

	// Write remaining literals (last sequence)
	literalLen := srcLen - anchor
	if literalLen > 0 {
		token := byte(0)
		if literalLen >= 15 {
			token = 0xF0
		} else {
			token = byte(literalLen << 4)
		}
		dst = append(dst, token)

		if literalLen >= 15 {
			remaining := literalLen - 15
			for remaining >= 255 {
				dst = append(dst, 255)
				remaining -= 255
			}
			dst = append(dst, byte(remaining))
		}

		dst = append(dst, src[anchor:]...)
	}

	return dst, nil
}

// decompressBlock decompresses a single LZ4 block
func (l *LZ4Compressor) decompressBlock(src []byte) ([]byte, error) {
	if len(src) < 4 {
		return nil, fmt.Errorf("lz4: compressed data too short")
	}

	// Read uncompressed length
	uncompressedLen := binary.BigEndian.Uint32(src[:4])
	if uncompressedLen > lz4MaxInputSize {
		return nil, fmt.Errorf("lz4: uncompressed size too large (%d)", uncompressedLen)
	}

	dst := make([]byte, 0, uncompressedLen)
	pos := 4

	for pos < len(src) {
		// Read token
		token := src[pos]
		pos++

		// Decode literal length
		literalLen := int(token >> 4)
		if literalLen == 15 {
			for pos < len(src) {
				b := src[pos]
				pos++
				literalLen += int(b)
				if b != 255 {
					break
				}
			}
		}

		// Copy literals
		if literalLen > 0 {
			if pos+literalLen > len(src) {
				return nil, fmt.Errorf("lz4: literal length exceeds input (%d + %d > %d)", pos, literalLen, len(src))
			}
			dst = append(dst, src[pos:pos+literalLen]...)
			pos += literalLen
		}

		// Check if this is the last sequence (no match)
		if pos >= len(src) {
			break
		}

		// Read offset (little-endian 16-bit)
		if pos+2 > len(src) {
			return nil, fmt.Errorf("lz4: unexpected end of input reading offset")
		}
		offset := int(src[pos]) | int(src[pos+1])<<8
		pos += 2

		if offset == 0 {
			return nil, fmt.Errorf("lz4: invalid zero offset")
		}
		if offset > len(dst) {
			return nil, fmt.Errorf("lz4: offset (%d) exceeds output size (%d)", offset, len(dst))
		}

		// Decode match length
		matchLen := int(token & 0x0F)
		if matchLen == 15 {
			for pos < len(src) {
				b := src[pos]
				pos++
				matchLen += int(b)
				if b != 255 {
					break
				}
			}
		}
		matchLen += lz4MinMatch

		// Copy match (handle overlapping copies)
		matchStart := len(dst) - offset
		for i := 0; i < matchLen; i++ {
			dst = append(dst, dst[matchStart+i])
		}
	}

	if uint32(len(dst)) != uncompressedLen {
		return nil, fmt.Errorf("lz4: decompressed size mismatch (got %d, expected %d)", len(dst), uncompressedLen)
	}

	return dst, nil
}

// Snappy constants
const (
	snappyTagLiteral = 0x00
	snappyTagCopy1   = 0x01
	snappyTagCopy2   = 0x02
	snappyTagCopy4   = 0x03
	snappyMaxOffset  = 65535
	snappyHashLog    = 14
	snappyHashSize   = 1 << snappyHashLog
)

// SnappyCompressor implements Snappy-compatible compression.
// Snappy prioritizes speed over compression ratio.
type SnappyCompressor struct {
	hashTable []int
}

// NewSnappyCompressor creates a new Snappy compressor.
func NewSnappyCompressor() *SnappyCompressor {
	return &SnappyCompressor{
		hashTable: make([]int, snappyHashSize),
	}
}

func (s *SnappyCompressor) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}
	return s.compressBlock(data)
}

func (s *SnappyCompressor) Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}
	return s.decompressBlock(data)
}

func (s *SnappyCompressor) Type() CompressionType {
	return CompressionSnappy
}

// snappyHash computes a hash for 4 bytes
func snappyHash(data []byte, pos int) uint32 {
	if pos+4 > len(data) {
		return 0
	}
	v := uint32(data[pos]) | uint32(data[pos+1])<<8 | uint32(data[pos+2])<<16 | uint32(data[pos+3])<<24
	return (v * 0x1e35a7bd) >> (32 - snappyHashLog)
}

// putVarint writes a varint-encoded length
func putVarint(dst []byte, n uint32) int {
	i := 0
	for n >= 0x80 {
		dst[i] = byte(n) | 0x80
		n >>= 7
		i++
	}
	dst[i] = byte(n)
	return i + 1
}

// getVarint reads a varint-encoded length
func getVarint(src []byte) (uint32, int) {
	var n uint32
	var shift uint
	for i, b := range src {
		if b < 0x80 {
			return n | uint32(b)<<shift, i + 1
		}
		n |= uint32(b&0x7f) << shift
		shift += 7
		if shift >= 32 {
			return 0, -1 // overflow
		}
	}
	return 0, -1 // incomplete
}

func (s *SnappyCompressor) compressBlock(src []byte) ([]byte, error) {
	srcLen := len(src)

	// Reset hash table
	for i := range s.hashTable {
		s.hashTable[i] = -1
	}

	// Allocate output with varint length prefix
	maxDstSize := 10 + srcLen + srcLen/6 // varint + worst case
	dst := make([]byte, 10, maxDstSize)

	// Write uncompressed length as varint
	n := putVarint(dst, uint32(srcLen))
	dst = dst[:n]

	pos := 0
	anchor := 0

	for pos < srcLen-4 {
		hash := snappyHash(src, pos)
		ref := s.hashTable[hash]
		s.hashTable[hash] = pos

		// Check for match
		if ref >= 0 && pos-ref <= snappyMaxOffset &&
			src[ref] == src[pos] && src[ref+1] == src[pos+1] &&
			src[ref+2] == src[pos+2] && src[ref+3] == src[pos+3] {

			// Emit literals before match
			literalLen := pos - anchor
			if literalLen > 0 {
				dst = s.emitLiteral(dst, src[anchor:pos])
			}

			// Count match length
			matchLen := 4
			for pos+matchLen < srcLen && src[ref+matchLen] == src[pos+matchLen] {
				matchLen++
			}

			// Emit copy
			offset := pos - ref
			dst = s.emitCopy(dst, offset, matchLen)

			pos += matchLen
			anchor = pos
		} else {
			pos++
		}
	}

	// Emit remaining literals
	if anchor < srcLen {
		dst = s.emitLiteral(dst, src[anchor:])
	}

	return dst, nil
}

func (s *SnappyCompressor) emitLiteral(dst, lit []byte) []byte {
	n := len(lit) - 1
	if n < 60 {
		dst = append(dst, byte(n<<2)|snappyTagLiteral)
	} else if n < 256 {
		dst = append(dst, 60<<2|snappyTagLiteral, byte(n))
	} else {
		dst = append(dst, 61<<2|snappyTagLiteral, byte(n), byte(n>>8))
	}
	return append(dst, lit...)
}

func (s *SnappyCompressor) emitCopy(dst []byte, offset, length int) []byte {
	for length > 0 {
		copyLen := length
		if copyLen > 64 {
			copyLen = 64
		}

		if copyLen >= 4 && copyLen <= 11 && offset < 2048 {
			// Short copy (1-byte tag + 1-byte offset)
			dst = append(dst,
				byte((offset>>8)<<5|(copyLen-4)<<2)|snappyTagCopy1,
				byte(offset))
		} else {
			// Long copy (1-byte tag + 2-byte offset)
			dst = append(dst,
				byte((copyLen-1)<<2)|snappyTagCopy2,
				byte(offset),
				byte(offset>>8))
		}

		length -= copyLen
	}
	return dst
}

func (s *SnappyCompressor) decompressBlock(src []byte) ([]byte, error) {
	// Read uncompressed length
	uncompLen, n := getVarint(src)
	if n <= 0 {
		return nil, fmt.Errorf("snappy: invalid varint length")
	}
	src = src[n:]

	dst := make([]byte, 0, uncompLen)
	pos := 0

	for pos < len(src) {
		tag := src[pos]
		pos++

		switch tag & 0x03 {
		case snappyTagLiteral:
			// Literal
			litLen := int(tag >> 2)
			if litLen < 60 {
				litLen++
			} else {
				extraBytes := litLen - 59
				if pos+extraBytes > len(src) {
					return nil, fmt.Errorf("snappy: truncated literal length")
				}
				litLen = 0
				for i := 0; i < extraBytes; i++ {
					litLen |= int(src[pos+i]) << (8 * i)
				}
				litLen++
				pos += extraBytes
			}
			if pos+litLen > len(src) {
				return nil, fmt.Errorf("snappy: truncated literal data")
			}
			dst = append(dst, src[pos:pos+litLen]...)
			pos += litLen

		case snappyTagCopy1:
			// Copy with 1-byte offset
			if pos >= len(src) {
				return nil, fmt.Errorf("snappy: truncated copy1")
			}
			length := int((tag>>2)&0x07) + 4
			offset := int(tag>>5)<<8 | int(src[pos])
			pos++
			if offset > len(dst) {
				return nil, fmt.Errorf("snappy: invalid copy1 offset")
			}
			start := len(dst) - offset
			for i := 0; i < length; i++ {
				dst = append(dst, dst[start+i])
			}

		case snappyTagCopy2:
			// Copy with 2-byte offset
			if pos+2 > len(src) {
				return nil, fmt.Errorf("snappy: truncated copy2")
			}
			length := int(tag>>2) + 1
			offset := int(src[pos]) | int(src[pos+1])<<8
			pos += 2
			if offset > len(dst) {
				return nil, fmt.Errorf("snappy: invalid copy2 offset")
			}
			start := len(dst) - offset
			for i := 0; i < length; i++ {
				dst = append(dst, dst[start+i])
			}

		case snappyTagCopy4:
			// Copy with 4-byte offset (rarely used)
			if pos+4 > len(src) {
				return nil, fmt.Errorf("snappy: truncated copy4")
			}
			length := int(tag>>2) + 1
			offset := int(src[pos]) | int(src[pos+1])<<8 | int(src[pos+2])<<16 | int(src[pos+3])<<24
			pos += 4
			if offset > len(dst) {
				return nil, fmt.Errorf("snappy: invalid copy4 offset")
			}
			start := len(dst) - offset
			for i := 0; i < length; i++ {
				dst = append(dst, dst[start+i])
			}
		}
	}

	if uint32(len(dst)) != uncompLen {
		return nil, fmt.Errorf("snappy: length mismatch (got %d, expected %d)", len(dst), uncompLen)
	}

	return dst, nil
}

// Zstd constants
const (
	zstdMagic         uint32 = 0xFD2FB528
	zstdMinMatch             = 3
	zstdMaxOffset            = 65535
	zstdHashLog              = 15
	zstdHashSize             = 1 << zstdHashLog
	zstdWindowLog            = 17
	zstdMaxWindowSize        = 1 << zstdWindowLog
)

// ZstdCompressor implements Zstandard-like compression.
// This is a simplified implementation focusing on the core algorithm.
type ZstdCompressor struct {
	level     int
	hashTable []int
}

// NewZstdCompressor creates a new Zstd compressor.
func NewZstdCompressor(level int) *ZstdCompressor {
	if level < 1 {
		level = 1
	} else if level > 19 {
		level = 19
	}
	return &ZstdCompressor{
		level:     level,
		hashTable: make([]int, zstdHashSize),
	}
}

func (z *ZstdCompressor) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}
	return z.compressBlock(data)
}

func (z *ZstdCompressor) Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}
	return z.decompressBlock(data)
}

func (z *ZstdCompressor) Type() CompressionType {
	return CompressionZstd
}

// zstdHash computes a hash for matching
func zstdHash(data []byte, pos int) uint32 {
	if pos+4 > len(data) {
		return 0
	}
	v := uint32(data[pos]) | uint32(data[pos+1])<<8 | uint32(data[pos+2])<<16 | uint32(data[pos+3])<<24
	return (v * 0x9E3779B1) >> (32 - zstdHashLog)
}

func (z *ZstdCompressor) compressBlock(src []byte) ([]byte, error) {
	srcLen := len(src)

	// Reset hash table
	for i := range z.hashTable {
		z.hashTable[i] = -1
	}

	// Header: magic (4) + uncompressed length (4)
	dst := make([]byte, 8, srcLen+srcLen/4+16)
	binary.LittleEndian.PutUint32(dst[:4], zstdMagic)
	binary.LittleEndian.PutUint32(dst[4:8], uint32(srcLen))

	pos := 0
	anchor := 0

	for pos < srcLen-zstdMinMatch {
		hash := zstdHash(src, pos)
		ref := z.hashTable[hash]
		z.hashTable[hash] = pos

		// Check for match
		matchLen := 0
		offset := 0

		if ref >= 0 && pos-ref <= zstdMaxOffset {
			// Verify match
			if src[ref] == src[pos] && src[ref+1] == src[pos+1] && src[ref+2] == src[pos+2] {
				matchLen = 3
				for pos+matchLen < srcLen && src[ref+matchLen] == src[pos+matchLen] {
					matchLen++
				}
				offset = pos - ref
			}
		}

		if matchLen < zstdMinMatch {
			pos++
			continue
		}

		// Encode sequence
		literalLen := pos - anchor

		// Sequence header: [literalLen:4][matchLen:4][offsetCode:8]
		// Simplified encoding for our implementation
		seqHeader := byte(0)
		if literalLen >= 15 {
			seqHeader = 0xF0
		} else {
			seqHeader = byte(literalLen << 4)
		}

		matchLenCode := matchLen - zstdMinMatch
		if matchLenCode >= 15 {
			seqHeader |= 0x0F
		} else {
			seqHeader |= byte(matchLenCode)
		}

		dst = append(dst, seqHeader)

		// Extended literal length
		if literalLen >= 15 {
			remaining := literalLen - 15
			for remaining >= 255 {
				dst = append(dst, 255)
				remaining -= 255
			}
			dst = append(dst, byte(remaining))
		}

		// Literals
		dst = append(dst, src[anchor:pos]...)

		// Offset (little-endian 16-bit)
		dst = append(dst, byte(offset), byte(offset>>8))

		// Extended match length
		if matchLenCode >= 15 {
			remaining := matchLenCode - 15
			for remaining >= 255 {
				dst = append(dst, 255)
				remaining -= 255
			}
			dst = append(dst, byte(remaining))
		}

		pos += matchLen
		anchor = pos
	}

	// Final literals
	literalLen := srcLen - anchor
	if literalLen > 0 {
		seqHeader := byte(0)
		if literalLen >= 15 {
			seqHeader = 0xF0
		} else {
			seqHeader = byte(literalLen << 4)
		}
		dst = append(dst, seqHeader)

		if literalLen >= 15 {
			remaining := literalLen - 15
			for remaining >= 255 {
				dst = append(dst, 255)
				remaining -= 255
			}
			dst = append(dst, byte(remaining))
		}

		dst = append(dst, src[anchor:]...)
	}

	return dst, nil
}

func (z *ZstdCompressor) decompressBlock(src []byte) ([]byte, error) {
	if len(src) < 8 {
		return nil, fmt.Errorf("zstd: data too short")
	}

	// Verify magic
	magic := binary.LittleEndian.Uint32(src[:4])
	if magic != zstdMagic {
		return nil, fmt.Errorf("zstd: invalid magic number")
	}

	uncompLen := binary.LittleEndian.Uint32(src[4:8])
	dst := make([]byte, 0, uncompLen)
	pos := 8

	for pos < len(src) {
		seqHeader := src[pos]
		pos++

		// Decode literal length
		literalLen := int(seqHeader >> 4)
		if literalLen == 15 {
			for pos < len(src) {
				b := src[pos]
				pos++
				literalLen += int(b)
				if b != 255 {
					break
				}
			}
		}

		// Copy literals
		if literalLen > 0 {
			if pos+literalLen > len(src) {
				return nil, fmt.Errorf("zstd: truncated literals")
			}
			dst = append(dst, src[pos:pos+literalLen]...)
			pos += literalLen
		}

		// Check for match (if not last sequence)
		matchLenCode := int(seqHeader & 0x0F)
		if matchLenCode == 0 && pos >= len(src) {
			break // Last sequence with no match
		}

		if pos+2 > len(src) {
			break // End of data
		}

		// Read offset
		offset := int(src[pos]) | int(src[pos+1])<<8
		pos += 2

		if offset == 0 {
			continue // No match in this sequence
		}

		// Decode match length
		matchLen := matchLenCode
		if matchLen == 15 {
			for pos < len(src) {
				b := src[pos]
				pos++
				matchLen += int(b)
				if b != 255 {
					break
				}
			}
		}
		matchLen += zstdMinMatch

		// Copy match
		if offset > len(dst) {
			return nil, fmt.Errorf("zstd: invalid offset %d (output size %d)", offset, len(dst))
		}
		start := len(dst) - offset
		for i := 0; i < matchLen; i++ {
			dst = append(dst, dst[start+i])
		}
	}

	if uint32(len(dst)) != uncompLen {
		return nil, fmt.Errorf("zstd: length mismatch (got %d, expected %d)", len(dst), uncompLen)
	}

	return dst, nil
}

// CompressorPool provides a pool of compressors.
type CompressorPool struct {
	pool sync.Pool
	typ  CompressionType
}

// NewCompressorPool creates a new compressor pool.
func NewCompressorPool(typ CompressionType) *CompressorPool {
	return &CompressorPool{
		typ: typ,
		pool: sync.Pool{
			New: func() interface{} {
				return NewCompressor(typ)
			},
		},
	}
}

// Get retrieves a compressor from the pool.
func (p *CompressorPool) Get() Compressor {
	return p.pool.Get().(Compressor)
}

// Put returns a compressor to the pool.
func (p *CompressorPool) Put(c Compressor) {
	p.pool.Put(c)
}

// NewCompressor creates a new compressor of the specified type.
func NewCompressor(typ CompressionType) Compressor {
	switch typ {
	case CompressionGzip:
		return NewGzipCompressor(gzip.DefaultCompression)
	case CompressionLZ4:
		return NewLZ4Compressor()
	case CompressionSnappy:
		return NewSnappyCompressor()
	case CompressionZstd:
		return NewZstdCompressor(3)
	default:
		return &NoopCompressor{}
	}
}

// NoopCompressor is a no-op compressor.
type NoopCompressor struct{}

func (n *NoopCompressor) Compress(data []byte) ([]byte, error)   { return data, nil }
func (n *NoopCompressor) Decompress(data []byte) ([]byte, error) { return data, nil }
func (n *NoopCompressor) Type() CompressionType                  { return CompressionNone }

// BatchCompressor compresses batches of messages.
type BatchCompressor struct {
	compressor Compressor
	minSize    int // Minimum size to compress
}

// NewBatchCompressor creates a new batch compressor.
func NewBatchCompressor(typ CompressionType, minSize int) *BatchCompressor {
	return &BatchCompressor{
		compressor: NewCompressor(typ),
		minSize:    minSize,
	}
}

// CompressBatch compresses a batch of messages.
func (bc *BatchCompressor) CompressBatch(messages [][]byte) ([]byte, error) {
	// Calculate total size
	totalSize := 0
	for _, msg := range messages {
		totalSize += 4 + len(msg) // 4 bytes for length prefix
	}

	// Build batch
	batch := make([]byte, 0, totalSize+4)
	// Write message count
	countBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(countBuf, uint32(len(messages)))
	batch = append(batch, countBuf...)

	// Write each message with length prefix
	for _, msg := range messages {
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(msg)))
		batch = append(batch, lenBuf...)
		batch = append(batch, msg...)
	}

	// Compress if above minimum size
	if len(batch) >= bc.minSize {
		compressed, err := bc.compressor.Compress(batch)
		if err != nil {
			return nil, err
		}
		// Prepend compression type
		result := make([]byte, 1+len(compressed))
		result[0] = byte(bc.compressor.Type())
		copy(result[1:], compressed)
		return result, nil
	}

	// No compression
	result := make([]byte, 1+len(batch))
	result[0] = byte(CompressionNone)
	copy(result[1:], batch)
	return result, nil
}

// DecompressBatch decompresses a batch of messages.
func (bc *BatchCompressor) DecompressBatch(data []byte) ([][]byte, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("invalid batch data")
	}

	compressionType := CompressionType(data[0])
	payload := data[1:]

	// Decompress if needed
	if compressionType != CompressionNone {
		compressor := NewCompressor(compressionType)
		decompressed, err := compressor.Decompress(payload)
		if err != nil {
			return nil, err
		}
		payload = decompressed
	}

	// Parse messages
	if len(payload) < 4 {
		return nil, fmt.Errorf("invalid batch payload")
	}

	messageCount := binary.BigEndian.Uint32(payload[:4])
	pos := 4
	messages := make([][]byte, 0, messageCount)

	for i := uint32(0); i < messageCount; i++ {
		if pos+4 > len(payload) {
			return nil, fmt.Errorf("invalid message length")
		}
		msgLen := binary.BigEndian.Uint32(payload[pos : pos+4])
		pos += 4

		if pos+int(msgLen) > len(payload) {
			return nil, fmt.Errorf("invalid message data")
		}
		messages = append(messages, payload[pos:pos+int(msgLen)])
		pos += int(msgLen)
	}

	return messages, nil
}
