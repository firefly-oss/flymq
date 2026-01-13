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

package performance

import (
	"bytes"
	"testing"
)

func TestCompressionTypeString(t *testing.T) {
	tests := []struct {
		ct       CompressionType
		expected string
	}{
		{CompressionNone, "none"},
		{CompressionGzip, "gzip"},
		{CompressionLZ4, "lz4"},
		{CompressionSnappy, "snappy"},
		{CompressionZstd, "zstd"},
		{CompressionType(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.ct.String(); got != tt.expected {
			t.Errorf("CompressionType(%d).String() = %s, want %s", tt.ct, got, tt.expected)
		}
	}
}

func TestParseCompressionType(t *testing.T) {
	tests := []struct {
		input    string
		expected CompressionType
	}{
		{"gzip", CompressionGzip},
		{"lz4", CompressionLZ4},
		{"snappy", CompressionSnappy},
		{"zstd", CompressionZstd},
		{"none", CompressionNone},
		{"unknown", CompressionNone},
	}

	for _, tt := range tests {
		if got := ParseCompressionType(tt.input); got != tt.expected {
			t.Errorf("ParseCompressionType(%s) = %d, want %d", tt.input, got, tt.expected)
		}
	}
}

func TestGzipCompressor(t *testing.T) {
	compressor := NewGzipCompressor(-1) // default compression

	original := []byte("Hello, World! This is a test message for compression.")

	compressed, err := compressor.Compress(original)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Error("Decompressed data does not match original")
	}

	if compressor.Type() != CompressionGzip {
		t.Errorf("Expected type Gzip, got %s", compressor.Type())
	}
}

func TestSnappyCompressor(t *testing.T) {
	compressor := NewSnappyCompressor()

	original := []byte("Test data for snappy compression")

	compressed, err := compressor.Compress(original)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Error("Decompressed data does not match original")
	}

	if compressor.Type() != CompressionSnappy {
		t.Errorf("Expected type Snappy, got %s", compressor.Type())
	}
}

func TestZstdCompressor(t *testing.T) {
	compressor := NewZstdCompressor(3)

	original := []byte("Test data for zstd compression")

	compressed, err := compressor.Compress(original)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Error("Decompressed data does not match original")
	}

	if compressor.Type() != CompressionZstd {
		t.Errorf("Expected type Zstd, got %s", compressor.Type())
	}
}

func TestNoopCompressor(t *testing.T) {
	compressor := &NoopCompressor{}

	original := []byte("No compression test")

	compressed, err := compressor.Compress(original)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	if !bytes.Equal(original, compressed) {
		t.Error("NoopCompressor should not modify data")
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Error("Decompressed data does not match original")
	}

	if compressor.Type() != CompressionNone {
		t.Errorf("Expected type None, got %s", compressor.Type())
	}
}

func TestNewCompressor(t *testing.T) {
	tests := []struct {
		typ      CompressionType
		expected CompressionType
	}{
		{CompressionNone, CompressionNone},
		{CompressionGzip, CompressionGzip},
		{CompressionLZ4, CompressionLZ4},
		{CompressionSnappy, CompressionSnappy},
		{CompressionZstd, CompressionZstd},
	}

	for _, tt := range tests {
		c := NewCompressor(tt.typ)
		if c.Type() != tt.expected {
			t.Errorf("NewCompressor(%d).Type() = %s, want %s", tt.typ, c.Type(), tt.expected)
		}
	}
}

func TestCompressorPool(t *testing.T) {
	pool := NewCompressorPool(CompressionGzip)

	c1 := pool.Get()
	if c1.Type() != CompressionGzip {
		t.Errorf("Expected Gzip compressor, got %s", c1.Type())
	}

	pool.Put(c1)

	c2 := pool.Get()
	if c2.Type() != CompressionGzip {
		t.Errorf("Expected Gzip compressor, got %s", c2.Type())
	}
}

func TestBatchCompressor(t *testing.T) {
	bc := NewBatchCompressor(CompressionGzip, 10) // min size 10 bytes

	messages := [][]byte{
		[]byte("message 1"),
		[]byte("message 2"),
		[]byte("message 3"),
	}

	compressed, err := bc.CompressBatch(messages)
	if err != nil {
		t.Fatalf("CompressBatch failed: %v", err)
	}

	decompressed, err := bc.DecompressBatch(compressed)
	if err != nil {
		t.Fatalf("DecompressBatch failed: %v", err)
	}

	if len(decompressed) != len(messages) {
		t.Errorf("Expected %d messages, got %d", len(messages), len(decompressed))
	}

	for i, msg := range messages {
		if !bytes.Equal(msg, decompressed[i]) {
			t.Errorf("Message %d mismatch", i)
		}
	}
}

func TestBatchCompressorNoCompression(t *testing.T) {
	bc := NewBatchCompressor(CompressionGzip, 10000) // high min size

	messages := [][]byte{
		[]byte("small"),
	}

	compressed, err := bc.CompressBatch(messages)
	if err != nil {
		t.Fatalf("CompressBatch failed: %v", err)
	}

	// First byte should indicate no compression
	if CompressionType(compressed[0]) != CompressionNone {
		t.Error("Expected no compression for small batch")
	}

	decompressed, err := bc.DecompressBatch(compressed)
	if err != nil {
		t.Fatalf("DecompressBatch failed: %v", err)
	}

	if !bytes.Equal(messages[0], decompressed[0]) {
		t.Error("Message mismatch")
	}
}

func TestLZ4Compressor(t *testing.T) {
	compressor := NewLZ4Compressor()

	// Test with simple data
	original := []byte("Hello World")

	compressed, err := compressor.Compress(original)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Errorf("Decompressed data does not match original: got %s, want %s", decompressed, original)
	}

	if compressor.Type() != CompressionLZ4 {
		t.Errorf("Expected type LZ4, got %s", compressor.Type())
	}
}

func TestLZ4CompressorWithLevel(t *testing.T) {
	levels := []int{1, 6, 12}
	for _, level := range levels {
		compressor := NewLZ4CompressorLevel(level)
		original := []byte("This is a test message with some repetition. This is a test message with some repetition.")

		compressed, err := compressor.Compress(original)
		if err != nil {
			t.Fatalf("Compress at level %d failed: %v", level, err)
		}

		decompressed, err := compressor.Decompress(compressed)
		if err != nil {
			t.Fatalf("Decompress at level %d failed: %v", level, err)
		}

		if !bytes.Equal(original, decompressed) {
			t.Errorf("Level %d: decompressed data does not match original", level)
		}
	}
}

func TestLZ4CompressorLargeData(t *testing.T) {
	compressor := NewLZ4Compressor()

	// Create large data with repetitive patterns (good for compression)
	original := make([]byte, 100000)
	pattern := []byte("ABCDEFGHIJ0123456789")
	for i := 0; i < len(original); i++ {
		original[i] = pattern[i%len(pattern)]
	}

	compressed, err := compressor.Compress(original)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	// Should achieve some compression
	if len(compressed) >= len(original) {
		t.Logf("Warning: no compression achieved (compressed: %d, original: %d)", len(compressed), len(original))
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Error("Decompressed data does not match original")
	}
}

func TestLZ4CompressorEmptyData(t *testing.T) {
	compressor := NewLZ4Compressor()

	compressed, err := compressor.Compress([]byte{})
	if err != nil {
		t.Fatalf("Compress empty data failed: %v", err)
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress empty data failed: %v", err)
	}

	if len(decompressed) != 0 {
		t.Errorf("Expected empty result, got %d bytes", len(decompressed))
	}
}

func TestSnappyCompressorLargeData(t *testing.T) {
	compressor := NewSnappyCompressor()

	// Create data with repetitive patterns
	original := make([]byte, 50000)
	for i := range original {
		original[i] = byte(i % 256)
	}

	compressed, err := compressor.Compress(original)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Error("Decompressed data does not match original")
	}
}

func TestSnappyCompressorEmptyData(t *testing.T) {
	compressor := NewSnappyCompressor()

	compressed, err := compressor.Compress([]byte{})
	if err != nil {
		t.Fatalf("Compress empty data failed: %v", err)
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress empty data failed: %v", err)
	}

	if len(decompressed) != 0 {
		t.Errorf("Expected empty result, got %d bytes", len(decompressed))
	}
}

func TestZstdCompressorLargeData(t *testing.T) {
	compressor := NewZstdCompressor(3)

	// Create data with repetitive patterns
	original := make([]byte, 50000)
	pattern := []byte("The quick brown fox jumps over the lazy dog. ")
	for i := 0; i < len(original); i++ {
		original[i] = pattern[i%len(pattern)]
	}

	compressed, err := compressor.Compress(original)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Error("Decompressed data does not match original")
	}
}

func TestZstdCompressorEmptyData(t *testing.T) {
	compressor := NewZstdCompressor(3)

	compressed, err := compressor.Compress([]byte{})
	if err != nil {
		t.Fatalf("Compress empty data failed: %v", err)
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress empty data failed: %v", err)
	}

	if len(decompressed) != 0 {
		t.Errorf("Expected empty result, got %d bytes", len(decompressed))
	}
}

func TestZstdCompressorLevels(t *testing.T) {
	levels := []int{1, 5, 10, 19}
	original := []byte("Test data for compression level testing. Repeated content helps compression.")

	for _, level := range levels {
		compressor := NewZstdCompressor(level)

		compressed, err := compressor.Compress(original)
		if err != nil {
			t.Fatalf("Compress at level %d failed: %v", level, err)
		}

		decompressed, err := compressor.Decompress(compressed)
		if err != nil {
			t.Fatalf("Decompress at level %d failed: %v", level, err)
		}

		if !bytes.Equal(original, decompressed) {
			t.Errorf("Level %d: decompressed data does not match original", level)
		}
	}
}

func TestDecompressInvalidData(t *testing.T) {
	compressor := NewGzipCompressor(-1)

	_, err := compressor.Decompress([]byte("invalid gzip data"))
	if err == nil {
		t.Error("Expected error for invalid gzip data")
	}
}

func TestLZ4DecompressInvalidData(t *testing.T) {
	compressor := NewLZ4Compressor()

	// Too short
	_, err := compressor.Decompress([]byte{0x01, 0x02})
	if err == nil {
		t.Error("Expected error for too short data")
	}

	// Invalid length header
	_, err = compressor.Decompress([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00})
	if err == nil {
		t.Error("Expected error for invalid length")
	}
}

func TestSnappyDecompressInvalidData(t *testing.T) {
	compressor := NewSnappyCompressor()

	// Empty data
	_, err := compressor.Decompress([]byte{})
	if err != nil {
		t.Error("Empty data should return empty result, not error")
	}

	// Invalid varint
	_, err = compressor.Decompress([]byte{0x80, 0x80, 0x80, 0x80, 0x80})
	if err == nil {
		t.Error("Expected error for invalid varint")
	}
}

func TestZstdDecompressInvalidData(t *testing.T) {
	compressor := NewZstdCompressor(3)

	// Too short
	_, err := compressor.Decompress([]byte{0x01, 0x02, 0x03})
	if err == nil {
		t.Error("Expected error for too short data")
	}

	// Invalid magic
	_, err = compressor.Decompress([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	if err == nil {
		t.Error("Expected error for invalid magic")
	}
}

func TestBatchDecompressInvalidData(t *testing.T) {
	bc := NewBatchCompressor(CompressionGzip, 10)

	_, err := bc.DecompressBatch([]byte{})
	if err == nil {
		t.Error("Expected error for empty data")
	}
}

func TestAllCompressorsRoundTrip(t *testing.T) {
	testData := [][]byte{
		[]byte("Hello, World!"),
		[]byte("Short"),
		bytes.Repeat([]byte("A"), 1000),
		bytes.Repeat([]byte("ABCDEFGHIJ"), 500),
		func() []byte {
			data := make([]byte, 10000)
			for i := range data {
				data[i] = byte(i * 7 % 256)
			}
			return data
		}(),
	}

	compressors := []Compressor{
		NewGzipCompressor(-1),
		NewLZ4Compressor(),
		NewSnappyCompressor(),
		NewZstdCompressor(3),
		&NoopCompressor{},
	}

	for _, compressor := range compressors {
		for i, original := range testData {
			compressed, err := compressor.Compress(original)
			if err != nil {
				t.Errorf("%s: Compress test %d failed: %v", compressor.Type(), i, err)
				continue
			}

			decompressed, err := compressor.Decompress(compressed)
			if err != nil {
				t.Errorf("%s: Decompress test %d failed: %v", compressor.Type(), i, err)
				continue
			}

			if !bytes.Equal(original, decompressed) {
				t.Errorf("%s: test %d: decompressed data mismatch", compressor.Type(), i)
			}
		}
	}
}

func BenchmarkLZ4Compress(b *testing.B) {
	compressor := NewLZ4Compressor()
	data := bytes.Repeat([]byte("benchmark data for compression testing "), 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressor.Compress(data)
	}
}

func BenchmarkLZ4Decompress(b *testing.B) {
	compressor := NewLZ4Compressor()
	data := bytes.Repeat([]byte("benchmark data for compression testing "), 1000)
	compressed, _ := compressor.Compress(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressor.Decompress(compressed)
	}
}

func BenchmarkSnappyCompress(b *testing.B) {
	compressor := NewSnappyCompressor()
	data := bytes.Repeat([]byte("benchmark data for compression testing "), 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressor.Compress(data)
	}
}

func BenchmarkSnappyDecompress(b *testing.B) {
	compressor := NewSnappyCompressor()
	data := bytes.Repeat([]byte("benchmark data for compression testing "), 1000)
	compressed, _ := compressor.Compress(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressor.Decompress(compressed)
	}
}

func BenchmarkZstdCompress(b *testing.B) {
	compressor := NewZstdCompressor(3)
	data := bytes.Repeat([]byte("benchmark data for compression testing "), 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressor.Compress(data)
	}
}

func BenchmarkZstdDecompress(b *testing.B) {
	compressor := NewZstdCompressor(3)
	data := bytes.Repeat([]byte("benchmark data for compression testing "), 1000)
	compressed, _ := compressor.Compress(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressor.Decompress(compressed)
	}
}

func BenchmarkGzipCompress(b *testing.B) {
	compressor := NewGzipCompressor(-1)
	data := bytes.Repeat([]byte("benchmark data for compression testing "), 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressor.Compress(data)
	}
}

func BenchmarkGzipDecompress(b *testing.B) {
	compressor := NewGzipCompressor(-1)
	data := bytes.Repeat([]byte("benchmark data for compression testing "), 1000)
	compressed, _ := compressor.Compress(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressor.Decompress(compressed)
	}
}
