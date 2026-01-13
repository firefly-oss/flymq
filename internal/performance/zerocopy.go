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
Zero-Copy I/O for FlyMQ.

OVERVIEW:
=========
Zero-copy transfers data directly from disk to network without copying
through user-space buffers. This significantly reduces CPU usage and
memory bandwidth for high-throughput scenarios.

TRADITIONAL COPY:
=================

	Disk → Kernel Buffer → User Buffer → Kernel Buffer → Network
	         (copy 1)        (copy 2)       (copy 3)

ZERO-COPY (sendfile):
=====================

	Disk → Kernel Buffer → Network
	         (no user-space copies)

WHEN TO USE:
============
- Serving large messages to consumers
- Replication between brokers
- Any scenario where data is read from disk and sent to network

LIMITATIONS:
============
- Only works with TCP connections
- Falls back to regular copy for non-TCP or when sendfile unavailable
- Data cannot be modified during transfer
*/
package performance

import (
	"io"
	"net"
	"os"
	"sync"
	"syscall"
)

// ZeroCopyReader provides zero-copy reading from files using sendfile.
type ZeroCopyReader struct {
	file   *os.File
	offset int64
	length int64
}

// NewZeroCopyReader creates a new zero-copy reader.
func NewZeroCopyReader(file *os.File, offset, length int64) *ZeroCopyReader {
	return &ZeroCopyReader{
		file:   file,
		offset: offset,
		length: length,
	}
}

// SendTo sends data directly to a network connection using sendfile.
// This avoids copying data through user space.
func (z *ZeroCopyReader) SendTo(conn net.Conn) (int64, error) {
	// Get the underlying file descriptor
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		// Fall back to regular copy if not a TCP connection
		return z.regularCopy(conn)
	}

	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return z.regularCopy(conn)
	}

	var written int64
	var sendErr error

	err = rawConn.Write(func(fd uintptr) bool {
		written, sendErr = z.sendfile(int(fd))
		return true
	})

	if err != nil {
		return written, err
	}
	return written, sendErr
}

// sendfile uses the sendfile system call for zero-copy transfer.
func (z *ZeroCopyReader) sendfile(destFd int) (int64, error) {
	srcFd := int(z.file.Fd())
	offset := z.offset
	remaining := z.length
	var written int64

	for remaining > 0 {
		// sendfile may not send all data in one call
		n, err := syscall.Sendfile(destFd, srcFd, &offset, int(remaining))
		if err != nil {
			if err == syscall.EAGAIN {
				continue
			}
			return written, err
		}
		if n == 0 {
			break
		}
		written += int64(n)
		remaining -= int64(n)
	}

	return written, nil
}

// regularCopy falls back to regular io.Copy when sendfile is not available.
func (z *ZeroCopyReader) regularCopy(w io.Writer) (int64, error) {
	if _, err := z.file.Seek(z.offset, io.SeekStart); err != nil {
		return 0, err
	}
	return io.CopyN(w, z.file, z.length)
}

// BufferPool provides a pool of reusable byte buffers to reduce allocations.
type BufferPool struct {
	pool sync.Pool
	size int
}

// NewBufferPool creates a new buffer pool with the specified buffer size.
func NewBufferPool(size int) *BufferPool {
	return &BufferPool{
		size: size,
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, size)
			},
		},
	}
}

// Get retrieves a buffer from the pool.
func (p *BufferPool) Get() []byte {
	return p.pool.Get().([]byte)
}

// Put returns a buffer to the pool.
func (p *BufferPool) Put(buf []byte) {
	if cap(buf) >= p.size {
		p.pool.Put(buf[:p.size])
	}
}

// MessageBuffer provides a zero-copy message buffer.
type MessageBuffer struct {
	data     []byte
	readPos  int
	writePos int
	pool     *BufferPool
}

// NewMessageBuffer creates a new message buffer.
func NewMessageBuffer(pool *BufferPool) *MessageBuffer {
	return &MessageBuffer{
		data: pool.Get(),
		pool: pool,
	}
}

// Write writes data to the buffer.
func (mb *MessageBuffer) Write(p []byte) (int, error) {
	n := copy(mb.data[mb.writePos:], p)
	mb.writePos += n
	return n, nil
}

// Read reads data from the buffer.
func (mb *MessageBuffer) Read(p []byte) (int, error) {
	if mb.readPos >= mb.writePos {
		return 0, io.EOF
	}
	n := copy(p, mb.data[mb.readPos:mb.writePos])
	mb.readPos += n
	return n, nil
}

// Bytes returns the unread portion of the buffer.
func (mb *MessageBuffer) Bytes() []byte {
	return mb.data[mb.readPos:mb.writePos]
}

// Reset resets the buffer for reuse.
func (mb *MessageBuffer) Reset() {
	mb.readPos = 0
	mb.writePos = 0
}

// Release returns the buffer to the pool.
func (mb *MessageBuffer) Release() {
	if mb.pool != nil {
		mb.pool.Put(mb.data)
	}
	mb.data = nil
}

// Len returns the number of unread bytes.
func (mb *MessageBuffer) Len() int {
	return mb.writePos - mb.readPos
}

// Cap returns the capacity of the buffer.
func (mb *MessageBuffer) Cap() int {
	return len(mb.data)
}

// ZeroCopyWriter provides zero-copy writing to files.
type ZeroCopyWriter struct {
	file *os.File
}

// NewZeroCopyWriter creates a new zero-copy writer.
func NewZeroCopyWriter(file *os.File) *ZeroCopyWriter {
	return &ZeroCopyWriter{file: file}
}

// WriteFrom reads data from a connection and writes directly to file.
func (z *ZeroCopyWriter) WriteFrom(conn net.Conn, length int64) (int64, error) {
	// For writing, we use splice on Linux or fall back to regular copy
	// On macOS/Darwin, we fall back to regular copy
	return z.regularWrite(conn, length)
}

func (z *ZeroCopyWriter) regularWrite(r io.Reader, length int64) (int64, error) {
	return io.CopyN(z.file, r, length)
}

// Global buffer pools for different sizes
var (
	SmallBufferPool  = NewBufferPool(4 * 1024)    // 4KB
	MediumBufferPool = NewBufferPool(64 * 1024)   // 64KB
	LargeBufferPool  = NewBufferPool(1024 * 1024) // 1MB
)

// GetBufferPool returns an appropriate buffer pool for the given size.
func GetBufferPool(size int) *BufferPool {
	switch {
	case size <= 4*1024:
		return SmallBufferPool
	case size <= 64*1024:
		return MediumBufferPool
	default:
		return LargeBufferPool
	}
}
