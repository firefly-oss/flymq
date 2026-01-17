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

//go:build linux

package performance

import (
	"io"
	"net"

	"golang.org/x/sys/unix"
)

// sendfile uses the Linux sendfile system call for zero-copy transfer.
// Linux sendfile: ssize_t sendfile(int out_fd, int in_fd, off_t *offset, size_t count)
func (z *ZeroCopyReader) sendfile(destFd int) (int64, error) {
	srcFd := int(z.file.Fd())
	offset := z.offset
	remaining := z.length
	var written int64

	for remaining > 0 {
		// Linux sendfile may not send all data in one call
		n, err := unix.Sendfile(destFd, srcFd, &offset, int(remaining))
		if err != nil {
			if err == unix.EAGAIN || err == unix.EINTR {
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

// Splice flags for Linux
const (
	spliceMove = 1 // SPLICE_F_MOVE
	spliceMore = 4 // SPLICE_F_MORE
)

// splice uses the Linux splice system call for zero-copy pipe transfer.
// This is used for network-to-file transfers (WriteFrom).
func (z *ZeroCopyWriter) splice(conn net.Conn, length int64) (int64, error) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return z.regularWrite(conn, length)
	}

	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return z.regularWrite(conn, length)
	}

	// Create a pipe for splice
	var pipeFds [2]int
	if err := unix.Pipe(pipeFds[:]); err != nil {
		return z.regularWrite(conn, length)
	}
	defer unix.Close(pipeFds[0])
	defer unix.Close(pipeFds[1])

	destFd := int(z.file.Fd())
	var written int64
	var spliceErr error

	err = rawConn.Read(func(srcFd uintptr) bool {
		remaining := length
		for remaining > 0 {
			// Splice from socket to pipe
			n, err := unix.Splice(int(srcFd), nil, pipeFds[1], nil, int(remaining), spliceMove|spliceMore)
			if err != nil {
				if err == unix.EAGAIN {
					return false // Need to wait for more data
				}
				if err == unix.EINTR {
					continue
				}
				spliceErr = err
				return true
			}
			if n == 0 {
				break
			}

			// Splice from pipe to file
			for n > 0 {
				m, err := unix.Splice(pipeFds[0], nil, destFd, nil, int(n), spliceMove|spliceMore)
				if err != nil {
					if err == unix.EINTR {
						continue
					}
					spliceErr = err
					return true
				}
				n -= m
				written += m
				remaining -= m
			}
		}
		return true
	})

	if err != nil {
		return written, err
	}
	return written, spliceErr
}

// WriteFrom reads data from a connection and writes directly to file using splice.
func (z *ZeroCopyWriter) WriteFrom(conn net.Conn, length int64) (int64, error) {
	return z.splice(conn, length)
}

func (z *ZeroCopyWriter) regularWrite(r io.Reader, length int64) (int64, error) {
	return io.CopyN(z.file, r, length)
}

// zeroCopySupported returns true on Linux where sendfile and splice are available.
func zeroCopySupported() bool {
	return true
}

