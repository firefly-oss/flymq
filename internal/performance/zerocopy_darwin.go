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

//go:build darwin

package performance

import (
	"io"
	"net"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// sendfile uses the Darwin sendfile system call for zero-copy transfer.
// Darwin sendfile: int sendfile(int fd, int s, off_t offset, off_t *len, struct sf_hdtr *hdtr, int flags)
// Note: Darwin sendfile has a different signature than Linux - it takes the socket as second arg
// and returns the number of bytes sent via a pointer parameter.
func (z *ZeroCopyReader) sendfile(destFd int) (int64, error) {
	srcFd := int(z.file.Fd())
	offset := z.offset
	remaining := z.length
	var written int64

	for remaining > 0 {
		// Darwin sendfile: sendfile(fd, s, offset, *len, hdtr, flags)
		// fd = source file descriptor
		// s = destination socket descriptor
		// offset = offset in source file
		// len = pointer to number of bytes to send (updated with actual bytes sent)
		// hdtr = optional header/trailer (nil for us)
		// flags = 0
		toSend := remaining
		_, err := sendfileDarwin(srcFd, destFd, offset, &toSend)
		if err != nil {
			if err == unix.EAGAIN || err == unix.EINTR {
				if toSend > 0 {
					written += toSend
					offset += toSend
					remaining -= toSend
				}
				continue
			}
			return written, err
		}
		if toSend == 0 {
			break
		}
		written += toSend
		offset += toSend
		remaining -= toSend
	}

	return written, nil
}

// sendfileDarwin wraps the Darwin sendfile syscall.
// int sendfile(int fd, int s, off_t offset, off_t *len, struct sf_hdtr *hdtr, int flags)
func sendfileDarwin(srcFd, destFd int, offset int64, length *int64) (int, error) {
	// SYS_SENDFILE on Darwin is 337
	_, _, errno := syscall.Syscall6(
		syscall.SYS_SENDFILE,
		uintptr(srcFd),
		uintptr(destFd),
		uintptr(offset),
		uintptr(unsafe.Pointer(length)),
		0, // hdtr = nil
		0, // flags = 0
	)
	if errno != 0 {
		return 0, errno
	}
	return int(*length), nil
}

// WriteFrom reads data from a connection and writes directly to file.
// Darwin doesn't have splice, so we fall back to regular copy.
func (z *ZeroCopyWriter) WriteFrom(conn net.Conn, length int64) (int64, error) {
	// Darwin doesn't have splice syscall, fall back to regular copy
	return z.regularWrite(conn, length)
}

func (z *ZeroCopyWriter) regularWrite(r io.Reader, length int64) (int64, error) {
	return io.CopyN(z.file, r, length)
}

// zeroCopySupported returns true on Darwin where sendfile is available.
func zeroCopySupported() bool {
	return true
}

