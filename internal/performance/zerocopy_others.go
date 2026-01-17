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

//go:build !linux && !darwin

package performance

import (
	"io"
	"net"
)

// sendfile falls back to regular copy on unsupported platforms.
// This includes Windows, FreeBSD, OpenBSD, and other operating systems
// where sendfile is either not available or has incompatible semantics.
func (z *ZeroCopyReader) sendfile(destFd int) (int64, error) {
	// On unsupported platforms, we cannot use sendfile.
	// Return an error to trigger fallback to regularCopy in SendTo.
	return 0, errSendfileNotSupported
}

// errSendfileNotSupported is returned when sendfile is not available.
var errSendfileNotSupported = &zeroCopyError{msg: "sendfile not supported on this platform"}

// zeroCopyError represents a zero-copy operation error.
type zeroCopyError struct {
	msg string
}

func (e *zeroCopyError) Error() string {
	return e.msg
}

// WriteFrom reads data from a connection and writes directly to file.
// On unsupported platforms, this uses regular io.Copy.
func (z *ZeroCopyWriter) WriteFrom(conn net.Conn, length int64) (int64, error) {
	return z.regularWrite(conn, length)
}

func (z *ZeroCopyWriter) regularWrite(r io.Reader, length int64) (int64, error) {
	return io.CopyN(z.file, r, length)
}

// zeroCopySupported returns false on unsupported platforms.
func zeroCopySupported() bool {
	return false
}

