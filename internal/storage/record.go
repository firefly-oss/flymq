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
Record defines the on-disk format for messages with keys.

RECORD FORMAT:
==============
Each record consists of:
  - 4 bytes: key length (uint32, big-endian)
  - N bytes: key data
  - 4 bytes: value length (uint32, big-endian)
  - M bytes: value data

This format allows efficient encoding/decoding while supporting
variable-length keys and values. A key length of 0 indicates no key.

COMPATIBILITY:
==============
For backward compatibility with existing data that doesn't have keys,
we detect old format by checking if the data can be decoded as a valid
record. If not, we treat the entire data as the value with no key.
*/
package storage

import (
	"encoding/binary"
	"errors"
)

// Record represents a message with an optional key.
type Record struct {
	Key   []byte
	Value []byte
}

// ErrInvalidRecord indicates the data is not a valid record format.
var ErrInvalidRecord = errors.New("invalid record format")

// EncodeRecord encodes a key and value into the record format.
// If key is nil or empty, key length will be 0.
func EncodeRecord(key, value []byte) []byte {
	keyLen := len(key)
	valueLen := len(value)

	// Total size: 4 (key len) + key + 4 (value len) + value
	buf := make([]byte, 4+keyLen+4+valueLen)

	// Write key length and key
	binary.BigEndian.PutUint32(buf[0:4], uint32(keyLen))
	if keyLen > 0 {
		copy(buf[4:4+keyLen], key)
	}

	// Write value length and value
	binary.BigEndian.PutUint32(buf[4+keyLen:8+keyLen], uint32(valueLen))
	if valueLen > 0 {
		copy(buf[8+keyLen:], value)
	}

	return buf
}

// DecodeRecord decodes record format data into key and value.
// Returns the Record or an error if the format is invalid.
func DecodeRecord(data []byte) (*Record, error) {
	if len(data) < 8 {
		// Too short to be a valid record, treat as legacy format
		return &Record{Key: nil, Value: data}, nil
	}

	keyLen := binary.BigEndian.Uint32(data[0:4])

	// Sanity check: key length shouldn't be unreasonably large
	if keyLen > uint32(len(data)-8) {
		// Not a valid record format, treat as legacy (value only)
		return &Record{Key: nil, Value: data}, nil
	}

	// Check we have enough data for the value length field
	if len(data) < int(4+keyLen+4) {
		return &Record{Key: nil, Value: data}, nil
	}

	valueLen := binary.BigEndian.Uint32(data[4+keyLen : 8+keyLen])

	// Verify total length matches expected
	expectedLen := 4 + keyLen + 4 + valueLen
	if uint32(len(data)) != expectedLen {
		// Length mismatch, treat as legacy format
		return &Record{Key: nil, Value: data}, nil
	}

	// Extract key
	var key []byte
	if keyLen > 0 {
		key = make([]byte, keyLen)
		copy(key, data[4:4+keyLen])
	}

	// Extract value
	var value []byte
	if valueLen > 0 {
		value = make([]byte, valueLen)
		copy(value, data[8+keyLen:])
	}

	return &Record{Key: key, Value: value}, nil
}

// IsRecordFormat checks if data appears to be in record format.
// This is used for backward compatibility detection.
func IsRecordFormat(data []byte) bool {
	if len(data) < 8 {
		return false
	}

	keyLen := binary.BigEndian.Uint32(data[0:4])
	if keyLen > uint32(len(data)-8) {
		return false
	}

	if len(data) < int(4+keyLen+4) {
		return false
	}

	valueLen := binary.BigEndian.Uint32(data[4+keyLen : 8+keyLen])
	expectedLen := 4 + keyLen + 4 + valueLen

	return uint32(len(data)) == expectedLen
}
