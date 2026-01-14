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
package com.firefly.flymq.exception;

import static com.firefly.flymq.protocol.Protocol.MAGIC_BYTE;
import static com.firefly.flymq.protocol.Protocol.MAX_MESSAGE_SIZE;
import static com.firefly.flymq.protocol.Protocol.PROTOCOL_VERSION;

/**
 * Exception for protocol-level errors.
 */
public class ProtocolException extends FlyMQException {

    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Thrown when the magic byte doesn't match.
     */
    public static class InvalidMagicException extends ProtocolException {
        private final byte received;

        public InvalidMagicException(byte received) {
            super(String.format("Invalid magic byte: 0x%02X, expected 0x%02X",
                    received & 0xFF, MAGIC_BYTE & 0xFF));
            this.received = received;
        }

        public byte getReceived() {
            return received;
        }
    }

    /**
     * Thrown when the protocol version is not supported.
     */
    public static class InvalidVersionException extends ProtocolException {
        private final byte received;

        public InvalidVersionException(byte received) {
            super(String.format("Unsupported protocol version: 0x%02X, expected 0x%02X",
                    received & 0xFF, PROTOCOL_VERSION & 0xFF));
            this.received = received;
        }

        public byte getReceived() {
            return received;
        }
    }

    /**
     * Thrown when a message exceeds the maximum size.
     */
    public static class MessageTooLargeException extends ProtocolException {
        private final int size;

        public MessageTooLargeException(int size) {
            super(String.format("Message too large: %d bytes, maximum is %d bytes",
                    size, MAX_MESSAGE_SIZE));
            this.size = size;
        }

        public int getSize() {
            return size;
        }
    }
}
