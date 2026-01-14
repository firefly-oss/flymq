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
package com.firefly.flymq.protocol;

import com.firefly.flymq.exception.ProtocolException;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * FlyMQ Binary Protocol Implementation.
 * 
 * <p>Protocol Format:
 * <pre>
 * +-------+-------+-------+-------+-------+-------+-------+-------+
 * | Magic | Ver   | Op    | Flags | Length (4 bytes, big-endian) |
 * +-------+-------+-------+-------+-------+-------+-------+-------+
 * |                      Payload (Length bytes)                   |
 * +---------------------------------------------------------------+
 * </pre>
 * 
 * <p>Header Fields:
 * <ul>
 *   <li>Magic (1 byte): 0xAF - Identifies FlyMQ protocol</li>
 *   <li>Version (1 byte): Protocol version (0x01)</li>
 *   <li>Op (1 byte): Operation code</li>
 *   <li>Flags (1 byte): Reserved</li>
 *   <li>Length (4 bytes): Payload length (big-endian)</li>
 * </ul>
 */
public final class Protocol {

    /** Magic byte that identifies FlyMQ protocol messages. */
    public static final byte MAGIC_BYTE = (byte) 0xAF;

    /** Current protocol version. */
    public static final byte PROTOCOL_VERSION = 0x01;

    /** Fixed size of the message header in bytes. */
    public static final int HEADER_SIZE = 8;

    /** Maximum allowed message payload size (32MB). */
    public static final int MAX_MESSAGE_SIZE = 32 * 1024 * 1024;

    private Protocol() {
        // Utility class
    }

    /**
     * Protocol message header (immutable record).
     *
     * @param magic   Magic byte (must be 0xAF)
     * @param version Protocol version
     * @param op      Operation code
     * @param flags   Flags (reserved)
     * @param length  Payload length in bytes
     */
    public record Header(byte magic, byte version, OpCode op, byte flags, int length) {

        /**
         * Creates a new header for sending a message.
         *
         * @param op            Operation code
         * @param payloadLength Length of the payload
         * @return a new Header instance
         */
        public static Header create(OpCode op, int payloadLength) {
            return new Header(MAGIC_BYTE, PROTOCOL_VERSION, op, (byte) 0, payloadLength);
        }

        /**
         * Serializes the header to bytes.
         *
         * @return 8-byte array containing the header
         */
        public byte[] toBytes() {
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
            buffer.put(magic);
            buffer.put(version);
            buffer.put(op.getCode());
            buffer.put(flags);
            buffer.putInt(length);
            return buffer.array();
        }

        /**
         * Deserializes a header from bytes.
         *
         * @param data 8-byte array containing the header
         * @return parsed Header instance
         * @throws IllegalArgumentException if data is not 8 bytes
         */
        public static Header fromBytes(byte[] data) {
            if (data.length != HEADER_SIZE) {
                throw new IllegalArgumentException(
                        "Invalid header size: " + data.length + ", expected " + HEADER_SIZE);
            }
            ByteBuffer buffer = ByteBuffer.wrap(data);
            byte magic = buffer.get();
            byte version = buffer.get();
            OpCode op = OpCode.fromCode(buffer.get() & 0xFF);
            byte flags = buffer.get();
            int length = buffer.getInt();
            return new Header(magic, version, op, flags, length);
        }
    }

    /**
     * Complete protocol message (immutable record).
     *
     * @param header  Message header
     * @param payload Message payload (may be empty)
     */
    public record Message(Header header, byte[] payload) {

        /**
         * Gets the operation code.
         *
         * @return the operation code
         */
        public OpCode op() {
            return header.op();
        }

        /**
         * Gets the payload as a string (UTF-8).
         *
         * @return payload as string
         */
        public String payloadAsString() {
            return new String(payload, java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    /**
     * Reads a message header from an input stream.
     *
     * @param in the input stream
     * @return the parsed header
     * @throws IOException       if an I/O error occurs
     * @throws ProtocolException if the header is invalid
     */
    public static Header readHeader(InputStream in) throws IOException, ProtocolException {
        byte[] data = new byte[HEADER_SIZE];
        int read = in.readNBytes(data, 0, HEADER_SIZE);

        if (read == 0) {
            throw new EOFException("Connection closed");
        }
        if (read < HEADER_SIZE) {
            throw new EOFException("Incomplete header: got " + read + " bytes, expected " + HEADER_SIZE);
        }

        Header header = Header.fromBytes(data);

        if (header.magic() != MAGIC_BYTE) {
            throw new ProtocolException.InvalidMagicException(header.magic());
        }

        if (header.version() != PROTOCOL_VERSION) {
            throw new ProtocolException.InvalidVersionException(header.version());
        }

        if (header.length() > MAX_MESSAGE_SIZE) {
            throw new ProtocolException.MessageTooLargeException(header.length());
        }

        return header;
    }

    /**
     * Writes a message header to an output stream.
     *
     * @param out    the output stream
     * @param header the header to write
     * @throws IOException if an I/O error occurs
     */
    public static void writeHeader(OutputStream out, Header header) throws IOException {
        out.write(header.toBytes());
    }

    /**
     * Reads a complete message from an input stream.
     *
     * @param in the input stream
     * @return the complete message
     * @throws IOException       if an I/O error occurs
     * @throws ProtocolException if the message is invalid
     */
    public static Message readMessage(InputStream in) throws IOException, ProtocolException {
        Header header = readHeader(in);

        byte[] payload = new byte[0];
        if (header.length() > 0) {
            payload = new byte[header.length()];
            int read = in.readNBytes(payload, 0, header.length());
            if (read < header.length()) {
                throw new EOFException(
                        "Incomplete payload: got " + read + " bytes, expected " + header.length());
            }
        }

        return new Message(header, payload);
    }

    /**
     * Writes a complete message to an output stream.
     *
     * @param out     the output stream
     * @param op      the operation code
     * @param payload the payload (may be empty or null)
     * @throws IOException if an I/O error occurs
     */
    public static void writeMessage(OutputStream out, OpCode op, byte[] payload) throws IOException {
        byte[] actualPayload = payload != null ? payload : new byte[0];
        Header header = Header.create(op, actualPayload.length);
        writeHeader(out, header);
        if (actualPayload.length > 0) {
            out.write(actualPayload);
        }
        out.flush();
    }

    /**
     * Writes an error response to an output stream.
     *
     * @param out     the output stream
     * @param message the error message
     * @throws IOException if an I/O error occurs
     */
    public static void writeError(OutputStream out, String message) throws IOException {
        writeMessage(out, OpCode.ERROR, message.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
