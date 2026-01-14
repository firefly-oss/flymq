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
package com.firefly.flymq;

import com.firefly.flymq.exception.ProtocolException;
import com.firefly.flymq.protocol.OpCode;
import com.firefly.flymq.protocol.Protocol;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.nio.ByteBuffer;

import static com.firefly.flymq.protocol.Protocol.*;
import static org.assertj.core.api.Assertions.*;

class ProtocolTest {

    @Test
    void testHeaderToBytes() {
        Header header = Header.create(OpCode.PRODUCE, 100);
        byte[] bytes = header.toBytes();

        assertThat(bytes).hasSize(HEADER_SIZE);
        assertThat(bytes[0]).isEqualTo(MAGIC_BYTE);
        assertThat(bytes[1]).isEqualTo(PROTOCOL_VERSION);
        assertThat(bytes[2]).isEqualTo(OpCode.PRODUCE.getCode());
        assertThat(bytes[3]).isEqualTo((byte) 0);
        assertThat(ByteBuffer.wrap(bytes, 4, 4).getInt()).isEqualTo(100);
    }

    @Test
    void testHeaderFromBytes() {
        Header original = Header.create(OpCode.CONSUME, 42);
        byte[] bytes = original.toBytes();
        Header restored = Header.fromBytes(bytes);

        assertThat(restored.magic()).isEqualTo(MAGIC_BYTE);
        assertThat(restored.version()).isEqualTo(PROTOCOL_VERSION);
        assertThat(restored.op()).isEqualTo(OpCode.CONSUME);
        assertThat(restored.flags()).isEqualTo((byte) 0);
        assertThat(restored.length()).isEqualTo(42);
    }

    @Test
    void testHeaderFromBytesInvalidSize() {
        assertThatThrownBy(() -> Header.fromBytes(new byte[4]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid header size");
    }

    @Test
    void testReadHeaderValidation() throws Exception {
        // Test invalid magic
        byte[] invalidMagic = new byte[HEADER_SIZE];
        invalidMagic[0] = 0x00;
        invalidMagic[1] = PROTOCOL_VERSION;
        invalidMagic[2] = OpCode.PRODUCE.getCode();
        ByteBuffer.wrap(invalidMagic, 4, 4).putInt(0);

        assertThatThrownBy(() -> readHeader(new ByteArrayInputStream(invalidMagic)))
                .isInstanceOf(ProtocolException.InvalidMagicException.class);

        // Test invalid version
        byte[] invalidVersion = new byte[HEADER_SIZE];
        invalidVersion[0] = MAGIC_BYTE;
        invalidVersion[1] = (byte) 0xFF;
        invalidVersion[2] = OpCode.PRODUCE.getCode();
        ByteBuffer.wrap(invalidVersion, 4, 4).putInt(0);

        assertThatThrownBy(() -> readHeader(new ByteArrayInputStream(invalidVersion)))
                .isInstanceOf(ProtocolException.InvalidVersionException.class);
    }

    @Test
    void testReadHeaderMessageTooLarge() {
        byte[] tooLarge = new byte[HEADER_SIZE];
        tooLarge[0] = MAGIC_BYTE;
        tooLarge[1] = PROTOCOL_VERSION;
        tooLarge[2] = OpCode.PRODUCE.getCode();
        ByteBuffer.wrap(tooLarge, 4, 4).putInt(MAX_MESSAGE_SIZE + 1);

        assertThatThrownBy(() -> readHeader(new ByteArrayInputStream(tooLarge)))
                .isInstanceOf(ProtocolException.MessageTooLargeException.class);
    }

    @Test
    void testReadHeaderEOF() {
        assertThatThrownBy(() -> readHeader(new ByteArrayInputStream(new byte[0])))
                .isInstanceOf(EOFException.class);
    }

    @Test
    void testWriteAndReadMessage() throws Exception {
        byte[] payload = "{\"topic\": \"test\", \"data\": \"hello\"}".getBytes();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeMessage(out, OpCode.PRODUCE, payload);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        Message message = readMessage(in);

        assertThat(message.op()).isEqualTo(OpCode.PRODUCE);
        assertThat(message.payload()).isEqualTo(payload);
    }

    @Test
    void testWriteAndReadEmptyPayload() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeMessage(out, OpCode.LIST_TOPICS, null);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        Message message = readMessage(in);

        assertThat(message.op()).isEqualTo(OpCode.LIST_TOPICS);
        assertThat(message.payload()).isEmpty();
    }

    @Test
    void testWriteError() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeError(out, "Something went wrong");

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        Message message = readMessage(in);

        assertThat(message.op()).isEqualTo(OpCode.ERROR);
        assertThat(message.payloadAsString()).isEqualTo("Something went wrong");
    }

    @Test
    void testOpCodeValues() {
        assertThat(OpCode.PRODUCE.getCodeAsInt()).isEqualTo(0x01);
        assertThat(OpCode.CONSUME.getCodeAsInt()).isEqualTo(0x02);
        assertThat(OpCode.CREATE_TOPIC.getCodeAsInt()).isEqualTo(0x03);
        assertThat(OpCode.SUBSCRIBE.getCodeAsInt()).isEqualTo(0x05);
        assertThat(OpCode.COMMIT.getCodeAsInt()).isEqualTo(0x06);
        assertThat(OpCode.FETCH.getCodeAsInt()).isEqualTo(0x07);
        assertThat(OpCode.LIST_TOPICS.getCodeAsInt()).isEqualTo(0x08);
        assertThat(OpCode.DELETE_TOPIC.getCodeAsInt()).isEqualTo(0x09);

        assertThat(OpCode.REGISTER_SCHEMA.getCodeAsInt()).isEqualTo(0x10);
        assertThat(OpCode.BEGIN_TX.getCodeAsInt()).isEqualTo(0x40);
        assertThat(OpCode.ERROR.getCodeAsInt()).isEqualTo(0xFF);
    }

    @Test
    void testOpCodeFromCode() {
        assertThat(OpCode.fromCode(0x01)).isEqualTo(OpCode.PRODUCE);
        assertThat(OpCode.fromCode(0xFF)).isEqualTo(OpCode.ERROR);

        assertThatThrownBy(() -> OpCode.fromCode(0xFE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testMessageRoundtrip() throws Exception {
        var testCases = new Object[][] {
                {OpCode.PRODUCE, "{\"topic\": \"test\"}".getBytes()},
                {OpCode.CONSUME, "{\"offset\": 42}".getBytes()},
                {OpCode.LIST_TOPICS, new byte[0]},
                {OpCode.ERROR, "Error message".getBytes()}
        };

        for (Object[] tc : testCases) {
            OpCode op = (OpCode) tc[0];
            byte[] payload = (byte[]) tc[1];

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            writeMessage(out, op, payload);

            ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
            Message message = readMessage(in);

            assertThat(message.op()).isEqualTo(op);
            assertThat(message.payload()).isEqualTo(payload);
        }
    }
}
