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

/**
 * Operation codes for FlyMQ protocol messages.
 * 
 * <p>Each operation has a unique code that tells the server what action to perform.
 * OpCodes are organized by category with reserved ranges for each feature area.
 */
public enum OpCode {
    // ========== Core Operations (0x01-0x0F) ==========
    PRODUCE(0x01),
    CONSUME(0x02),
    CREATE_TOPIC(0x03),
    METADATA(0x04),
    SUBSCRIBE(0x05),
    COMMIT(0x06),
    FETCH(0x07),
    LIST_TOPICS(0x08),
    DELETE_TOPIC(0x09),

    // ========== Schema Operations (0x10-0x1F) ==========
    REGISTER_SCHEMA(0x10),
    GET_SCHEMA(0x11),
    LIST_SCHEMAS(0x12),
    VALIDATE_SCHEMA(0x13),
    PRODUCE_WITH_SCHEMA(0x14),
    DELETE_SCHEMA(0x15),

    // ========== Dead Letter Queue Operations (0x20-0x2F) ==========
    FETCH_DLQ(0x20),
    REPLAY_DLQ(0x21),
    PURGE_DLQ(0x22),

    // ========== Delayed Message Operations (0x30-0x3F) ==========
    PRODUCE_DELAYED(0x30),
    CANCEL_DELAYED(0x31),
    PRODUCE_WITH_TTL(0x35),

    // ========== Transaction Operations (0x40-0x4F) ==========
    BEGIN_TX(0x40),
    COMMIT_TX(0x41),
    ABORT_TX(0x42),
    PRODUCE_TX(0x43),

    // ========== Cluster Operations (0x50-0x5F) ==========
    CLUSTER_JOIN(0x50),
    CLUSTER_LEAVE(0x51),
    CLUSTER_STATUS(0x52),

    // ========== Consumer Group Operations (0x60-0x6F) ==========
    GET_OFFSET(0x60),
    RESET_OFFSET(0x61),
    LIST_GROUPS(0x62),
    DESCRIBE_GROUP(0x63),
    GET_LAG(0x64),
    DELETE_GROUP(0x65),
    SEEK_TO_TIMESTAMP(0x66),

    // ========== Authentication Operations (0x70-0x7F) ==========
    AUTH(0x70),
    AUTH_RESPONSE(0x71),
    WHOAMI(0x72),

    // ========== User Management Operations (0x73-0x7F) ==========
    USER_CREATE(0x73),
    USER_DELETE(0x74),
    USER_UPDATE(0x75),
    USER_LIST(0x76),
    USER_GET(0x77),
    ACL_SET(0x78),
    ACL_GET(0x79),
    ACL_DELETE(0x7A),
    ACL_LIST(0x7B),
    PASSWORD_CHANGE(0x7C),
    ROLE_LIST(0x7D),

    // ========== Error Response (0xFF) ==========
    ERROR(0xFF);

    private final int code;

    OpCode(int code) {
        this.code = code;
    }

    /**
     * Gets the numeric code for this operation.
     *
     * @return the operation code as a byte value
     */
    public byte getCode() {
        return (byte) code;
    }

    /**
     * Gets the numeric code as an integer.
     *
     * @return the operation code
     */
    public int getCodeAsInt() {
        return code;
    }

    /**
     * Finds an OpCode by its numeric value.
     *
     * @param code the numeric code
     * @return the corresponding OpCode
     * @throws IllegalArgumentException if no OpCode matches the given code
     */
    public static OpCode fromCode(int code) {
        for (OpCode op : values()) {
            if (op.code == code) {
                return op;
            }
        }
        throw new IllegalArgumentException("Unknown OpCode: 0x" + Integer.toHexString(code));
    }
}
