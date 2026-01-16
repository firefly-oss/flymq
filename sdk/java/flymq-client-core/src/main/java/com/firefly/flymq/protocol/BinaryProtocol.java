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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Binary protocol encoder/decoder for FlyMQ binary message format.
 * All methods use big-endian byte order matching the Go server implementation.
 */
public final class BinaryProtocol {

    private BinaryProtocol() {
        // Utility class
    }

    // =========================================================================
    // Primitive Encoding Helpers
    // =========================================================================

    /**
     * Encodes a string with 2-byte length prefix.
     */
    public static byte[] encodeString(String value) {
        byte[] bytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
        ByteBuffer buf = ByteBuffer.allocate(2 + bytes.length).order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) bytes.length);
        buf.put(bytes);
        return buf.array();
    }

    /**
     * Encodes a byte array with 4-byte length prefix.
     */
    public static byte[] encodeBytes(byte[] data) {
        byte[] bytes = data != null ? data : new byte[0];
        ByteBuffer buf = ByteBuffer.allocate(4 + bytes.length).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(bytes.length);
        buf.put(bytes);
        return buf.array();
    }

    /**
     * Decodes a string with 2-byte length prefix.
     */
    public static String decodeString(ByteBuffer buf) {
        int len = buf.getShort() & 0xFFFF;
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Decodes a byte array with 4-byte length prefix.
     */
    public static byte[] decodeBytes(ByteBuffer buf) {
        int len = buf.getInt();
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return bytes;
    }

    // =========================================================================
    // Produce Request/Response
    // =========================================================================

    /**
     * Encodes a produce request.
     * Format: [2B topic_len][topic][4B key_len][key][4B value_len][value][4B partition]
     *
     * Note: This uses the full binary format expected by the server.
     * For messages without a key, an empty key (length 0) is sent.
     * Partition -1 means auto-partition selection.
     */
    public static byte[] encodeProduceRequest(String topic, byte[] data) {
        return encodeProduceWithKeyAndPartitionRequest(topic, new byte[0], data, -1);
    }

    /**
     * Encodes a produce request with key.
     * Format: [2B topic_len][topic][4B key_len][key][4B value_len][value][4B partition]
     *
     * Messages with the same key are routed to the same partition.
     */
    public static byte[] encodeProduceWithKeyRequest(String topic, byte[] key, byte[] data) {
        return encodeProduceWithKeyAndPartitionRequest(topic, key, data, -1);
    }

    /**
     * Encodes a produce request with partition.
     * Format: [2B topic_len][topic][4B key_len][key][4B value_len][value][4B partition]
     *
     * Explicitly routes the message to the specified partition.
     */
    public static byte[] encodeProduceWithPartitionRequest(String topic, int partition, byte[] data) {
        return encodeProduceWithKeyAndPartitionRequest(topic, new byte[0], data, partition);
    }

    /**
     * Encodes a produce request with both key and partition.
     * Format: [2B topic_len][topic][4B key_len][key][4B value_len][value][4B partition]
     *
     * This is the canonical encoding method that matches the server's expected format.
     */
    public static byte[] encodeProduceWithKeyAndPartitionRequest(String topic, byte[] key, byte[] data, int partition) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        byte[] keyBytes = key != null ? key : new byte[0];
        byte[] dataBytes = data != null ? data : new byte[0];

        // Format: [2B topic_len][topic][4B key_len][key][4B value_len][value][4B partition]
        ByteBuffer buf = ByteBuffer.allocate(2 + topicBytes.length + 4 + keyBytes.length + 4 + dataBytes.length + 4)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        buf.putInt(dataBytes.length);
        buf.put(dataBytes);
        buf.putInt(partition);
        return buf.array();
    }

    /**
     * RecordMetadata represents complete metadata for a produced record.
     * Similar to Kafka's RecordMetadata, this contains all information about
     * where and when the message was stored.
     *
     * @param topic Topic name
     * @param partition Partition the record was sent to
     * @param offset Offset of the record in the partition
     * @param timestamp Timestamp in milliseconds (Unix epoch)
     * @param keySize Size of the key in bytes (-1 if no key)
     * @param valueSize Size of the value in bytes
     */
    public record RecordMetadata(
        String topic,
        int partition,
        long offset,
        long timestamp,
        int keySize,
        int valueSize
    ) {
        /**
         * Returns true if the record has a key.
         */
        public boolean hasKey() {
            return keySize >= 0;
        }

        /**
         * Returns the timestamp as an Instant.
         */
        public java.time.Instant timestampAsInstant() {
            return java.time.Instant.ofEpochMilli(timestamp);
        }
    }

    /**
     * Decodes RecordMetadata from binary format.
     * Format: [2B topic_len][topic][4B partition][8B offset][8B timestamp][4B key_size][4B value_size]
     *
     * @param payload the response payload
     * @return RecordMetadata with complete metadata
     */
    public static RecordMetadata decodeRecordMetadata(byte[] payload) {
        if (payload.length < 2) {
            throw new IllegalArgumentException("Buffer too small: " + payload.length + " < 2");
        }

        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);

        // Topic
        int topicLen = buf.getShort() & 0xFFFF;
        if (payload.length < 2 + topicLen + 28) { // 4+8+8+4+4 = 28
            throw new IllegalArgumentException("Buffer too small for RecordMetadata");
        }
        byte[] topicBytes = new byte[topicLen];
        buf.get(topicBytes);
        String topic = new String(topicBytes, StandardCharsets.UTF_8);

        // Partition
        int partition = buf.getInt();

        // Offset
        long offset = buf.getLong();

        // Timestamp
        long timestamp = buf.getLong();

        // Key size
        int keySize = buf.getInt();

        // Value size
        int valueSize = buf.getInt();

        return new RecordMetadata(topic, partition, offset, timestamp, keySize, valueSize);
    }

    /**
     * Decodes a produce response (legacy, returns only offset).
     * @deprecated Use {@link #decodeRecordMetadata(byte[])} for complete metadata
     */
    @Deprecated
    public static long decodeProduceResponse(byte[] payload) {
        RecordMetadata meta = decodeRecordMetadata(payload);
        return meta.offset();
    }

    // =========================================================================
    // Consume Request/Response
    // =========================================================================

    /**
     * Encodes a consume request.
     * Format: [2B topic_len][topic][4B partition][8B offset]
     */
    public static byte[] encodeConsumeRequest(String topic, int partition, long offset) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + topicBytes.length + 4 + 8)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(partition);
        buf.putLong(offset);
        return buf.array();
    }

    /**
     * Decodes a consume response.
     * Format: [4B key_len][key][4B value_len][value]
     *
     * This matches the server's BinaryConsumeResponse format.
     * Note: offset and timestamp are not included in the binary response.
     */
    public static ConsumeResult decodeConsumeResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        byte[] key = decodeBytes(buf);
        byte[] data = decodeBytes(buf);
        // Server does not send offset and timestamp in binary consume response
        return new ConsumeResult(key, data, -1, -1);
    }

    /**
     * Result of a consume operation.
     *
     * @param key The message key (may be empty)
     * @param data The message value/data
     * @param offset The message offset (-1 if not available in binary response)
     * @param timestamp The message timestamp (-1 if not available in binary response)
     */
    public record ConsumeResult(byte[] key, byte[] data, long offset, long timestamp) {}

    // =========================================================================
    // Topic Operations
    // =========================================================================

    /**
     * Encodes a create topic request.
     * Format: [2B topic_len][topic][4B partitions]
     */
    public static byte[] encodeCreateTopicRequest(String topic, int partitions) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + topicBytes.length + 4)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(partitions);
        return buf.array();
    }

    /**
     * Encodes a delete topic request.
     * Format: [2B topic_len][topic]
     */
    public static byte[] encodeDeleteTopicRequest(String topic) {
        return encodeString(topic);
    }

    /**
     * Decodes a list topics response.
     * Format: [4B count][topics...]
     */
    public static List<String> decodeListTopicsResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int count = buf.getInt();
        List<String> topics = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            topics.add(decodeString(buf));
        }
        return topics;
    }

    // =========================================================================
    // Authentication
    // =========================================================================

    /**
     * Encodes an authentication request.
     * Format: [2B user_len][user][2B pass_len][pass]
     */
    public static byte[] encodeAuthRequest(String username, String password) {
        byte[] userBytes = username.getBytes(StandardCharsets.UTF_8);
        byte[] passBytes = password.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + userBytes.length + 2 + passBytes.length)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) userBytes.length);
        buf.put(userBytes);
        buf.putShort((short) passBytes.length);
        buf.put(passBytes);
        return buf.array();
    }

    /**
     * Decodes an authentication response.
     * Format: [1B success][2B user_len][user][4B role_count][roles...][4B perm_count][perms...]
     */
    public static AuthResult decodeAuthResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        boolean success = buf.get() == 1;
        String username = decodeString(buf);
        int roleCount = buf.getInt();
        List<String> roles = new ArrayList<>(roleCount);
        for (int i = 0; i < roleCount; i++) {
            roles.add(decodeString(buf));
        }
        int permCount = buf.getInt();
        List<String> permissions = new ArrayList<>(permCount);
        for (int i = 0; i < permCount; i++) {
            permissions.add(decodeString(buf));
        }
        return new AuthResult(success, username, roles, permissions);
    }

    public record AuthResult(boolean success, String username, List<String> roles, List<String> permissions) {}

    // =========================================================================
    // Transaction Operations
    // =========================================================================

    /**
     * Encodes a transaction ID request (for commit/rollback).
     * Format: [2B txn_id_len][txn_id]
     */
    public static byte[] encodeTxnRequest(String txnId) {
        return encodeString(txnId);
    }

    /**
     * Encodes a transaction produce request.
     * Format: [2B txn_id_len][txn_id][2B topic_len][topic][4B data_len][data]
     */
    public static byte[] encodeTxnProduceRequest(String txnId, String topic, byte[] data) {
        byte[] txnBytes = txnId.getBytes(StandardCharsets.UTF_8);
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + txnBytes.length + 2 + topicBytes.length + 4 + data.length)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) txnBytes.length);
        buf.put(txnBytes);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(data.length);
        buf.put(data);
        return buf.array();
    }

    /**
     * Decodes a transaction response (begin transaction).
     * Returns just the transaction ID.
     * Format: [2B txn_id_len][txn_id]
     */
    public static String decodeTxnResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        return decodeString(buf);
    }

    // =========================================================================
    // DLQ Operations
    // =========================================================================

    /**
     * Encodes a DLQ fetch request.
     * Format: [2B topic_len][topic][4B max_messages]
     */
    public static byte[] encodeDLQRequest(String topic, int maxMessages) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + topicBytes.length + 4)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(maxMessages);
        return buf.array();
    }

    /**
     * Encodes a replay DLQ request.
     * Format: [2B topic_len][topic][2B msg_id_len][msg_id]
     */
    public static byte[] encodeReplayDLQRequest(String topic, String messageId) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        byte[] msgIdBytes = messageId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + topicBytes.length + 2 + msgIdBytes.length)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putShort((short) msgIdBytes.length);
        buf.put(msgIdBytes);
        return buf.array();
    }

    /**
     * Encodes a purge DLQ request.
     * Format: [2B topic_len][topic]
     */
    public static byte[] encodePurgeDLQRequest(String topic) {
        return encodeString(topic);
    }

    /**
     * Decodes a DLQ response.
     * Format: [4B count][messages...]
     */
    public static List<Map<String, Object>> decodeDLQResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int count = buf.getInt();
        List<Map<String, Object>> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String id = decodeString(buf);
            byte[] data = decodeBytes(buf);
            String error = decodeString(buf);
            int retries = buf.getInt();
            Map<String, Object> msg = new HashMap<>();
            msg.put("id", id);
            msg.put("data", java.util.Base64.getEncoder().encodeToString(data));
            msg.put("error", error);
            msg.put("retries", retries);
            messages.add(msg);
        }
        return messages;
    }

    public record DLQMessage(String id, byte[] data, String error, int retries) {}

    // =========================================================================
    // Schema Operations
    // =========================================================================

    /**
     * Encodes a register schema request.
     * Format: [2B name_len][name][1B type_len][type][4B schema_len][schema]
     */
    public static byte[] encodeRegisterSchemaRequest(String name, String type, byte[] schema) {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        byte[] typeBytes = type.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + nameBytes.length + 1 + typeBytes.length + 4 + schema.length)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) nameBytes.length);
        buf.put(nameBytes);
        buf.put((byte) typeBytes.length);
        buf.put(typeBytes);
        buf.putInt(schema.length);
        buf.put(schema);
        return buf.array();
    }

    /**
     * Encodes a list schemas request.
     * Format: [2B topic_len][topic]
     */
    public static byte[] encodeListSchemasRequest(String topic) {
        return encodeString(topic != null ? topic : "");
    }

    /**
     * Decodes a list schemas response.
     * Format: [4B count][schemas...]
     */
    public static List<Map<String, Object>> decodeListSchemasResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int count = buf.getInt();
        List<Map<String, Object>> schemas = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String name = decodeString(buf);
            int typeLen = buf.get() & 0xFF;
            byte[] typeBytes = new byte[typeLen];
            buf.get(typeBytes);
            String type = new String(typeBytes, StandardCharsets.UTF_8);
            int version = buf.getInt();
            Map<String, Object> schema = new HashMap<>();
            schema.put("name", name);
            schema.put("type", type);
            schema.put("version", version);
            schemas.add(schema);
        }
        return schemas;
    }

    public record SchemaInfo(String name, String type, int version) {}

    // =========================================================================
    // Delayed/TTL Operations
    // =========================================================================

    /**
     * Encodes a produce delayed request.
     * Format: [2B topic_len][topic][4B data_len][data][8B delay_ms]
     */
    public static byte[] encodeProduceDelayedRequest(String topic, byte[] data, long delayMs) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + topicBytes.length + 4 + data.length + 8)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(data.length);
        buf.put(data);
        buf.putLong(delayMs);
        return buf.array();
    }

    /**
     * Encodes a produce with TTL request.
     * Format: [2B topic_len][topic][4B data_len][data][8B ttl_ms]
     */
    public static byte[] encodeProduceWithTTLRequest(String topic, byte[] data, long ttlMs) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + topicBytes.length + 4 + data.length + 8)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(data.length);
        buf.put(data);
        buf.putLong(ttlMs);
        return buf.array();
    }

    /**
     * Encodes a produce with schema request.
     * Format: [2B topic_len][topic][4B data_len][data][2B schema_len][schema]
     */
    public static byte[] encodeProduceWithSchemaRequest(String topic, byte[] data, String schemaName) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        byte[] schemaBytes = schemaName.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + topicBytes.length + 4 + data.length + 2 + schemaBytes.length)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(data.length);
        buf.put(data);
        buf.putShort((short) schemaBytes.length);
        buf.put(schemaBytes);
        return buf.array();
    }

    // =========================================================================
    // User Management Operations
    // =========================================================================

    /**
     * Encodes a user request (create/update).
     * Format: [2B user_len][user][2B pass_len][pass][2B old_pass_len][old_pass][4B role_count][roles...][1B enabled]
     */
    public static byte[] encodeUserRequest(String username, String password, String oldPassword, 
                                           List<String> roles, boolean enabled) {
        byte[] userBytes = username.getBytes(StandardCharsets.UTF_8);
        byte[] passBytes = password != null ? password.getBytes(StandardCharsets.UTF_8) : new byte[0];
        byte[] oldPassBytes = oldPassword != null ? oldPassword.getBytes(StandardCharsets.UTF_8) : new byte[0];
        
        int rolesSize = 4;
        for (String role : roles) {
            rolesSize += 2 + role.getBytes(StandardCharsets.UTF_8).length;
        }
        
        ByteBuffer buf = ByteBuffer.allocate(2 + userBytes.length + 2 + passBytes.length + 
                2 + oldPassBytes.length + rolesSize + 1)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) userBytes.length);
        buf.put(userBytes);
        buf.putShort((short) passBytes.length);
        buf.put(passBytes);
        buf.putShort((short) oldPassBytes.length);
        buf.put(oldPassBytes);
        buf.putInt(roles.size());
        for (String role : roles) {
            byte[] roleBytes = role.getBytes(StandardCharsets.UTF_8);
            buf.putShort((short) roleBytes.length);
            buf.put(roleBytes);
        }
        buf.put((byte) (enabled ? 1 : 0));
        return buf.array();
    }

    /**
     * Decodes a user list response.
     */
    public static List<UserInfo> decodeUserListResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int count = buf.getInt();
        List<UserInfo> users = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String username = decodeString(buf);
            int roleCount = buf.getInt();
            List<String> roles = new ArrayList<>(roleCount);
            for (int j = 0; j < roleCount; j++) {
                roles.add(decodeString(buf));
            }
            int permCount = buf.getInt();
            List<String> permissions = new ArrayList<>(permCount);
            for (int j = 0; j < permCount; j++) {
                permissions.add(decodeString(buf));
            }
            boolean enabled = buf.get() == 1;
            long createdAt = buf.getLong();
            long updatedAt = buf.getLong();
            users.add(new UserInfo(username, roles, permissions, enabled, createdAt, updatedAt));
        }
        return users;
    }

    /**
     * Decodes a single user info response.
     */
    public static UserInfo decodeUserInfo(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        String username = decodeString(buf);
        int roleCount = buf.getInt();
        List<String> roles = new ArrayList<>(roleCount);
        for (int j = 0; j < roleCount; j++) {
            roles.add(decodeString(buf));
        }
        int permCount = buf.getInt();
        List<String> permissions = new ArrayList<>(permCount);
        for (int j = 0; j < permCount; j++) {
            permissions.add(decodeString(buf));
        }
        boolean enabled = buf.get() == 1;
        long createdAt = buf.getLong();
        long updatedAt = buf.getLong();
        return new UserInfo(username, roles, permissions, enabled, createdAt, updatedAt);
    }

    public record UserInfo(String username, List<String> roles, List<String> permissions, 
                           boolean enabled, long createdAt, long updatedAt) {}

    // =========================================================================
    // ACL Operations
    // =========================================================================

    /**
     * Encodes an ACL set request.
     * Format: [2B topic_len][topic][1B public][4B user_count][users...][4B role_count][roles...]
     */
    public static byte[] encodeACLRequest(String topic, boolean isPublic, 
                                          List<String> allowedUsers, List<String> allowedRoles) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        
        int usersSize = 4;
        for (String user : allowedUsers) {
            usersSize += 2 + user.getBytes(StandardCharsets.UTF_8).length;
        }
        int rolesSize = 4;
        for (String role : allowedRoles) {
            rolesSize += 2 + role.getBytes(StandardCharsets.UTF_8).length;
        }
        
        ByteBuffer buf = ByteBuffer.allocate(2 + topicBytes.length + 1 + usersSize + rolesSize)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.put((byte) (isPublic ? 1 : 0));
        buf.putInt(allowedUsers.size());
        for (String user : allowedUsers) {
            byte[] userBytes = user.getBytes(StandardCharsets.UTF_8);
            buf.putShort((short) userBytes.length);
            buf.put(userBytes);
        }
        buf.putInt(allowedRoles.size());
        for (String role : allowedRoles) {
            byte[] roleBytes = role.getBytes(StandardCharsets.UTF_8);
            buf.putShort((short) roleBytes.length);
            buf.put(roleBytes);
        }
        return buf.array();
    }

    /**
     * Decodes an ACL info response.
     */
    public static ACLInfo decodeACLInfo(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        String topic = decodeString(buf);
        boolean exists = buf.get() == 1;
        boolean isPublic = buf.get() == 1;
        boolean defaultPublic = buf.get() == 1;
        int userCount = buf.getInt();
        List<String> allowedUsers = new ArrayList<>(userCount);
        for (int i = 0; i < userCount; i++) {
            allowedUsers.add(decodeString(buf));
        }
        int roleCount = buf.getInt();
        List<String> allowedRoles = new ArrayList<>(roleCount);
        for (int i = 0; i < roleCount; i++) {
            allowedRoles.add(decodeString(buf));
        }
        return new ACLInfo(topic, exists, isPublic, defaultPublic, allowedUsers, allowedRoles);
    }

    /**
     * Decodes an ACL list response.
     */
    public static ACLListResult decodeACLListResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int count = buf.getInt();
        List<ACLInfo> acls = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String topic = decodeString(buf);
            boolean exists = buf.get() == 1;
            boolean isPublic = buf.get() == 1;
            int userCount = buf.getInt();
            List<String> allowedUsers = new ArrayList<>(userCount);
            for (int j = 0; j < userCount; j++) {
                allowedUsers.add(decodeString(buf));
            }
            int roleCount = buf.getInt();
            List<String> allowedRoles = new ArrayList<>(roleCount);
            for (int j = 0; j < roleCount; j++) {
                allowedRoles.add(decodeString(buf));
            }
            acls.add(new ACLInfo(topic, exists, isPublic, false, allowedUsers, allowedRoles));
        }
        boolean defaultPublic = buf.hasRemaining() && buf.get() == 1;
        return new ACLListResult(acls, defaultPublic);
    }

    public record ACLInfo(String topic, boolean exists, boolean isPublic, boolean defaultPublic, 
                          List<String> allowedUsers, List<String> allowedRoles) {}
    
    public record ACLListResult(List<ACLInfo> acls, boolean defaultPublic) {}

    // =========================================================================
    // Role Operations
    // =========================================================================

    /**
     * Decodes a role list response.
     */
    public static List<RoleInfo> decodeRoleListResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int count = buf.getInt();
        List<RoleInfo> roles = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String name = decodeString(buf);
            int permCount = buf.getInt();
            List<String> permissions = new ArrayList<>(permCount);
            for (int j = 0; j < permCount; j++) {
                permissions.add(decodeString(buf));
            }
            String description = decodeString(buf);
            roles.add(new RoleInfo(name, permissions, description));
        }
        return roles;
    }

    public record RoleInfo(String name, List<String> permissions, String description) {}

    // =========================================================================
    // Cluster Operations
    // =========================================================================

    /**
     * Encodes a cluster join request.
     * Format: [2B peer_len][peer]
     */
    public static byte[] encodeClusterJoinRequest(String peerAddr) {
        return encodeString(peerAddr);
    }

    // =========================================================================
    // Consumer Group Operations
    // =========================================================================

    /**
     * Encodes a subscribe request for partition 0.
     * Format: [2B topic_len][topic][2B group_len][group][4B partition][1B mode_len][mode]
     *
     * This is a convenience method that subscribes to partition 0.
     */
    public static byte[] encodeSubscribeRequest(String topic, String groupId, String mode) {
        return encodeSubscribeRequest(topic, groupId, 0, mode);
    }

    /**
     * Encodes a subscribe request with partition.
     * Format: [2B topic_len][topic][2B group_len][group][4B partition][1B mode_len][mode]
     */
    public static byte[] encodeSubscribeRequest(String topic, String groupId, int partition, String mode) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        byte[] groupBytes = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] modeBytes = mode.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + topicBytes.length + 2 + groupBytes.length + 4 + 1 + modeBytes.length)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putShort((short) groupBytes.length);
        buf.put(groupBytes);
        buf.putInt(partition);
        buf.put((byte) modeBytes.length);
        buf.put(modeBytes);
        return buf.array();
    }

    /**
     * Decodes a subscribe response.
     * Format: [8B offset]
     */
    public static long decodeSubscribeResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        return buf.getLong();
    }

    /**
     * Encodes a get offset request.
     * Format: [2B topic_len][topic][2B group_len][group][4B partition]
     */
    public static byte[] encodeGetOffsetRequest(String topic, String groupId, int partition) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        byte[] groupBytes = groupId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + topicBytes.length + 2 + groupBytes.length + 4)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putShort((short) groupBytes.length);
        buf.put(groupBytes);
        buf.putInt(partition);
        return buf.array();
    }

    /**
     * Decodes an offset response.
     * Format: [8B offset]
     */
    public static long decodeOffsetResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        return buf.getLong();
    }

    /**
     * Encodes a reset offset request.
     * Format: [2B topic_len][topic][2B group_len][group][4B partition][1B mode_len][mode][8B offset]
     */
    public static byte[] encodeResetOffsetRequest(String topic, String groupId, int partition, String mode, long offset) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        byte[] groupBytes = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] modeBytes = mode.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + topicBytes.length + 2 + groupBytes.length + 4 + 1 + modeBytes.length + 8)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putShort((short) groupBytes.length);
        buf.put(groupBytes);
        buf.putInt(partition);
        buf.put((byte) modeBytes.length);
        buf.put(modeBytes);
        buf.putLong(offset);
        return buf.array();
    }

    /**
     * Encodes a get lag request.
     * Format: [2B topic_len][topic][2B group_len][group][4B partition]
     */
    public static byte[] encodeGetLagRequest(String topic, String groupId, int partition) {
        return encodeGetOffsetRequest(topic, groupId, partition);
    }

    /**
     * Decodes a lag response.
     * Format: [8B current_offset][8B committed_offset][8B latest_offset][8B lag]
     */
    public static LagInfo decodeLagResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        long currentOffset = buf.getLong();
        long committedOffset = buf.getLong();
        long latestOffset = buf.getLong();
        long lag = buf.getLong();
        return new LagInfo(currentOffset, committedOffset, latestOffset, lag);
    }

    public record LagInfo(long currentOffset, long committedOffset, long latestOffset, long lag) {}

    /**
     * Decodes a list groups response.
     * Format: [4B count][groups...]
     */
    public static List<ConsumerGroupData> decodeListGroupsResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int count = buf.getInt();
        List<ConsumerGroupData> groups = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String groupId = decodeString(buf);
            String state = decodeString(buf);
            int memberCount = buf.getInt();
            List<String> members = new ArrayList<>(memberCount);
            for (int j = 0; j < memberCount; j++) {
                members.add(decodeString(buf));
            }
            int topicCount = buf.getInt();
            List<String> topics = new ArrayList<>(topicCount);
            for (int j = 0; j < topicCount; j++) {
                topics.add(decodeString(buf));
            }
            String coordinator = decodeString(buf);
            groups.add(new ConsumerGroupData(groupId, state, members, topics, 
                coordinator.isEmpty() ? null : coordinator));
        }
        return groups;
    }

    /**
     * Encodes a describe group request.
     * Format: [2B group_len][group]
     */
    public static byte[] encodeDescribeGroupRequest(String groupId) {
        return encodeString(groupId);
    }

    /**
     * Decodes a describe group response.
     */
    public static ConsumerGroupData decodeDescribeGroupResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        String groupId = decodeString(buf);
        String state = decodeString(buf);
        int memberCount = buf.getInt();
        List<String> members = new ArrayList<>(memberCount);
        for (int j = 0; j < memberCount; j++) {
            members.add(decodeString(buf));
        }
        int topicCount = buf.getInt();
        List<String> topics = new ArrayList<>(topicCount);
        for (int j = 0; j < topicCount; j++) {
            topics.add(decodeString(buf));
        }
        String coordinator = decodeString(buf);
        return new ConsumerGroupData(groupId, state, members, topics, 
            coordinator.isEmpty() ? null : coordinator);
    }

    /**
     * Encodes a delete group request.
     * Format: [2B group_len][group]
     */
    public static byte[] encodeDeleteGroupRequest(String groupId) {
        return encodeString(groupId);
    }

    public record ConsumerGroupData(String groupId, String state, List<String> members, 
                                    List<String> topics, String coordinator) {}

    /**
     * Encodes a commit offset request.
     * Format: [2B topic_len][topic][2B group_len][group][4B partition][8B offset]
     */
    public static byte[] encodeCommitRequest(String topic, String groupId, int partition, long offset) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        byte[] groupBytes = groupId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + topicBytes.length + 2 + groupBytes.length + 4 + 8)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putShort((short) groupBytes.length);
        buf.put(groupBytes);
        buf.putInt(partition);
        buf.putLong(offset);
        return buf.array();
    }

    /**
     * Encodes a fetch request (batch consume).
     * Format: [2B topic_len][topic][4B partition][8B offset][4B max_messages]
     *
     * This matches the server's expected BinaryFetchRequest format.
     *
     * @param topic The topic to fetch from
     * @param partition The partition to fetch from
     * @param offset The starting offset
     * @param maxMessages Maximum number of messages to fetch
     * @return Encoded fetch request bytes
     */
    public static byte[] encodeFetchRequest(String topic, int partition, long offset, int maxMessages) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + topicBytes.length + 4 + 8 + 4)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(partition);
        buf.putLong(offset);
        buf.putInt(maxMessages);
        return buf.array();
    }

    /**
     * Legacy fetch request encoding for consumer group-based consumption.
     * This is kept for backward compatibility but should be migrated to the new format.
     *
     * @deprecated Use {@link #encodeFetchRequest(String, int, long, int)} instead
     */
    @Deprecated
    public static byte[] encodeFetchRequestWithGroup(String topic, String groupId, int maxMessages, int timeoutMs) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        byte[] groupBytes = groupId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(2 + topicBytes.length + 2 + groupBytes.length + 4 + 4)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putShort((short) groupBytes.length);
        buf.put(groupBytes);
        buf.putInt(maxMessages);
        buf.putInt(timeoutMs);
        return buf.array();
    }

    /**
     * Decodes a fetch response (batch consume).
     * Format: [4B count][8B nextOffset][messages...]
     * Each message: [8B offset][4B keyLen][key][4B valueLen][value]
     *
     * This matches the server's EncodeBinaryFetchResponse format.
     */
    public static List<FetchedMessage> decodeFetchResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int count = buf.getInt();
        long nextOffset = buf.getLong(); // Read but not returned in this method

        List<FetchedMessage> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long offset = buf.getLong();
            byte[] key = decodeBytes(buf);
            byte[] data = decodeBytes(buf);
            // Server doesn't send timestamp or partition in fetch response
            messages.add(new FetchedMessage(key, data, offset, -1, 0));
        }
        return messages;
    }

    /**
     * Decodes a fetch response and returns both messages and next offset.
     * Format: [4B count][8B nextOffset][messages...]
     * Each message: [8B offset][4B keyLen][key][4B valueLen][value]
     */
    public static FetchResponseWithOffset decodeFetchResponseWithOffset(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int count = buf.getInt();
        long nextOffset = buf.getLong();

        List<FetchedMessage> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long offset = buf.getLong();
            byte[] key = decodeBytes(buf);
            byte[] data = decodeBytes(buf);
            messages.add(new FetchedMessage(key, data, offset, -1, 0));
        }
        return new FetchResponseWithOffset(messages, nextOffset);
    }

    /**
     * Result of a fetch operation including messages and next offset.
     */
    public record FetchResponseWithOffset(List<FetchedMessage> messages, long nextOffset) {}

    /**
     * A single fetched message.
     *
     * @param key The message key (may be empty)
     * @param data The message value/data
     * @param offset The message offset
     * @param timestamp The message timestamp (-1 if not available)
     * @param partition The partition (0 if not available in response)
     */
    public record FetchedMessage(byte[] key, byte[] data, long offset, long timestamp, int partition) {}

    // =========================================================================
    // Metadata Operations
    // =========================================================================

    /**
     * Encodes a metadata request.
     * Format: [2B topic_len][topic]
     */
    public static byte[] encodeMetadataRequest(String topic) {
        return encodeString(topic);
    }

    /**
     * Decodes a metadata response.
     */
    public static TopicMetadata decodeMetadataResponse(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        String topic = decodeString(buf);
        int partitions = buf.getInt();
        int replicationFactor = buf.getInt();
        return new TopicMetadata(topic, partitions, replicationFactor);
    }

    public record TopicMetadata(String topic, int partitions, int replicationFactor) {}

    // =========================================================================
    // Success Response
    // =========================================================================

    /**
     * Decodes a simple success response.
     * Format: [1B success][2B msg_len][msg]
     */
    public static SuccessResult decodeSuccessResponse(byte[] payload) {
        if (payload == null || payload.length < 1) {
            return new SuccessResult(false, "");
        }
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        boolean success = buf.get() == 1;
        String message = "";
        if (buf.remaining() >= 2) {
            message = decodeString(buf);
        }
        return new SuccessResult(success, message);
    }

    public record SuccessResult(boolean success, String message) {}
}
