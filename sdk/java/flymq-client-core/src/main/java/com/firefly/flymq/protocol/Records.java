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

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Immutable record types (DTOs) for FlyMQ protocol messages.
 */
public final class Records {

    private Records() {
        // Utility class
    }

    // ========== Core Records ==========

    /**
     * Result of a produce operation.
     */
    public record ProduceResult(
            String topic,
            long offset,
            int partition,
            Instant timestamp
    ) {
        public ProduceResult(String topic, long offset) {
            this(topic, offset, 0, Instant.now());
        }
    }

    /**
     * A message consumed from a topic.
     */
    public record ConsumedMessage(
            String topic,
            int partition,
            long offset,
            byte[] key,
            byte[] data,
            Instant timestamp,
            Map<String, String> headers
    ) {
        /**
         * Constructor without key (backward compatibility).
         */
        public ConsumedMessage(String topic, int partition, long offset, byte[] data) {
            this(topic, partition, offset, null, data, null, Map.of());
        }

        /**
         * Constructor with key but without timestamp/headers.
         */
        public ConsumedMessage(String topic, int partition, long offset, byte[] key, byte[] data) {
            this(topic, partition, offset, key, data, null, Map.of());
        }

        /**
         * Gets the message key as a UTF-8 string.
         * @return The key as a string, or null if no key.
         */
        public String keyAsString() {
            return key != null ? new String(key, java.nio.charset.StandardCharsets.UTF_8) : null;
        }

        /**
         * Gets the message data as a UTF-8 string.
         */
        public String dataAsString() {
            return new String(data, java.nio.charset.StandardCharsets.UTF_8);
        }

        /**
         * Checks if this message has a key.
         */
        public boolean hasKey() {
            return key != null && key.length > 0;
        }
    }

    /**
     * Result of a fetch operation.
     */
    public record FetchResult(
            List<ConsumedMessage> messages,
            long nextOffset
    ) {}

    /**
     * Metadata about a topic.
     */
    public record TopicMetadata(
            String name,
            int partitions,
            int replicationFactor,
            Map<String, String> config
    ) {
        public TopicMetadata(String name, int partitions) {
            this(name, partitions, 1, Map.of());
        }
    }

    /**
     * Subscribe mode for consumer positioning.
     */
    public enum SubscribeMode {
        EARLIEST("earliest"),
        LATEST("latest"),
        COMMIT("commit");

        private final String value;

        SubscribeMode(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    // ========== Schema Records ==========

    /**
     * Schema type enumeration.
     */
    public enum SchemaType {
        JSON("json"),
        AVRO("avro"),
        PROTOBUF("protobuf");

        private final String value;

        SchemaType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /**
     * Schema metadata.
     */
    public record SchemaInfo(
            String id,
            String name,
            SchemaType type,
            int version,
            String definition,
            Instant createdAt
    ) {}

    // ========== DLQ Records ==========

    /**
     * A message in the dead letter queue.
     */
    public record DLQMessage(
            String id,
            String topic,
            byte[] data,
            String error,
            int retries,
            Instant timestamp,
            Long originalOffset
    ) {}

    // ========== Transaction Records ==========

    /**
     * Transaction state.
     */
    public record TransactionInfo(
            String txnId,
            boolean active
    ) {}

    // ========== Cluster Records ==========

    /**
     * Node information.
     */
    public record NodeInfo(
            String id,
            String address,
            String clusterAddr,
            String state,
            String raftState,
            boolean isLeader,
            String uptime,
            double memoryUsedMb,
            int goroutines
    ) {}

    /**
     * Cluster information.
     */
    public record ClusterInfo(
            String clusterId,
            String leaderId,
            long raftTerm,
            long raftCommitIndex,
            int nodeCount,
            int topicCount,
            long totalMessages,
            List<NodeInfo> nodes
    ) {}

    // ========== Consumer Group Records ==========

    /**
     * Consumer group information.
     */
    public record ConsumerGroupInfo(
            String groupId,
            String state,
            List<String> members,
            List<String> topics,
            String coordinator
    ) {
        public ConsumerGroupInfo(String groupId, String state) {
            this(groupId, state, List.of(), List.of(), null);
        }
    }

    /**
     * Consumer group offset information.
     */
    public record ConsumerGroupOffset(
            String topic,
            int partition,
            long offset,
            long lag
    ) {}

    /**
     * Consumer lag information.
     */
    public record ConsumerLag(
            String topic,
            int partition,
            long currentOffset,
            long committedOffset,
            long latestOffset,
            long lag
    ) {}

    // ========== Request Records (for internal use) ==========

    public record ProduceRequest(String topic, byte[] data) {}
    public record ConsumeRequest(String topic, long offset) {}
    public record FetchRequest(String topic, int partition, long offset, int maxMessages) {}
    public record CreateTopicRequest(String topic, int partitions) {}
    public record DeleteTopicRequest(String topic) {}
    public record SubscribeRequest(String topic, String groupId, int partition, String mode) {}
    public record CommitRequest(String topic, String groupId, int partition, long offset) {}
    public record ProduceDelayedRequest(String topic, byte[] data, long delayMs) {}
    public record ProduceWithTTLRequest(String topic, byte[] data, long ttlMs) {}
    public record ProduceWithSchemaRequest(String topic, byte[] data, String schemaName) {}
    public record RegisterSchemaRequest(String name, String type, byte[] schema) {}
    public record TxnRequest(String txnId) {}
    public record TxnProduceRequest(String txnId, String topic, byte[] data) {}
    public record GetOffsetRequest(String topic, String groupId, int partition) {}
    public record ResetOffsetRequest(String topic, String groupId, int partition, String mode, Long offset) {}
    public record GetLagRequest(String topic, String groupId, int partition) {}
    public record DescribeGroupRequest(String groupId) {}
    public record DeleteGroupRequest(String groupId) {}

    // ========== Audit Trail Records ==========

    /**
     * Represents a single audit event.
     */
    public record AuditEvent(
            String id,
            Instant timestamp,
            String type,
            String user,
            String clientIp,
            String resource,
            String action,
            String result,
            Map<String, String> details,
            String nodeId
    ) {}

    /**
     * Filter criteria for querying audit events.
     */
    public record AuditQueryFilter(
            Instant startTime,
            Instant endTime,
            List<String> eventTypes,
            String user,
            String resource,
            String result,
            String search,
            int limit,
            int offset
    ) {
        public AuditQueryFilter() {
            this(null, null, List.of(), "", "", "", "", 100, 0);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private Instant startTime;
            private Instant endTime;
            private List<String> eventTypes = List.of();
            private String user = "";
            private String resource = "";
            private String result = "";
            private String search = "";
            private int limit = 100;
            private int offset = 0;

            public Builder startTime(Instant startTime) {
                this.startTime = startTime;
                return this;
            }

            public Builder endTime(Instant endTime) {
                this.endTime = endTime;
                return this;
            }

            public Builder eventTypes(List<String> eventTypes) {
                this.eventTypes = eventTypes;
                return this;
            }

            public Builder user(String user) {
                this.user = user;
                return this;
            }

            public Builder resource(String resource) {
                this.resource = resource;
                return this;
            }

            public Builder result(String result) {
                this.result = result;
                return this;
            }

            public Builder search(String search) {
                this.search = search;
                return this;
            }

            public Builder limit(int limit) {
                this.limit = limit;
                return this;
            }

            public Builder offset(int offset) {
                this.offset = offset;
                return this;
            }

            public AuditQueryFilter build() {
                return new AuditQueryFilter(startTime, endTime, eventTypes, user, resource, result, search, limit, offset);
            }
        }
    }

    /**
     * Result of an audit query.
     */
    public record AuditQueryResult(
            List<AuditEvent> events,
            int totalCount,
            boolean hasMore
    ) {}
    /**
     * Request to re-inject a DLQ message by offset.
     */
    public record ReInjectDLQRequest(
            String topic,
            long offset
    ) {}

    /**
     * Response to a success/failure operation.
     */
    public record SuccessResponse(
            boolean success,
            String message
    ) {}
}
