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
package com.firefly.flymq.consumer;

/**
 * Configuration for FlyMQ consumers.
 *
 * <p>This record provides Kafka-like configuration options for consumers.
 *
 * @param enableAutoCommit     whether to automatically commit offsets
 * @param autoCommitIntervalMs interval between auto-commits in milliseconds
 * @param maxPollRecords       maximum records to return per poll
 * @param sessionTimeoutMs     session timeout in milliseconds
 * @param heartbeatIntervalMs  heartbeat interval in milliseconds
 * @param autoOffsetReset      what to do when there is no initial offset ("earliest", "latest")
 */
public record ConsumerConfig(
        boolean enableAutoCommit,
        long autoCommitIntervalMs,
        int maxPollRecords,
        int sessionTimeoutMs,
        int heartbeatIntervalMs,
        String autoOffsetReset
) {

    /**
     * Default auto-commit interval (5 seconds).
     */
    public static final long DEFAULT_AUTO_COMMIT_INTERVAL_MS = 5000;

    /**
     * Default max poll records.
     */
    public static final int DEFAULT_MAX_POLL_RECORDS = 500;

    /**
     * Default session timeout (30 seconds).
     */
    public static final int DEFAULT_SESSION_TIMEOUT_MS = 30000;

    /**
     * Default heartbeat interval (3 seconds).
     */
    public static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 3000;

    /**
     * Creates a default configuration.
     *
     * @return default consumer configuration
     */
    public static ConsumerConfig defaults() {
        return new ConsumerConfig(
                true,
                DEFAULT_AUTO_COMMIT_INTERVAL_MS,
                DEFAULT_MAX_POLL_RECORDS,
                DEFAULT_SESSION_TIMEOUT_MS,
                DEFAULT_HEARTBEAT_INTERVAL_MS,
                "earliest"
        );
    }

    /**
     * Creates a builder for custom configuration.
     *
     * @return new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for ConsumerConfig.
     */
    public static class Builder {
        private boolean enableAutoCommit = true;
        private long autoCommitIntervalMs = DEFAULT_AUTO_COMMIT_INTERVAL_MS;
        private int maxPollRecords = DEFAULT_MAX_POLL_RECORDS;
        private int sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MS;
        private int heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS;
        private String autoOffsetReset = "earliest";

        public Builder enableAutoCommit(boolean enable) {
            this.enableAutoCommit = enable;
            return this;
        }

        public Builder autoCommitIntervalMs(long intervalMs) {
            this.autoCommitIntervalMs = intervalMs;
            return this;
        }

        public Builder maxPollRecords(int maxRecords) {
            this.maxPollRecords = maxRecords;
            return this;
        }

        public Builder sessionTimeoutMs(int timeoutMs) {
            this.sessionTimeoutMs = timeoutMs;
            return this;
        }

        public Builder heartbeatIntervalMs(int intervalMs) {
            this.heartbeatIntervalMs = intervalMs;
            return this;
        }

        public Builder autoOffsetReset(String reset) {
            this.autoOffsetReset = reset;
            return this;
        }

        public ConsumerConfig build() {
            return new ConsumerConfig(
                    enableAutoCommit,
                    autoCommitIntervalMs,
                    maxPollRecords,
                    sessionTimeoutMs,
                    heartbeatIntervalMs,
                    autoOffsetReset
            );
        }
    }
}

