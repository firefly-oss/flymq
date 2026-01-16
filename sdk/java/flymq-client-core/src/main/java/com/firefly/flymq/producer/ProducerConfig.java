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
package com.firefly.flymq.producer;

/**
 * Configuration for the high-level producer.
 *
 * <p>Example:
 * <pre>{@code
 * ProducerConfig config = ProducerConfig.builder()
 *     .batchSize(32768)
 *     .lingerMs(10)
 *     .retries(5)
 *     .build();
 * }</pre>
 */
public class ProducerConfig {

    private final int batchSize;
    private final int lingerMs;
    private final int maxBatchMessages;
    private final String acks;
    private final int retries;
    private final int retryBackoffMs;

    private ProducerConfig(Builder builder) {
        this.batchSize = builder.batchSize;
        this.lingerMs = builder.lingerMs;
        this.maxBatchMessages = builder.maxBatchMessages;
        this.acks = builder.acks;
        this.retries = builder.retries;
        this.retryBackoffMs = builder.retryBackoffMs;
    }

    public static ProducerConfig defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getBatchSize() { return batchSize; }
    public int getLingerMs() { return lingerMs; }
    public int getMaxBatchMessages() { return maxBatchMessages; }
    public String getAcks() { return acks; }
    public int getRetries() { return retries; }
    public int getRetryBackoffMs() { return retryBackoffMs; }

    public static class Builder {
        private int batchSize = 16384;
        private int lingerMs = 0;
        private int maxBatchMessages = 1000;
        private String acks = "leader";
        private int retries = 3;
        private int retryBackoffMs = 100;

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder lingerMs(int lingerMs) {
            this.lingerMs = lingerMs;
            return this;
        }

        public Builder maxBatchMessages(int maxBatchMessages) {
            this.maxBatchMessages = maxBatchMessages;
            return this;
        }

        public Builder acks(String acks) {
            this.acks = acks;
            return this;
        }

        public Builder retries(int retries) {
            this.retries = retries;
            return this;
        }

        public Builder retryBackoffMs(int retryBackoffMs) {
            this.retryBackoffMs = retryBackoffMs;
            return this;
        }

        public ProducerConfig build() {
            return new ProducerConfig(this);
        }
    }
}

