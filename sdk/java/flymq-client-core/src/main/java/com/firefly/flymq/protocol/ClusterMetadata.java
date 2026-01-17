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

import java.util.List;
import java.util.Optional;

/**
 * Cluster metadata containing partition-to-node mappings for smart routing.
 * 
 * <p>This enables clients to route requests directly to partition leaders,
 * improving throughput and reducing latency in distributed deployments.
 */
public class ClusterMetadata {
    private final String clusterId;
    private final List<TopicMetadata> topics;

    public ClusterMetadata(String clusterId, List<TopicMetadata> topics) {
        this.clusterId = clusterId;
        this.topics = topics;
    }

    public String getClusterId() {
        return clusterId;
    }

    public List<TopicMetadata> getTopics() {
        return topics;
    }

    /**
     * Get the leader info for a specific partition.
     *
     * @param topic the topic name
     * @param partition the partition number
     * @return the partition metadata, or empty if not found
     */
    public Optional<PartitionMetadata> getPartitionLeader(String topic, int partition) {
        return topics.stream()
                .filter(t -> t.getTopic().equals(topic))
                .flatMap(t -> t.getPartitions().stream())
                .filter(p -> p.getPartition() == partition)
                .findFirst();
    }

    /**
     * Get all partition info for a topic.
     *
     * @param topic the topic name
     * @return list of partition metadata
     */
    public List<PartitionMetadata> getTopicPartitions(String topic) {
        return topics.stream()
                .filter(t -> t.getTopic().equals(topic))
                .findFirst()
                .map(TopicMetadata::getPartitions)
                .orElse(List.of());
    }

    /**
     * Metadata for a topic's partitions.
     */
    public static class TopicMetadata {
        private final String topic;
        private final List<PartitionMetadata> partitions;

        public TopicMetadata(String topic, List<PartitionMetadata> partitions) {
            this.topic = topic;
            this.partitions = partitions;
        }

        public String getTopic() {
            return topic;
        }

        public List<PartitionMetadata> getPartitions() {
            return partitions;
        }
    }

    /**
     * Metadata for a single partition.
     */
    public static class PartitionMetadata {
        private final int partition;
        private final String leaderId;
        private final String leaderAddr;
        private final long epoch;
        private final String state;
        private final List<String> replicas;
        private final List<String> isr;

        public PartitionMetadata(int partition, String leaderId, String leaderAddr, long epoch) {
            this(partition, leaderId, leaderAddr, epoch, "online", List.of(leaderId), List.of(leaderId));
        }

        public PartitionMetadata(int partition, String leaderId, String leaderAddr, long epoch,
                                 String state, List<String> replicas, List<String> isr) {
            this.partition = partition;
            this.leaderId = leaderId;
            this.leaderAddr = leaderAddr;
            this.epoch = epoch;
            this.state = state;
            this.replicas = replicas;
            this.isr = isr;
        }

        public int getPartition() {
            return partition;
        }

        public String getLeaderId() {
            return leaderId;
        }

        public String getLeaderAddr() {
            return leaderAddr;
        }

        public long getEpoch() {
            return epoch;
        }

        public String getState() {
            return state;
        }

        public List<String> getReplicas() {
            return replicas;
        }

        public List<String> getIsr() {
            return isr;
        }
    }
}

