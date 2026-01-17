package com.firefly.flymq.consumer;

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.exception.FlyMQException;
import com.firefly.flymq.protocol.Records.ConsumedMessage;
import com.firefly.flymq.protocol.Records.SubscribeMode;
import com.firefly.flymq.protocol.TopicMatcher;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * High-level consumer group that handles multiple topics and pattern matching.
 * Similar to Kafka's KafkaConsumer but with FlyMQ's pattern matching.
 */
public class ConsumerGroup implements AutoCloseable {

    private final FlyMQClient client;
    private final List<String> topics;
    private final String groupId;
    private final ConsumerConfig config;
    private final Map<String, List<Consumer>> consumers = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ConsumerGroup(FlyMQClient client, List<String> topics, String groupId) {
        this(client, topics, groupId, ConsumerConfig.defaults());
    }

    public ConsumerGroup(FlyMQClient client, List<String> topics, String groupId, ConsumerConfig config) {
        this.client = client;
        this.topics = new ArrayList<>(topics);
        this.groupId = groupId;
        this.config = config;
    }

    public void subscribe() throws FlyMQException {
        if (closed.get()) throw new IllegalStateException("ConsumerGroup is closed");
        if (running.get()) return;

        List<String> allTopics;
        try {
            allTopics = client.listTopics();
        } catch (FlyMQException e) {
            allTopics = topics;
        }

        Set<String> resolvedTopics = new HashSet<>();
        for (String t : topics) {
            if (TopicMatcher.isPattern(t)) {
                for (String at : allTopics) {
                    if (TopicMatcher.matchPattern(t, at)) {
                        resolvedTopics.add(at);
                    }
                }
            } else {
                resolvedTopics.add(t);
            }
        }

        SubscribeMode mode = "earliest".equalsIgnoreCase(config.autoOffsetReset()) ? 
                SubscribeMode.EARLIEST : SubscribeMode.LATEST;

        for (String topic : resolvedTopics) {
            List<com.firefly.flymq.protocol.ClusterMetadata.PartitionMetadata> partitions;
            try {
                partitions = client.getClusterMetadata(topic).getTopicPartitions(topic);
            } catch (FlyMQException e) {
                partitions = List.of();
            }

            if (partitions.isEmpty()) {
                // Fallback to partition 0 if no metadata found
                Consumer consumer = new Consumer(client, topic, groupId, 0, config);
                consumer.subscribe(mode);
                consumers.computeIfAbsent(topic, k -> new ArrayList<>()).add(consumer);
            } else {
                for (com.firefly.flymq.protocol.ClusterMetadata.PartitionMetadata p : partitions) {
                    Consumer consumer = new Consumer(client, topic, groupId, p.getPartition(), config);
                    consumer.subscribe(mode);
                    consumers.computeIfAbsent(topic, k -> new ArrayList<>()).add(consumer);
                }
            }
        }

        running.set(true);
    }

    public List<ConsumedMessage> poll(Duration timeout) throws FlyMQException {
        if (!running.get()) subscribe();

        List<ConsumedMessage> allMessages = new ArrayList<>();
        for (List<Consumer> topicConsumers : consumers.values()) {
            for (Consumer consumer : topicConsumers) {
                allMessages.addAll(consumer.poll(timeout));
            }
        }
        return allMessages;
    }

    public void commitSync() throws FlyMQException {
        for (List<Consumer> topicConsumers : consumers.values()) {
            for (Consumer consumer : topicConsumers) {
                consumer.commitSync();
            }
        }
    }

    public Map<String, Long> getLag() throws FlyMQException {
        Map<String, Long> totalLag = new HashMap<>();
        for (String topic : topics) {
            if (TopicMatcher.isPattern(topic)) continue; // Patterns are handled via resolved topics in consumers map

            long lag = 0;
            List<Consumer> topicConsumers = consumers.get(topic);
            if (topicConsumers != null) {
                for (Consumer c : topicConsumers) {
                    lag += client.getLag(topic, groupId, c.getPartition()).getLag();
                }
            }
            totalLag.put(topic, lag);
        }
        
        // Also handle resolved topics from patterns
        for (Map.Entry<String, List<Consumer>> entry : consumers.entrySet()) {
            String topic = entry.getKey();
            if (!totalLag.containsKey(topic)) {
                long lag = 0;
                for (Consumer c : entry.getValue()) {
                    lag += client.getLag(topic, groupId, c.getPartition()).getLag();
                }
                totalLag.put(topic, lag);
            }
        }
        return totalLag;
    }

    public void resetOffsets(String mode) throws FlyMQException {
        for (Map.Entry<String, List<Consumer>> entry : consumers.entrySet()) {
            String topic = entry.getKey();
            for (Consumer c : entry.getValue()) {
                client.resetOffset(topic, groupId, c.getPartition(), mode, null);
            }
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            running.set(false);
            for (List<Consumer> topicConsumers : consumers.values()) {
                for (Consumer consumer : topicConsumers) {
                    consumer.close();
                }
            }
            consumers.clear();
        }
    }
}
