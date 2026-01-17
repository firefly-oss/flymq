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

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.exception.FlyMQException;
import com.firefly.flymq.protocol.Records.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * High-level consumer with Kafka-like API for consuming messages from FlyMQ.
 *
 * <p>Example usage:
 * <pre>{@code
 * try (Consumer consumer = new Consumer(client, "my-topic", "my-group")) {
 *     consumer.subscribe();
 *     while (true) {
 *         List<ConsumedMessage> messages = consumer.poll(Duration.ofSeconds(1));
 *         for (ConsumedMessage msg : messages) {
 *             process(msg);
 *         }
 *         consumer.commitSync();
 *     }
 * }
 * }</pre>
 */
public class Consumer implements AutoCloseable {

    private final FlyMQClient client;
    private final String topic;
    private final String groupId;
    private final int partition;
    private final ConsumerConfig config;

    private final AtomicLong currentOffset = new AtomicLong(0);
    private final AtomicLong lastCommittedOffset = new AtomicLong(-1);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean subscribed = new AtomicBoolean(false);

    private long lastAutoCommitTime = 0;

    /**
     * Creates a new consumer.
     *
     * @param client  FlyMQ client
     * @param topic   topic to consume from
     * @param groupId consumer group ID
     */
    public Consumer(FlyMQClient client, String topic, String groupId) {
        this(client, topic, groupId, 0, ConsumerConfig.defaults());
    }

    /**
     * Creates a new consumer with configuration.
     *
     * @param client    FlyMQ client
     * @param topic     topic to consume from
     * @param groupId   consumer group ID
     * @param partition partition to consume from
     * @param config    consumer configuration
     */
    public Consumer(FlyMQClient client, String topic, String groupId, int partition, ConsumerConfig config) {
        this.client = client;
        this.topic = topic;
        this.groupId = groupId;
        this.partition = partition;
        this.config = config;
    }

    /**
     * Subscribes to the topic starting from the committed offset or earliest.
     *
     * @throws FlyMQException if subscription fails
     */
    public void subscribe() throws FlyMQException {
        subscribe(SubscribeMode.COMMIT);
    }

    /**
     * Subscribes to the topic with the specified start mode.
     *
     * @param mode start position (EARLIEST, LATEST, or COMMIT)
     * @throws FlyMQException if subscription fails
     */
    public void subscribe(SubscribeMode mode) throws FlyMQException {
        ensureNotClosed();
        long offset = client.subscribe(topic, groupId, partition, mode);
        currentOffset.set(offset);
        subscribed.set(true);
    }

    /**
     * Polls for messages.
     *
     * @param timeout maximum time to wait
     * @return list of consumed messages
     * @throws FlyMQException if polling fails
     */
    public List<ConsumedMessage> poll(Duration timeout) throws FlyMQException {
        ensureNotClosed();
        ensureSubscribed();

        // Auto-commit if enabled
        maybeAutoCommit();

        FetchResult result = client.fetch(topic, partition, currentOffset.get(), config.maxPollRecords(), config.messageFilter());
        currentOffset.set(result.nextOffset());

        return result.messages();
    }

    /**
     * Commits the current offset synchronously.
     *
     * @throws FlyMQException if commit fails
     */
    public void commitSync() throws FlyMQException {
        ensureNotClosed();
        long offset = currentOffset.get();
        client.commitOffset(topic, groupId, partition, offset);
        lastCommittedOffset.set(offset);
        lastAutoCommitTime = System.currentTimeMillis();
    }

    /**
     * Commits a specific offset synchronously.
     *
     * @param offset offset to commit
     * @throws FlyMQException if commit fails
     */
    public void commitSync(long offset) throws FlyMQException {
        ensureNotClosed();
        client.commitOffset(topic, groupId, partition, offset);
        lastCommittedOffset.set(offset);
    }

    /**
     * Seeks to a specific offset.
     *
     * @param offset offset to seek to
     */
    public void seek(long offset) {
        ensureNotClosed();
        currentOffset.set(offset);
    }

    /**
     * Seeks to the beginning of the partition.
     *
     * @throws FlyMQException if seek fails
     */
    public void seekToBeginning() throws FlyMQException {
        ensureNotClosed();
        long offset = client.subscribe(topic, groupId, partition, SubscribeMode.EARLIEST);
        currentOffset.set(offset);
    }

    /**
     * Seeks to the end of the partition (latest offset).
     *
     * @throws FlyMQException if seek fails
     */
    public void seekToEnd() throws FlyMQException {
        ensureNotClosed();
        long offset = client.subscribe(topic, groupId, partition, SubscribeMode.LATEST);
        currentOffset.set(offset);
    }

    /**
     * Gets the current position (offset).
     *
     * @return current offset
     */
    public long position() {
        return currentOffset.get();
    }

    /**
     * Gets the last committed offset.
     *
     * @return committed offset, or -1 if never committed
     * @throws FlyMQException if operation fails
     */
    public long committed() throws FlyMQException {
        return client.getCommittedOffset(topic, groupId, partition);
    }

    /**
     * Gets the consumer lag.
     *
     * @return consumer lag information
     * @throws FlyMQException if operation fails
     */
    public ConsumerLag lag() throws FlyMQException {
        return client.getLag(topic, groupId, partition);
    }

    /**
     * Pauses consumption (no-op for now, for API compatibility).
     */
    public void pause() {
        // Future: implement pause functionality
    }

    /**
     * Resumes consumption (no-op for now, for API compatibility).
     */
    public void resume() {
        // Future: implement resume functionality
    }

    /**
     * Gets the topic being consumed.
     *
     * @return topic name
     */
    public String topic() {
        return topic;
    }

    /**
     * Gets the consumer group ID.
     *
     * @return group ID
     */
    public String groupId() {
        return groupId;
    }

    /**
     * Gets the partition being consumed.
     *
     * @return partition number
     */
    public int partition() {
        return partition;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // Commit any pending offsets if auto-commit is enabled
            if (config.enableAutoCommit() && lastCommittedOffset.get() < currentOffset.get()) {
                try {
                    commitSync();
                } catch (FlyMQException e) {
                    // Log but don't throw on close
                }
            }
        }
    }

    private void ensureNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException("Consumer is closed");
        }
    }

    private void ensureSubscribed() {
        if (!subscribed.get()) {
            throw new IllegalStateException("Consumer is not subscribed");
        }
    }

    private void maybeAutoCommit() throws FlyMQException {
        if (!config.enableAutoCommit()) {
            return;
        }

        long now = System.currentTimeMillis();
        if (now - lastAutoCommitTime >= config.autoCommitIntervalMs()) {
            if (lastCommittedOffset.get() < currentOffset.get()) {
                commitSync();
            }
            lastAutoCommitTime = now;
        }
    }
}

