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

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.exception.FlyMQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * High-level producer with batching, callbacks, and retries.
 *
 * <p>Similar to Kafka's KafkaProducer, this producer:
 * <ul>
 *   <li>Batches messages for improved throughput</li>
 *   <li>Supports async send with callbacks</li>
 *   <li>Automatically retries on transient failures</li>
 *   <li>Thread-safe for concurrent use</li>
 * </ul>
 *
 * <p>Example:
 * <pre>{@code
 * try (HighLevelProducer producer = new HighLevelProducer(client)) {
 *     // Simple send
 *     producer.send("topic", "message".getBytes());
 *     
 *     // With callback
 *     producer.send("topic", "data".getBytes())
 *         .whenComplete((metadata, error) -> {
 *             if (error != null) {
 *                 System.err.println("Failed: " + error);
 *             } else {
 *                 System.out.println("Sent at offset: " + metadata.offset());
 *             }
 *         });
 *     
 *     producer.flush();
 * }
 * }</pre>
 */
public class HighLevelProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HighLevelProducer.class);

    private final FlyMQClient client;
    private final ProducerConfig config;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final List<PendingRecord> batch = new ArrayList<>();
    private int batchBytes = 0;
    private long lastFlushTime = System.currentTimeMillis();

    private ScheduledExecutorService flusher;

    /**
     * Creates a producer with default configuration.
     *
     * @param client FlyMQ client
     */
    public HighLevelProducer(FlyMQClient client) {
        this(client, ProducerConfig.defaults());
    }

    /**
     * Creates a producer with custom configuration.
     *
     * @param client FlyMQ client
     * @param config producer configuration
     */
    public HighLevelProducer(FlyMQClient client, ProducerConfig config) {
        this.client = client;
        this.config = config;

        if (config.getLingerMs() > 0) {
            startFlusher();
        }
    }

    private void startFlusher() {
        flusher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "flymq-producer-flusher");
            t.setDaemon(true);
            return t;
        });

        flusher.scheduleAtFixedRate(() -> {
            lock.lock();
            try {
                if (!batch.isEmpty() && 
                    (System.currentTimeMillis() - lastFlushTime) >= config.getLingerMs()) {
                    flushBatch();
                }
            } finally {
                lock.unlock();
            }
        }, config.getLingerMs(), config.getLingerMs(), TimeUnit.MILLISECONDS);
    }

    /**
     * Sends a message to a topic.
     *
     * @param topic target topic
     * @param value message value
     * @return future that completes when the message is sent
     */
    public ProduceFuture send(String topic, byte[] value) {
        return send(topic, null, null, value);
    }

    /**
     * Sends a string message to a topic.
     *
     * @param topic target topic
     * @param value message value
     * @return future that completes when the message is sent
     */
    public ProduceFuture send(String topic, String value) {
        return send(topic, value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Sends a message with a key.
     *
     * @param topic target topic
     * @param key   message key for partitioning
     * @param value message value
     * @return future that completes when the message is sent
     */
    public ProduceFuture send(String topic, byte[] key, byte[] value) {
        return send(topic, null, key, value);
    }

