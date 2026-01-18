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
import com.firefly.flymq.protocol.BinaryProtocol;
import com.firefly.flymq.protocol.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

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

    /**
     * Sends a message with optional partition and key.
     *
     * @param topic     target topic
     * @param partition optional partition (null for auto)
     * @param key       optional message key
     * @param value     message value
     * @return future that completes when the message is sent
     */
    public ProduceFuture send(String topic, Integer partition, byte[] key, byte[] value) {
        if (closed.get()) {
            ProduceFuture future = new ProduceFuture();
            future.completeExceptionally(new IllegalStateException("Producer is closed"));
            return future;
        }

        ProduceFuture future = new ProduceFuture();
        PendingRecord record = new PendingRecord(topic, partition, key, value, future);

        lock.lock();
        try {
            // If no batching, send immediately
            if (config.getLingerMs() == 0) {
                sendRecord(record);
                return future;
            }

            // Add to batch
            batch.add(record);
            batchBytes += value.length;

            // Check if batch should be flushed
            if (batchBytes >= config.getBatchSize() ||
                batch.size() >= config.getMaxBatchMessages()) {
                flushBatch();
            }
        } finally {
            lock.unlock();
        }

        return future;
    }

    private void sendRecord(PendingRecord record) {
        Exception lastError = null;

        for (int attempt = 0; attempt <= config.getRetries(); attempt++) {
            try {
                BinaryProtocol.RecordMetadata meta;
                meta = client.doProduce(record.topic, record.key, record.value, record.partition, config.getCompressionType());

                ProduceMetadata metadata = new ProduceMetadata(
                    meta.topic(),
                    meta.partition(),
                    meta.offset(),
                    meta.timestamp(),
                    meta.keySize(),
                    meta.valueSize()
                );
                record.future.complete(metadata);
                return;

            } catch (FlyMQException e) {
                lastError = e;
                if (attempt < config.getRetries()) {
                    try {
                        Thread.sleep(config.getRetryBackoffMs());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        record.future.completeExceptionally(ie);
                        return;
                    }
                }
            }
        }

        // All retries failed
        record.future.completeExceptionally(lastError);
    }

    private void flushBatch() {
        if (batch.isEmpty()) {
            return;
        }

        for (PendingRecord record : batch) {
            sendRecord(record);
        }

        batch.clear();
        batchBytes = 0;
        lastFlushTime = System.currentTimeMillis();
    }

    /**
     * Flushes all pending messages.
     *
     * <p>Blocks until all messages in the batch are sent.
     */
    public void flush() {
        lock.lock();
        try {
            flushBatch();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes the producer.
     *
     * <p>Flushes any pending messages before closing.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (flusher != null) {
                flusher.shutdown();
                try {
                    flusher.awaitTermination(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            flush();
            log.debug("Producer closed");
        }
    }

    private record PendingRecord(
        String topic,
        Integer partition,
        byte[] key,
        byte[] value,
        ProduceFuture future
    ) {}
}
