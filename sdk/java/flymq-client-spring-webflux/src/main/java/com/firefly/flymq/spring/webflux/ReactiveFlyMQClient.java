/*
 * Copyright (c) 2026 Firefly Software Solutions Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.firefly.flymq.spring.webflux;

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.config.ClientConfig;
import com.firefly.flymq.exception.FlyMQException;
import com.firefly.flymq.protocol.BinaryProtocol;
import com.firefly.flymq.protocol.Records.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reactive FlyMQ client for Spring WebFlux applications.
 * 
 * <p>Provides non-blocking reactive streams API using Project Reactor.
 * All operations return Mono or Flux and execute on bounded elastic scheduler.</p>
 * 
 * <h2>Example Usage:</h2>
 * <pre>{@code
 * @Autowired
 * ReactiveFlyMQClient client;
 * 
 * // Produce a message
 * client.produce("my-topic", "Hello!".getBytes())
 *     .subscribe(offset -> log.info("Produced at offset: {}", offset));
 * 
 * // Consume messages as a stream
 * client.consumeStream("my-topic", "my-group", 0)
 *     .doOnNext(msg -> log.info("Received: {}", msg.dataAsString()))
 *     .subscribe();
 * }</pre>
 * 
 * @author Firefly Software Solutions Inc.
 * @since 1.0.0
 */
public class ReactiveFlyMQClient implements AutoCloseable {
    
    private final FlyMQClient delegate;
    
    /**
     * Create a reactive client wrapping a synchronous client.
     * 
     * @param bootstrapServers comma-separated list of servers
     * @throws FlyMQException if connection fails
     */
    public ReactiveFlyMQClient(String bootstrapServers) throws FlyMQException {
        this.delegate = new FlyMQClient(bootstrapServers);
    }
    
    /**
     * Create a reactive client with configuration.
     * 
     * @param config client configuration
     * @throws FlyMQException if connection fails
     */
    public ReactiveFlyMQClient(ClientConfig config) throws FlyMQException {
        this.delegate = new FlyMQClient(config);
    }
    
    /**
     * Wrap an existing synchronous client.
     * 
     * @param client existing client
     */
    public ReactiveFlyMQClient(FlyMQClient client) {
        this.delegate = client;
    }
    
    // ========================================================================
    // Topic Operations
    // ========================================================================
    
    /**
     * Create a new topic.
     * 
     * @param topic topic name
     * @param partitions number of partitions
     * @return Mono completing when topic is created
     */
    public Mono<Void> createTopic(String topic, int partitions) {
        return Mono.fromCallable(() -> {
            delegate.createTopic(topic, partitions);
            return null;
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }
    
    /**
     * Delete a topic.
     * 
     * @param topic topic name
     * @return Mono completing when topic is deleted
     */
    public Mono<Void> deleteTopic(String topic) {
        return Mono.fromCallable(() -> {
            delegate.deleteTopic(topic);
            return null;
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }
    
    /**
     * List all topics.
     * 
     * @return Flux of topic names
     */
    public Flux<String> listTopics() {
        return Mono.fromCallable(delegate::listTopics)
            .subscribeOn(Schedulers.boundedElastic())
            .flatMapMany(Flux::fromIterable);
    }
    
    // ========================================================================
    // Produce Operations
    // ========================================================================

    /**
     * Produce a message to a topic.
     *
     * @param topic topic name
     * @param data message data
     * @return Mono with RecordMetadata containing topic, partition, offset, timestamp
     */
    public Mono<BinaryProtocol.RecordMetadata> produce(String topic, byte[] data) {
        return Mono.fromCallable(() -> delegate.produce(topic, data))
            .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Produce a string message.
     *
     * @param topic topic name
     * @param message message string
     * @return Mono with RecordMetadata
     */
    public Mono<BinaryProtocol.RecordMetadata> produce(String topic, String message) {
        return Mono.fromCallable(() -> delegate.produce(topic, message))
            .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Produce a message with a key for partition assignment.
     * Messages with the same key will be routed to the same partition.
     *
     * @param topic topic name
     * @param key message key for partitioning
     * @param data message data
     * @return Mono with RecordMetadata
     */
    public Mono<BinaryProtocol.RecordMetadata> produceWithKey(String topic, byte[] key, byte[] data) {
        return Mono.fromCallable(() -> delegate.produceWithKey(topic, key, data))
            .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Produce a string message with a key for partition assignment.
     * Messages with the same key will be routed to the same partition.
     *
     * @param topic topic name
     * @param key message key for partitioning
     * @param message message string
     * @return Mono with RecordMetadata
     */
    public Mono<BinaryProtocol.RecordMetadata> produceWithKey(String topic, String key, String message) {
        return Mono.fromCallable(() -> delegate.produceWithKey(topic, key, message))
            .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Produce a delayed message.
     *
     * @param topic topic name
     * @param data message data
     * @param delayMs delay in milliseconds
     * @return Mono with RecordMetadata
     */
    public Mono<BinaryProtocol.RecordMetadata> produceDelayed(String topic, byte[] data, long delayMs) {
        return Mono.fromCallable(() -> delegate.produceDelayed(topic, data, delayMs))
            .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Produce a message with TTL.
     *
     * @param topic topic name
     * @param data message data
     * @param ttlMs time-to-live in milliseconds
     * @return Mono with RecordMetadata
     */
    public Mono<BinaryProtocol.RecordMetadata> produceWithTTL(String topic, byte[] data, long ttlMs) {
        return Mono.fromCallable(() -> delegate.produceWithTTL(topic, data, ttlMs))
            .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Produce a message with schema validation.
     *
     * @param topic topic name
     * @param data message data
     * @param schemaName schema to validate against
     * @return Mono with RecordMetadata
     */
    public Mono<BinaryProtocol.RecordMetadata> produceWithSchema(String topic, byte[] data, String schemaName) {
        return Mono.fromCallable(() -> delegate.produceWithSchema(topic, data, schemaName))
            .subscribeOn(Schedulers.boundedElastic());
    }
    
    // ========================================================================
    // Consume Operations
    // ========================================================================
    
    /**
     * Consume a single message from a topic.
     * 
     * @param topic topic name
     * @param offset offset to read from
     * @return Mono with message data
     */
    public Mono<byte[]> consume(String topic, long offset) {
        return Mono.fromCallable(() -> delegate.consume(topic, offset))
            .subscribeOn(Schedulers.boundedElastic());
    }
    
    /**
     * Consume a single message from a topic with its key.
     * 
     * @param topic topic name
     * @param offset offset to read from
     * @return Mono with consumed message including key
     */
    public Mono<ConsumedMessage> consumeWithKey(String topic, long offset) {
        return Mono.fromCallable(() -> delegate.consumeWithKey(topic, offset))
            .subscribeOn(Schedulers.boundedElastic());
    }
    
    /**
     * Fetch multiple messages.
     * 
     * @param topic topic name
     * @param partition partition number
     * @param offset starting offset
     * @param maxMessages maximum messages to fetch
     * @return Mono with fetch result
     */
    public Mono<FetchResult> fetch(String topic, int partition, long offset, int maxMessages) {
        return Mono.fromCallable(() -> delegate.fetch(topic, partition, offset, maxMessages))
            .subscribeOn(Schedulers.boundedElastic());
    }
    
    /**
     * Create an infinite stream of messages from a topic.
     * 
     * <p>This method polls continuously and emits messages as they arrive.
     * Use takeUntil or timeout operators to limit the stream.</p>
     * 
     * @param topic topic name
     * @param groupId consumer group ID
     * @param partition partition number
     * @return Flux of consumed messages
     */
    public Flux<ConsumedMessage> consumeStream(String topic, String groupId, int partition) {
        return consumeStream(topic, groupId, partition, Duration.ofMillis(100), 100);
    }
    
    /**
     * Create an infinite stream of messages with custom polling settings.
     * 
     * @param topic topic name
     * @param groupId consumer group ID
     * @param partition partition number
     * @param pollInterval interval between polls
     * @param maxPollRecords maximum records per poll
     * @return Flux of consumed messages
     */
    public Flux<ConsumedMessage> consumeStream(
            String topic, 
            String groupId, 
            int partition,
            Duration pollInterval,
            int maxPollRecords) {
        
        AtomicLong currentOffset = new AtomicLong(-1);
        AtomicBoolean initialized = new AtomicBoolean(false);
        
        return Flux.interval(pollInterval)
            .onBackpressureDrop()
            .flatMap(tick -> {
                // Initialize offset on first poll
                if (!initialized.getAndSet(true)) {
                    return Mono.fromCallable(() -> {
                        long startOffset = delegate.subscribe(
                            topic, groupId, partition, SubscribeMode.LATEST
                        );
                        currentOffset.set(startOffset);
                        return startOffset;
                    })
                    .subscribeOn(Schedulers.boundedElastic())
                    .thenReturn(List.<ConsumedMessage>of());
                }
                
                // Fetch messages
                return Mono.fromCallable(() -> {
                    FetchResult result = delegate.fetch(
                        topic, partition, currentOffset.get(), maxPollRecords
                    );
                    currentOffset.set(result.nextOffset());
                    return result.messages();
                }).subscribeOn(Schedulers.boundedElastic());
            })
            .flatMapIterable(messages -> messages);
    }
    
    // ========================================================================
    // Consumer Group Operations
    // ========================================================================
    
    /**
     * Subscribe to a topic with a consumer group.
     * 
     * @param topic topic name
     * @param groupId consumer group ID
     * @param partition partition number
     * @param mode where to start consuming
     * @return Mono with starting offset
     */
    public Mono<Long> subscribe(String topic, String groupId, int partition, SubscribeMode mode) {
        return Mono.fromCallable(() -> delegate.subscribe(topic, groupId, partition, mode))
            .subscribeOn(Schedulers.boundedElastic());
    }
    
    /**
     * Commit consumer group offset.
     * 
     * @param topic topic name
     * @param groupId consumer group ID
     * @param partition partition number
     * @param offset offset to commit
     * @return Mono completing when offset is committed
     */
    public Mono<Void> commitOffset(String topic, String groupId, int partition, long offset) {
        return Mono.fromCallable(() -> {
            delegate.commitOffset(topic, groupId, partition, offset);
            return null;
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }
    
    // ========================================================================
    // Schema Operations
    // ========================================================================
    
    /**
     * Register a schema.
     * 
     * @param name schema name
     * @param type schema type (json, avro, protobuf)
     * @param definition schema definition
     * @return Mono completing when schema is registered
     */
    public Mono<Void> registerSchema(String name, String type, String definition) {
        return Mono.fromCallable(() -> {
            delegate.registerSchema(name, type, definition);
            return null;
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }
    
    // ========================================================================
    // Transaction Operations
    // ========================================================================
    
    /**
     * Begin a new transaction.
     * 
     * @return Mono with reactive transaction
     */
    public Mono<ReactiveTransaction> beginTransaction() {
        return Mono.fromCallable(() -> new ReactiveTransaction(delegate.beginTransaction()))
            .subscribeOn(Schedulers.boundedElastic());
    }
    
    
    // ========================================================================
    // DLQ Operations
    // ========================================================================
    
    /**
     * Fetch messages from dead letter queue.
     * 
     * @param topic topic name
     * @param limit maximum messages to fetch
     * @return Flux of DLQ messages
     */
    public Flux<Map<String, Object>> fetchDLQ(String topic, int limit) {
        return Mono.fromCallable(() -> delegate.fetchDLQ(topic, limit))
            .subscribeOn(Schedulers.boundedElastic())
            .flatMapMany(Flux::fromIterable);
    }
    
    /**
     * Replay a message from DLQ.
     * 
     * @param topic topic name
     * @param messageId message ID
     * @return Mono completing when message is replayed
     */
    public Mono<Void> replayDLQ(String topic, String messageId) {
        return Mono.fromCallable(() -> {
            delegate.replayDLQ(topic, messageId);
            return null;
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }
    
    // ========================================================================
    // Lifecycle
    // ========================================================================
    
    /**
     * Close the client and release resources.
     */
    @Override
    public void close() {
        delegate.close();
    }
    
    /**
     * Get the underlying synchronous client.
     * 
     * @return synchronous client
     */
    public FlyMQClient getDelegate() {
        return delegate;
    }
}
