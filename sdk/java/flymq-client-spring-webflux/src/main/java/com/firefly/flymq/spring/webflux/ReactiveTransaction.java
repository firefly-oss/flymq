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

import com.firefly.flymq.protocol.BinaryProtocol;
import com.firefly.flymq.transaction.Transaction;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Reactive transaction wrapper for Spring WebFlux.
 * 
 * <p>Wraps the synchronous Transaction with reactive operations.</p>
 * 
 * <h2>Example Usage:</h2>
 * <pre>{@code
 * client.beginTransaction()
 *     .flatMap(txn -> 
 *         txn.produce("topic1", "msg1".getBytes())
 *            .then(txn.produce("topic2", "msg2".getBytes()))
 *            .then(txn.commit())
 *            .onErrorResume(e -> txn.rollback().then(Mono.error(e)))
 *     )
 *     .subscribe();
 * }</pre>
 * 
 * @author Firefly Software Solutions Inc.
 * @since 1.0.0
 */
public class ReactiveTransaction {
    
    private final Transaction delegate;
    
    /**
     * Create a reactive transaction wrapper.
     * 
     * @param transaction underlying transaction
     */
    public ReactiveTransaction(Transaction transaction) {
        this.delegate = transaction;
    }
    
    /**
     * Get the transaction ID.
     * 
     * @return transaction ID
     */
    public String getTxnId() {
        return delegate.getTxnId();
    }
    
    /**
     * Check if transaction is active.
     * 
     * @return true if active
     */
    public boolean isActive() {
        return delegate.isActive();
    }
    
    /**
     * Produce a message within the transaction.
     *
     * @param topic topic name
     * @param data message data
     * @return Mono with RecordMetadata
     */
    public Mono<BinaryProtocol.RecordMetadata> produce(String topic, byte[] data) {
        return Mono.fromCallable(() -> delegate.produce(topic, data))
            .subscribeOn(Schedulers.boundedElastic());
    }
    
    /**
     * Commit the transaction.
     * 
     * @return Mono completing when committed
     */
    public Mono<Void> commit() {
        return Mono.fromCallable(() -> {
            delegate.commit();
            return null;
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }
    
    /**
     * Rollback the transaction.
     * 
     * @return Mono completing when rolled back
     */
    public Mono<Void> rollback() {
        return Mono.fromCallable(() -> {
            delegate.rollback();
            return null;
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }
    
    /**
     * Get the underlying synchronous transaction.
     * 
     * @return synchronous transaction
     */
    public Transaction getDelegate() {
        return delegate;
    }
}
