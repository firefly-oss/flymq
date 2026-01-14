/*
 * Copyright (c) 2026 Firefly Software Solutions Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.firefly.flymq.crypto;

/**
 * Exception thrown when cryptographic operations fail.
 * 
 * <p>This includes key validation errors, encryption failures, and decryption
 * failures (including authentication failures when data has been tampered).</p>
 * 
 * @author Firefly Software Solutions Inc.
 * @since 1.0.0
 */
public class CryptoException extends RuntimeException {
    
    /**
     * Create a crypto exception with a message.
     * 
     * @param message error description
     */
    public CryptoException(String message) {
        super(message);
    }
    
    /**
     * Create a crypto exception with a message and cause.
     * 
     * @param message error description
     * @param cause underlying exception
     */
    public CryptoException(String message, Throwable cause) {
        super(message, cause);
    }
}
