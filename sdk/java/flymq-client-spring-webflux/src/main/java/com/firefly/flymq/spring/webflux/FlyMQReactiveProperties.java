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

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for FlyMQ reactive client.
 * 
 * <p>Configure via application.properties or application.yml:</p>
 * <pre>
 * flymq:
 *   enabled: true
 *   bootstrap-servers: localhost:9092,localhost:9192
 *   encryption-key: your-64-char-hex-key
 *   tls-enabled: false
 * </pre>
 * 
 * @author Firefly Software Solutions Inc.
 * @since 1.0.0
 */
@Data
@ConfigurationProperties(prefix = "flymq")
public class FlyMQReactiveProperties {
    
    /** Enable FlyMQ auto-configuration */
    private boolean enabled = true;
    
    /** Comma-separated list of bootstrap servers */
    private String bootstrapServers = "localhost:9092";
    
    /** Connection timeout in milliseconds */
    private int connectTimeoutMs = 10000;
    
    /** Request timeout in milliseconds */
    private int requestTimeoutMs = 30000;
    
    /** Maximum retry attempts */
    private int maxRetries = 3;
    
    /** Enable TLS/SSL */
    private boolean tlsEnabled = false;
    
    /** Path to CA certificate file */
    private String tlsCaFile;
    
    /** Path to client certificate file */
    private String tlsCertFile;
    
    /** Path to client key file */
    private String tlsKeyFile;
    
    /** Skip TLS certificate verification (not recommended for production) */
    private boolean tlsInsecureSkipVerify = false;
    
    /** AES-256 encryption key (64-char hex string) for data-in-motion encryption */
    private String encryptionKey;
    
    /** Client identifier */
    private String clientId = "flymq-webflux-client";
}
