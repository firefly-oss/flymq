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
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Flux;

/**
 * Spring Boot auto-configuration for FlyMQ reactive client.
 * 
 * <p>Automatically configures a {@link ReactiveFlyMQClient} bean when
 * Spring WebFlux is on the classpath and flymq is enabled.</p>
 * 
 * <h2>Configuration Properties:</h2>
 * <pre>
 * flymq.enabled=true
 * flymq.bootstrap-servers=localhost:9092
 * flymq.encryption-key=your-64-char-hex-key
 * </pre>
 * 
 * @author Firefly Software Solutions Inc.
 * @since 1.0.0
 */
@AutoConfiguration
@ConditionalOnClass({Flux.class, FlyMQClient.class})
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
@ConditionalOnProperty(prefix = "flymq", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(FlyMQReactiveProperties.class)
public class FlyMQReactiveAutoConfiguration {
    
    /**
     * Create the reactive FlyMQ client bean.
     * 
     * @param properties configuration properties
     * @return configured reactive client
     */
    @Bean
    @ConditionalOnMissingBean
    public ReactiveFlyMQClient reactiveFlyMQClient(FlyMQReactiveProperties properties) throws FlyMQException {
        ClientConfig config = ClientConfig.builder()
            .bootstrapServers(properties.getBootstrapServers())
            .connectTimeoutMs(properties.getConnectTimeoutMs())
            .requestTimeoutMs(properties.getRequestTimeoutMs())
            .maxRetries(properties.getMaxRetries())
            .tlsEnabled(properties.isTlsEnabled())
            .tlsCaFile(properties.getTlsCaFile())
            .tlsCertFile(properties.getTlsCertFile())
            .tlsKeyFile(properties.getTlsKeyFile())
            .tlsInsecureSkipVerify(properties.isTlsInsecureSkipVerify())
            .encryptionKey(properties.getEncryptionKey())
            .clientId(properties.getClientId())
            .build();
        
        return new ReactiveFlyMQClient(config);
    }
    
    /**
     * Create the synchronous FlyMQ client bean (for cases where both are needed).
     * 
     * @param reactiveClient reactive client
     * @return synchronous client from reactive wrapper
     */
    @Bean
    @ConditionalOnMissingBean(FlyMQClient.class)
    public FlyMQClient flyMQClient(ReactiveFlyMQClient reactiveClient) {
        return reactiveClient.getDelegate();
    }
}
