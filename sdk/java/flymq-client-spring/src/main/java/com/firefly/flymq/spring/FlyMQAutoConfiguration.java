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
package com.firefly.flymq.spring;

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.config.ClientConfig;
import com.firefly.flymq.exception.FlyMQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot auto-configuration for FlyMQ client.
 *
 * <p>Enable by adding the following to your application.properties:
 * <pre>
 * flymq.enabled=true
 * flymq.bootstrap-servers=localhost:9092
 * </pre>
 *
 * <p>Or use the @EnableFlyMQ annotation on your configuration class.
 */
@AutoConfiguration
@ConditionalOnClass(FlyMQClient.class)
@ConditionalOnProperty(prefix = "flymq", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(FlyMQProperties.class)
public class FlyMQAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(FlyMQAutoConfiguration.class);

    /**
     * Creates the FlyMQ client configuration bean.
     *
     * @param properties the FlyMQ properties
     * @return ClientConfig bean
     */
    @Bean
    @ConditionalOnMissingBean
    public ClientConfig flyMQClientConfig(FlyMQProperties properties) {
        return ClientConfig.builder()
                .bootstrapServers(properties.getBootstrapServers())
                .connectTimeoutMs(properties.getConnectTimeoutMs())
                .requestTimeoutMs(properties.getRequestTimeoutMs())
                .maxRetries(properties.getMaxRetries())
                .retryDelayMs(properties.getRetryDelayMs())
                .tlsEnabled(properties.isTlsEnabled())
                .tlsCaFile(properties.getTlsCaFile())
                .tlsCertFile(properties.getTlsCertFile())
                .tlsKeyFile(properties.getTlsKeyFile())
                .tlsInsecureSkipVerify(properties.isTlsInsecureSkipVerify())
                .encryptionKey(properties.getEncryptionKey())
                .clientId(properties.getClientId())
                .build();
    }

    /**
     * Creates the FlyMQ client bean.
     *
     * @param config the client configuration
     * @return FlyMQClient bean
     * @throws FlyMQException if connection fails
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public FlyMQClient flyMQClient(ClientConfig config) throws FlyMQException {
        log.info("Creating FlyMQ client with bootstrap servers: {}", config.getBootstrapServers());
        return new FlyMQClient(config);
    }
}
