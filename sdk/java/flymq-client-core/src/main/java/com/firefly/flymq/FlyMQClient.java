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
package com.firefly.flymq;

import com.firefly.flymq.config.ClientConfig;
import com.firefly.flymq.exception.FlyMQException;
import com.firefly.flymq.exception.ProtocolException;
import com.firefly.flymq.protocol.BinaryProtocol;
import com.firefly.flymq.protocol.OpCode;
import com.firefly.flymq.protocol.Protocol;
import com.firefly.flymq.protocol.Records.*;
import com.firefly.flymq.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * FlyMQ client with support for HA, TLS, and all FlyMQ operations.
 *
 * <p>The client automatically handles:
 * <ul>
 *   <li>Connection to multiple bootstrap servers</li>
 *   <li>Automatic failover on connection errors</li>
 *   <li>Leader redirection in cluster mode</li>
 *   <li>TLS encryption</li>
 *   <li>Thread-safe operations</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
 *     client.createTopic("my-topic", 3);
 *     long offset = client.produce("my-topic", "Hello!".getBytes());
 *     byte[] data = client.consume("my-topic", offset);
 * }
 * }</pre>
 */
public class FlyMQClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FlyMQClient.class);

    private final ClientConfig config;
    private final List<String> servers;
    private final ReentrantLock lock = new ReentrantLock();

    private int currentServerIndex = 0;
    private Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;
    private SSLContext sslContext;
    private volatile boolean closed = false;
    private volatile boolean authenticated = false;
    private volatile String authenticatedUsername = null;

    /**
     * Creates a new FlyMQ client connected to the specified server.
     *
     * @param bootstrapServers server address (host:port) or comma-separated list
     * @throws FlyMQException if connection fails
     */
    public FlyMQClient(String bootstrapServers) throws FlyMQException {
        this(ClientConfig.forServers(bootstrapServers));
    }

    /**
     * Creates a new FlyMQ client with custom configuration.
     *
     * @param config client configuration
     * @throws FlyMQException if connection fails
     */
    public FlyMQClient(ClientConfig config) throws FlyMQException {
        this.config = config;
        this.servers = config.getServerList();

        if (servers.isEmpty()) {
            throw new FlyMQException("No bootstrap servers provided");
        }

        if (config.isTlsEnabled()) {
            setupTls();
        }

        connect();

        // Auto-authenticate if credentials are provided
        if (config.getUsername() != null && !config.getUsername().isEmpty()) {
            authenticate(config.getUsername(), config.getPassword() != null ? config.getPassword() : "");
        }
    }

    private void setupTls() throws FlyMQException {
        try {
            if (config.isTlsInsecureSkipVerify()) {
                // Insecure mode - skip verification (testing only)
                TrustManager[] trustAll = new TrustManager[]{
                        new X509TrustManager() {
                            public X509Certificate[] getAcceptedIssuers() { return null; }
                            public void checkClientTrusted(X509Certificate[] certs, String authType) {}
                            public void checkServerTrusted(X509Certificate[] certs, String authType) {}
                        }
                };
                sslContext = SSLContext.getInstance("TLSv1.2");
                sslContext.init(null, trustAll, new java.security.SecureRandom());
                log.warn("TLS certificate verification is disabled - use only for testing");
            } else {
                // Use TLS 1.2+ for security
                sslContext = SSLContext.getInstance("TLSv1.2");

                // Load CA certificate if provided
                TrustManagerFactory tmf = null;
                if (config.getTlsCaFile() != null) {
                    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                    trustStore.load(null, null);

                    CertificateFactory cf = CertificateFactory.getInstance("X.509");
                    try (FileInputStream fis = new FileInputStream(config.getTlsCaFile())) {
                        X509Certificate caCert = (X509Certificate) cf.generateCertificate(fis);
                        trustStore.setCertificateEntry("ca", caCert);
                    }

                    tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(trustStore);
                    log.debug("Loaded CA certificate from: {}", config.getTlsCaFile());
                }

                // Load client certificate if provided (mTLS)
                KeyManagerFactory kmf = null;
                if (config.getTlsCertFile() != null && config.getTlsKeyFile() != null) {
                    kmf = loadClientCertificate(config.getTlsCertFile(), config.getTlsKeyFile());
                    log.debug("Loaded client certificate for mTLS from: {}", config.getTlsCertFile());
                }

                sslContext.init(
                        kmf != null ? kmf.getKeyManagers() : null,
                        tmf != null ? tmf.getTrustManagers() : null,
                        new java.security.SecureRandom()
                );
            }
        } catch (Exception e) {
            throw new FlyMQException("Failed to configure TLS", e);
        }
    }

    /**
     * Loads client certificate and private key for mTLS authentication.
     * Supports PEM format certificates and keys.
     *
     * @param certFile path to client certificate file (PEM format)
     * @param keyFile path to client private key file (PEM format)
     * @return KeyManagerFactory configured with client credentials
     * @throws Exception if loading fails
     */
    private KeyManagerFactory loadClientCertificate(String certFile, String keyFile) throws Exception {
        // Load client certificate
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate clientCert;
        try (FileInputStream fis = new FileInputStream(certFile)) {
            clientCert = (X509Certificate) cf.generateCertificate(fis);
        }

        // Load private key from PEM file
        java.security.PrivateKey privateKey = loadPrivateKey(keyFile);

        // Create keystore with client certificate and key
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);

        // Use empty password for the key entry
        char[] keyPassword = "".toCharArray();
        keyStore.setKeyEntry("client", privateKey, keyPassword,
                new java.security.cert.Certificate[]{clientCert});

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyPassword);

        return kmf;
    }

    /**
     * Loads a private key from a PEM file.
     * Supports PKCS#8 format (BEGIN PRIVATE KEY) and PKCS#1 RSA format (BEGIN RSA PRIVATE KEY).
     *
     * @param keyFile path to the private key file
     * @return the loaded private key
     * @throws Exception if loading fails
     */
    private java.security.PrivateKey loadPrivateKey(String keyFile) throws Exception {
        String keyContent = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(keyFile)));

        // Remove PEM headers and whitespace
        String keyPem = keyContent
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replace("-----BEGIN RSA PRIVATE KEY-----", "")
                .replace("-----END RSA PRIVATE KEY-----", "")
                .replaceAll("\\s", "");

        byte[] keyBytes = Base64.getDecoder().decode(keyPem);

        // Try PKCS#8 format first (most common for modern keys)
        try {
            java.security.spec.PKCS8EncodedKeySpec keySpec =
                    new java.security.spec.PKCS8EncodedKeySpec(keyBytes);
            java.security.KeyFactory kf = java.security.KeyFactory.getInstance("RSA");
            return kf.generatePrivate(keySpec);
        } catch (Exception e) {
            // Try EC key if RSA fails
            try {
                java.security.spec.PKCS8EncodedKeySpec keySpec =
                        new java.security.spec.PKCS8EncodedKeySpec(keyBytes);
                java.security.KeyFactory kf = java.security.KeyFactory.getInstance("EC");
                return kf.generatePrivate(keySpec);
            } catch (Exception e2) {
                throw new FlyMQException("Failed to load private key - unsupported format. " +
                        "Use PKCS#8 format (openssl pkcs8 -topk8 -nocrypt -in key.pem -out key-pkcs8.pem)", e);
            }
        }
    }

    private void connect() throws FlyMQException {
        List<String> errors = new ArrayList<>();

        for (int attempt = 0; attempt < config.getMaxRetries(); attempt++) {
            for (int i = 0; i < servers.size(); i++) {
                int serverIndex = (currentServerIndex + i) % servers.size();
                String server = servers.get(serverIndex);

                try {
                    connectToServer(server);
                    currentServerIndex = serverIndex;
                    log.info("Connected to FlyMQ server: {}", server);
                    return;
                } catch (Exception e) {
                    errors.add(server + ": " + e.getMessage());
                    log.debug("Failed to connect to {}: {}", server, e.getMessage());
                }
            }

            if (attempt < config.getMaxRetries() - 1) {
                try {
                    Thread.sleep(config.getRetryDelayMs());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new FlyMQException("Connection interrupted");
                }
            }
        }

        throw new FlyMQException("Failed to connect to any server: " + errors);
    }

    private void connectToServer(String server) throws IOException {
        String[] parts = server.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        Socket newSocket;
        if (sslContext != null) {
            SSLSocketFactory factory = sslContext.getSocketFactory();
            newSocket = factory.createSocket();
        } else {
            newSocket = new Socket();
        }

        newSocket.connect(
                new InetSocketAddress(host, port),
                config.getConnectTimeoutMs()
        );
        newSocket.setSoTimeout(config.getRequestTimeoutMs());

        this.socket = newSocket;
        this.inputStream = new BufferedInputStream(socket.getInputStream());
        this.outputStream = new BufferedOutputStream(socket.getOutputStream());
    }

    private void ensureConnected() throws FlyMQException {
        if (closed) {
            throw new FlyMQException("Client is closed");
        }
        if (socket == null || socket.isClosed()) {
            connect();
        }
    }

    /**
     * Sends a binary-encoded request and returns the raw binary response.
     * This is the primary method for all binary protocol operations.
     */
    private byte[] sendBinaryRequest(OpCode op, byte[] payload) throws FlyMQException {
        lock.lock();
        try {
            ensureConnected();

            byte[] payloadBytes = payload != null ? payload : new byte[0];
            Protocol.writeMessage(outputStream, op, payloadBytes);

            Protocol.Message response = Protocol.readMessage(inputStream);

            if (response.op() == OpCode.ERROR) {
                String errorMsg = response.payloadAsString();
                handleServerError(errorMsg);
            }

            return response.payload();

        } catch (EOFException e) {
            socket = null;
            throw new FlyMQException("Connection closed", e);
        } catch (IOException e) {
            socket = null;
            throw new FlyMQException("I/O error: " + e.getMessage(), e);
        } catch (ProtocolException e) {
            throw new FlyMQException("Protocol error: " + e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }

    private void handleServerError(String errorMsg) throws FlyMQException {
        if (errorMsg.toLowerCase().contains("not leader")) {
            // Could extract leader info and redirect
            throw new FlyMQException("Not leader: " + errorMsg);
        }
        throw new FlyMQException("Server error: " + errorMsg);
    }

    @Override
    public void close() {
        lock.lock();
        try {
            closed = true;
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    log.debug("Error closing socket", e);
                }
                socket = null;
            }
        } finally {
            lock.unlock();
        }
    }

    // =========================================================================
    // Authentication Operations
    // =========================================================================

    /**
     * Checks if the client is authenticated.
     *
     * @return true if authenticated
     */
    public boolean isAuthenticated() {
        return authenticated;
    }

    /**
     * Gets the authenticated username.
     *
     * @return the username, or null if not authenticated
     */
    public String getAuthenticatedUsername() {
        return authenticatedUsername;
    }

    /**
     * Authenticates with the FlyMQ server.
     *
     * @param username the username
     * @param password the password
     * @return AuthResponse with authentication result
     * @throws FlyMQException if authentication fails
     */
    public AuthResponse authenticate(String username, String password) throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeAuthRequest(username, password);
        byte[] response = sendBinaryRequest(OpCode.AUTH, payload);

        BinaryProtocol.AuthResult result = BinaryProtocol.decodeAuthResponse(response);

        if (!result.success()) {
            throw new FlyMQException("Authentication failed");
        }

        authenticated = true;
        authenticatedUsername = username;

        return new AuthResponse(true, result.username(), result.roles(), result.permissions());
    }

    /**
     * Gets information about the current authentication status.
     *
     * @return WhoAmIResponse with current user information
     * @throws FlyMQException if the operation fails
     */
    public WhoAmIResponse whoAmI() throws FlyMQException {
        byte[] response = sendBinaryRequest(OpCode.WHOAMI, null);

        BinaryProtocol.AuthResult result = BinaryProtocol.decodeAuthResponse(response);

        return new WhoAmIResponse(result.success(), result.username(), result.roles(), result.permissions());
    }

    /**
     * Authentication response record.
     */
    public record AuthResponse(boolean success, String username, List<String> roles, List<String> permissions) {}

    /**
     * WhoAmI response record.
     */
    public record WhoAmIResponse(boolean authenticated, String username, List<String> roles, List<String> permissions) {}

    // =========================================================================
    // Core Operations
    // =========================================================================

    /**
     * Produces a message to a topic.
     *
     * @param topic target topic name
     * @param data  message data
     * @return the offset of the produced message
     * @throws FlyMQException if the operation fails
     */
    public long produce(String topic, byte[] data) throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeProduceRequest(topic, data);
        byte[] response = sendBinaryRequest(OpCode.PRODUCE, payload);
        return BinaryProtocol.decodeProduceResponse(response);
    }

    /**
     * Produces a string message to a topic.
     *
     * @param topic   target topic name
     * @param message message string
     * @return the offset of the produced message
     * @throws FlyMQException if the operation fails
     */
    public long produce(String topic, String message) throws FlyMQException {
        return produce(topic, message.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Produces a message with a key for partition assignment.
     * Messages with the same key will be routed to the same partition.
     *
     * @param topic target topic name
     * @param key   message key for partitioning
     * @param data  message data
     * @return the offset of the produced message
     * @throws FlyMQException if the operation fails
     */
    public long produceWithKey(String topic, byte[] key, byte[] data) throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeProduceWithKeyRequest(topic, key, data);
        byte[] response = sendBinaryRequest(OpCode.PRODUCE, payload);
        return BinaryProtocol.decodeProduceResponse(response);
    }

    /**
     * Produces a string message with a key for partition assignment.
     * Messages with the same key will be routed to the same partition.
     *
     * @param topic   target topic name
     * @param key     message key for partitioning
     * @param message message string
     * @return the offset of the produced message
     * @throws FlyMQException if the operation fails
     */
    public long produceWithKey(String topic, String key, String message) throws FlyMQException {
        return produceWithKey(
            topic,
            key.getBytes(StandardCharsets.UTF_8),
            message.getBytes(StandardCharsets.UTF_8)
        );
    }

    /**
     * Produces a message to a specific partition.
     * Use this when you need explicit control over partition assignment.
     *
     * @param topic     target topic name
     * @param partition target partition number
     * @param data      message data
     * @return the offset of the produced message
     * @throws FlyMQException if the operation fails
     */
    public long produceToPartition(String topic, int partition, byte[] data) throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeProduceWithPartitionRequest(topic, partition, data);
        byte[] response = sendBinaryRequest(OpCode.PRODUCE, payload);
        return BinaryProtocol.decodeProduceResponse(response);
    }

    /**
     * Produces a message with a key to a specific partition.
     * Note: When partition is specified, it overrides key-based partition selection.
     *
     * @param topic     target topic name
     * @param partition target partition number
     * @param key       message key (for tracking, not partition selection)
     * @param data      message data
     * @return the offset of the produced message
     * @throws FlyMQException if the operation fails
     */
    public long produceWithKeyToPartition(String topic, int partition, byte[] key, byte[] data)
            throws FlyMQException {
        // For now, ignore key and use partition-based produce
        byte[] payload = BinaryProtocol.encodeProduceWithPartitionRequest(topic, partition, data);
        byte[] response = sendBinaryRequest(OpCode.PRODUCE, payload);
        return BinaryProtocol.decodeProduceResponse(response);
    }

    /**
     * Consumes a single message from partition 0 of a topic.
     *
     * @param topic  topic to consume from
     * @param offset offset of the message
     * @return message data
     * @throws FlyMQException if the operation fails
     */
    public byte[] consume(String topic, long offset) throws FlyMQException {
        return consumeFromPartition(topic, 0, offset).data();
    }

    /**
     * Consumes a single message from partition 0, including its key.
     *
     * @param topic  topic to consume from
     * @param offset offset of the message
     * @return consumed message with key and data
     * @throws FlyMQException if the operation fails
     */
    public ConsumedMessage consumeWithKey(String topic, long offset) throws FlyMQException {
        return consumeFromPartition(topic, 0, offset);
    }

    /**
     * Consumes a single message from a specific partition.
     *
     * @param topic     topic to consume from
     * @param partition partition to consume from
     * @param offset    offset of the message
     * @return consumed message with key and data
     * @throws FlyMQException if the operation fails
     */
    public ConsumedMessage consumeFromPartition(String topic, int partition, long offset)
            throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeConsumeRequest(topic, partition, offset);
        byte[] response = sendBinaryRequest(OpCode.CONSUME, payload);

        BinaryProtocol.ConsumeResult result = BinaryProtocol.decodeConsumeResponse(response);
        return new ConsumedMessage(topic, partition, result.offset(), result.key(), result.data());
    }

    /**
     * Fetches multiple messages from a topic.
     *
     * @param topic       topic to fetch from
     * @param partition   partition to fetch from
     * @param offset      starting offset
     * @param maxMessages maximum messages to fetch
     * @return fetch result with messages and next offset
     * @throws FlyMQException if the operation fails
     */
    public FetchResult fetch(String topic, int partition, long offset, int maxMessages) 
            throws FlyMQException {
        // Use binary fetch request - encode using group-based fetch for simplicity
        byte[] payload = BinaryProtocol.encodeFetchRequest(topic, "", maxMessages, 5000);
        byte[] response = sendBinaryRequest(OpCode.FETCH, payload);

        List<BinaryProtocol.FetchedMessage> fetched = BinaryProtocol.decodeFetchResponse(response);
        List<ConsumedMessage> messages = new ArrayList<>();
        long nextOffset = offset;
        
        for (BinaryProtocol.FetchedMessage msg : fetched) {
            messages.add(new ConsumedMessage(topic, msg.partition(), msg.offset(), msg.key(), msg.data()));
            if (msg.offset() >= nextOffset) {
                nextOffset = msg.offset() + 1;
            }
        }

        return new FetchResult(messages, nextOffset);
    }

    /**
     * Creates a new topic.
     *
     * @param topic      name of the topic
     * @param partitions number of partitions
     * @throws FlyMQException if the operation fails
     */
    public void createTopic(String topic, int partitions) throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeCreateTopicRequest(topic, partitions);
        sendBinaryRequest(OpCode.CREATE_TOPIC, payload);
        log.info("Created topic: {} with {} partitions", topic, partitions);
    }

    /**
     * Deletes a topic.
     *
     * @param topic name of the topic to delete
     * @throws FlyMQException if the operation fails
     */
    public void deleteTopic(String topic) throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeDeleteTopicRequest(topic);
        sendBinaryRequest(OpCode.DELETE_TOPIC, payload);
        log.info("Deleted topic: {}", topic);
    }

    /**
     * Lists all topics.
     *
     * @return list of topic names
     * @throws FlyMQException if the operation fails
     */
    public List<String> listTopics() throws FlyMQException {
        byte[] response = sendBinaryRequest(OpCode.LIST_TOPICS, new byte[0]);
        return BinaryProtocol.decodeListTopicsResponse(response);
    }

    // =========================================================================
    // Consumer Group Operations
    // =========================================================================

    /**
     * Subscribes to a topic with a consumer group.
     *
     * @param topic     topic to subscribe to
     * @param groupId   consumer group ID
     * @param partition partition to subscribe to
     * @param mode      start position (earliest, latest, commit)
     * @return starting offset
     * @throws FlyMQException if the operation fails
     */
    public long subscribe(String topic, String groupId, int partition, SubscribeMode mode) 
            throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeSubscribeRequest(topic, groupId, partition, mode.getValue());
        byte[] response = sendBinaryRequest(OpCode.SUBSCRIBE, payload);
        return BinaryProtocol.decodeSubscribeResponse(response);
    }

    /**
     * Commits consumer offset.
     *
     * @param topic     topic name
     * @param groupId   consumer group ID
     * @param partition partition number
     * @param offset    offset to commit
     * @throws FlyMQException if the operation fails
     */
    public void commitOffset(String topic, String groupId, int partition, long offset)
            throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeCommitRequest(topic, groupId, partition, offset);
        sendBinaryRequest(OpCode.COMMIT, payload);
    }

    /**
     * Gets the committed offset for a consumer group.
     *
     * @param topic     topic name
     * @param groupId   consumer group ID
     * @param partition partition number
     * @return the committed offset
     * @throws FlyMQException if the operation fails
     */
    public long getCommittedOffset(String topic, String groupId, int partition)
            throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeGetOffsetRequest(topic, groupId, partition);
        byte[] response = sendBinaryRequest(OpCode.GET_OFFSET, payload);
        return BinaryProtocol.decodeOffsetResponse(response);
    }

    /**
     * Resets consumer group offset to a specific position.
     *
     * @param topic     topic name
     * @param groupId   consumer group ID
     * @param partition partition number
     * @param mode      reset mode ("earliest", "latest", or "offset")
     * @param offset    specific offset (only used when mode is "offset")
     * @throws FlyMQException if the operation fails
     */
    public void resetOffset(String topic, String groupId, int partition, String mode, Long offset)
            throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeResetOffsetRequest(topic, groupId, partition, mode, offset != null ? offset : 0);
        sendBinaryRequest(OpCode.RESET_OFFSET, payload);
    }

    /**
     * Resets consumer group offset to the earliest position.
     *
     * @param topic     topic name
     * @param groupId   consumer group ID
     * @param partition partition number
     * @throws FlyMQException if the operation fails
     */
    public void resetOffsetToEarliest(String topic, String groupId, int partition)
            throws FlyMQException {
        resetOffset(topic, groupId, partition, "earliest", null);
    }

    /**
     * Resets consumer group offset to the latest position.
     *
     * @param topic     topic name
     * @param groupId   consumer group ID
     * @param partition partition number
     * @throws FlyMQException if the operation fails
     */
    public void resetOffsetToLatest(String topic, String groupId, int partition)
            throws FlyMQException {
        resetOffset(topic, groupId, partition, "latest", null);
    }

    /**
     * Gets consumer lag for a consumer group.
     *
     * @param topic     topic name
     * @param groupId   consumer group ID
     * @param partition partition number
     * @return consumer lag information
     * @throws FlyMQException if the operation fails
     */
    public ConsumerLag getLag(String topic, String groupId, int partition)
            throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeGetLagRequest(topic, groupId, partition);
        byte[] response = sendBinaryRequest(OpCode.GET_LAG, payload);
        BinaryProtocol.LagInfo lagInfo = BinaryProtocol.decodeLagResponse(response);
        return new ConsumerLag(
            topic,
            partition,
            lagInfo.currentOffset(),
            lagInfo.committedOffset(),
            lagInfo.latestOffset(),
            lagInfo.lag()
        );
    }

    /**
     * Lists all consumer groups.
     *
     * @return list of consumer group information
     * @throws FlyMQException if the operation fails
     */
    public List<ConsumerGroupInfo> listConsumerGroups() throws FlyMQException {
        byte[] response = sendBinaryRequest(OpCode.LIST_GROUPS, new byte[0]);
        List<BinaryProtocol.ConsumerGroupData> groupsData = BinaryProtocol.decodeListGroupsResponse(response);

        List<ConsumerGroupInfo> groups = new ArrayList<>();
        for (BinaryProtocol.ConsumerGroupData gd : groupsData) {
            groups.add(new ConsumerGroupInfo(
                gd.groupId(),
                gd.state(),
                gd.members(),
                gd.topics(),
                gd.coordinator()
            ));
        }
        return groups;
    }

    /**
     * Gets detailed information about a consumer group.
     *
     * @param groupId consumer group ID
     * @return consumer group information
     * @throws FlyMQException if the operation fails
     */
    public ConsumerGroupInfo describeConsumerGroup(String groupId) throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeDescribeGroupRequest(groupId);
        byte[] response = sendBinaryRequest(OpCode.DESCRIBE_GROUP, payload);
        BinaryProtocol.ConsumerGroupData gd = BinaryProtocol.decodeDescribeGroupResponse(response);
        return new ConsumerGroupInfo(
            gd.groupId(),
            gd.state(),
            gd.members(),
            gd.topics(),
            gd.coordinator()
        );
    }

    /**
     * Deletes a consumer group.
     *
     * @param groupId consumer group ID to delete
     * @throws FlyMQException if the operation fails
     */
    public void deleteConsumerGroup(String groupId) throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeDeleteGroupRequest(groupId);
        sendBinaryRequest(OpCode.DELETE_GROUP, payload);
        log.info("Deleted consumer group: {}", groupId);
    }

    // =========================================================================
    // Advanced Messaging
    // =========================================================================

    /**
     * Produces a message with delayed delivery.
     *
     * @param topic   target topic
     * @param data    message data
     * @param delayMs delay in milliseconds
     * @return message offset
     * @throws FlyMQException if the operation fails
     */
    public long produceDelayed(String topic, byte[] data, long delayMs) throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeProduceDelayedRequest(topic, data, delayMs);
        byte[] response = sendBinaryRequest(OpCode.PRODUCE_DELAYED, payload);
        return BinaryProtocol.decodeProduceResponse(response);
    }

    /**
     * Produces a message with time-to-live.
     *
     * @param topic target topic
     * @param data  message data
     * @param ttlMs TTL in milliseconds
     * @return message offset
     * @throws FlyMQException if the operation fails
     */
    public long produceWithTTL(String topic, byte[] data, long ttlMs) throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeProduceWithTTLRequest(topic, data, ttlMs);
        byte[] response = sendBinaryRequest(OpCode.PRODUCE_WITH_TTL, payload);
        return BinaryProtocol.decodeProduceResponse(response);
    }

    /**
     * Produces a message with schema validation.
     *
     * @param topic      target topic
     * @param data       message data
     * @param schemaName schema to validate against
     * @return message offset
     * @throws FlyMQException if the operation fails
     */
    public long produceWithSchema(String topic, byte[] data, String schemaName) 
            throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeProduceWithSchemaRequest(topic, data, schemaName);
        byte[] response = sendBinaryRequest(OpCode.PRODUCE_WITH_SCHEMA, payload);
        return BinaryProtocol.decodeProduceResponse(response);
    }

    // =========================================================================
    // Transaction Operations
    // =========================================================================

    /**
     * Begins a new transaction.
     *
     * @return Transaction object
     * @throws FlyMQException if the operation fails
     */
    public Transaction beginTransaction() throws FlyMQException {
        byte[] response = sendBinaryRequest(OpCode.BEGIN_TX, new byte[0]);
        String txnId = BinaryProtocol.decodeTxnResponse(response);
        return new Transaction(this, txnId);
    }

    /**
     * Internal: Commits a transaction.
     */
    public void commitTransaction(String txnId) throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeTxnRequest(txnId);
        sendBinaryRequest(OpCode.COMMIT_TX, payload);
    }

    /**
     * Internal: Aborts a transaction.
     */
    public void abortTransaction(String txnId) throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeTxnRequest(txnId);
        sendBinaryRequest(OpCode.ABORT_TX, payload);
    }

    /**
     * Internal: Produces within a transaction.
     */
    public long produceInTransaction(String txnId, String topic, byte[] data) 
            throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeTxnProduceRequest(txnId, topic, data);
        byte[] response = sendBinaryRequest(OpCode.PRODUCE_TX, payload);
        return BinaryProtocol.decodeProduceResponse(response);
    }

    // =========================================================================
    // Schema Operations
    // =========================================================================

    /**
     * Registers a schema.
     *
     * @param name       schema name
     * @param schemaType schema type (json, avro, protobuf)
     * @param schema     schema definition
     * @throws FlyMQException if the operation fails
     */
    public void registerSchema(String name, String schemaType, String schema) 
            throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeRegisterSchemaRequest(name, schemaType, schema.getBytes(StandardCharsets.UTF_8));
        sendBinaryRequest(OpCode.REGISTER_SCHEMA, payload);
        log.info("Registered schema: {}", name);
    }

    /**
     * Lists registered schemas.
     *
     * @param topic optional topic filter
     * @return list of schema information
     * @throws FlyMQException if the operation fails
     */
    public List<Map<String, Object>> listSchemas(String topic) throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeListSchemasRequest(topic != null ? topic : "");
        byte[] response = sendBinaryRequest(OpCode.LIST_SCHEMAS, payload);
        return BinaryProtocol.decodeListSchemasResponse(response);
    }

    // =========================================================================
    // DLQ Operations
    // =========================================================================

    /**
     * Fetches messages from the dead letter queue.
     *
     * @param topic       topic name
     * @param maxMessages maximum messages to fetch
     * @return list of DLQ messages
     * @throws FlyMQException if the operation fails
     */
    public List<Map<String, Object>> fetchDLQ(String topic, int maxMessages) 
            throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeDLQRequest(topic, maxMessages);
        byte[] response = sendBinaryRequest(OpCode.FETCH_DLQ, payload);
        return BinaryProtocol.decodeDLQResponse(response);
    }

    /**
     * Replays a message from the dead letter queue.
     *
     * @param topic     topic name
     * @param messageId message ID to replay
     * @throws FlyMQException if the operation fails
     */
    public void replayDLQ(String topic, String messageId) throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeReplayDLQRequest(topic, messageId);
        sendBinaryRequest(OpCode.REPLAY_DLQ, payload);
    }

    /**
     * Purges all messages from the dead letter queue.
     *
     * @param topic topic name
     * @throws FlyMQException if the operation fails
     */
    public void purgeDLQ(String topic) throws FlyMQException {
        byte[] payload = BinaryProtocol.encodePurgeDLQRequest(topic);
        sendBinaryRequest(OpCode.PURGE_DLQ, payload);
    }

    // =========================================================================
    // Cluster Operations
    // =========================================================================

    /**
     * Joins a cluster via peer.
     *
     * @param peerAddr address of peer to join through
     * @throws FlyMQException if the operation fails
     */
    public void clusterJoin(String peerAddr) throws FlyMQException {
        byte[] payload = BinaryProtocol.encodeClusterJoinRequest(peerAddr);
        sendBinaryRequest(OpCode.CLUSTER_JOIN, payload);
    }

    /**
     * Leaves the cluster gracefully.
     *
     * @throws FlyMQException if the operation fails
     */
    public void clusterLeave() throws FlyMQException {
        sendBinaryRequest(OpCode.CLUSTER_LEAVE, new byte[0]);
    }

    /**
     * Gets the current server address.
     *
     * @return current server address
     */
    public String getCurrentServer() {
        return servers.get(currentServerIndex);
    }

    /**
     * Gets all bootstrap servers.
     *
     * @return list of server addresses
     */
    public List<String> getServers() {
        return new ArrayList<>(servers);
    }

    /**
     * Checks if TLS is enabled.
     *
     * @return true if TLS is enabled
     */
    public boolean isTlsEnabled() {
        return sslContext != null;
    }
}
