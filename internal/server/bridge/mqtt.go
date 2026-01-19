// Package bridge provides protocol bridges for FlyMQ.
//
// The MQTT bridge enables MQTT v3.1.1 clients to interact with FlyMQ topics
// using the standard MQTT protocol. This allows existing MQTT-based IoT devices
// and applications to seamlessly integrate with FlyMQ.
//
// # Protocol Support
//
// The bridge implements a subset of MQTT v3.1.1 (OASIS Standard):
//   - CONNECT: Client authentication with username/password
//   - CONNACK: Connection acknowledgment
//   - PUBLISH: Message publishing (QoS 0 only)
//   - SUBSCRIBE: Topic subscription (QoS 0 only)
//   - SUBACK: Subscription acknowledgment
//   - PINGREQ/PINGRESP: Keep-alive mechanism
//   - DISCONNECT: Clean session termination
//
// # Limitations
//
// The following MQTT features are NOT supported:
//   - QoS 1 (at-least-once) and QoS 2 (exactly-once) delivery
//   - Retained messages
//   - Will messages (parsed but not implemented)
//   - MQTT v5.0 features
//   - Wildcard subscriptions (+, #)
//   - UNSUBSCRIBE command
//   - Session persistence
//
// # Topic Mapping
//
// MQTT topics are mapped directly to FlyMQ topics. The MQTT topic name
// becomes the FlyMQ topic name. All messages are published to partition 0.
package bridge

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"flymq/internal/auth"
	"flymq/internal/broker"
	"flymq/internal/config"
	"flymq/internal/crypto"
	"flymq/internal/logging"
	"flymq/internal/protocol"
)

// Default configuration values for MQTT bridge
const (
	// DefaultSubscriptionPollInterval is the default interval for polling messages in subscriptions
	DefaultMQTTSubscriptionPollInterval = 100 * time.Millisecond
	// DefaultReadTimeout is the default timeout for read operations
	DefaultMQTTReadTimeout = 60 * time.Second
	// DefaultWriteTimeout is the default timeout for write operations
	DefaultMQTTWriteTimeout = 10 * time.Second
	// DefaultMaxMessageSize is the maximum allowed MQTT message size (256KB)
	DefaultMaxMessageSize = 256 * 1024
)

// Broker is a minimal interface for the MQTT bridge to interact with the broker.
type Broker interface {
	ProduceWithKeyAndPartition(topic string, key, data []byte) (uint64, int, error)
	Subscribe(topic, groupID string, partition int, mode protocol.SubscribeMode) (uint64, error)
	FetchWithKeys(topic string, partition int, offset uint64, maxMessages int, filter string) ([]broker.FetchedMessage, uint64, error)
}

// MQTTBridge provides a bridge for MQTT v3.1.1 clients to interact with FlyMQ.
//
// The bridge translates MQTT protocol messages to FlyMQ operations:
//   - PUBLISH → ProduceWithKeyAndPartition
//   - SUBSCRIBE → Subscribe + FetchWithKeys polling
//
// See package documentation for supported features and limitations.
type MQTTBridge struct {
	broker       Broker
	config       *config.Config
	authorizer   *auth.Authorizer
	logger       *logging.Logger
	ln           net.Listener
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// NewMQTTBridge creates a new MQTT bridge with default timeouts.
func NewMQTTBridge(cfg *config.Config, b Broker, authorizer *auth.Authorizer, logger *logging.Logger) *MQTTBridge {
	return &MQTTBridge{
		broker:       b,
		config:       cfg,
		authorizer:   authorizer,
		logger:       logger,
		readTimeout:  DefaultMQTTReadTimeout,
		writeTimeout: DefaultMQTTWriteTimeout,
	}
}

// Start starts the MQTT bridge.
func (b *MQTTBridge) Start() error {
	addr := b.config.MQTT.Addr
	var ln net.Listener
	var err error

	tlsEnabled, certFile, keyFile, caFile := b.config.GetMQTTTLSConfig()
	if tlsEnabled {
		tlsCfg, err := crypto.NewServerTLSConfig(crypto.TLSConfig{
			CertFile: certFile,
			KeyFile:  keyFile,
			CAFile:   caFile,
		})
		if err != nil {
			return fmt.Errorf("failed to configure TLS for MQTT: %w", err)
		}
		ln, err = tls.Listen("tcp", addr, tlsCfg)
		if err != nil {
			return fmt.Errorf("failed to start MQTT bridge with TLS: %w", err)
		}
		b.logger.Info("MQTT bridge listening (TLS)", "addr", addr)
	} else {
		ln, err = net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("failed to start MQTT bridge: %w", err)
		}
		b.logger.Info("MQTT bridge listening", "addr", addr)
	}

	b.ln = ln
	go b.acceptLoop()
	return nil
}

func (b *MQTTBridge) acceptLoop() {
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			return
		}
		go b.handleConn(conn)
	}
}

// mqttConn is a thread-safe wrapper for MQTT connections.
//
// It provides:
//   - Thread-safe write operations
//   - Write timeouts to prevent blocking on slow clients
//   - User authentication state tracking
type mqttConn struct {
	conn         net.Conn
	mu           sync.Mutex
	username     string
	writeTimeout time.Duration
}

// Write writes data to the connection with a timeout.
func (c *mqttConn) Write(p []byte) (n int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writeTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	return c.conn.Write(p)
}

func (b *MQTTBridge) handleConn(conn net.Conn) {
	defer conn.Close()
	b.logger.Info("New MQTT connection", "remote", conn.RemoteAddr())

	mqtt := &mqttConn{
		conn:         conn,
		writeTimeout: b.writeTimeout,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Basic MQTT packet reader with read timeout
	for {
		// Set read deadline for idle connection detection
		if b.readTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(b.readTimeout))
		}

		header := make([]byte, 1)
		_, err := io.ReadFull(conn, header)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				b.logger.Debug("MQTT connection timed out", "remote", conn.RemoteAddr())
			}
			return
		}

		packetType := header[0] >> 4

		// Read remaining length (variable length encoding)
		remainingLen, err := b.readRemainingLength(conn)
		if err != nil {
			b.logger.Error("Failed to read MQTT remaining length", "error", err)
			return
		}

		payload := make([]byte, remainingLen)
		if remainingLen > 0 {
			_, err = io.ReadFull(conn, payload)
			if err != nil {
				return
			}
		}

		switch packetType {
		case 1: // CONNECT
			// Basic CONNECT handling
			if len(payload) < 10 {
				return
			}
			// Variable header: Protocol Name (6), Version (1), Connect Flags (1), Keep Alive (2)
			// Protocol Name "MQTT" or "MQIsdp"
			protocolNameLen := int(binary.BigEndian.Uint16(payload[0:2]))
			curr := 2 + protocolNameLen + 1 // version
			connectFlags := payload[curr]
			curr += 3 // Flags (1) + Keep Alive (2)

			// Payload
			// Client ID
			if curr+2 > len(payload) {
				return
			}
			clientIDLen := int(binary.BigEndian.Uint16(payload[curr : curr+2]))
			curr += 2 + clientIDLen

			// Will Topic/Message (skipped if flags not set)
			if connectFlags&0x04 != 0 { // Will Flag
				if curr+2 > len(payload) {
					return
				}
				willTopicLen := int(binary.BigEndian.Uint16(payload[curr : curr+2]))
				curr += 2 + willTopicLen
				if curr+2 > len(payload) {
					return
				}
				willMsgLen := int(binary.BigEndian.Uint16(payload[curr : curr+2]))
				curr += 2 + willMsgLen
			}

			username := ""
			password := ""

			// User Name
			if connectFlags&0x80 != 0 {
				if curr+2 > len(payload) {
					return
				}
				usernameLen := int(binary.BigEndian.Uint16(payload[curr : curr+2]))
				curr += 2
				if curr+usernameLen > len(payload) {
					return
				}
				username = string(payload[curr : curr+usernameLen])
				curr += usernameLen
			}

			// Password
			if connectFlags&0x40 != 0 {
				if curr+2 > len(payload) {
					return
				}
				passwordLen := int(binary.BigEndian.Uint16(payload[curr : curr+2]))
				curr += 2
				if curr+passwordLen > len(payload) {
					return
				}
				password = string(payload[curr : curr+passwordLen])
				curr += passwordLen
			}

			// Authenticate
			if b.config.Auth.Enabled {
				if username == "" && !b.authorizer.AllowAnonymous() {
					// Connection Refused, identifier rejected (or not authorized)
					// MQTT v3.1.1 CONNACK: [0x20, 0x02, 0x00, 0x04] (Bad user name or password)
					mqtt.Write([]byte{0x20, 0x02, 0x00, 0x04})
					return
				}
				if username != "" {
					if _, err := b.authorizer.Authenticate(username, password); err != nil {
						mqtt.Write([]byte{0x20, 0x02, 0x00, 0x04})
						return
					}
					mqtt.username = username
				}
			}

			// MQTT v3.1.1 CONNACK: [0x20, 0x02, 0x00, 0x00] (Accepted)
			mqtt.Write([]byte{0x20, 0x02, 0x00, 0x00})
			b.logger.Info("MQTT client connected", "remote", conn.RemoteAddr(), "user", username)
		case 3: // PUBLISH
			// Basic parsing: topic length (2 bytes), topic, payload
			if len(payload) < 2 {
				continue
			}
			topicLen := int(binary.BigEndian.Uint16(payload[0:2]))
			if len(payload) < 2+topicLen {
				continue
			}
			topic := string(payload[2 : 2+topicLen])
			data := payload[2+topicLen:]

			// Check authorization
			if b.config.Auth.Enabled {
				if err := b.authorizer.AuthorizeTopicAccess(mqtt.username, topic, auth.PermissionWrite); err != nil {
					b.logger.Warn("MQTT publish unauthorized", "topic", topic, "user", mqtt.username, "error", err)
					// MQTT doesn't have a simple way to return authorization error for PUBLISH in v3.1.1
					// without closing the connection or using QoS > 0 with error PUBACK (not in 3.1.1).
					// We just skip it or log it.
					continue
				}
			}

			_, _, err := b.broker.ProduceWithKeyAndPartition(topic, nil, data)
			if err != nil {
				b.logger.Error("MQTT bridge failed to produce", "topic", topic, "error", err)
			} else {
				b.logger.Debug("MQTT message bridged", "topic", topic, "len", len(data))
			}
		case 8: // SUBSCRIBE
			if len(payload) < 2 {
				continue
			}
			packetID := binary.BigEndian.Uint16(payload[0:2])
			// Payload: topic filter length (2), topic filter, qos (1)
			curr := 2
			var qosList []byte
			for curr < len(payload) {
				topicLen := int(binary.BigEndian.Uint16(payload[curr : curr+2]))
				curr += 2
				topic := string(payload[curr : curr+topicLen])
				curr += topicLen
				qos := payload[curr]
				curr++
				qosList = append(qosList, qos)

				// Check authorization
				if b.config.Auth.Enabled {
					if err := b.authorizer.AuthorizeTopicAccess(mqtt.username, topic, auth.PermissionRead); err != nil {
						b.logger.Warn("MQTT subscribe unauthorized", "topic", topic, "user", mqtt.username, "error", err)
						// MQTT v3.1.1 SUBACK error is 0x80
						qosList[len(qosList)-1] = 0x80
						continue
					}
				}

				// Start subscription
				offset, err := b.broker.Subscribe(topic, "mqtt-bridge", 0, protocol.SubscribeFromLatest)
				if err != nil {
					b.logger.Error("MQTT subscribe failed", "topic", topic, "error", err)
				} else {
					go b.subscriptionLoop(mqtt, ctx, topic, 0, offset)
				}
			}
			// Send SUBACK
			suback := make([]byte, 2+2+len(qosList))
			suback[0] = 0x90
			suback[1] = byte(2 + len(qosList))
			binary.BigEndian.PutUint16(suback[2:4], packetID)
			copy(suback[4:], qosList)
			mqtt.Write(suback)

		case 12: // PINGREQ
			// Send PINGRESP [0xD0, 0x00]
			mqtt.Write([]byte{0xD0, 0x00})
		case 14: // DISCONNECT
			return
		}
	}
}

// subscriptionLoop continuously polls for new messages and sends them to the MQTT client.
//
// The loop uses a configurable poll interval to balance latency vs CPU usage.
// It implements exponential backoff on errors to prevent overwhelming the broker.
func (b *MQTTBridge) subscriptionLoop(mqtt *mqttConn, ctx context.Context, topic string, partition int, offset uint64) {
	pollInterval := DefaultMQTTSubscriptionPollInterval
	errorBackoff := time.Second

	for {
		select {
		case <-ctx.Done():
			b.logger.Debug("MQTT subscription loop ended", "topic", topic, "partition", partition)
			return
		default:
			msgs, nextOffset, err := b.broker.FetchWithKeys(topic, partition, offset, 10, "")
			if err != nil {
				b.logger.Error("MQTT subscription fetch failed", "topic", topic, "partition", partition, "error", err)
				// Exponential backoff on errors, capped at 30 seconds
				time.Sleep(errorBackoff)
				if errorBackoff < 30*time.Second {
					errorBackoff *= 2
				}
				continue
			}
			// Reset backoff on success
			errorBackoff = time.Second

			if len(msgs) == 0 {
				time.Sleep(pollInterval)
				continue
			}

			for _, msg := range msgs {
				// Send MQTT PUBLISH packet (QoS 0)
				topicBytes := []byte(topic)
				varLen := 2 + len(topicBytes)
				remainingLen := varLen + len(msg.Value)

				packet := make([]byte, 0, 5+remainingLen)
				packet = append(packet, 0x30) // PUBLISH, QoS 0, no retain
				packet = append(packet, b.encodeRemainingLength(remainingLen)...)

				packet = append(packet, byte(len(topicBytes)>>8), byte(len(topicBytes)&0xFF))
				packet = append(packet, topicBytes...)
				packet = append(packet, msg.Value...)

				if _, err := mqtt.Write(packet); err != nil {
					b.logger.Debug("MQTT subscription write failed, closing", "error", err)
					return
				}
			}
			offset = nextOffset
		}
	}
}

func (b *MQTTBridge) encodeRemainingLength(length int) []byte {
	var res []byte
	for {
		digit := byte(length % 128)
		length /= 128
		if length > 0 {
			digit |= 0x80
		}
		res = append(res, digit)
		if length == 0 {
			break
		}
	}
	return res
}

func (b *MQTTBridge) readRemainingLength(r io.Reader) (int, error) {
	multiplier := 1
	value := 0
	for {
		b := make([]byte, 1)
		if _, err := io.ReadFull(r, b); err != nil {
			return 0, err
		}
		digit := b[0]
		value += int(digit&127) * multiplier
		if multiplier > 128*128*128 {
			return 0, fmt.Errorf("malformed remaining length")
		}
		multiplier *= 128
		if digit&128 == 0 {
			break
		}
	}
	return value, nil
}

// Stop stops the MQTT bridge.
func (b *MQTTBridge) Stop() {
	if b.ln != nil {
		b.ln.Close()
	}
}
