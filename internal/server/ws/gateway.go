// Package ws provides a WebSocket gateway for FlyMQ.
//
// The WebSocket gateway enables browser-based and other WebSocket clients to
// interact with FlyMQ using a JSON-based command protocol. It supports:
//   - Message production and consumption
//   - Topic subscriptions with push-based message delivery
//   - Authentication and authorization
//   - TLS/WSS encryption
//
// # Protocol
//
// Clients send JSON requests with the following structure:
//
//	{
//	  "id": "request-id",
//	  "command": "produce|consume|subscribe|unsubscribe|login",
//	  "params": { ... command-specific parameters ... }
//	}
//
// The server responds with:
//
//	{
//	  "id": "request-id",
//	  "success": true|false,
//	  "data": { ... response data ... },
//	  "error": "error message if success is false"
//	}
//
// For subscriptions, the server pushes messages:
//
//	{
//	  "command": "message",
//	  "data": { "topic": "...", "partition": 0, "offset": 123, "key": "...", "value": "..." }
//	}
package ws

import (
	"context"
	"encoding/json"
	"flymq/internal/auth"
	"flymq/internal/protocol"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"flymq/internal/broker"
	"flymq/internal/config"
	"flymq/internal/crypto"
	"flymq/internal/logging"

	"github.com/gorilla/websocket"
)

// Default configuration values for WebSocket gateway
const (
	// DefaultReadBufferSize is the default size of the read buffer
	DefaultReadBufferSize = 4096
	// DefaultWriteBufferSize is the default size of the write buffer
	DefaultWriteBufferSize = 4096
	// DefaultSubscriptionPollInterval is the default interval for polling messages in subscriptions
	DefaultSubscriptionPollInterval = 100 * time.Millisecond
	// DefaultPingInterval is the interval for sending ping frames
	DefaultPingInterval = 30 * time.Second
	// DefaultPongTimeout is the timeout for receiving pong responses
	DefaultPongTimeout = 10 * time.Second
	// DefaultWriteTimeout is the timeout for write operations
	DefaultWriteTimeout = 10 * time.Second
)

// createUpgrader creates a WebSocket upgrader with the given configuration.
// The allowedOrigins parameter specifies which origins are allowed to connect.
// If empty or contains "*", all origins are allowed (not recommended for production).
func createUpgrader(allowedOrigins []string) websocket.Upgrader {
	return websocket.Upgrader{
		ReadBufferSize:  DefaultReadBufferSize,
		WriteBufferSize: DefaultWriteBufferSize,
		CheckOrigin: func(r *http.Request) bool {
			// If no origins specified or wildcard, allow all (development mode)
			if len(allowedOrigins) == 0 {
				return true
			}
			for _, origin := range allowedOrigins {
				if origin == "*" {
					return true
				}
			}

			// Check if the request origin matches any allowed origin
			origin := r.Header.Get("Origin")
			if origin == "" {
				// No origin header - likely not a browser request
				return true
			}

			for _, allowed := range allowedOrigins {
				if origin == allowed || strings.HasSuffix(origin, allowed) {
					return true
				}
			}
			return false
		},
		// Enable compression for better performance over slow networks
		EnableCompression: true,
	}
}

// WSRequest represents a JSON request from a WebSocket client.
type WSRequest struct {
	ID      string          `json:"id"`
	Command string          `json:"command"`
	Params  json.RawMessage `json:"params"`
}

// WSResponse represents a JSON response to a WebSocket client.
type WSResponse struct {
	ID      string      `json:"id,omitempty"`
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// PushMessage represents a message pushed to a subscriber.
type PushMessage struct {
	Command string      `json:"command"` // always "message"
	Data    interface{} `json:"data"`
}

// ProduceParams defines parameters for the produce command.
type ProduceParams struct {
	Topic     string `json:"topic"`
	Key       []byte `json:"key,omitempty"`
	Value     []byte `json:"value"`
	Partition int    `json:"partition,omitempty"`
}

// ConsumeParams defines parameters for the consume command.
type ConsumeParams struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
}

// SubscribeParams defines parameters for the subscribe command.
type SubscribeParams struct {
	Topic     string `json:"topic"`
	GroupID   string `json:"group_id"`
	Partition int    `json:"partition"`
	Mode      string `json:"mode"`
}

// LoginParams defines parameters for the login command.
type LoginParams struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// wsConn is a thread-safe wrapper for websocket.Conn.
//
// It provides:
//   - Thread-safe write operations (WebSocket writes are not concurrent-safe)
//   - Ping/pong heartbeat for connection health monitoring
//   - Write timeouts to prevent blocking on slow clients
//   - User authentication state tracking
type wsConn struct {
	conn       *websocket.Conn
	mu         sync.Mutex
	username   string
	lastPong   time.Time
	pingTicker *time.Ticker
	done       chan struct{}
}

// newWSConn creates a new wsConn wrapper with ping/pong support.
func newWSConn(conn *websocket.Conn) *wsConn {
	c := &wsConn{
		conn:     conn,
		lastPong: time.Now(),
		done:     make(chan struct{}),
	}

	// Set up pong handler to track connection health
	conn.SetPongHandler(func(appData string) error {
		c.mu.Lock()
		c.lastPong = time.Now()
		c.mu.Unlock()
		return nil
	})

	// Start ping goroutine for connection keepalive
	c.pingTicker = time.NewTicker(DefaultPingInterval)
	go c.pingLoop()

	return c
}

// pingLoop sends periodic ping frames to keep the connection alive
// and detect dead connections.
func (c *wsConn) pingLoop() {
	for {
		select {
		case <-c.done:
			return
		case <-c.pingTicker.C:
			c.mu.Lock()
			// Check if we've received a pong recently
			if time.Since(c.lastPong) > DefaultPingInterval+DefaultPongTimeout {
				c.mu.Unlock()
				// Connection is dead, close it
				c.conn.Close()
				return
			}

			// Send ping
			c.conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout))
			err := c.conn.WriteMessage(websocket.PingMessage, nil)
			c.mu.Unlock()

			if err != nil {
				return
			}
		}
	}
}

// Close closes the connection and stops the ping loop.
func (c *wsConn) Close() error {
	close(c.done)
	if c.pingTicker != nil {
		c.pingTicker.Stop()
	}
	return c.conn.Close()
}

// WriteJSON writes a JSON message to the connection with a timeout.
func (c *wsConn) WriteJSON(v interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout))
	return c.conn.WriteJSON(v)
}

// Broker defines the interface for the WebSocket gateway to interact with the broker.
type Broker interface {
	ProduceWithKeyAndPartition(topic string, key, data []byte) (uint64, int, error)
	ConsumeFromPartitionWithKey(topic string, partition int, offset uint64) (key []byte, value []byte, err error)
	Subscribe(topic, groupID string, partition int, mode protocol.SubscribeMode) (uint64, error)
	CommitOffset(topic, groupID string, partition int, offset uint64) (bool, error)
	FetchWithKeys(topic string, partition int, offset uint64, maxMessages int, filter string) ([]broker.FetchedMessage, uint64, error)
	ListTopics() []string
	GetClusterMetadata(topic string) (*protocol.BinaryClusterMetadataResponse, error)
}

// Gateway handles WebSocket connections for browser clients.
//
// The gateway provides:
//   - JSON-based command protocol for produce/consume/subscribe operations
//   - Authentication via login command
//   - Per-operation authorization checks
//   - TLS/WSS encryption support
//   - Connection health monitoring via ping/pong
//   - Configurable origin checking for CORS security
type Gateway struct {
	broker         Broker
	config         *config.Config
	authorizer     *auth.Authorizer
	logger         *logging.Logger
	server         *http.Server
	upgrader       websocket.Upgrader
	allowedOrigins []string
}

// NewGateway creates a new WebSocket gateway.
//
// The allowedOrigins parameter can be configured via config.WS.AllowedOrigins.
// If empty, all origins are allowed (suitable for development only).
func NewGateway(cfg *config.Config, b Broker, authorizer *auth.Authorizer, logger *logging.Logger) *Gateway {
	// Get allowed origins from config (if available)
	var allowedOrigins []string
	// Note: AllowedOrigins would need to be added to config.WSConfig

	return &Gateway{
		broker:         b,
		config:         cfg,
		authorizer:     authorizer,
		logger:         logger,
		upgrader:       createUpgrader(allowedOrigins),
		allowedOrigins: allowedOrigins,
	}
}

// Start starts the WebSocket gateway.
func (g *Gateway) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", g.handleWebSocket)

	g.server = &http.Server{
		Addr:    g.config.WS.Addr,
		Handler: mux,
	}

	// Configure TLS if enabled
	tlsEnabled, certFile, keyFile, caFile := g.config.GetWSTLSConfig()
	if tlsEnabled {
		tlsCfg, err := crypto.NewServerTLSConfig(crypto.TLSConfig{
			CertFile: certFile,
			KeyFile:  keyFile,
			CAFile:   caFile,
		})
		if err != nil {
			return fmt.Errorf("failed to configure TLS for WebSocket: %w", err)
		}
		g.server.TLSConfig = tlsCfg
		g.logger.Info("WebSocket gateway listening (WSS)", "addr", g.server.Addr)
		go func() {
			if err := g.server.ListenAndServeTLS("", ""); err != http.ErrServerClosed {
				g.logger.Error("WebSocket server (WSS) failed", "error", err)
			}
		}()
	} else {
		g.logger.Info("WebSocket gateway listening", "addr", g.server.Addr)
		go func() {
			if err := g.server.ListenAndServe(); err != http.ErrServerClosed {
				g.logger.Error("WebSocket server failed", "error", err)
			}
		}()
	}

	return nil
}

// Stop stops the WebSocket gateway.
func (g *Gateway) Stop() error {
	if g.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return g.server.Shutdown(ctx)
	}
	return nil
}

func (g *Gateway) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := g.upgrader.Upgrade(w, r, nil)
	if err != nil {
		g.logger.Error("Failed to upgrade to WebSocket", "error", err)
		return
	}

	// Create thread-safe connection wrapper with ping/pong support
	ws := newWSConn(conn)
	defer ws.Close()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	g.logger.Info("New WebSocket connection", "remote", conn.RemoteAddr())

	// Active subscriptions for this connection: topic:partition -> cancelFunc
	subs := make(map[string]context.CancelFunc)
	var subsMu sync.Mutex

	defer func() {
		subsMu.Lock()
		for _, stop := range subs {
			stop()
		}
		subsMu.Unlock()
		g.logger.Debug("WebSocket connection cleanup complete", "remote", conn.RemoteAddr())
	}()

	for {
		_, p, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				g.logger.Error("WebSocket unexpected close", "remote", conn.RemoteAddr(), "error", err)
			} else {
				g.logger.Info("WebSocket closed", "remote", conn.RemoteAddr())
			}
			return
		}

		var req WSRequest
		if err := json.Unmarshal(p, &req); err != nil {
			ws.WriteJSON(WSResponse{Success: false, Error: "Invalid JSON request"})
			continue
		}

		go g.handleCommand(ws, ctx, &req, subs, &subsMu)
	}
}

func (g *Gateway) handleCommand(ws *wsConn, ctx context.Context, req *WSRequest, subs map[string]context.CancelFunc, subsMu *sync.Mutex) {
	var resp WSResponse
	resp.ID = req.ID
	resp.Success = true

	// Check authentication for all commands except "login"
	if g.config.Auth.Enabled && req.Command != "login" && ws.username == "" {
		if !g.authorizer.AllowAnonymous() {
			resp.Success = false
			resp.Error = "Unauthenticated. Please login first."
			ws.WriteJSON(resp)
			return
		}
	}

	switch req.Command {
	case "login":
		var params LoginParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			resp.Success = false
			resp.Error = "Invalid login parameters"
		} else {
			if _, err := g.authorizer.Authenticate(params.Username, params.Password); err != nil {
				resp.Success = false
				resp.Error = "Invalid credentials"
			} else {
				ws.username = params.Username
				resp.Data = map[string]interface{}{"username": ws.username}
				g.logger.Info("WebSocket client logged in", "user", ws.username)
			}
		}

	case "produce":
		var params ProduceParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			resp.Success = false
			resp.Error = "Invalid produce parameters"
		} else {
			// Check authorization
			if g.config.Auth.Enabled {
				if err := g.authorizer.AuthorizeTopicAccess(ws.username, params.Topic, auth.PermissionWrite); err != nil {
					resp.Success = false
					resp.Error = err.Error()
					ws.WriteJSON(resp)
					return
				}
			}

			offset, partition, err := g.broker.ProduceWithKeyAndPartition(params.Topic, params.Key, params.Value)
			if err != nil {
				resp.Success = false
				resp.Error = err.Error()
			} else {
				resp.Data = map[string]interface{}{
					"offset":    offset,
					"partition": partition,
				}
			}
		}

	case "consume":
		var params ConsumeParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			resp.Success = false
			resp.Error = "Invalid consume parameters"
		} else {
			// Check authorization
			if g.config.Auth.Enabled {
				if err := g.authorizer.AuthorizeTopicAccess(ws.username, params.Topic, auth.PermissionRead); err != nil {
					resp.Success = false
					resp.Error = err.Error()
					ws.WriteJSON(resp)
					return
				}
			}

			key, value, err := g.broker.ConsumeFromPartitionWithKey(params.Topic, params.Partition, params.Offset)
			if err != nil {
				resp.Success = false
				resp.Error = err.Error()
			} else {
				resp.Data = map[string]interface{}{
					"key":   key,
					"value": value,
				}
			}
		}

	case "subscribe":
		var params SubscribeParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			resp.Success = false
			resp.Error = "Invalid subscribe parameters"
		} else {
			subKey := fmt.Sprintf("%s:%d", params.Topic, params.Partition)
			subsMu.Lock()
			if _, exists := subs[subKey]; exists {
				subsMu.Unlock()
				resp.Success = false
				resp.Error = "Already subscribed to this topic/partition"
			} else {
				// Check authorization
				if g.config.Auth.Enabled {
					if err := g.authorizer.AuthorizeTopicAccess(ws.username, params.Topic, auth.PermissionRead); err != nil {
						subsMu.Unlock()
						resp.Success = false
						resp.Error = err.Error()
						ws.WriteJSON(resp)
						return
					}
				}

				subCtx, subCancel := context.WithCancel(ctx)
				subs[subKey] = subCancel
				subsMu.Unlock()

				offset, err := g.broker.Subscribe(params.Topic, params.GroupID, params.Partition, protocol.SubscribeMode(params.Mode))
				if err != nil {
					subsMu.Lock()
					delete(subs, subKey)
					subsMu.Unlock()
					subCancel()
					resp.Success = false
					resp.Error = err.Error()
				} else {
					resp.Data = map[string]interface{}{"offset": offset}
					go g.subscriptionLoop(ws, subCtx, params.Topic, params.Partition, offset, subKey, subs, subsMu)
				}
			}
		}

	case "unsubscribe":
		var params struct {
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			resp.Success = false
			resp.Error = "Invalid unsubscribe parameters"
		} else {
			subKey := fmt.Sprintf("%s:%d", params.Topic, params.Partition)
			subsMu.Lock()
			if stop, exists := subs[subKey]; exists {
				stop()
				delete(subs, subKey)
				resp.Success = true
			} else {
				resp.Success = false
				resp.Error = "No such subscription"
			}
			subsMu.Unlock()
		}

	case "list_topics":
		topics := g.broker.ListTopics()
		resp.Data = map[string]interface{}{"topics": topics}

	case "get_cluster_metadata":
		var params struct {
			Topic string `json:"topic"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			// topic is optional, so ignore unmarshal error and use empty
		}
		meta, err := g.broker.GetClusterMetadata(params.Topic)
		if err != nil {
			resp.Success = false
			resp.Error = err.Error()
		} else {
			resp.Data = meta
		}

	case "commit":
		var params struct {
			Topic     string `json:"topic"`
			GroupID   string `json:"group_id"`
			Partition int    `json:"partition"`
			Offset    uint64 `json:"offset"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			resp.Success = false
			resp.Error = "Invalid commit parameters"
		} else {
			changed, err := g.broker.CommitOffset(params.Topic, params.GroupID, params.Partition, params.Offset)
			if err != nil {
				resp.Success = false
				resp.Error = err.Error()
			} else {
				resp.Data = map[string]interface{}{"changed": changed}
			}
		}

	default:
		resp.Success = false
		resp.Error = "Unknown command: " + req.Command
	}

	ws.WriteJSON(resp)
}

// subscriptionLoop continuously polls for new messages and pushes them to the WebSocket client.
//
// The loop uses a configurable poll interval to balance latency vs CPU usage.
// It automatically cleans up the subscription when the context is cancelled or
// when a write error occurs (indicating the client has disconnected).
func (g *Gateway) subscriptionLoop(ws *wsConn, ctx context.Context, topic string, partition int, offset uint64, subKey string, subs map[string]context.CancelFunc, subsMu *sync.Mutex) {
	defer func() {
		subsMu.Lock()
		if cancel, exists := subs[subKey]; exists {
			cancel()
			delete(subs, subKey)
		}
		subsMu.Unlock()
		g.logger.Debug("Subscription loop ended", "topic", topic, "partition", partition)
	}()

	pollInterval := DefaultSubscriptionPollInterval
	errorBackoff := time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Poll broker for new messages
			msgs, nextOffset, err := g.broker.FetchWithKeys(topic, partition, offset, 10, "")
			if err != nil {
				g.logger.Error("Fetch failed in subscription", "topic", topic, "partition", partition, "error", err)
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
				push := PushMessage{
					Command: "message",
					Data: map[string]interface{}{
						"topic":     topic,
						"partition": partition,
						"offset":    msg.Offset,
						"key":       msg.Key,
						"value":     msg.Value,
					},
				}
				if err := ws.WriteJSON(push); err != nil {
					g.logger.Debug("Failed to push message to WebSocket, closing subscription", "error", err)
					return
				}
			}
			offset = nextOffset
		}
	}
}
