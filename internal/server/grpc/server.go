package grpc

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	flymqv1 "flymq/api/proto/flymq/v1"
	"flymq/internal/auth"
	"flymq/internal/broker"
	"flymq/internal/config"
	"flymq/internal/crypto"
	"flymq/internal/logging"
	"flymq/internal/protocol"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// Broker is a minimal interface for the gRPC server to interact with the broker.
type Broker interface {
	ProduceWithKeyAndPartition(topic string, key []byte, value []byte) (uint64, int, error)
	FetchWithKeys(topic string, partition int, offset uint64, maxMessages int, filter string) ([]broker.FetchedMessage, uint64, error)
	GetTopicMetadata(topic string) (interface{}, error)
	GetClusterMetadata(topic string) (*protocol.BinaryClusterMetadataResponse, error)
}

type contextKey string

const usernameKey contextKey = "username"

// wrappedStream wraps grpc.ServerStream to override context
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}

func newWrappedStream(s grpc.ServerStream, ctx context.Context) grpc.ServerStream {
	return &wrappedStream{s, ctx}
}

// Server implements the FlyMQService gRPC service.
type Server struct {
	flymqv1.UnimplementedFlyMQServiceServer
	broker     Broker
	config     *config.Config
	authorizer *auth.Authorizer
	logger     *logging.Logger
	gs         *grpc.Server
}

// NewServer creates a new gRPC server.
func NewServer(cfg *config.Config, b Broker, authorizer *auth.Authorizer, logger *logging.Logger) *Server {
	s := &Server{
		broker:     b,
		config:     cfg,
		authorizer: authorizer,
		logger:     logger,
	}

	var opts []grpc.ServerOption

	// Configure TLS if enabled
	tlsEnabled, certFile, keyFile, caFile := cfg.GetGRPCTLSConfig()
	if tlsEnabled {
		tlsCfg, err := crypto.NewServerTLSConfig(crypto.TLSConfig{
			CertFile: certFile,
			KeyFile:  keyFile,
			CAFile:   caFile,
		})
		if err != nil {
			logger.Error("Failed to configure TLS for gRPC", "error", err)
		} else {
			creds := credentials.NewTLS(tlsCfg)
			opts = append(opts, grpc.Creds(creds))
			logger.Info("gRPC server configured with TLS")
		}
	}

	// Add authentication interceptors if enabled
	if cfg.Auth.Enabled {
		opts = append(opts, grpc.UnaryInterceptor(s.unaryAuthInterceptor))
		opts = append(opts, grpc.StreamInterceptor(s.streamAuthInterceptor))
		logger.Info("gRPC server configured with authentication")
	}

	gs := grpc.NewServer(opts...)
	flymqv1.RegisterFlyMQServiceServer(gs, s)
	reflection.Register(gs)
	s.gs = gs

	return s
}

// Start starts the gRPC server.
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.config.GRPC.Addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.logger.Info("gRPC server listening", "addr", s.config.GRPC.Addr)
	go func() {
		if err := s.gs.Serve(lis); err != nil {
			s.logger.Error("gRPC server failed", "error", err)
		}
	}()

	return nil
}

// Stop stops the gRPC server.
func (s *Server) Stop() {
	if s.gs != nil {
		s.gs.GracefulStop()
	}
}

func (s *Server) unaryAuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if s.authorizer == nil || !s.authorizer.IsEnabled() {
		return handler(ctx, req)
	}

	username, password, ok := s.getCredentials(ctx)
	if !ok {
		if s.authorizer.AllowAnonymous() {
			return handler(ctx, req)
		}
		return nil, status.Error(codes.Unauthenticated, "missing credentials")
	}

	if _, err := s.authorizer.Authenticate(username, password); err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}

	// Add username to context for handler
	ctx = context.WithValue(ctx, usernameKey, username)
	return handler(ctx, req)
}

func (s *Server) streamAuthInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if s.authorizer == nil || !s.authorizer.IsEnabled() {
		return handler(srv, ss)
	}

	ctx := ss.Context()
	username, password, ok := s.getCredentials(ctx)
	if !ok {
		if s.authorizer.AllowAnonymous() {
			return handler(srv, ss)
		}
		return status.Error(codes.Unauthenticated, "missing credentials")
	}

	if _, err := s.authorizer.Authenticate(username, password); err != nil {
		return status.Error(codes.Unauthenticated, "invalid credentials")
	}

	// Add username to context for handler
	ctx = context.WithValue(ctx, usernameKey, username)
	return handler(srv, newWrappedStream(ss, ctx))
}

func (s *Server) getUsername(ctx context.Context) string {
	if username, ok := ctx.Value(usernameKey).(string); ok {
		return username
	}
	return ""
}

func (s *Server) getCredentials(ctx context.Context) (string, string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", false
	}

	usernames := md.Get("username")
	passwords := md.Get("password")

	if len(usernames) > 0 && len(passwords) > 0 {
		return usernames[0], passwords[0], true
	}

	// Check for authorization header (Basic auth style)
	auths := md.Get("authorization")
	if len(auths) > 0 && strings.HasPrefix(auths[0], "Basic ") {
		// We could decode basic auth here, but for now we expect explicit username/password metadata
		// following FlyMQ's pattern.
	}

	return "", "", false
}

// Produce handles message production via gRPC.
func (s *Server) Produce(ctx context.Context, req *flymqv1.ProduceRequest) (*flymqv1.ProduceResponse, error) {
	// Check authorization
	if s.config.Auth.Enabled {
		username := s.getUsername(ctx)
		if err := s.authorizer.AuthorizeTopicAccess(username, req.Topic, auth.PermissionWrite); err != nil {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
	}

	// Handle single message or batch
	if len(req.Messages) > 0 {
		// Basic batch implementation - in a real scenario we might want a broker-level batch produce
		var lastOffset uint64
		var lastPartition int
		for _, msg := range req.Messages {
			off, part, err := s.broker.ProduceWithKeyAndPartition(req.Topic, msg.Key, msg.Value)
			if err != nil {
				return nil, err
			}
			lastOffset = off
			lastPartition = part
		}
		return &flymqv1.ProduceResponse{
			Offset:    lastOffset,
			Partition: int32(lastPartition),
		}, nil
	}

	// Single message produce
	off, part, err := s.broker.ProduceWithKeyAndPartition(req.Topic, req.Key, req.Value)
	if err != nil {
		return nil, err
	}

	return &flymqv1.ProduceResponse{
		Offset:    off,
		Partition: int32(part),
	}, nil
}

// Consume handles message consumption via gRPC stream.
func (s *Server) Consume(req *flymqv1.ConsumeRequest, stream flymqv1.FlyMQService_ConsumeServer) error {
	// Check authorization
	if s.config.Auth.Enabled {
		username := s.getUsername(stream.Context())
		if err := s.authorizer.AuthorizeTopicAccess(username, req.Topic, auth.PermissionRead); err != nil {
			return status.Error(codes.PermissionDenied, err.Error())
		}
	}

	// Simple polling loop for now. In production, this should use a subscription mechanism.
	offset := req.Offset
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			msgs, nextOffset, err := s.broker.FetchWithKeys(req.Topic, int(req.Partition), offset, int(req.MaxMessages), "")
			if err != nil {
				return err
			}

			if len(msgs) == 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			for _, msg := range msgs {
				resp := &flymqv1.ConsumeResponse{
					Offset: msg.Offset,
					Key:    msg.Key,
					Value:  msg.Value,
					// Headers could be added here if supported by broker
				}
				if err := stream.Send(resp); err != nil {
					return err
				}
			}
			offset = nextOffset
		}
	}
}

// GetMetadata handles metadata requests via gRPC.
func (s *Server) GetMetadata(ctx context.Context, req *flymqv1.MetadataRequest) (*flymqv1.MetadataResponse, error) {
	resp := &flymqv1.MetadataResponse{}

	// Get full cluster metadata from broker
	clusterMeta, err := s.broker.GetClusterMetadata("")
	if err != nil {
		s.logger.Error("Failed to get cluster metadata", "error", err)
		// Fallback to basic metadata if possible
	}

	// Track brokers we've seen to avoid duplicates in response
	seenBrokers := make(map[string]bool)

	if clusterMeta != nil {
		for _, t := range clusterMeta.Topics {
			// If topics requested, only include those
			if len(req.Topics) > 0 {
				found := false
				for _, requested := range req.Topics {
					if t.Topic == requested {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}

			tMeta := &flymqv1.TopicMetadata{
				Name: t.Topic,
			}

			for _, p := range t.Partitions {
				tMeta.Partitions = append(tMeta.Partitions, &flymqv1.PartitionMetadata{
					Id:     p.Partition,
					Leader: p.LeaderID,
				})

				// Add broker if not seen before
				if p.LeaderID != "" && !seenBrokers[p.LeaderID] {
					resp.Brokers = append(resp.Brokers, &flymqv1.BrokerNode{
						Id:      p.LeaderID,
						Address: p.LeaderAddr,
					})
					seenBrokers[p.LeaderID] = true
				}
			}
			resp.Topics = append(resp.Topics, tMeta)
		}
	}

	return resp, nil
}
