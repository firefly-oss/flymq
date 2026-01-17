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

/*
FlyMQ Server - Main Entry Point.

USAGE:
======

	flymq [options]

OPTIONS:
========

	-config string    Path to configuration file (JSON format)
	-version          Show version information
	-help             Show help message

ENVIRONMENT VARIABLES:
======================

	FLYMQ_BIND_ADDR      Server bind address (default: :9092)
	FLYMQ_CLUSTER_ADDR   Cluster communication address
	FLYMQ_DATA_DIR       Data directory path

STARTUP SEQUENCE:
=================
1. Parse command line flags and config file
2. Initialize logging
3. Start storage engine
4. Start broker
5. Start TCP server
6. Start admin API and health endpoints
7. Wait for shutdown signal
*/
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"flymq/internal/admin"
	"flymq/internal/banner"
	"flymq/internal/broker"
	"flymq/internal/cluster"
	"flymq/internal/config"
	"flymq/internal/dlq"
	"flymq/internal/health"
	"flymq/internal/logging"
	"flymq/internal/metrics"
	"flymq/internal/server"
)

func printHelp() {
	banner.Print()
	fmt.Println()
	fmt.Println("\033[1;36mUsage:\033[0m")
	fmt.Println("  flymq [options]")
	fmt.Println()
	fmt.Println("\033[1;36mOptions:\033[0m")
	fmt.Println("  -config string    Path to configuration file (JSON format)")
	fmt.Println("  -human-readable   Use human-readable log format instead of JSON")
	fmt.Println("  -quiet            Skip banner and config display, output logs only")
	fmt.Println("  -version          Show version information")
	fmt.Println("  -help, -h         Show this help message")
	fmt.Println()
	fmt.Println("\033[1;36mEnvironment Variables:\033[0m")
	fmt.Println("  FLYMQ_BIND_ADDR          Server bind address (default: :9092)")
	fmt.Println("  FLYMQ_CLUSTER_ADDR       Cluster communication address (default: :9093)")
	fmt.Println("  FLYMQ_DATA_DIR           Data directory path")
	fmt.Println("  FLYMQ_NODE_ID            Unique node identifier")
	fmt.Println("  FLYMQ_LOG_LEVEL          Log level: debug, info, warn, error")
	fmt.Println("  FLYMQ_LOG_JSON           Enable JSON log output (default: true)")
	fmt.Println("  FLYMQ_SEGMENT_BYTES      Log segment size in bytes")
	fmt.Println("  FLYMQ_TLS_ENABLED        Enable TLS (true/false)")
	fmt.Println("  FLYMQ_TLS_CERT_FILE      Path to TLS certificate file")
	fmt.Println("  FLYMQ_TLS_KEY_FILE       Path to TLS private key file")
	fmt.Println("  FLYMQ_TLS_CA_FILE        Path to CA certificate for client auth")
	fmt.Println("  FLYMQ_ENCRYPTION_ENABLED Enable data-at-rest encryption (true/false)")
	fmt.Println("  FLYMQ_ENCRYPTION_KEY     AES-256 encryption key (64 hex chars)")
	fmt.Println()
	fmt.Println("\033[1;36mExamples:\033[0m")
	fmt.Println("  # Start with default settings (JSON logs)")
	fmt.Println("  flymq")
	fmt.Println()
	fmt.Println("  # Start with human-readable logs for development")
	fmt.Println("  flymq -human-readable")
	fmt.Println()
	fmt.Println("  # Start in quiet mode (logs only, no banner)")
	fmt.Println("  flymq -quiet")
	fmt.Println()
	fmt.Println("  # Start with custom config file")
	fmt.Println("  flymq -config /etc/flymq/flymq.json")
	fmt.Println()
}

func main() {
	// Custom flag handling for help
	for _, arg := range os.Args[1:] {
		if arg == "-h" || arg == "--help" || arg == "-help" || arg == "help" {
			printHelp()
			return
		}
	}

	// Parse flags
	configPath := flag.String("config", "", "Path to configuration file")
	humanReadable := flag.Bool("human-readable", false, "Use human-readable log format instead of JSON")
	quietMode := flag.Bool("quiet", false, "Skip banner and config display, output logs only")
	showVersion := flag.Bool("version", false, "Show version information")
	flag.Usage = printHelp
	flag.Parse()

	// Show version and exit
	if *showVersion {
		banner.Print()
		return
	}

	// Load configuration first (before banner, so we can display it)
	cfgMgr := config.Global()
	if *configPath != "" {
		if err := cfgMgr.LoadFromFile(*configPath); err != nil {
			fmt.Printf("Error loading config file: %v\n", err)
			os.Exit(1)
		}
	}
	cfgMgr.LoadFromEnv()
	cfg := cfgMgr.Get()

	// Finalize config (auto-enable linked settings)
	cfg.Finalize()

	// Override log format if --human-readable flag is set
	if *humanReadable {
		cfg.LogJSON = false
	}

	// Display startup banner with full configuration (unless quiet mode)
	if !*quietMode {
		banner.PrintServerWithConfig(cfg)
	}

	// Setup logging
	logging.SetGlobalLevel(logging.ParseLevel(cfg.LogLevel))
	logging.SetJSONMode(cfg.LogJSON)
	logger := logging.NewLogger("main")

	logger.Info("Starting FlyMQ", "version", banner.Version, "node_id", cfg.NodeID)

	// Initialize Broker
	b, err := broker.NewBroker(cfg)
	if err != nil {
		logger.Error("Failed to initialize broker", "error", err)
		os.Exit(1)
	}
	defer b.Close()

	// Initialize Cluster
	// Cluster is started if ClusterAddr is set (even without peers for bootstrap nodes)
	var c *cluster.Cluster
	if cfg.ClusterAddr != "" {
		c, err = cluster.NewCluster(cfg)
		if err != nil {
			logger.Error("Failed to initialize cluster", "error", err)
			os.Exit(1)
		}
		// Wire cluster to broker bidirectionally
		// Cluster needs broker to apply committed commands
		// Broker needs cluster to propose commands
		c.SetApplier(b)
		b.SetCluster(c)
		if err := c.Start(context.Background()); err != nil {
			logger.Error("Failed to start cluster", "error", err)
			os.Exit(1)
		}
		if len(cfg.Peers) > 0 {
			logger.Info("Cluster started", "node_id", cfg.NodeID, "addr", cfg.ClusterAddr, "peers", cfg.Peers)
		} else {
			logger.Info("Cluster started (bootstrap mode, no initial peers)", "node_id", cfg.NodeID, "addr", cfg.ClusterAddr)
		}
	} else {
		logger.Info("Running in standalone mode (no cluster config)")
	}

	// Initialize Server
	srv := server.NewServer(cfg, b)

	// Wire schema registry from server to broker for cluster replication
	if registry := srv.GetSchemaRegistry(); registry != nil {
		b.SetSchemaApplier(registry)
		logger.Info("Schema registry wired to broker for cluster replication")
	}

	// Wire authorizer to cluster for user/ACL replication
	if c != nil {
		if authorizer := srv.GetAuthorizer(); authorizer != nil && authorizer.IsEnabled() {
			c.SetAuthApplier(authorizer)
			logger.Info("Authorizer wired to cluster for user/ACL replication")
		}
	}

	// Start Server
	if err := srv.Start(); err != nil {
		logger.Error("Failed to start server", "error", err)
		os.Exit(1)
	}

	// ========================================================================
	// Observability Services
	// ========================================================================

	// Initialize and start Metrics Server
	var metricsServer *metrics.Server
	if cfg.Observability.Metrics.Enabled {
		metricsServer = metrics.NewServer(&cfg.Observability.Metrics)
		if err := metricsServer.Start(); err != nil {
			logger.Error("Failed to start metrics server", "error", err)
		} else {
			logger.Info("Metrics server started", "addr", cfg.Observability.Metrics.Addr)
		}
	}

	// Initialize and start Health Check Server
	var healthServer *health.Server
	if cfg.Observability.Health.Enabled {
		healthChecker := health.NewChecker(banner.Version)
		// Register health checks
		healthChecker.RegisterCheck("storage", health.StorageCheck(func() error {
			// Simple storage health check
			return nil
		}))
		healthServer = health.NewServer(&cfg.Observability.Health, healthChecker)
		if err := healthServer.Start(); err != nil {
			logger.Error("Failed to start health server", "error", err)
		} else {
			logger.Info("Health server started", "addr", cfg.Observability.Health.Addr)
		}
	}

	// Initialize DLQ Manager if enabled
	var dlqManager *dlq.Manager
	if cfg.DLQ.Enabled {
		dlqManager = dlq.NewManager(&cfg.DLQ, b)
		logger.Info("DLQ manager initialized", "max_retries", cfg.DLQ.MaxRetries, "topic_suffix", cfg.DLQ.TopicSuffix)
	}

	// Initialize and start Admin API Server
	var adminServer *admin.Server
	if cfg.Observability.Admin.Enabled {
		adminHandler := broker.NewAdminHandler(b, cfg, banner.Version)
		// Wire authorizer to admin handler for user/ACL management via REST API
		authorizer := srv.GetAuthorizer()
		if authorizer != nil {
			adminHandler.SetAuthorizer(authorizer)
		}
		// Wire DLQ manager to admin handler for DLQ operations via REST API
		if dlqManager != nil {
			adminHandler.SetDLQManager(dlqManager)
		}
		adminServer = admin.NewServer(&cfg.Observability.Admin, adminHandler)
		// Wire authorizer to admin server for HTTP authentication
		if authorizer != nil {
			adminServer.SetAuthorizer(authorizer)
		}
		if err := adminServer.Start(); err != nil {
			logger.Error("Failed to start admin server", "error", err)
		} else {
			logger.Info("Admin API server started", "addr", cfg.Observability.Admin.Addr,
				"auth_enabled", cfg.Observability.Admin.AuthEnabled)
		}
	}

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down...")

	// Stop observability services
	if adminServer != nil {
		if err := adminServer.Stop(); err != nil {
			logger.Error("Error stopping admin server", "error", err)
		}
	}
	if healthServer != nil {
		if err := healthServer.Stop(); err != nil {
			logger.Error("Error stopping health server", "error", err)
		}
	}
	if metricsServer != nil {
		if err := metricsServer.Stop(); err != nil {
			logger.Error("Error stopping metrics server", "error", err)
		}
	}

	if c != nil {
		if err := c.Stop(); err != nil {
			logger.Error("Error stopping cluster", "error", err)
		}
	}

	if err := srv.Stop(); err != nil {
		logger.Error("Error stopping server", "error", err)
	}
}
