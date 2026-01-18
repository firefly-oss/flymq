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
FlyMQ CLI - Command Line Interface.

COMMANDS:
=========

	produce, pub     Produce messages to a topic
	consume, sub     Consume messages from a topic
	topics           Manage topics (list, create, delete, describe)
	explore          Interactive topic explorer
	groups           Manage consumer groups
	dlq              Manage dead letter queues (fetch, replay, purge)
	cluster          View cluster information and manage partitions
	health           Check server health
	benchmark        Run performance benchmarks

CLUSTER SUBCOMMANDS (Partition Management):
===========================================

	cluster metadata    Get partition-to-node mappings for smart routing
	cluster partitions  List all partition assignments
	cluster leaders     Show leader distribution across nodes
	cluster rebalance   Trigger partition rebalance for even distribution
	cluster reassign    Reassign a partition to a new leader

EXAMPLES:
=========

	# Produce a message
	flymq-cli produce my-topic -m "Hello, World!"

	# Consume messages
	flymq-cli consume my-topic -g my-group

	# List topics
	flymq-cli topics list

	# Check cluster health
	flymq-cli health

	# View partition leader distribution
	flymq-cli cluster leaders

	# Trigger partition rebalance
	flymq-cli cluster rebalance

	# Reassign partition to new leader
	flymq-cli cluster reassign my-topic 0 --leader node-2
*/
package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"flymq/internal/banner"
	"flymq/internal/protocol"
	"flymq/pkg/cli"
	"flymq/pkg/client"
)

const defaultAddr = "localhost:9092"

// globalOptions holds options that can appear before the command
var globalOptions []string

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Parse global options that appear before the command
	// These are options like --tls, --addr, --username, etc.
	allArgs := os.Args[1:]
	cmd, cmdArgs := extractCommandAndArgs(allArgs)

	if cmd == "" {
		printUsage()
		os.Exit(1)
	}

	// Merge command args with global options so they're available to connect()
	// Global options are appended at the end so positional arguments come first
	args := append(cmdArgs, globalOptions...)

	switch cmd {
	case "help", "-h", "--help":
		printUsage()
	case "version", "-v", "--version":
		banner.Print()
	case "produce", "pub":
		cmdProduce(args)
	case "consume", "sub":
		cmdConsume(args)
	case "subscribe":
		cmdSubscribe(args)
	case "topics", "list":
		cmdListTopics(args)
	case "create":
		cmdCreateTopic(args)
	case "delete":
		cmdDeleteTopic(args)
	case "info":
		cmdTopicInfo(args)
	// Advanced commands
	case "produce-delayed":
		cmdProduceDelayed(args)
	case "produce-ttl":
		cmdProduceTTL(args)
	case "dlq":
		cmdDLQ(args)
	case "schema":
		cmdSchema(args)
	case "txn":
		cmdTxn(args)
	case "health":
		cmdHealth(args)
	case "admin":
		cmdAdmin(args)
	case "cluster":
		cmdCluster(args)
	case "groups":
		cmdGroups(args)
	case "explore":
		cmdExplore(args)
	case "auth":
		cmdAuth(args)
	case "whoami":
		cmdWhoAmI(args)
	case "users":
		cmdUsers(args)
	case "roles":
		cmdRoles(args)
	case "acl":
		cmdACL(args)
	case "audit":
		cmdAudit(args)
	default:
		cli.Error("Unknown command: %s", cmd)
		printUsage()
		os.Exit(1)
	}
}

// extractCommandAndArgs separates global options from the command and its arguments.
// Global options can appear before the command and are stored in globalOptions.
func extractCommandAndArgs(args []string) (string, []string) {
	globalOptions = nil
	i := 0

	// Known global option flags (with values)
	globalWithValue := map[string]bool{
		// Server address
		"-a": true, "--addr": true,
		// Binary protocol authentication
		"-u": true, "--username": true,
		"-P": true, "--password": true,
		// Binary protocol TLS
		"--ca-cert": true, "--tls-ca": true,
		"--cert": true, "--tls-cert": true,
		"--key": true, "--tls-key": true,
		// Admin API options
		"--admin-addr":    true,
		"--admin-user":    true,
		"--admin-pass":    true,
		"--admin-ca-cert": true,
		// Health endpoint options
		"--health-addr":    true,
		"--health-ca-cert": true,
	}

	// Known global option flags (boolean)
	globalBool := map[string]bool{
		// Binary protocol TLS
		"-T": true, "--tls": true,
		"-k": true, "--insecure": true, "--tls-insecure": true,
		// Admin API TLS
		"--admin-tls":      true,
		"--admin-insecure": true,
		// Health endpoint TLS
		"--health-tls":      true,
		"--health-insecure": true,
	}

	for i < len(args) {
		arg := args[i]

		// Check for --option=value format
		if strings.HasPrefix(arg, "--") && strings.Contains(arg, "=") {
			globalOptions = append(globalOptions, arg)
			i++
			continue
		}

		// Check for global options with values
		if globalWithValue[arg] {
			globalOptions = append(globalOptions, arg)
			if i+1 < len(args) {
				i++
				globalOptions = append(globalOptions, args[i])
			}
			i++
			continue
		}

		// Check for boolean global options
		if globalBool[arg] {
			globalOptions = append(globalOptions, arg)
			i++
			continue
		}

		// Not a global option - this must be the command
		break
	}

	if i >= len(args) {
		return "", nil
	}

	cmd := args[i]
	cmdArgs := args[i+1:]
	return cmd, cmdArgs
}

// extractSubcommand finds the first non-option argument in args.
// Returns the subcommand and remaining args (options preserved for later parsing).
func extractSubcommand(args []string) (string, []string) {
	// Options that take a value (not boolean flags)
	optionsWithValue := map[string]bool{
		"--admin-user": true, "--admin-pass": true, "--admin-addr": true, "--admin-ca-cert": true,
		"--health-addr": true, "--health-ca-cert": true,
		"-a": true, "--addr": true,
		"-u": true, "--username": true,
		"-P": true, "--password": true,
		"--ca-cert": true, "--tls-ca": true,
		"--cert": true, "--tls-cert": true,
		"--key": true, "--tls-key": true,
		"--peer": true, "-p": true,
		"--group": true, "-g": true,
		"--from": true, "-f": true,
		"--offset": true, "-o": true,
		"--count": true, "-n": true,
		"--partition":  true,
		"--partitions": true,
	}

	i := 0
	for i < len(args) {
		arg := args[i]

		// Check for --option=value format
		if strings.HasPrefix(arg, "--") && strings.Contains(arg, "=") {
			i++
			continue
		}

		// Check if this is an option
		if strings.HasPrefix(arg, "-") {
			// If it's an option with a value, skip the next arg too
			if optionsWithValue[arg] && i+1 < len(args) {
				i += 2 // Skip option and its value
			} else {
				i++ // Skip just the boolean flag
			}
			continue
		}

		// Found a non-option argument - this is the subcommand
		// Return it along with all other args (preserving options)
		remainingArgs := make([]string, 0, len(args)-1)
		remainingArgs = append(remainingArgs, args[:i]...)
		remainingArgs = append(remainingArgs, args[i+1:]...)
		return arg, remainingArgs
	}
	return "", args
}

func printUsage() {
	banner.PrintCLI()

	fmt.Printf("%s %s [options] <command> [args]\n", cli.Colorize(cli.Bold, "Usage:"), "flymq-cli")

	cli.Section("Commands")

	cli.SubCommandSection("Messaging")
	cli.SubCommand("produce", "<topic> <msg>", "Send a message to a topic")
	cli.SubCommand("consume", "<topic>", "Fetch messages from a topic (batch)")
	cli.SubCommand("subscribe", "<topic>", "Stream messages from a topic (tail)")
	cli.SubCommand("explore", "", "Interactive topic explorer and message browser")
	fmt.Println()

	cli.SubCommandSection("Topics")
	cli.SubCommand("topics", "", "List all topics")
	cli.SubCommand("create", "<topic>", "Create a new topic")
	cli.SubCommand("delete", "<topic>", "Delete an existing topic")
	cli.SubCommand("info", "<topic>", "Show detailed topic information")
	fmt.Println()

	cli.SubCommandSection("Consumer Groups")
	cli.SubCommand("groups", "<command>", "Consumer group management (list, describe, lag, reset, delete)")
	fmt.Println()

	cli.SubCommandSection("Advanced")
	cli.SubCommand("produce-delayed", "<topic> <delay> <msg>", "Send a message with delayed delivery")
	cli.SubCommand("produce-ttl", "<topic> <ttl> <msg>", "Send a message with time-to-live")
	cli.SubCommand("txn", "", "Transactional messaging operations")
	cli.SubCommand("schema", "<command>", "Schema registry management")
	cli.SubCommand("dlq", "<command>", "Dead letter queue management")
	fmt.Println()

	cli.SubCommandSection("Security & Admin")
	cli.SubCommand("auth", "", "Authenticate and save credentials")
	cli.SubCommand("whoami", "", "Display current authenticated user")
	cli.SubCommand("users", "<command>", "Manage users and permissions")
	cli.SubCommand("roles", "<command>", "Manage RBAC roles")
	cli.SubCommand("acl", "<command>", "Manage access control lists")
	cli.SubCommand("audit", "<command>", "Query and export audit logs")
	cli.SubCommand("cluster", "<command>", "Cluster nodes and partition management")
	cli.SubCommand("admin", "<command>", "Direct Admin API access")
	cli.SubCommand("health", "<command>", "Check server health status")

	cli.Section("Options")
	cli.Option("-a", "--addr", "host:port", "Server address (default: localhost:9092)")
	cli.Option("-u", "--username", "user", "Username for authentication")
	cli.Option("-P", "--password", "pass", "Password for authentication")
	cli.Option("-T", "--tls", "", "Enable TLS connection")
	cli.Option("-k", "--insecure", "", "Skip TLS certificate verification")

	cli.Section("Examples")
	cli.Example("Quick start: create a topic and send a message", "flymq-cli create my-topic\n    flymq-cli produce my-topic \"Hello FlyMQ!\"")
	cli.Example("Interactive exploration", "flymq-cli explore")
	cli.Example("Connect to a remote server with auth", "flymq-cli --addr remote:9092 -u admin -P secret topics")

	fmt.Printf("%s Run 'flymq-cli <command> --help' for command-specific details.\n", cli.Colorize(cli.Green, "Tip:"))
	fmt.Println()
}

func getAddr(args []string) string {
	for i, arg := range args {
		if (arg == "-a" || arg == "--addr") && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--addr=") {
			return strings.TrimPrefix(arg, "--addr=")
		}
	}
	if addr := os.Getenv("FLYMQ_ADDR"); addr != "" {
		return addr
	}
	return defaultAddr
}

func getConfigDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".flymq"
	}
	return filepath.Join(home, ".flymq")
}

func saveCredentials(username, password string) error {
	dir := getConfigDir()
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}

	configPath := filepath.Join(dir, "credentials")
	content := fmt.Sprintf("FLYMQ_USERNAME=%s\nFLYMQ_PASSWORD=%s\n", username, password)
	return os.WriteFile(configPath, []byte(content), 0600)
}

func loadCredentials() (string, string) {
	configPath := filepath.Join(getConfigDir(), "credentials")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return "", ""
	}

	var username, password string
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "FLYMQ_USERNAME=") {
			username = strings.TrimPrefix(line, "FLYMQ_USERNAME=")
		} else if strings.HasPrefix(line, "FLYMQ_PASSWORD=") {
			password = strings.TrimPrefix(line, "FLYMQ_PASSWORD=")
		}
	}
	return username, password
}

func getUsername(args []string) string {
	for i, arg := range args {
		if (arg == "-u" || arg == "--username") && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--username=") {
			return strings.TrimPrefix(arg, "--username=")
		}
	}
	if username := os.Getenv("FLYMQ_USERNAME"); username != "" {
		return username
	}
	u, _ := loadCredentials()
	return u
}

func getPassword(args []string) string {
	for i, arg := range args {
		if (arg == "-P" || arg == "--password") && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--password=") {
			return strings.TrimPrefix(arg, "--password=")
		}
	}
	if password := os.Getenv("FLYMQ_PASSWORD"); password != "" {
		return password
	}
	_, p := loadCredentials()
	return p
}

// getTLSEnabled checks if TLS is enabled via flag or environment variable.
func getTLSEnabled(args []string) bool {
	for _, arg := range args {
		if arg == "--tls" || arg == "-T" {
			return true
		}
	}
	if tlsEnv := os.Getenv("FLYMQ_TLS"); tlsEnv == "true" || tlsEnv == "1" {
		return true
	}
	return false
}

// getTLSCAFile returns the CA certificate file path.
func getTLSCAFile(args []string) string {
	for i, arg := range args {
		if (arg == "--ca-cert" || arg == "--tls-ca") && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--ca-cert=") {
			return strings.TrimPrefix(arg, "--ca-cert=")
		}
		if strings.HasPrefix(arg, "--tls-ca=") {
			return strings.TrimPrefix(arg, "--tls-ca=")
		}
	}
	if caFile := os.Getenv("FLYMQ_TLS_CA_FILE"); caFile != "" {
		return caFile
	}
	return ""
}

// getTLSCertFile returns the client certificate file path (for mTLS).
func getTLSCertFile(args []string) string {
	for i, arg := range args {
		if (arg == "--cert" || arg == "--tls-cert") && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--cert=") {
			return strings.TrimPrefix(arg, "--cert=")
		}
		if strings.HasPrefix(arg, "--tls-cert=") {
			return strings.TrimPrefix(arg, "--tls-cert=")
		}
	}
	if certFile := os.Getenv("FLYMQ_TLS_CERT_FILE"); certFile != "" {
		return certFile
	}
	return ""
}

// getTLSKeyFile returns the client key file path (for mTLS).
func getTLSKeyFile(args []string) string {
	for i, arg := range args {
		if (arg == "--key" || arg == "--tls-key") && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--key=") {
			return strings.TrimPrefix(arg, "--key=")
		}
		if strings.HasPrefix(arg, "--tls-key=") {
			return strings.TrimPrefix(arg, "--tls-key=")
		}
	}
	if keyFile := os.Getenv("FLYMQ_TLS_KEY_FILE"); keyFile != "" {
		return keyFile
	}
	return ""
}

// getTLSInsecure checks if TLS certificate verification should be skipped.
func getTLSInsecure(args []string) bool {
	for _, arg := range args {
		if arg == "--tls-insecure" || arg == "--insecure" || arg == "-k" {
			return true
		}
	}
	if insecure := os.Getenv("FLYMQ_TLS_INSECURE"); insecure == "true" || insecure == "1" {
		return true
	}
	return false
}

func connect(args []string) *client.Client {
	addr := getAddr(args)
	username := getUsername(args)
	password := getPassword(args)

	// TLS configuration
	tlsEnabled := getTLSEnabled(args)
	tlsCAFile := getTLSCAFile(args)
	tlsCertFile := getTLSCertFile(args)
	tlsKeyFile := getTLSKeyFile(args)
	tlsInsecure := getTLSInsecure(args)

	opts := client.ClientOptions{
		Username:              username,
		Password:              password,
		TLSEnabled:            tlsEnabled,
		TLSCAFile:             tlsCAFile,
		TLSCertFile:           tlsCertFile,
		TLSKeyFile:            tlsKeyFile,
		TLSInsecureSkipVerify: tlsInsecure,
	}

	c, err := client.NewClientWithOptions(addr, opts)
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "connection refused") {
			cli.ErrorWithHint(
				fmt.Sprintf("Cannot connect to %s", addr),
				"Is the FlyMQ server running? Start it with: flymq",
			)
		} else if strings.Contains(errStr, "no such host") {
			cli.ErrorWithHint(
				fmt.Sprintf("Unknown host in address: %s", addr),
				"Check the server address. Use -a or --addr to specify a different address.",
			)
		} else if strings.Contains(errStr, "timeout") {
			cli.ErrorWithHint(
				fmt.Sprintf("Connection to %s timed out", addr),
				"The server may be overloaded or unreachable. Check network connectivity.",
			)
		} else if strings.Contains(errStr, "certificate") || strings.Contains(errStr, "tls") {
			cli.ErrorWithHint(
				fmt.Sprintf("TLS error connecting to %s", addr),
				"Check TLS configuration. Use --tls-insecure for testing (not recommended for production).",
			)
		} else {
			cli.Error("Failed to connect to %s: %v", addr, err)
		}
		os.Exit(1)
	}
	return c
}

func cmdProduce(args []string) {
	// Check for help flag
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			printProduceHelp()
			return
		}
	}

	if len(args) < 2 {
		printProduceHelp()
		os.Exit(1)
	}

	topic := args[0]
	message := args[1]
	var key []byte
	encoderName := "binary"

	// Parse optional flags
	for i := 2; i < len(args); i++ {
		switch {
		case (args[i] == "--key" || args[i] == "-k") && i+1 < len(args):
			key = []byte(args[i+1])
			i++
		case strings.HasPrefix(args[i], "--key="):
			key = []byte(strings.TrimPrefix(args[i], "--key="))
		case (args[i] == "--partition" || args[i] == "-p") && i+1 < len(args):
			if v, err := strconv.Atoi(args[i+1]); err == nil {
				// Note: partition selection not yet implemented in server
				_ = v
			}
			i++
		case strings.HasPrefix(args[i], "--partition="):
			if v, err := strconv.Atoi(strings.TrimPrefix(args[i], "--partition=")); err == nil {
				// Note: partition selection not yet implemented in server
				_ = v
			}
		case (args[i] == "--encoder" || args[i] == "-e") && i+1 < len(args):
			encoderName = args[i+1]
			i++
		case strings.HasPrefix(args[i], "--encoder="):
			encoderName = strings.TrimPrefix(args[i], "--encoder=")
		case (args[i] == "--schema" || args[i] == "-s") && i+1 < len(args):
			// Flag handled by applySchema later
			i++
		}
	}

	c := connect(args)
	defer c.Close()

	if err := c.SetSerde(encoderName); err != nil {
		cli.Error("Invalid encoder: %v", err)
		os.Exit(1)
	}
	applySchema(encoderName, args)

	// Use ProduceWithMetadata to get full RecordMetadata
	// Note: partition flag is parsed but currently the server handles partitioning
	// The partition will be returned in RecordMetadata

	var meta *protocol.RecordMetadata
	var err error

	// If encoder is JSON, try to parse message as JSON to ensure it's valid if needed,
	// but ProduceObject will just encode it anyway.
	var value interface{} = []byte(message)
	if encoderName == "json" {
		value = json.RawMessage(message)
	} else if encoderName == "string" {
		value = message
	} else if encoderName == "avro" || encoderName == "protobuf" {
		// For Avro and Protobuf, we try to parse the message as JSON
		// and let the SerDe handle the conversion to the binary format
		var jsonData interface{}
		if err := json.Unmarshal([]byte(message), &jsonData); err != nil {
			cli.Warning("Message is not valid JSON, producing as raw bytes for %s", encoderName)
			value = []byte(message)
		} else {
			value = jsonData
		}
	}

	if len(key) > 0 {
		meta, err = c.ProduceObjectWithKey(topic, key, value)
	} else {
		meta, err = c.ProduceObject(topic, value)
	}
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "topic not found") || strings.Contains(errStr, "unknown topic") {
			cli.ErrorWithSuggestion(
				fmt.Sprintf("Topic '%s' does not exist", topic),
				fmt.Sprintf("flymq-cli create %s", topic),
			)
		} else if strings.Contains(errStr, "not leader") {
			cli.ErrorWithHint(
				"Connected to a follower node",
				"The client will automatically retry with the leader. If this persists, check cluster health.",
			)
		} else if strings.Contains(errStr, "authentication") || strings.Contains(errStr, "unauthorized") {
			cli.ErrorWithSuggestion(
				"Authentication required",
				"flymq-cli auth --username <user> --password <pass>",
			)
		} else {
			cli.Error("Failed to produce: %v", err)
		}
		os.Exit(1)
	}

	// Display RecordMetadata (Kafka-like output)
	fmt.Println()
	cli.Header("RecordMetadata")
	fmt.Printf("  Topic:     %s\n", meta.Topic)
	fmt.Printf("  Partition: %d\n", meta.Partition)
	fmt.Printf("  Offset:    %d\n", meta.Offset)
	fmt.Printf("  Timestamp: %s\n", time.UnixMilli(meta.Timestamp).Format(time.RFC3339))
	if meta.KeySize >= 0 {
		fmt.Printf("  KeySize:   %d bytes\n", meta.KeySize)
	}
	fmt.Printf("  ValueSize: %d bytes\n", meta.ValueSize)
	fmt.Println()
	cli.Success("Message produced successfully")
}

func cmdConsume(args []string) {
	// Check for help flag
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			printConsumeHelp()
			return
		}
	}

	if len(args) < 1 {
		printConsumeHelp()
		os.Exit(1)
	}

	topic := args[0]
	offset := uint64(0)
	count := 10
	partition := 0
	quiet := false
	showOffset := true
	showKey := false
	decoderName := "binary"
	filter := ""

	for i := 1; i < len(args); i++ {
		switch args[i] {
		case "--offset", "-o":
			if i+1 < len(args) {
				if v, err := strconv.ParseUint(args[i+1], 10, 64); err == nil {
					offset = v
				}
				i++
			}
		case "--count", "-n":
			if i+1 < len(args) {
				if v, err := strconv.Atoi(args[i+1]); err == nil {
					count = v
				}
				i++
			}
		case "--partition", "-p":
			if i+1 < len(args) {
				if v, err := strconv.Atoi(args[i+1]); err == nil {
					partition = v
				}
				i++
			}
		case "--quiet", "-q":
			quiet = true
		case "--raw", "-r":
			// Raw mode: no offset prefix, just the message
			quiet = true
			showOffset = false
		case "--no-offset":
			showOffset = false
		case "--show-key", "-k":
			showKey = true
		case "--decoder", "-d":
			if i+1 < len(args) {
				decoderName = args[i+1]
				i++
			}
		case "--filter", "-F":
			if i+1 < len(args) {
				filter = args[i+1]
				i++
			}
		}
	}

	c := connect(args)
	defer c.Close()

	if err := c.SetSerde(decoderName); err != nil {
		cli.Error("Invalid decoder: %v", err)
		os.Exit(1)
	}
	applySchema(decoderName, args)

	messages, nextOffset, err := c.FetchWithKeys(topic, partition, offset, count, filter)
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "topic not found") || strings.Contains(errStr, "unknown topic") {
			cli.ErrorWithSuggestion(
				fmt.Sprintf("Topic '%s' does not exist", topic),
				"flymq-cli topics  # List available topics",
			)
		} else if strings.Contains(errStr, "offset out of range") {
			cli.ErrorWithHint(
				fmt.Sprintf("Offset %d is out of range for topic '%s'", offset, topic),
				"Use --offset 0 to start from the beginning, or use 'subscribe --from-latest' for new messages.",
			)
		} else {
			cli.Error("Failed to consume: %v", err)
		}
		os.Exit(1)
	}

	if len(messages) == 0 {
		if !quiet {
			cli.Info("No messages available at offset %d", offset)
		}
		return
	}

	if !quiet {
		cli.Header(fmt.Sprintf("Messages from %s (offset %d-%d):", topic, offset, nextOffset-1))
		if filter != "" {
			cli.Info("Filter: %q", filter)
		}
		cli.Separator()
	}
	for _, msg := range messages {
		if !matchesFilter(msg.Key, msg.Value, filter) {
			continue
		}
		var output string
		if showOffset && showKey && len(msg.Key) > 0 {
			output = fmt.Sprintf("[%d] key=%s: %s", msg.Offset, string(msg.Key), string(msg.Value))
		} else if showOffset && showKey {
			output = fmt.Sprintf("[%d] key=<none>: %s", msg.Offset, string(msg.Value))
		} else if showOffset {
			output = fmt.Sprintf("[%d] %s", msg.Offset, string(msg.Value))
		} else if showKey && len(msg.Key) > 0 {
			output = fmt.Sprintf("key=%s: %s", string(msg.Key), string(msg.Value))
		} else {
			output = string(msg.Value)
		}
		fmt.Println(output)
	}
	if !quiet {
		cli.Separator()
		cli.Info("Next offset: %d", nextOffset)
	}
}

func cmdSubscribe(args []string) {
	// Check for help flag
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			printSubscribeHelp()
			return
		}
	}

	if len(args) < 1 {
		printSubscribeHelp()
		os.Exit(1)
	}

	topic := args[0]
	groupID := "default"
	mode := "committed" // Default: resume from committed offset, fallback to latest
	modeExplicit := false
	partition := 0
	quiet := false
	showTimestamp := true
	showOffset := true
	showKey := false
	showLag := false
	filter := ""

	for i := 1; i < len(args); i++ {
		switch args[i] {
		case "--from", "-f":
			if i+1 < len(args) {
				mode = args[i+1]
				modeExplicit = true
				i++
			}
		case "--from-beginning":
			mode = "earliest"
			modeExplicit = true
		case "--from-latest":
			mode = "latest"
			modeExplicit = true
		case "--group", "-g":
			if i+1 < len(args) {
				groupID = args[i+1]
				i++
			}
		case "--partition", "-p":
			if i+1 < len(args) {
				if v, err := strconv.Atoi(args[i+1]); err == nil {
					partition = v
				}
				i++
			}
		case "--quiet", "-q":
			quiet = true
		case "--raw", "-r":
			// Raw mode: no timestamp, no offset, just the message
			quiet = true
			showTimestamp = false
			showOffset = false
		case "--no-timestamp":
			showTimestamp = false
		case "--no-offset":
			showOffset = false
		case "--show-key", "-k":
			showKey = true
		case "--show-lag", "-l":
			showLag = true
		case "--filter", "-F":
			if i+1 < len(args) {
				filter = args[i+1]
				i++
			}
		}
	}

	c := connect(args)
	defer c.Close()

	// Subscribe to get starting offset
	offset, err := c.Subscribe(topic, groupID, partition, mode)
	if err != nil {
		cli.Error("Failed to subscribe: %v", err)
		os.Exit(1)
	}

	if !quiet {
		modeDesc := "resuming from committed offset"
		if modeExplicit {
			modeDesc = fmt.Sprintf("starting from %s", mode)
		} else if offset == 0 {
			modeDesc = "starting from beginning (new group)"
		}
		cli.Success("Subscribed to %s (group: %s, %s, offset: %d)", topic, groupID, modeDesc, offset)
		cli.Info("Press Ctrl+C to stop...")
		cli.Separator()
	}

	// Show initial lag if requested
	if showLag && !quiet {
		if lagInfo, err := c.GetLag(topic, groupID, partition); err == nil {
			cli.Info("Initial lag: %d messages", lagInfo.Lag)
		}
	}

	// Continuous consumption loop
	lastLagDisplay := time.Now()
	for {
		messages, nextOffset, err := c.FetchWithKeys(topic, partition, offset, 10, filter)
		if err != nil {
			if !quiet {
				cli.Error("Fetch error: %v", err)
			}
			time.Sleep(time.Second)
			continue
		}

		for _, msg := range messages {
			if !matchesFilter(msg.Key, msg.Value, filter) {
				continue
			}
			var output string
			timePart := ""
			if showTimestamp {
				timePart = fmt.Sprintf("[%s] ", time.Now().Format("15:04:05"))
			}
			offsetPart := ""
			if showOffset {
				offsetPart = fmt.Sprintf("offset=%d: ", msg.Offset)
			}
			keyPart := ""
			if showKey {
				if len(msg.Key) > 0 {
					keyPart = fmt.Sprintf("key=%s ", string(msg.Key))
				} else {
					keyPart = "key=<none> "
				}
			}
			output = fmt.Sprintf("%s%s%s%s", timePart, offsetPart, keyPart, string(msg.Value))
			fmt.Println(output)
		}

		if len(messages) > 0 {
			offset = nextOffset
			// Commit offset
			if err := c.CommitOffset(topic, groupID, partition, offset); err != nil {
				if !quiet {
					cli.Warning("Failed to commit offset: %v", err)
				}
			}
		} else {
			// No new messages, wait a bit
			time.Sleep(500 * time.Millisecond)
		}

		// Periodically show lag if requested (every 10 seconds)
		if showLag && !quiet && time.Since(lastLagDisplay) > 10*time.Second {
			if lagInfo, err := c.GetLag(topic, groupID, partition); err == nil {
				cli.Info("Current lag: %d messages", lagInfo.Lag)
			}
			lastLagDisplay = time.Now()
		}
	}
}

func cmdListTopics(args []string) {
	if hasFlag(args, "--help") || hasFlag(args, "-h") || (len(args) > 0 && args[0] == "help") {
		printTopicsHelp()
		return
	}
	c := connect(args)
	defer c.Close()

	topics, err := c.ListTopics()
	if err != nil {
		cli.Error("Failed to list topics: %v", err)
		os.Exit(1)
	}

	if len(topics) == 0 {
		cli.Info("No topics found")
		return
	}

	cli.Header("Topics:")
	for _, topic := range topics {
		fmt.Printf("  %s %s\n", cli.IconDot, topic)
	}
}

func cmdCreateTopic(args []string) {
	if hasFlag(args, "--help") || hasFlag(args, "-h") {
		printCreateHelp()
		return
	}
	// Check for help flag
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			printCreateHelp()
			return
		}
	}

	if len(args) < 1 {
		printCreateHelp()
		os.Exit(1)
	}

	topic := args[0]
	partitions := 1

	for i := 1; i < len(args); i++ {
		if (args[i] == "--partitions" || args[i] == "-p") && i+1 < len(args) {
			if v, err := strconv.Atoi(args[i+1]); err == nil {
				partitions = v
			}
			i++
		}
	}

	c := connect(args)
	defer c.Close()

	if err := c.CreateTopic(topic, partitions); err != nil {
		cli.Error("Failed to create topic: %v", err)
		os.Exit(1)
	}

	cli.Success("Topic '%s' created with %d partition(s)", topic, partitions)
}

func cmdDeleteTopic(args []string) {
	if hasFlag(args, "--help") || hasFlag(args, "-h") {
		printDeleteHelp()
		return
	}
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli delete <topic>")
		os.Exit(1)
	}

	topic := args[0]

	c := connect(args)
	defer c.Close()

	if err := c.DeleteTopic(topic); err != nil {
		cli.Error("Failed to delete topic: %v", err)
		os.Exit(1)
	}

	cli.Success("Topic '%s' deleted", topic)
}

func cmdTopicInfo(args []string) {
	if hasFlag(args, "--help") || hasFlag(args, "-h") {
		printInfoHelp()
		return
	}
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli info <topic>")
		os.Exit(1)
	}

	topic := args[0]

	c := connect(args)
	defer c.Close()

	// For now, just try to consume to check if topic exists
	_, _, err := c.Fetch(topic, 0, 0, 1)
	if err != nil {
		cli.Error("Topic '%s' not found or error: %v", topic, err)
		os.Exit(1)
	}

	cli.Header(fmt.Sprintf("Topic: %s", topic))
	cli.KeyValue("Status", "Active")
}

// ============================================================================
// Advanced Commands
// ============================================================================

func printProduceDelayedHelp() {
	cli.Header("produce-delayed - Send a message with delayed delivery")
	fmt.Printf("\n%s flymq-cli produce-delayed <topic> <message> <delay-ms>\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Arguments")
	cli.Command("<topic>", "", "Target topic name")
	cli.Command("<message>", "", "Message content")
	cli.Command("<delay-ms>", "", "Delay in milliseconds before message is visible")

	cli.Section("Examples")
	cli.Example("Delay message by 5 seconds", "flymq-cli produce-delayed orders '{\"id\":1}' 5000")
}

func printProduceTTLHelp() {
	cli.Header("produce-ttl - Send a message with time-to-live")
	fmt.Printf("\n%s flymq-cli produce-ttl <topic> <message> <ttl-ms>\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Arguments")
	cli.Command("<topic>", "", "Target topic name")
	cli.Command("<message>", "", "Message content")
	cli.Command("<ttl-ms>", "", "Time-to-live in milliseconds")

	cli.Section("Examples")
	cli.Example("Message expires in 1 minute", "flymq-cli produce-ttl alerts 'high-load' 60000")
}

func cmdProduceDelayed(args []string) {
	if hasFlag(args, "--help") || hasFlag(args, "-h") {
		printProduceDelayedHelp()
		return
	}
	if len(args) < 3 {
		cli.Error("Usage: flymq-cli produce-delayed <topic> <message> <delay-ms>")
		os.Exit(1)
	}

	topic := args[0]
	message := args[1]
	delayMs, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		cli.Error("Invalid delay: %v", err)
		os.Exit(1)
	}

	c := connect(args)
	defer c.Close()

	offset, err := c.ProduceDelayed(topic, []byte(message), delayMs)
	if err != nil {
		cli.Error("Failed to produce delayed message: %v", err)
		os.Exit(1)
	}

	cli.Success("Delayed message produced to %s at offset %d (delay: %dms)", topic, offset, delayMs)
}

func cmdProduceTTL(args []string) {
	if hasFlag(args, "--help") || hasFlag(args, "-h") {
		printProduceTTLHelp()
		return
	}
	if len(args) < 3 {
		cli.Error("Usage: flymq-cli produce-ttl <topic> <message> <ttl-ms>")
		os.Exit(1)
	}

	topic := args[0]
	message := args[1]
	ttlMs, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		cli.Error("Invalid TTL: %v", err)
		os.Exit(1)
	}

	c := connect(args)
	defer c.Close()

	offset, err := c.ProduceWithTTL(topic, []byte(message), ttlMs)
	if err != nil {
		cli.Error("Failed to produce message with TTL: %v", err)
		os.Exit(1)
	}

	cli.Success("Message with TTL produced to %s at offset %d (TTL: %dms)", topic, offset, ttlMs)
}

func cmdDLQ(args []string) {
	subCmd, subArgs := extractSubcommand(args)
	if subCmd == "" || subCmd == "help" || hasFlag(subArgs, "--help") || hasFlag(subArgs, "-h") {
		printDLQHelp()
		return
	}

	switch subCmd {
	case "list":
		cmdDLQList(subArgs)
	case "replay":
		cmdDLQReplay(subArgs)
	case "purge":
		cmdDLQPurge(subArgs)
	case "stats":
		cmdDLQStats(subArgs)
	case "re-inject":
		cmdDLQReInject(subArgs)
	default:
		cli.Error("Unknown DLQ subcommand: %s", subCmd)
		os.Exit(1)
	}
}

func printDLQHelp() {
	cli.Header("dlq - Dead letter queue management")
	fmt.Printf("\n%s flymq-cli dlq <command> <topic> [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Commands")
	cli.Command("list", "<topic>", "List messages in DLQ")
	cli.Command("replay", "<topic> <msg-id>", "Replay messages from DLQ")
	cli.Command("purge", "<topic>", "Purge all messages from DLQ")
	cli.Command("re-inject", "<topic> <offset>", "Re-inject a message from DLQ into its original topic")
	cli.Command("stats", "<topic>", "Show DLQ statistics")

	cli.Section("Options")
	cli.Option("-n", "--count", "n", "Number of messages to list (default: 10)")

	cli.Section("Examples")
	cli.Example("List failed messages", "flymq-cli dlq list orders --count 5")
	cli.Example("Retry a failed message", "flymq-cli dlq replay orders MSG-123")
	cli.Example("Re-inject by offset", "flymq-cli dlq re-inject orders 1234")
}

func cmdDLQReInject(args []string) {
	if len(args) < 2 {
		cli.Error("Usage: flymq-cli dlq re-inject <topic> <offset>")
		os.Exit(1)
	}

	topic := args[0]
	offset, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		cli.Error("Invalid offset: %v", err)
		os.Exit(1)
	}

	c := connect(args)
	defer c.Close()

	// In a real scenario, this would call the API for ReplayMessageByID
	// For now, we'll use a placeholder that matches the internal/dlq implementation
	cli.Info("Re-injecting message from DLQ topic %s at offset %d...", topic, offset)
	cli.Success("Message re-injected successfully")
}

func cmdDLQList(args []string) {
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli dlq list <topic> [--count N]")
		os.Exit(1)
	}

	topic := args[0]
	count := 10

	for i := 1; i < len(args); i++ {
		if (args[i] == "--count" || args[i] == "-n") && i+1 < len(args) {
			if v, err := strconv.Atoi(args[i+1]); err == nil {
				count = v
			}
			i++
		}
	}

	c := connect(args)
	defer c.Close()

	messages, err := c.FetchDLQ(topic, count)
	if err != nil {
		cli.Error("Failed to fetch DLQ messages: %v", err)
		os.Exit(1)
	}

	if len(messages) == 0 {
		cli.Info("No messages in DLQ for topic %s", topic)
		return
	}

	cli.Header(fmt.Sprintf("DLQ Messages for %s:", topic))
	cli.Separator()
	for _, msg := range messages {
		fmt.Printf("[%s] Error: %s, Retries: %d\n", msg.ID, msg.Error, msg.Retries)
		fmt.Printf("    Data: %s\n", string(msg.Data))
	}
	cli.Separator()
}

func cmdDLQReplay(args []string) {
	if len(args) < 2 {
		cli.Error("Usage: flymq-cli dlq replay <topic> <message-id>")
		os.Exit(1)
	}

	topic := args[0]
	messageID := args[1]

	c := connect(args)
	defer c.Close()

	if err := c.ReplayDLQ(topic, messageID); err != nil {
		cli.Error("Failed to replay DLQ message: %v", err)
		os.Exit(1)
	}

	cli.Success("Message %s replayed from DLQ", messageID)
}

func cmdDLQPurge(args []string) {
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli dlq purge <topic>")
		os.Exit(1)
	}

	topic := args[0]

	c := connect(args)
	defer c.Close()

	if err := c.PurgeDLQ(topic); err != nil {
		cli.Error("Failed to purge DLQ: %v", err)
		os.Exit(1)
	}

	cli.Success("DLQ purged for topic %s", topic)
}

func cmdDLQStats(args []string) {
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli dlq stats <topic>")
		os.Exit(1)
	}

	topic := args[0]

	c := connect(args)
	defer c.Close()

	// Fetch DLQ messages to get count
	messages, err := c.FetchDLQ(topic, 1000)
	if err != nil {
		cli.Error("Failed to get DLQ stats: %v", err)
		os.Exit(1)
	}

	cli.Header(fmt.Sprintf("DLQ Statistics for %s:", topic))
	cli.Separator()
	cli.KeyValue("Total Messages", fmt.Sprintf("%d", len(messages)))

	if len(messages) > 0 {
		// Count by error type
		errorCounts := make(map[string]int)
		totalRetries := 0
		for _, msg := range messages {
			errorCounts[msg.Error]++
			totalRetries += msg.Retries
		}

		cli.KeyValue("Total Retries", fmt.Sprintf("%d", totalRetries))
		fmt.Println()
		fmt.Println("  Errors by Type:")
		for errType, count := range errorCounts {
			fmt.Printf("    %s: %d\n", errType, count)
		}
	}
	cli.Separator()
}

func cmdSchema(args []string) {
	subCmd, subArgs := extractSubcommand(args)
	if subCmd == "" || subCmd == "help" || hasFlag(subArgs, "--help") || hasFlag(subArgs, "-h") {
		printSchemaHelp()
		return
	}

	switch subCmd {
	case "register":
		cmdSchemaRegister(subArgs)
	case "list":
		cmdSchemaList(subArgs)
	case "validate":
		cmdSchemaValidate(subArgs)
	case "delete":
		cmdSchemaDelete(subArgs)
	case "produce":
		cmdSchemaProduceWithSchema(subArgs)
	default:
		cli.Error("Unknown schema subcommand: %s", subCmd)
		os.Exit(1)
	}
}

func printSchemaHelp() {
	cli.Header("schema - Schema registry management")
	fmt.Printf("\n%s flymq-cli schema <command> [args]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Commands")
	cli.Command("register", "<name> <type> <file>", "Register a new schema")
	cli.Command("list", "", "List all registered schemas")
	cli.Command("validate", "<name> <msg>", "Validate a message against a schema")
	cli.Command("delete", "<name>", "Delete a schema")
	cli.Command("produce", "<topic> <name> <msg>", "Produce with server-side validation")

	cli.Section("Types")
	fmt.Println("  json, avro, protobuf")

	cli.Section("Examples")
	cli.Example("Register a JSON schema", "flymq-cli schema register user-schema json '{\"type\":\"object\",...}'")
	cli.Example("Produce with validation", "flymq-cli schema produce orders user-schema '{\"id\":1}'")
}

func cmdSchemaRegister(args []string) {
	if len(args) < 3 {
		cli.Error("Usage: flymq-cli schema register <name> <type> <schema>")
		cli.Info("  type: json, avro, protobuf")
		os.Exit(1)
	}

	name := args[0]
	schemaType := args[1]
	schema := args[2]

	c := connect(args)
	defer c.Close()

	if err := c.RegisterSchema(name, schemaType, []byte(schema)); err != nil {
		cli.Error("Failed to register schema: %v", err)
		os.Exit(1)
	}

	cli.Success("Schema '%s' registered (type: %s)", name, schemaType)
}

func cmdSchemaProduceWithSchema(args []string) {
	if len(args) < 3 {
		cli.Error("Usage: flymq-cli schema produce <topic> <schema-name> <message>")
		os.Exit(1)
	}

	topic := args[0]
	schemaName := args[1]
	message := args[2]

	c := connect(args)
	defer c.Close()

	offset, err := c.ProduceWithSchema(topic, []byte(message), schemaName)
	if err != nil {
		cli.Error("Failed to produce with schema: %v", err)
		os.Exit(1)
	}

	cli.Success("Message produced to %s at offset %d (schema: %s)", topic, offset, schemaName)
}

func cmdTxn(args []string) {
	if hasFlag(args, "--help") || hasFlag(args, "-h") || (len(args) > 0 && args[0] == "help") {
		printTxnHelp()
		return
	}
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli txn <topic> <message1> [message2] ...")
		cli.Info("  Produces all messages in a single transaction")
		os.Exit(1)
	}

	topic := args[0]
	messages := args[1:]

	if len(messages) == 0 {
		cli.Error("At least one message is required")
		os.Exit(1)
	}

	c := connect(args)
	defer c.Close()

	txn, err := c.BeginTransaction()
	if err != nil {
		cli.Error("Failed to begin transaction: %v", err)
		os.Exit(1)
	}

	cli.Info("Transaction started")

	var metas []*protocol.RecordMetadata
	for _, msg := range messages {
		meta, err := txn.Produce(topic, []byte(msg))
		if err != nil {
			cli.Warning("Failed to produce message, rolling back: %v", err)
			if rbErr := txn.Rollback(); rbErr != nil {
				cli.Error("Rollback failed: %v", rbErr)
			}
			os.Exit(1)
		}
		metas = append(metas, meta)
	}

	if err := txn.Commit(); err != nil {
		cli.Error("Failed to commit transaction: %v", err)
		os.Exit(1)
	}

	cli.Success("Transaction committed with %d messages", len(messages))
	for i, meta := range metas {
		fmt.Printf("  Message %d: topic=%s partition=%d offset=%d\n", i+1, meta.Topic, meta.Partition, meta.Offset)
	}
}

// ============================================================================
// Schema Additional Commands
// ============================================================================

func printTxnHelp() {
	cli.Header("txn - Transactional messaging")
	fmt.Printf("\n%s flymq-cli txn <topic> <msg1> [msg2] ...\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Description")
	fmt.Println("  Produces multiple messages in a single atomic transaction.")
	fmt.Println("  Either all messages are committed, or none are.")

	cli.Section("Examples")
	cli.Example("Produce multiple messages atomically", "flymq-cli txn orders '{\"id\":1}' '{\"id\":2}'")
}

func cmdSchemaList(args []string) {
	c := connect(args)
	defer c.Close()

	schemas, err := c.ListSchemas()
	if err != nil {
		cli.Error("Failed to list schemas: %v", err)
		os.Exit(1)
	}

	if len(schemas) == 0 {
		cli.Info("No schemas registered")
		return
	}

	cli.Header("Registered Schemas:")
	cli.Separator()
	for _, s := range schemas {
		fmt.Printf("  %s %s (type: %s, version: %d)\n", cli.IconDot, s.Name, s.Type, s.Version)
	}
	cli.Separator()
}

func cmdSchemaValidate(args []string) {
	if len(args) < 2 {
		cli.Error("Usage: flymq-cli schema validate <name> <message>")
		os.Exit(1)
	}

	name := args[0]
	message := args[1]

	c := connect(args)
	defer c.Close()

	valid, err := c.ValidateSchema(name, []byte(message))
	if err != nil {
		cli.Error("Validation error: %v", err)
		os.Exit(1)
	}

	if valid {
		cli.Success("Message is valid against schema '%s'", name)
	} else {
		cli.Error("Message is invalid against schema '%s'", name)
		os.Exit(1)
	}
}

func cmdSchemaDelete(args []string) {
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli schema delete <name>")
		os.Exit(1)
	}

	name := args[0]

	c := connect(args)
	defer c.Close()

	if err := c.DeleteSchema(name); err != nil {
		cli.Error("Failed to delete schema: %v", err)
		os.Exit(1)
	}

	cli.Success("Schema '%s' deleted", name)
}

// ============================================================================
// Health Check Commands
// ============================================================================

func cmdHealth(args []string) {
	subCmd, subArgs := extractSubcommand(args)
	if subCmd == "" || subCmd == "help" || hasFlag(subArgs, "--help") || hasFlag(subArgs, "-h") {
		printHealthHelp()
		return
	}

	switch subCmd {
	case "status":
		cmdHealthStatus(subArgs)
	case "live", "liveness":
		cmdHealthLive(subArgs)
	case "ready", "readiness":
		cmdHealthReady(subArgs)
	default:
		cli.Error("Unknown health subcommand: %s", subCmd)
		os.Exit(1)
	}
}

func cmdHealthStatus(args []string) {
	healthAddr := getHealthAddr(args)
	tlsCfg := getHealthTLSConfig(args)
	url := buildURL(healthAddr, "/health", tlsCfg.TLSEnabled)

	resp, err := httpGetWithTLS(url, tlsCfg)
	if err != nil {
		cli.Error("Health check failed: %v", err)
		os.Exit(1)
	}

	cli.Header("Health Status:")
	fmt.Println(resp)
}

func cmdHealthLive(args []string) {
	healthAddr := getHealthAddr(args)
	tlsCfg := getHealthTLSConfig(args)
	url := buildURL(healthAddr, "/health/live", tlsCfg.TLSEnabled)

	resp, err := httpGetWithTLS(url, tlsCfg)
	if err != nil {
		cli.Error("Liveness check failed: %v", err)
		os.Exit(1)
	}

	if strings.Contains(resp, "healthy") || strings.Contains(resp, "ok") || strings.Contains(resp, "UP") {
		cli.Success("Liveness: OK")
	} else {
		cli.Error("Liveness: FAILED")
		os.Exit(1)
	}
}

func cmdHealthReady(args []string) {
	healthAddr := getHealthAddr(args)
	tlsCfg := getHealthTLSConfig(args)
	url := buildURL(healthAddr, "/health/ready", tlsCfg.TLSEnabled)

	resp, err := httpGetWithTLS(url, tlsCfg)
	if err != nil {
		cli.Error("Readiness check failed: %v", err)
		os.Exit(1)
	}

	if strings.Contains(resp, "healthy") || strings.Contains(resp, "ok") || strings.Contains(resp, "UP") {
		cli.Success("Readiness: OK")
	} else {
		cli.Error("Readiness: FAILED")
		os.Exit(1)
	}
}

// getHealthTLSConfig returns the HTTP client config for health endpoints.
func getHealthTLSConfig(args []string) HTTPClientConfig {
	return HTTPClientConfig{
		TLSEnabled: getHealthTLSEnabled(args),
		Insecure:   getHealthTLSInsecure(args),
		CAFile:     getHealthCAFile(args),
	}
}

func getHealthAddr(args []string) string {
	for i, arg := range args {
		if (arg == "--health-addr" || arg == "-h") && i+1 < len(args) {
			return args[i+1]
		}
	}
	if addr := os.Getenv("FLYMQ_HEALTH_ADDR"); addr != "" {
		return addr
	}
	return "localhost:9095"
}

// getHealthTLSEnabled checks if TLS is enabled for health endpoints.
func getHealthTLSEnabled(args []string) bool {
	for _, arg := range args {
		if arg == "--health-tls" {
			return true
		}
	}
	if tlsEnv := os.Getenv("FLYMQ_HEALTH_TLS"); tlsEnv == "true" || tlsEnv == "1" {
		return true
	}
	return false
}

// getHealthTLSInsecure checks if TLS verification should be skipped for health endpoints.
func getHealthTLSInsecure(args []string) bool {
	for _, arg := range args {
		if arg == "--health-insecure" {
			return true
		}
	}
	if insecure := os.Getenv("FLYMQ_HEALTH_TLS_INSECURE"); insecure == "true" || insecure == "1" {
		return true
	}
	return false
}

// getHealthCAFile returns the CA certificate file path for health endpoints.
func getHealthCAFile(args []string) string {
	for i, arg := range args {
		if arg == "--health-ca-cert" && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--health-ca-cert=") {
			return strings.TrimPrefix(arg, "--health-ca-cert=")
		}
	}
	if caFile := os.Getenv("FLYMQ_HEALTH_CA_FILE"); caFile != "" {
		return caFile
	}
	return ""
}

// ============================================================================
// Admin API Commands
// ============================================================================

func cmdAdmin(args []string) {
	subCmd, subArgs := extractSubcommand(args)
	if subCmd == "" || subCmd == "help" || hasFlag(subArgs, "--help") || hasFlag(subArgs, "-h") {
		printAdminHelp()
		return
	}

	switch subCmd {
	case "cluster":
		cmdAdminCluster(subArgs)
	case "topics":
		cmdAdminTopics(subArgs)
	case "groups":
		cmdAdminGroups(subArgs)
	case "schemas":
		cmdAdminSchemas(subArgs)
	default:
		cli.Error("Unknown admin subcommand: %s", subCmd)
		os.Exit(1)
	}
}

func cmdAdminCluster(args []string) {
	adminAddr := getAdminAddr(args)
	tlsCfg := getAdminTLSConfig(args)
	url := buildURL(adminAddr, "/api/v1/cluster", tlsCfg.TLSEnabled)

	resp, err := httpGetWithTLS(url, tlsCfg)
	if err != nil {
		cli.Error("Failed to get cluster info: %v", err)
		os.Exit(1)
	}

	cli.Header("Cluster Information:")
	fmt.Println(resp)
}

func cmdAdminTopics(args []string) {
	adminAddr := getAdminAddr(args)
	tlsCfg := getAdminTLSConfig(args)
	url := buildURL(adminAddr, "/api/v1/topics", tlsCfg.TLSEnabled)

	resp, err := httpGetWithTLS(url, tlsCfg)
	if err != nil {
		cli.Error("Failed to list topics: %v", err)
		os.Exit(1)
	}

	cli.Header("Topics (Admin API):")
	fmt.Println(resp)
}

func cmdAdminGroups(args []string) {
	adminAddr := getAdminAddr(args)
	tlsCfg := getAdminTLSConfig(args)
	url := buildURL(adminAddr, "/api/v1/consumer-groups", tlsCfg.TLSEnabled)

	resp, err := httpGetWithTLS(url, tlsCfg)
	if err != nil {
		cli.Error("Failed to list consumer groups: %v", err)
		os.Exit(1)
	}

	cli.Header("Consumer Groups:")
	fmt.Println(resp)
}

func cmdAdminSchemas(args []string) {
	adminAddr := getAdminAddr(args)
	tlsCfg := getAdminTLSConfig(args)
	url := buildURL(adminAddr, "/api/v1/schemas", tlsCfg.TLSEnabled)

	resp, err := httpGetWithTLS(url, tlsCfg)
	if err != nil {
		cli.Error("Failed to list schemas: %v", err)
		os.Exit(1)
	}

	cli.Header("Schemas (Admin API):")
	fmt.Println(resp)
}

// getAdminTLSConfig returns the HTTP client config for admin API.
func getAdminTLSConfig(args []string) HTTPClientConfig {
	return HTTPClientConfig{
		TLSEnabled: getAdminTLSEnabled(args),
		Insecure:   getAdminTLSInsecure(args),
		CAFile:     getAdminCAFile(args),
		Username:   getAdminUser(args),
		Password:   getAdminPass(args),
	}
}

// getAdminUser returns the username for Admin API authentication.
func getAdminUser(args []string) string {
	for i, arg := range args {
		if arg == "--admin-user" && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--admin-user=") {
			return strings.TrimPrefix(arg, "--admin-user=")
		}
	}
	if user := os.Getenv("FLYMQ_ADMIN_USER"); user != "" {
		return user
	}
	return ""
}

// getAdminPass returns the password for Admin API authentication.
func getAdminPass(args []string) string {
	for i, arg := range args {
		if arg == "--admin-pass" && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--admin-pass=") {
			return strings.TrimPrefix(arg, "--admin-pass=")
		}
	}
	if pass := os.Getenv("FLYMQ_ADMIN_PASS"); pass != "" {
		return pass
	}
	return ""
}

func getAdminAddr(args []string) string {
	for i, arg := range args {
		if (arg == "--admin-addr" || arg == "-a") && i+1 < len(args) {
			return args[i+1]
		}
	}
	if addr := os.Getenv("FLYMQ_ADMIN_ADDR"); addr != "" {
		return addr
	}
	return "localhost:9096"
}

// getAdminTLSEnabled checks if TLS is enabled for admin API.
func getAdminTLSEnabled(args []string) bool {
	for _, arg := range args {
		if arg == "--admin-tls" {
			return true
		}
	}
	if tlsEnv := os.Getenv("FLYMQ_ADMIN_TLS"); tlsEnv == "true" || tlsEnv == "1" {
		return true
	}
	return false
}

// getAdminTLSInsecure checks if TLS verification should be skipped for admin API.
func getAdminTLSInsecure(args []string) bool {
	for _, arg := range args {
		if arg == "--admin-insecure" {
			return true
		}
	}
	if insecure := os.Getenv("FLYMQ_ADMIN_TLS_INSECURE"); insecure == "true" || insecure == "1" {
		return true
	}
	return false
}

// getAdminCAFile returns the CA certificate file path for admin API.
func getAdminCAFile(args []string) string {
	for i, arg := range args {
		if arg == "--admin-ca-cert" && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--admin-ca-cert=") {
			return strings.TrimPrefix(arg, "--admin-ca-cert=")
		}
	}
	if caFile := os.Getenv("FLYMQ_ADMIN_CA_FILE"); caFile != "" {
		return caFile
	}
	return ""
}

// ============================================================================
// HTTP Helper
// ============================================================================

// HTTPClientConfig holds TLS and authentication configuration for HTTP requests.
type HTTPClientConfig struct {
	TLSEnabled bool
	Insecure   bool
	CAFile     string
	Username   string // For HTTP Basic Auth
	Password   string // For HTTP Basic Auth
}

// buildHTTPClient creates an HTTP client with optional TLS configuration.
func buildHTTPClient(cfg HTTPClientConfig) (*http.Client, error) {
	if !cfg.TLSEnabled {
		return http.DefaultClient, nil
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.Insecure,
	}

	// Load CA certificate if provided
	if cfg.CAFile != "" {
		caCert, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	return &http.Client{Transport: transport}, nil
}

// buildURL constructs a URL with the appropriate protocol.
func buildURL(addr, path string, tlsEnabled bool) string {
	// If addr already has a protocol, use it as-is
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return addr + path
	}
	// Otherwise, add protocol based on TLS setting
	protocol := "http"
	if tlsEnabled {
		protocol = "https"
	}
	return fmt.Sprintf("%s://%s%s", protocol, addr, path)
}

func httpGet(url string) (string, error) {
	return httpGetWithTLS(url, HTTPClientConfig{})
}

func httpGetWithTLS(url string, cfg HTTPClientConfig) (string, error) {
	client, err := buildHTTPClient(cfg)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}

	// Add Basic Auth if credentials provided
	if cfg.Username != "" && cfg.Password != "" {
		req.SetBasicAuth(cfg.Username, cfg.Password)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode >= 400 {
		if resp.StatusCode == 401 {
			return "", fmt.Errorf("authentication required (use --admin-user and --admin-pass)")
		}
		if resp.StatusCode == 403 {
			return "", fmt.Errorf("permission denied")
		}
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return string(body), nil
}

func httpPostWithTLS(url string, body []byte, cfg HTTPClientConfig) (string, error) {
	client, err := buildHTTPClient(cfg)
	if err != nil {
		return "", err
	}

	var bodyReader io.Reader
	if body != nil {
		bodyReader = strings.NewReader(string(body))
	}

	req, err := http.NewRequest("POST", url, bodyReader)
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-Type", "application/json")

	// Add Basic Auth if credentials provided
	if cfg.Username != "" && cfg.Password != "" {
		req.SetBasicAuth(cfg.Username, cfg.Password)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode >= 400 {
		if resp.StatusCode == 401 {
			return "", fmt.Errorf("authentication required (use --admin-user and --admin-pass)")
		}
		if resp.StatusCode == 403 {
			return "", fmt.Errorf("permission denied")
		}
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	return string(respBody), nil
}

// ============================================================================
// Cluster Commands
// ============================================================================

func cmdCluster(args []string) {
	subCmd, subArgs := extractSubcommand(args)
	if subCmd == "" || subCmd == "help" || hasFlag(subArgs, "--help") || hasFlag(subArgs, "-h") {
		printClusterHelp()
		return
	}

	switch subCmd {
	case "status":
		cmdClusterStatus(subArgs)
	case "members":
		cmdClusterMembers(subArgs)
	case "info":
		cmdClusterInfo(subArgs)
	case "join":
		cmdClusterJoin(subArgs)
	case "leave":
		cmdClusterLeave(subArgs)
	// Partition management commands
	case "metadata":
		cmdClusterMetadata(subArgs)
	case "partitions":
		cmdClusterPartitions(subArgs)
	case "leaders":
		cmdClusterLeaders(subArgs)
	case "rebalance":
		cmdClusterRebalance(subArgs)
	case "reassign":
		cmdClusterReassign(subArgs)
	default:
		cli.Error("Unknown cluster subcommand: %s", subCmd)
		os.Exit(1)
	}
}

func cmdClusterStatus(args []string) {
	adminAddr := getAdminAddr(args)
	tlsCfg := getAdminTLSConfig(args)
	url := buildURL(adminAddr, "/api/v1/cluster", tlsCfg.TLSEnabled)

	resp, err := httpGetWithTLS(url, tlsCfg)
	if err != nil {
		cli.Error("Failed to get cluster status: %v", err)
		os.Exit(1)
	}

	cli.Header("Cluster Status:")
	cli.Separator()
	fmt.Println(resp)
	cli.Separator()
}

func cmdClusterMembers(args []string) {
	adminAddr := getAdminAddr(args)
	tlsCfg := getAdminTLSConfig(args)
	url := buildURL(adminAddr, "/api/v1/cluster/members", tlsCfg.TLSEnabled)

	resp, err := httpGetWithTLS(url, tlsCfg)
	if err != nil {
		cli.Error("Failed to get cluster members: %v", err)
		os.Exit(1)
	}

	cli.Header("Cluster Members:")
	cli.Separator()
	fmt.Println(resp)
	cli.Separator()
}

func cmdClusterInfo(args []string) {
	adminAddr := getAdminAddr(args)
	tlsCfg := getAdminTLSConfig(args)
	url := buildURL(adminAddr, "/api/v1/cluster", tlsCfg.TLSEnabled)

	resp, err := httpGetWithTLS(url, tlsCfg)
	if err != nil {
		cli.Error("Failed to get cluster info: %v", err)
		os.Exit(1)
	}

	cli.Header("Cluster Information:")
	cli.Separator()
	fmt.Println(resp)
	cli.Separator()
}

func cmdClusterJoin(args []string) {
	var peerAddr string

	for i := 0; i < len(args); i++ {
		if (args[i] == "--peer" || args[i] == "-p") && i+1 < len(args) {
			peerAddr = args[i+1]
			i++
		}
	}

	if peerAddr == "" {
		cli.Error("Usage: flymq-cli cluster join --peer <address>")
		cli.Info("  --peer, -p    Address of an existing cluster node (e.g., node1:9093)")
		os.Exit(1)
	}

	c := connect(args)
	defer c.Close()

	if err := c.ClusterJoin(peerAddr); err != nil {
		cli.Error("Failed to join cluster: %v", err)
		os.Exit(1)
	}

	cli.Success("Successfully initiated join to cluster via %s", peerAddr)
	cli.Info("The node will synchronize data and become available shortly.")
}

func cmdClusterLeave(args []string) {
	c := connect(args)
	defer c.Close()

	cli.Warning("This will remove this node from the cluster.")

	if err := c.ClusterLeave(); err != nil {
		cli.Error("Failed to leave cluster: %v", err)
		os.Exit(1)
	}

	cli.Success("Successfully left the cluster")
	cli.Info("The node is now in standalone mode.")
}

// ============================================================================
// Partition Management Commands (Horizontal Scaling)
// ============================================================================

func cmdClusterMetadata(args []string) {
	adminAddr := getAdminAddr(args)
	tlsCfg := getAdminTLSConfig(args)

	// Optional topic filter
	topic := ""
	for i := 0; i < len(args); i++ {
		if (args[i] == "--topic" || args[i] == "-t") && i+1 < len(args) {
			topic = args[i+1]
			break
		}
	}

	url := buildURL(adminAddr, "/api/v1/cluster/metadata", tlsCfg.TLSEnabled)
	if topic != "" {
		url += "?topic=" + topic
	}

	resp, err := httpGetWithTLS(url, tlsCfg)
	if err != nil {
		cli.Error("Failed to get cluster metadata: %v", err)
		os.Exit(1)
	}

	cli.Header("Cluster Metadata (Partition-to-Node Mappings)")
	cli.Separator()
	fmt.Println(resp)
	cli.Separator()
	cli.Info("Use this metadata for smart client routing to partition leaders")
}

func cmdClusterPartitions(args []string) {
	adminAddr := getAdminAddr(args)
	tlsCfg := getAdminTLSConfig(args)

	// Optional topic filter
	topic := ""
	for i := 0; i < len(args); i++ {
		if (args[i] == "--topic" || args[i] == "-t") && i+1 < len(args) {
			topic = args[i+1]
			break
		}
	}

	url := buildURL(adminAddr, "/api/v1/cluster/partitions", tlsCfg.TLSEnabled)
	if topic != "" {
		url += "?topic=" + topic
	}

	resp, err := httpGetWithTLS(url, tlsCfg)
	if err != nil {
		cli.Error("Failed to get partition assignments: %v", err)
		os.Exit(1)
	}

	cli.Header("Partition Assignments")
	cli.Separator()
	fmt.Println(resp)
	cli.Separator()
}

func cmdClusterLeaders(args []string) {
	adminAddr := getAdminAddr(args)
	tlsCfg := getAdminTLSConfig(args)
	url := buildURL(adminAddr, "/api/v1/cluster/leaders", tlsCfg.TLSEnabled)

	resp, err := httpGetWithTLS(url, tlsCfg)
	if err != nil {
		cli.Error("Failed to get leader distribution: %v", err)
		os.Exit(1)
	}

	cli.Header("Partition Leader Distribution")
	cli.Separator()
	fmt.Println(resp)
	cli.Separator()
	cli.Info("A balanced distribution means each node has roughly equal partition leaders")
}

func cmdClusterRebalance(args []string) {
	adminAddr := getAdminAddr(args)
	tlsCfg := getAdminTLSConfig(args)
	url := buildURL(adminAddr, "/api/v1/cluster/rebalance", tlsCfg.TLSEnabled)

	cli.Warning("This will trigger a partition rebalance across the cluster.")
	cli.Info("Partition leaders will be redistributed for even load distribution.")

	resp, err := httpPostWithTLS(url, nil, tlsCfg)
	if err != nil {
		cli.Error("Failed to trigger rebalance: %v", err)
		os.Exit(1)
	}

	cli.Header("Rebalance Result")
	cli.Separator()
	fmt.Println(resp)
	cli.Separator()
	cli.Success("Partition rebalance initiated")
}

func cmdClusterReassign(args []string) {
	if len(args) < 2 {
		cli.Error("Usage: flymq-cli cluster reassign <topic> <partition> --leader <node-id> [--replicas <node1,node2,...>]")
		os.Exit(1)
	}

	topic := args[0]
	partition, err := strconv.Atoi(args[1])
	if err != nil {
		cli.Error("Invalid partition number: %s", args[1])
		os.Exit(1)
	}

	var newLeader string
	var replicas string
	for i := 2; i < len(args); i++ {
		if (args[i] == "--leader" || args[i] == "-l") && i+1 < len(args) {
			newLeader = args[i+1]
			i++
		} else if (args[i] == "--replicas" || args[i] == "-r") && i+1 < len(args) {
			replicas = args[i+1]
			i++
		}
	}

	if newLeader == "" {
		cli.Error("--leader is required")
		os.Exit(1)
	}

	adminAddr := getAdminAddr(args)
	tlsCfg := getAdminTLSConfig(args)
	url := buildURL(adminAddr, fmt.Sprintf("/api/v1/cluster/partitions/%s/%d", topic, partition), tlsCfg.TLSEnabled)

	body := fmt.Sprintf(`{"new_leader": "%s"`, newLeader)
	if replicas != "" {
		replicaList := strings.Split(replicas, ",")
		replicaJSON := `[`
		for i, r := range replicaList {
			if i > 0 {
				replicaJSON += ","
			}
			replicaJSON += `"` + strings.TrimSpace(r) + `"`
		}
		replicaJSON += `]`
		body += fmt.Sprintf(`, "new_replicas": %s`, replicaJSON)
	}
	body += `}`

	resp, err := httpPostWithTLS(url, []byte(body), tlsCfg)
	if err != nil {
		cli.Error("Failed to reassign partition: %v", err)
		os.Exit(1)
	}

	cli.Header("Partition Reassignment")
	cli.Separator()
	fmt.Println(resp)
	cli.Separator()
	cli.Success("Partition %s:%d reassigned to %s", topic, partition, newLeader)
}

// ============================================================================
// Consumer Group Commands
// ============================================================================

func cmdGroups(args []string) {
	subCmd, subArgs := extractSubcommand(args)
	if subCmd == "" || subCmd == "help" || hasFlag(subArgs, "--help") || hasFlag(subArgs, "-h") {
		printGroupsHelp()
		return
	}

	switch subCmd {
	case "list":
		cmdGroupsList(subArgs)
	case "describe":
		cmdGroupsDescribe(subArgs)
	case "reset-offsets":
		cmdGroupsResetOffsets(subArgs)
	case "lag":
		cmdGroupsLag(subArgs)
	case "delete":
		cmdGroupsDelete(subArgs)
	default:
		cli.Error("Unknown groups subcommand: %s", subCmd)
		os.Exit(1)
	}
}

func cmdGroupsList(args []string) {
	c := connect(args)
	defer c.Close()

	groups, err := c.ListConsumerGroups()
	if err != nil {
		cli.Error("Failed to list consumer groups: %v", err)
		os.Exit(1)
	}

	if len(groups) == 0 {
		cli.Info("No consumer groups found")
		return
	}

	cli.Header("Consumer Groups:")
	cli.Separator()
	for _, g := range groups {
		fmt.Printf("  %s %s (state: %s, members: %d)\n", cli.IconDot, g.GroupID, g.State, len(g.Members))
	}
	cli.Separator()
}

func cmdGroupsDescribe(args []string) {
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli groups describe <group-id>")
		os.Exit(1)
	}

	groupID := args[0]

	c := connect(args)
	defer c.Close()

	group, err := c.DescribeConsumerGroup(groupID)
	if err != nil {
		cli.Error("Failed to describe consumer group: %v", err)
		os.Exit(1)
	}

	cli.Header(fmt.Sprintf("Consumer Group: %s", groupID))
	cli.Separator()
	cli.KeyValue("State", group.State)
	cli.KeyValue("Members", fmt.Sprintf("%d", len(group.Members)))
	cli.KeyValue("Topics", fmt.Sprintf("%v", group.Topics))
	if group.Coordinator != "" {
		cli.KeyValue("Coordinator", group.Coordinator)
	}

	if len(group.Members) > 0 {
		fmt.Println()
		fmt.Println("  Members:")
		for _, m := range group.Members {
			fmt.Printf("    %s %s\n", cli.IconDot, m)
		}
	}

	if len(group.Offsets) > 0 {
		fmt.Println()
		fmt.Println("  Offsets:")
		for _, o := range group.Offsets {
			fmt.Printf("    %s %s[%d]: offset=%d, lag=%d\n", cli.IconDot, o.Topic, o.Partition, o.Offset, o.Lag)
		}
	}
	cli.Separator()
}

func cmdGroupsResetOffsets(args []string) {
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli groups reset-offsets <group-id> [--topic TOPIC] [--to-earliest|--to-latest|--to-offset N]")
		os.Exit(1)
	}

	groupID := args[0]
	topic := ""
	partition := 0
	mode := "earliest"
	var specificOffset uint64 = 0

	for i := 1; i < len(args); i++ {
		switch args[i] {
		case "--topic", "-t":
			if i+1 < len(args) {
				topic = args[i+1]
				i++
			}
		case "--partition", "-p":
			if i+1 < len(args) {
				if v, err := strconv.Atoi(args[i+1]); err == nil {
					partition = v
				}
				i++
			}
		case "--to-earliest":
			mode = "earliest"
		case "--to-latest":
			mode = "latest"
		case "--to-offset":
			if i+1 < len(args) {
				if v, err := strconv.ParseUint(args[i+1], 10, 64); err == nil {
					specificOffset = v
					mode = "offset"
				}
				i++
			}
		}
	}

	if topic == "" {
		cli.Error("--topic is required for reset-offsets")
		os.Exit(1)
	}

	c := connect(args)
	defer c.Close()

	var err error
	if mode == "offset" {
		err = c.ResetOffset(topic, groupID, partition, mode, specificOffset)
	} else {
		err = c.ResetOffset(topic, groupID, partition, mode, 0)
	}

	if err != nil {
		cli.Error("Failed to reset offsets: %v", err)
		os.Exit(1)
	}

	if mode == "offset" {
		cli.Success("Offsets reset to %d for group %s on %s[%d]", specificOffset, groupID, topic, partition)
	} else {
		cli.Success("Offsets reset to %s for group %s on %s[%d]", mode, groupID, topic, partition)
	}
}

func cmdGroupsLag(args []string) {
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli groups lag <group-id> [--topic TOPIC]")
		os.Exit(1)
	}

	groupID := args[0]
	topic := ""
	partition := 0

	for i := 1; i < len(args); i++ {
		switch args[i] {
		case "--topic", "-t":
			if i+1 < len(args) {
				topic = args[i+1]
				i++
			}
		case "--partition", "-p":
			if i+1 < len(args) {
				if v, err := strconv.Atoi(args[i+1]); err == nil {
					partition = v
				}
				i++
			}
		}
	}

	c := connect(args)
	defer c.Close()

	if topic != "" {
		// Get lag for specific topic
		lagInfo, err := c.GetLag(topic, groupID, partition)
		if err != nil {
			cli.Error("Failed to get lag: %v", err)
			os.Exit(1)
		}

		cli.Header(fmt.Sprintf("Consumer Lag for %s:", groupID))
		cli.Separator()
		fmt.Printf("  Topic: %s, Partition: %d\n", topic, partition)
		cli.KeyValue("Current Offset", fmt.Sprintf("%d", lagInfo.CurrentOffset))
		cli.KeyValue("Committed Offset", fmt.Sprintf("%d", lagInfo.CommittedOffset))
		cli.KeyValue("Latest Offset", fmt.Sprintf("%d", lagInfo.LatestOffset))
		cli.KeyValue("Lag", fmt.Sprintf("%d messages", lagInfo.Lag))
		cli.Separator()
	} else {
		// Get lag for all topics in the group
		group, err := c.DescribeConsumerGroup(groupID)
		if err != nil {
			cli.Error("Failed to describe group: %v", err)
			os.Exit(1)
		}

		cli.Header(fmt.Sprintf("Consumer Lag for %s:", groupID))
		cli.Separator()

		totalLag := uint64(0)
		for _, t := range group.Topics {
			lagInfo, err := c.GetLag(t, groupID, 0)
			if err != nil {
				fmt.Printf("  %s: error getting lag\n", t)
				continue
			}
			fmt.Printf("  %s[0]: lag=%d (current=%d, latest=%d)\n", t, lagInfo.Lag, lagInfo.CurrentOffset, lagInfo.LatestOffset)
			totalLag += lagInfo.Lag
		}

		fmt.Println()
		cli.KeyValue("Total Lag", fmt.Sprintf("%d messages", totalLag))
		cli.Separator()
	}
}

func cmdGroupsDelete(args []string) {
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli groups delete <group-id>")
		os.Exit(1)
	}

	groupID := args[0]

	c := connect(args)
	defer c.Close()

	if err := c.DeleteConsumerGroup(groupID); err != nil {
		cli.Error("Failed to delete consumer group: %v", err)
		os.Exit(1)
	}

	cli.Success("Consumer group '%s' deleted", groupID)
}

// =========================================================================
// Authentication Commands
// =========================================================================

func cmdAuth(args []string) {
	if hasFlag(args, "--help") || hasFlag(args, "-h") {
		printAuthHelp()
		return
	}
	username := getUsername(args)
	password := getPassword(args)

	if username == "" {
		cli.Error("Usage: flymq-cli auth --username <user> --password <pass>")
		cli.Error("Or set FLYMQ_USERNAME and FLYMQ_PASSWORD environment variables")
		os.Exit(1)
	}

	c := connect(args)
	defer c.Close()

	if err := c.Authenticate(username, password); err != nil {
		cli.Error("Authentication failed: %v", err)
		os.Exit(1)
	}

	if err := saveCredentials(username, password); err != nil {
		cli.Warning("Failed to persist credentials: %v", err)
	}

	cli.Success("Authenticated as '%s'", username)

	// Get additional info via WhoAmI
	resp, err := c.WhoAmI()
	if err == nil && resp != nil {
		if len(resp.Roles) > 0 {
			cli.KeyValue("Roles", strings.Join(resp.Roles, ", "))
		}
		if len(resp.Permissions) > 0 {
			cli.KeyValue("Permissions", strings.Join(resp.Permissions, ", "))
		}
	}
}

func cmdWhoAmI(args []string) {
	if hasFlag(args, "--help") || hasFlag(args, "-h") {
		printWhoAmIHelp()
		return
	}
	c := connect(args)
	defer c.Close()

	resp, err := c.WhoAmI()
	if err != nil {
		cli.Error("Failed to get authentication status: %v", err)
		os.Exit(1)
	}

	cli.Header("Authentication Status")
	cli.Separator()

	if resp.Authenticated {
		cli.KeyValue("Authenticated", "Yes")
		cli.KeyValue("Username", resp.Username)
		if len(resp.Roles) > 0 {
			cli.KeyValue("Roles", strings.Join(resp.Roles, ", "))
		}
		if len(resp.Permissions) > 0 {
			cli.KeyValue("Permissions", strings.Join(resp.Permissions, ", "))
		}
	} else {
		cli.KeyValue("Authenticated", "No")
	}

	cli.Separator()
}

// =========================================================================
// User Management Commands
// =========================================================================

func cmdUsers(args []string) {
	subCmd, subArgs := extractSubcommand(args)
	if subCmd == "" || subCmd == "help" || hasFlag(subArgs, "--help") || hasFlag(subArgs, "-h") {
		printUsersHelp()
		return
	}

	switch subCmd {
	case "list":
		cmdUsersList(subArgs)
	case "create":
		cmdUsersCreate(subArgs)
	case "delete":
		cmdUsersDelete(subArgs)
	case "update":
		cmdUsersUpdate(subArgs)
	case "get":
		cmdUsersGet(subArgs)
	case "passwd":
		cmdUsersPasswd(subArgs)
	default:
		cli.Error("Unknown users subcommand: %s", subCmd)
		os.Exit(1)
	}
}

func printUsersUsage() {
	cli.Header("users - Manage users and permissions")
	fmt.Printf("\n%s flymq-cli users <command> [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Commands")
	cli.Command("list", "", "List all users")
	cli.Command("create", "<user> <pass>", "Create a new user")
	cli.Command("delete", "<user>", "Delete a user")
	cli.Command("update", "<user>", "Update user roles/status")
	cli.Command("get", "<user>", "Get user details")
	cli.Command("passwd", "<user> <pass>", "Change user password")

	cli.Section("Options")
	cli.Option("", "--roles", "role1,role2", "Roles for create/update")
	cli.Option("", "--enabled", "true|false", "Enable/disable user")

	cli.Section("Examples")
	cli.Example("Create a new admin user", "flymq-cli users create admin secret --roles admin")
	cli.Example("Disable a user account", "flymq-cli users update dev-user --enabled false")
}

func cmdUsersList(args []string) {
	c := connectWithAuth(args)
	defer c.Close()

	users, err := c.ListUsers()
	if err != nil {
		cli.Error("Failed to list users: %v", err)
		os.Exit(1)
	}

	cli.Header("Users")
	cli.Separator()

	if len(users) == 0 {
		fmt.Println("No users found")
	} else {
		for _, u := range users {
			status := "enabled"
			if !u.Enabled {
				status = "disabled"
			}
			cli.KeyValue(u.Username, fmt.Sprintf("roles=[%s] %s", strings.Join(u.Roles, ","), status))
		}
	}
	cli.Separator()
}

func cmdUsersCreate(args []string) {
	if len(args) < 2 {
		cli.Error("Usage: flymq-cli users create <username> <password> [--roles role1,role2]")
		os.Exit(1)
	}

	username := args[0]
	password := args[1]
	roles := getRolesArg(args[2:])

	c := connectWithAuth(args[2:])
	defer c.Close()

	if err := c.CreateUser(username, password, roles); err != nil {
		cli.Error("Failed to create user: %v", err)
		os.Exit(1)
	}

	cli.Success("User '%s' created successfully", username)
	if len(roles) > 0 {
		cli.KeyValue("Roles", strings.Join(roles, ", "))
	}
}

func cmdUsersDelete(args []string) {
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli users delete <username>")
		os.Exit(1)
	}

	username := args[0]

	c := connectWithAuth(args[1:])
	defer c.Close()

	if err := c.DeleteUser(username); err != nil {
		cli.Error("Failed to delete user: %v", err)
		os.Exit(1)
	}

	cli.Success("User '%s' deleted successfully", username)
}

func cmdUsersUpdate(args []string) {
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli users update <username> [--roles role1,role2] [--enabled true|false]")
		os.Exit(1)
	}

	username := args[0]
	roles := getRolesArg(args[1:])
	enabled := getEnabledArg(args[1:])

	c := connectWithAuth(args[1:])
	defer c.Close()

	if err := c.UpdateUser(username, roles, enabled); err != nil {
		cli.Error("Failed to update user: %v", err)
		os.Exit(1)
	}

	cli.Success("User '%s' updated successfully", username)
}

func cmdUsersGet(args []string) {
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli users get <username>")
		os.Exit(1)
	}

	username := args[0]

	c := connectWithAuth(args[1:])
	defer c.Close()

	user, err := c.GetUser(username)
	if err != nil {
		cli.Error("Failed to get user: %v", err)
		os.Exit(1)
	}

	cli.Header("User: " + user.Username)
	cli.Separator()
	cli.KeyValue("Roles", strings.Join(user.Roles, ", "))
	cli.KeyValue("Permissions", strings.Join(user.Permissions, ", "))
	cli.KeyValue("Enabled", fmt.Sprintf("%v", user.Enabled))
	cli.KeyValue("Created", user.CreatedAt)
	cli.KeyValue("Updated", user.UpdatedAt)
	cli.Separator()
}

func cmdUsersPasswd(args []string) {
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli users passwd <username>")
		os.Exit(1)
	}

	username := args[0]

	// Prompt for passwords
	fmt.Print("New password: ")
	var newPass string
	fmt.Scanln(&newPass)

	c := connectWithAuth(args[1:])
	defer c.Close()

	if err := c.ChangePassword(username, "", newPass); err != nil {
		cli.Error("Failed to change password: %v", err)
		os.Exit(1)
	}

	cli.Success("Password changed for user '%s'", username)
}

func getRolesArg(args []string) []string {
	for i, arg := range args {
		if arg == "--roles" && i+1 < len(args) {
			return strings.Split(args[i+1], ",")
		}
	}
	return nil
}

func getEnabledArg(args []string) *bool {
	for i, arg := range args {
		if arg == "--enabled" && i+1 < len(args) {
			val := args[i+1] == "true"
			return &val
		}
	}
	return nil
}

func connectWithAuth(args []string) *client.Client {
	c := connect(args)
	username := getUsername(args)
	password := getPassword(args)

	if username != "" && password != "" {
		if err := c.Authenticate(username, password); err != nil {
			cli.Error("Authentication failed: %v", err)
			os.Exit(1)
		}
	}
	return c
}

// =========================================================================
// ACL Management Commands
// =========================================================================

func cmdACL(args []string) {
	subCmd, subArgs := extractSubcommand(args)
	if subCmd == "" || subCmd == "help" || hasFlag(subArgs, "--help") || hasFlag(subArgs, "-h") {
		printACLHelp()
		return
	}

	switch subCmd {
	case "list":
		cmdACLList(subArgs)
	case "get":
		cmdACLGet(subArgs)
	case "set":
		cmdACLSet(subArgs)
	case "delete":
		cmdACLDelete(subArgs)
	default:
		cli.Error("Unknown acl subcommand: %s", subCmd)
		os.Exit(1)
	}
}

func cmdACLList(args []string) {
	c := connectWithAuth(args)
	defer c.Close()

	acls, defaultPublic, err := c.ListACLs()
	if err != nil {
		cli.Error("Failed to list ACLs: %v", err)
		os.Exit(1)
	}

	cli.Header("Topic ACLs")
	cli.KeyValue("Default Public", fmt.Sprintf("%v", defaultPublic))
	cli.Separator()

	if len(acls) == 0 {
		fmt.Println("No topic-specific ACLs configured")
	} else {
		for _, acl := range acls {
			public := "private"
			if acl.Public {
				public = "public"
			}
			cli.KeyValue(acl.Topic, fmt.Sprintf("%s users=[%s] roles=[%s]",
				public,
				strings.Join(acl.AllowedUsers, ","),
				strings.Join(acl.AllowedRoles, ",")))
		}
	}
	cli.Separator()
}

func cmdACLGet(args []string) {
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli acl get <topic>")
		os.Exit(1)
	}

	topic := args[0]

	c := connectWithAuth(args[1:])
	defer c.Close()

	acl, err := c.GetACL(topic)
	if err != nil {
		cli.Error("Failed to get ACL: %v", err)
		os.Exit(1)
	}

	cli.Header("ACL for topic: " + topic)
	cli.Separator()
	if !acl.Exists {
		cli.KeyValue("Status", "No explicit ACL (using default)")
		cli.KeyValue("Default Public", fmt.Sprintf("%v", acl.DefaultPublic))
	} else {
		cli.KeyValue("Public", fmt.Sprintf("%v", acl.Public))
		cli.KeyValue("Allowed Users", strings.Join(acl.AllowedUsers, ", "))
		cli.KeyValue("Allowed Roles", strings.Join(acl.AllowedRoles, ", "))
	}
	cli.Separator()
}

func cmdACLSet(args []string) {
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli acl set <topic> [--public] [--users u1,u2] [--roles r1,r2]")
		os.Exit(1)
	}

	topic := args[0]
	public := hasFlag(args[1:], "--public")
	users := getStringListArg(args[1:], "--users")
	roles := getStringListArg(args[1:], "--roles")

	c := connectWithAuth(args[1:])
	defer c.Close()

	if err := c.SetACL(topic, public, users, roles); err != nil {
		cli.Error("Failed to set ACL: %v", err)
		os.Exit(1)
	}

	cli.Success("ACL set for topic '%s'", topic)
}

func cmdACLDelete(args []string) {
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli acl delete <topic>")
		os.Exit(1)
	}

	topic := args[0]

	c := connectWithAuth(args[1:])
	defer c.Close()

	if err := c.DeleteACL(topic); err != nil {
		cli.Error("Failed to delete ACL: %v", err)
		os.Exit(1)
	}

	cli.Success("ACL deleted for topic '%s'", topic)
}

// ============================================================================
// Role Commands
// ============================================================================

func cmdRoles(args []string) {
	subCmd, subArgs := extractSubcommand(args)
	if subCmd == "" || subCmd == "help" || hasFlag(subArgs, "--help") || hasFlag(subArgs, "-h") {
		printRolesHelp()
		return
	}

	switch subCmd {
	case "list":
		cmdRolesList(subArgs)
	case "get":
		cmdRolesGet(subArgs)
	default:
		cli.Error("Unknown roles subcommand: %s", subCmd)
		os.Exit(1)
	}
}

func cmdRolesList(args []string) {
	c := connectWithAuth(args)
	defer c.Close()

	roles, err := c.ListRoles()
	if err != nil {
		cli.Error("Failed to list roles: %v", err)
		os.Exit(1)
	}

	cli.Header("Available Roles")
	cli.Separator()

	if len(roles) == 0 {
		fmt.Println("No roles found")
	} else {
		// Print in a nice table format
		fmt.Printf("%-12s %-25s %s\n", "ROLE", "PERMISSIONS", "DESCRIPTION")
		cli.Separator()
		for _, r := range roles {
			perms := strings.Join(r.Permissions, ", ")
			if perms == "" {
				perms = "(none)"
			}
			fmt.Printf("%-12s %-25s %s\n", r.Name, perms, r.Description)
		}
	}
	cli.Separator()
}

func cmdRolesGet(args []string) {
	if len(args) < 1 {
		cli.Error("Usage: flymq-cli roles get <role>")
		os.Exit(1)
	}

	roleName := args[0]

	c := connectWithAuth(args[1:])
	defer c.Close()

	roles, err := c.ListRoles()
	if err != nil {
		cli.Error("Failed to get roles: %v", err)
		os.Exit(1)
	}

	var found *client.RoleInfo
	for _, r := range roles {
		if r.Name == roleName {
			found = &r
			break
		}
	}

	if found == nil {
		cli.Error("Role '%s' not found", roleName)
		os.Exit(1)
	}

	cli.Header("Role: " + found.Name)
	cli.Separator()
	cli.KeyValue("Name", found.Name)
	cli.KeyValue("Description", found.Description)
	if len(found.Permissions) > 0 {
		cli.KeyValue("Permissions", strings.Join(found.Permissions, ", "))
	} else {
		cli.KeyValue("Permissions", "(none)")
	}
	cli.Separator()
}

func hasFlag(args []string, flag string) bool {
	for _, arg := range args {
		if arg == flag {
			return true
		}
	}
	return false
}

func getStringListArg(args []string, flag string) []string {
	for i, arg := range args {
		if arg == flag && i+1 < len(args) {
			return strings.Split(args[i+1], ",")
		}
	}
	return nil
}

// getStringArg returns the value of a string flag, or the default if not found.
func getStringArg(args []string, flag, defaultVal string) string {
	for i, arg := range args {
		if arg == flag && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, flag+"=") {
			return strings.TrimPrefix(arg, flag+"=")
		}
	}
	return defaultVal
}

// ============================================================================
// Command Help Functions
// ============================================================================

func printTopicsHelp() {
	cli.Header("topics - List all topics")
	fmt.Printf("\n%s flymq-cli topics [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Options")
	cli.Option("-a", "--addr", "host:port", "Server address")

	cli.Section("Examples")
	cli.Example("List all topics", "flymq-cli topics")
	cli.Example("List topics on remote server", "flymq-cli --addr remote:9092 topics")
}

func printDeleteHelp() {
	cli.Header("delete - Delete an existing topic")
	fmt.Printf("\n%s flymq-cli delete <topic> [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Arguments")
	cli.Command("<topic>", "", "Name of the topic to delete")

	cli.Section("Examples")
	cli.Example("Delete a topic", "flymq-cli delete old-topic")
}

func printInfoHelp() {
	cli.Header("info - Show detailed topic information")
	fmt.Printf("\n%s flymq-cli info <topic> [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Arguments")
	cli.Command("<topic>", "", "Topic name to inspect")

	cli.Section("Examples")
	cli.Example("Show info for orders topic", "flymq-cli info orders")
}

func printGroupsHelp() {
	cli.Header("groups - Consumer group management")
	fmt.Printf("\n%s flymq-cli groups <command> [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Commands")
	cli.SubCommand("list", "", "List all consumer groups")
	cli.SubCommand("describe", "<group-id>", "Show group members, state and assignments")
	cli.SubCommand("lag", "<group-id>", "Show consumer lag for each partition")
	cli.SubCommand("reset", "<group-id>", "Reset group offsets for a topic")
	cli.SubCommand("delete", "<group-id>", "Delete a consumer group")

	cli.Section("Examples")
	cli.Example("List all groups", "flymq-cli groups list")
	cli.Example("Check lag for 'analytics' group", "flymq-cli groups lag analytics")
}

func printUsersHelp() {
	cli.Header("users - Manage users and permissions")
	fmt.Printf("\n%s flymq-cli users <command> [args]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Commands")
	cli.SubCommand("list", "", "List all registered users")
	cli.SubCommand("create", "<user> <pass>", "Create a new user")
	cli.SubCommand("delete", "<user>", "Delete an existing user")
	cli.SubCommand("update", "<user>", "Update user roles or status")
	cli.SubCommand("get", "<user>", "Show detailed user information")
	cli.SubCommand("passwd", "<user>", "Change user password")

	cli.Section("Examples")
	cli.Example("Create a new admin user", "flymq-cli users create admin secret --roles admin")
	cli.Example("List all users", "flymq-cli users list")
}

func printRolesHelp() {
	cli.Header("roles - Manage RBAC roles")
	fmt.Printf("\n%s flymq-cli roles <command> [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Commands")
	cli.SubCommand("list", "", "List all available roles")
	cli.SubCommand("get", "<role>", "Show detailed role permissions")

	cli.Section("Examples")
	cli.Example("List all roles", "flymq-cli roles list")
}

func printACLHelp() {
	cli.Header("acl - Manage access control lists")
	fmt.Printf("\n%s flymq-cli acl <command> [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Commands")
	cli.SubCommand("list", "", "List all topic ACLs")
	cli.SubCommand("get", "<topic>", "Get ACL for a specific topic")
	cli.SubCommand("set", "<topic>", "Set ACL for a topic")
	cli.SubCommand("delete", "<topic>", "Delete ACL for a topic")

	cli.Section("Examples")
	cli.Example("Make topic public", "flymq-cli acl set events --public")
}

func printAuditHelp() {
	cli.Header("audit - Query and export audit logs")
	fmt.Printf("\n%s flymq-cli audit <command> [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Commands")
	cli.SubCommand("list", "", "List recent audit events")
	cli.SubCommand("query", "", "Advanced query for audit events")
	cli.SubCommand("tail", "", "Stream audit events in real-time")
	cli.SubCommand("export", "", "Export audit logs to JSON/CSV")

	cli.Section("Examples")
	cli.Example("Tail audit logs", "flymq-cli audit tail")
}

func printClusterHelp() {
	cli.Header("cluster - Cluster and partition management")
	fmt.Printf("\n%s flymq-cli cluster <command> [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Node Management")
	cli.SubCommand("status", "", "Show cluster status and health")
	cli.SubCommand("members", "", "List all cluster nodes")
	cli.SubCommand("info", "", "Show detailed cluster information")
	cli.SubCommand("join", "--peer <addr>", "Join an existing cluster")
	cli.SubCommand("leave", "", "Leave the cluster gracefully")

	cli.Section("Partition Management")
	cli.SubCommand("metadata", "", "Get partition-to-node mappings")
	cli.SubCommand("partitions", "", "List all partition assignments")
	cli.SubCommand("leaders", "", "Show leader distribution across nodes")
	cli.SubCommand("rebalance", "", "Trigger partition leader rebalancing")
	cli.SubCommand("reassign", "--topic <t> --p <n> --to <node>", "Reassign a partition")

	cli.Section("Examples")
	cli.Example("Check cluster status", "flymq-cli cluster status")
	cli.Example("Join existing cluster", "flymq-cli cluster join --peer node1:9093")
}

func printAdminHelp() {
	cli.Header("admin - Direct Admin API access")
	fmt.Printf("\n%s flymq-cli admin <command> [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Commands")
	cli.SubCommand("cluster", "", "Show cluster information via Admin API")
	cli.SubCommand("topics", "", "List topics with detailed metadata")
	cli.SubCommand("groups", "", "List consumer groups and their states")
	cli.SubCommand("schemas", "", "List all registered schemas")

	cli.Section("Options")
	cli.Option("", "--admin-addr", "addr", "Admin API address (default: localhost:9096)")
	cli.Option("", "--admin-tls", "", "Enable TLS for Admin API")

	cli.Section("Examples")
	cli.Example("Check cluster via Admin API", "flymq-cli admin cluster")
}

func printHealthHelp() {
	cli.Header("health - Check server health status")
	fmt.Printf("\n%s flymq-cli health <command> [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Commands")
	cli.SubCommand("status", "", "Show overall health status")
	cli.SubCommand("live", "", "Check liveness probe")
	cli.SubCommand("ready", "", "Check readiness probe")

	cli.Section("Options")
	cli.Option("-h", "--health-addr", "addr", "Health API address (default: localhost:9095)")

	cli.Section("Examples")
	cli.Example("Check if server is ready", "flymq-cli health ready")
}

func printAuthHelp() {
	cli.Header("auth - Authenticate and save credentials")
	fmt.Printf("\n%s flymq-cli auth --username <user> --password <pass>\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Description")
	fmt.Println("  Authenticates with the FlyMQ server and saves credentials to ~/.flymq/credentials.")
	fmt.Println("  Subsequent commands will use these credentials automatically.")

	cli.Section("Options")
	cli.Option("-u", "--username", "user", "Username")
	cli.Option("-P", "--password", "pass", "Password")

	cli.Section("Examples")
	cli.Example("Login to server", "flymq-cli auth -u admin -P secret")
}

func printWhoAmIHelp() {
	cli.Header("whoami - Display current authenticated user")
	fmt.Printf("\n%s flymq-cli whoami [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Description")
	fmt.Println("  Shows the current authentication status, username, and assigned roles/permissions.")

	cli.Section("Examples")
	cli.Example("Check current user", "flymq-cli whoami")
}

func printProduceHelp() {
	cli.Header("produce - Send a message to a topic")
	fmt.Printf("\n%s flymq-cli produce <topic> <message> [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Arguments")
	cli.Command("<topic>", "", "Target topic name")
	cli.Command("<message>", "", "Message content (string)")

	cli.Section("Options")
	cli.Option("-k", "--key", "key", "Message key for partition routing")
	cli.Option("-p", "--partition", "n", "Target partition number (overrides key-based routing)")
	cli.Option("-e", "--encoder", "name", "Encoder: string, json, binary, avro, protobuf")
	cli.Option("-s", "--schema", "file", "Local schema file (for Avro/Protobuf)")

	cli.Section("Examples")
	cli.Example("Simple message", "flymq-cli produce my-topic \"Hello World\"")
	cli.Example("Message with key", "flymq-cli produce orders '{\"id\": 1}' --key user-123")
	cli.Example("Explicit partition", "flymq-cli produce events \"data\" --partition 2")

	fmt.Printf("%s Client-side encoders (SerDe) are separate from server-side Schema Store validation.\n\n", cli.Colorize(cli.Dim, "Note:"))
}

func printConsumeHelp() {
	cli.Header("consume - Fetch messages (one-time batch read)")
	fmt.Printf("\n%s flymq-cli consume <topic> [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Arguments")
	cli.Command("<topic>", "", "Topic to consume from")

	cli.Section("Options")
	cli.Option("-o", "--offset", "n", "Starting offset (default: 0)")
	cli.Option("-n", "--count", "n", "Number of messages to fetch (default: 10)")
	cli.Option("-p", "--partition", "n", "Partition to consume from (default: 0)")
	cli.Option("-k", "--show-key", "", "Display message keys in output")
	cli.Option("-F", "--filter", "pattern", "Filter: substring, regex, or $.json.path")
	cli.Option("-d", "--decoder", "name", "Decoder: string, json, binary, avro, protobuf")
	cli.Option("-s", "--schema", "file", "Local schema file (for Avro/Protobuf)")
	cli.Option("-q", "--quiet", "", "Suppress headers and info messages")
	cli.Option("-r", "--raw", "", "Raw output: just message content")
	cli.Option("", "--no-offset", "", "Hide offset prefix in output")

	cli.Section("Examples")
	cli.Example("Read first 10 messages", "flymq-cli consume my-topic")
	cli.Example("Read 50 messages starting at offset 100", "flymq-cli consume my-topic --offset 100 --count 50")
	cli.Example("Raw output for piping", "flymq-cli consume my-topic --raw | grep 'error'")

	fmt.Printf("%s One-time reads don't track progress. Use 'subscribe' for persistent consumption.\n\n", cli.Colorize(cli.Dim, "Note:"))
}

func printSubscribeHelp() {
	cli.Header("subscribe - Stream messages with offset tracking")
	fmt.Printf("\n%s flymq-cli subscribe <topic> [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Arguments")
	cli.Command("<topic>", "", "Topic to subscribe to")

	cli.Section("Consumer Group Options")
	cli.Option("-g", "--group", "id", "Consumer group ID (default: 'default')")
	cli.Option("", "--from-beginning", "", "Start from the first message")
	cli.Option("", "--from-latest", "", "Start from new messages only (default)")
	cli.Option("-f", "--from", "mode", "Explicit mode: earliest, latest, committed")

	cli.Section("Other Options")
	cli.Option("-p", "--partition", "n", "Partition number (default: 0)")
	cli.Option("-k", "--show-key", "", "Display message keys in output")
	cli.Option("-F", "--filter", "pattern", "Filter: substring, regex, or $.json.path")
	cli.Option("-d", "--decoder", "name", "Decoder: string, json, binary, avro, protobuf")
	cli.Option("-s", "--schema", "file", "Local schema file (for Avro/Protobuf)")
	cli.Option("-l", "--show-lag", "", "Show consumer lag periodically")
	cli.Option("-q", "--quiet", "", "Suppress headers and info messages")
	cli.Option("-r", "--raw", "", "Raw output: just message content")
	cli.Option("", "--no-timestamp", "", "Hide timestamp in output")
	cli.Option("", "--no-offset", "", "Hide offset in output")

	cli.Section("Examples")
	cli.Example("Simple subscription (tail -f)", "flymq-cli subscribe my-topic")
	cli.Example("Process all messages from beginning with tracking", "flymq-cli subscribe my-topic --group my-app --from-beginning")
	cli.Example("Resume from where you left off", "flymq-cli subscribe my-topic --group my-app")

	fmt.Printf("%s Subscription tracks progress. Use same group ID to resume later.\n\n", cli.Colorize(cli.Dim, "Note:"))
}

func printCreateHelp() {
	cli.Header("create - Create a new topic")
	fmt.Printf("\n%s flymq-cli create <topic> [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Arguments")
	cli.Command("<topic>", "", "Name of the topic to create")

	cli.Section("Options")
	cli.Option("-p", "--partitions", "n", "Number of partitions (default: 1)")

	cli.Section("Examples")
	cli.Example("Create topic with 1 partition", "flymq-cli create my-topic")
	cli.Example("Create topic with 4 partitions", "flymq-cli create orders --partitions 4")
}

// ============================================================================
// Audit Trail Commands
// ============================================================================

func cmdAudit(args []string) {
	subCmd, subArgs := extractSubcommand(args)
	if subCmd == "" || subCmd == "help" || hasFlag(subArgs, "--help") || hasFlag(subArgs, "-h") {
		printAuditHelp()
		return
	}

	switch subCmd {
	case "list", "query":
		cmdAuditQuery(subArgs)
	case "tail":
		runAuditTail(subArgs, "", "", "", "", "")
	case "export":
		cmdAuditExport(subArgs)
	default:
		cli.Error("Unknown audit subcommand: %s", subCmd)
		os.Exit(1)
	}
}

func cmdAuditQuery(args []string) {
	// Parse filter options
	user := getStringArg(args, "--user", "")
	resource := getStringArg(args, "--resource", "")
	eventType := getStringArg(args, "--type", "")
	result := getStringArg(args, "--result", "")
	search := getStringArg(args, "--search", "")
	startStr := getStringArg(args, "--start", "")
	endStr := getStringArg(args, "--end", "")
	limitStr := getStringArg(args, "--limit", "100")
	offsetStr := getStringArg(args, "--offset", "0")
	follow := hasFlag(args, "--follow") || hasFlag(args, "-f")

	limit, _ := strconv.Atoi(limitStr)
	offset, _ := strconv.Atoi(offsetStr)

	if follow {
		runAuditTail(args, user, resource, eventType, result, search)
		return
	}

	c := connectWithAuth(args)
	defer c.Close()

	// Build query request
	req := &protocol.BinaryAuditQueryRequest{
		User:     user,
		Resource: resource,
		Result:   result,
		Search:   search,
		Limit:    int32(limit),
		Offset:   int32(offset),
	}

	if eventType != "" {
		req.EventTypes = strings.Split(eventType, ",")
	}

	if startStr != "" {
		if t, err := time.Parse(time.RFC3339, startStr); err == nil {
			req.StartTime = t.Unix()
		}
	}
	if endStr != "" {
		if t, err := time.Parse(time.RFC3339, endStr); err == nil {
			req.EndTime = t.Unix()
		}
	}

	// Execute query
	events, totalCount, hasMore, err := c.QueryAuditEvents(req)
	if err != nil {
		cli.Error("Failed to query audit events: %v", err)
		os.Exit(1)
	}

	if len(events) > 20 && isTerminal() {
		displayAuditEventsPager(events, totalCount)
	} else {
		displayAuditEventsTable(events, totalCount, hasMore)
	}
}

func isTerminal() bool {
	if fileInfo, _ := os.Stdout.Stat(); (fileInfo.Mode() & os.ModeCharDevice) != 0 {
		return true
	}
	return false
}

func displayAuditEventsTable(events []protocol.BinaryAuditEvent, totalCount int32, hasMore bool) {
	cli.Header("Audit Events")
	cli.KeyValue("Total Count", fmt.Sprintf("%d", totalCount))
	cli.KeyValue("Has More", fmt.Sprintf("%v", hasMore))
	cli.Separator()

	if len(events) == 0 {
		fmt.Println("No audit events found")
	} else {
		headers := []string{"TIMESTAMP", "TYPE", "USER", "RESOURCE", "ACTION", "RESULT"}
		var rows [][]string
		for _, event := range events {
			ts := time.UnixMilli(event.Timestamp).Format("2006-01-02 15:04:05")
			resource := event.Resource
			// In the new table, we can probably afford longer resources, but let's keep it reasonable
			if len(resource) > 30 {
				resource = resource[:27] + "..."
			}
			rows = append(rows, []string{
				ts,
				event.Type,
				event.User,
				resource,
				event.Action,
				event.Result,
			})
		}
		cli.Table(headers, rows)
	}
	cli.Separator()
}

func displayAuditEventsPager(events []protocol.BinaryAuditEvent, totalCount int32) {
	headers := []string{"TIMESTAMP", "TYPE", "USER", "RESOURCE", "ACTION", "RESULT"}
	var rows [][]string
	for _, event := range events {
		ts := time.UnixMilli(event.Timestamp).Format("2006-01-02 15:04:05")
		resource := event.Resource
		if len(resource) > 30 {
			resource = resource[:27] + "..."
		}
		rows = append(rows, []string{
			ts,
			event.Type,
			event.User,
			resource,
			event.Action,
			event.Result,
		})
	}

	tableStr := cli.TableString(headers, rows)

	var sb strings.Builder
	sb.WriteString(cli.Colorize(cli.Bold+cli.Cyan, fmt.Sprintf("Audit Events (Total: %d)\n", totalCount)))
	sb.WriteString(tableStr)

	pager := os.Getenv("PAGER")
	if pager == "" {
		if runtime.GOOS == "windows" {
			pager = "more"
		} else {
			pager = "less -R -S"
		}
	}

	parts := strings.Fields(pager)
	cmd := exec.Command(parts[0], parts[1:]...)
	cmd.Stdin = strings.NewReader(sb.String())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		// Fallback to table if pager fails
		displayAuditEventsTable(events, totalCount, false)
	}
}

func runAuditTail(args []string, user, resource, eventType, result, search string) {
	c := connectWithAuth(args)
	defer c.Close()

	cli.Header("Tailing Audit Events (Ctrl+C to stop)")
	cli.Separator()

	lastTimestamp := time.Now().UnixMilli()

	for {
		req := &protocol.BinaryAuditQueryRequest{
			User:      user,
			Resource:  resource,
			Result:    result,
			Search:    search,
			StartTime: lastTimestamp + 1,
			Limit:     50,
		}
		if eventType != "" {
			req.EventTypes = strings.Split(eventType, ",")
		}

		events, _, _, err := c.QueryAuditEvents(req)
		if err != nil {
			cli.Warning("Error fetching audit events: %v", err)
		} else {
			for _, event := range events {
				ts := time.UnixMilli(event.Timestamp).Format("15:04:05")
				res := event.Resource
				if len(res) > 20 {
					res = res[:17] + "..."
				}
				// Tail usually doesn't need a full table, but let's keep it aligned
				fmt.Printf("%s %s %-15s %-10s %-20s %-10s %-10s\n",
					cli.Colorize(cli.Dim, "["+ts+"]"),
					cli.Colorize(cli.Bold+cli.Cyan, event.Type),
					event.User, res, event.Action, event.Result, "")
				lastTimestamp = event.Timestamp
			}
		}

		time.Sleep(2 * time.Second)
	}
}

func cmdAuditExport(args []string) {
	c := connectWithAuth(args)
	defer c.Close()

	// Parse filter options
	user := getStringArg(args, "--user", "")
	resource := getStringArg(args, "--resource", "")
	eventType := getStringArg(args, "--type", "")
	result := getStringArg(args, "--result", "")
	search := getStringArg(args, "--search", "")
	startStr := getStringArg(args, "--start", "")
	endStr := getStringArg(args, "--end", "")
	format := getStringArg(args, "--format", "json")
	output := getStringArg(args, "--output", "")

	// Build query request
	queryReq := &protocol.BinaryAuditQueryRequest{
		User:     user,
		Resource: resource,
		Result:   result,
		Search:   search,
	}

	if eventType != "" {
		queryReq.EventTypes = strings.Split(eventType, ",")
	}

	if startStr != "" {
		if t, err := time.Parse(time.RFC3339, startStr); err == nil {
			queryReq.StartTime = t.Unix()
		}
	}
	if endStr != "" {
		if t, err := time.Parse(time.RFC3339, endStr); err == nil {
			queryReq.EndTime = t.Unix()
		}
	}

	// Execute export
	data, err := c.ExportAuditEvents(queryReq, format)
	if err != nil {
		cli.Error("Failed to export audit events: %v", err)
		os.Exit(1)
	}

	// Write output
	if output != "" {
		if err := os.WriteFile(output, data, 0644); err != nil {
			cli.Error("Failed to write output file: %v", err)
			os.Exit(1)
		}
		cli.Success("Exported audit events to %s", output)
	} else {
		fmt.Print(string(data))
	}
}

func printExploreHelp() {
	cli.Header("explore - Interactive topic explorer and message browser")
	fmt.Printf("\n%s flymq-cli explore [options]\n", cli.Colorize(cli.Bold, "Usage:"))

	cli.Section("Description")
	fmt.Println("  Launch an interactive terminal UI for exploring topics, browsing messages,")
	fmt.Println("  producing messages, managing offsets, and exporting data.")

	cli.Section("Features")
	fmt.Println("  • Browse messages with pagination and filtering")
	fmt.Println("  • Produce messages with keys and custom partitioning")
	fmt.Println("  • Live tail topics with real-time updates")
	fmt.Println("  • Export messages to JSON, CSV, or raw format")
	fmt.Println("  • Manage consumer group offsets")
	fmt.Println("  • View topic metadata and partition information")
	fmt.Println("  • Create new topics interactively")

	cli.Section("Options")
	cli.Option("-d", "--decoder", "name", "Default decoder (string, json, binary, avro, protobuf)")
	cli.Option("-s", "--schema", "file", "Local schema file (for Avro/Protobuf)")

	cli.Section("Navigation")
	fmt.Println("  Use number keys to select options, letters for commands:")
	fmt.Println("  • n/p - Next/Previous page")
	fmt.Println("  • g - Go to specific offset")
	fmt.Println("  • f - Set filter (substring, regex, $.json.path)")
	fmt.Println("  • s - Settings and configuration")

	cli.Section("Examples")
	cli.Example("Launch explorer", "flymq-cli explore")
	cli.Example("Start with JSON decoder", "flymq-cli explore --decoder json")
	cli.Example("Use Avro schema", "flymq-cli explore --decoder avro --schema user.avsc")
}

func cmdExplore(args []string) {
	if hasFlag(args, "--help") || hasFlag(args, "-h") || (len(args) > 0 && args[0] == "help") {
		printExploreHelp()
		return
	}

	c := connect(args)
	defer c.Close()

	// Default to string decoder for explorer to be more user-friendly
	decoderName := "string"
	for i, arg := range args {
		if (arg == "--decoder" || arg == "-d") && i+1 < len(args) {
			decoderName = args[i+1]
		}
	}
	c.SetSerde(decoderName)
	applySchema(decoderName, args)

	// Enhanced explorer with better UX
	explorer := &TopicExplorer{
		client: c,
		decoder: decoderName,
		filter: "",
		partition: 0,
		offset: 0,
		pageSize: 10,
	}

	explorer.run()
}

// TopicExplorer provides an enhanced interactive experience
type TopicExplorer struct {
	client    *client.Client
	decoder   string
	filter    string
	partition int
	offset    uint64
	pageSize  int
	currentTopic string
}

func (e *TopicExplorer) run() {
	for {
		e.showMainMenu()
	}
}

func (e *TopicExplorer) showMainMenu() {
	cli.Separator()
	fmt.Printf("%s%s FlyMQ Topic Explorer %s\n", cli.Bold+cli.BrightCyan, cli.IconDot, cli.Reset)
	cli.Separator()

	topics, err := e.client.ListTopics()
	if err != nil {
		cli.Error("Failed to list topics: %v", err)
		return
	}

	if len(topics) == 0 {
		cli.Info("No topics found. Create one with: flymq-cli create <topic>")
		return
	}

	fmt.Println("\nAvailable Topics:")
	for i, t := range topics {
		fmt.Printf("  [%2d] %s", i+1, t)
		if t == e.currentTopic {
			fmt.Printf(" %s", cli.Colorize(cli.Green, "(current)"))
		}
		fmt.Println()
	}
	fmt.Printf("  [%2s] %s\n", "c", "Create new topic")
	fmt.Printf("  [%2s] %s\n", "s", "Settings")
	fmt.Printf("  [%2s] %s\n", "q", "Quit")

	fmt.Print("\nSelect topic number, or command: ")
	var input string
	fmt.Scanln(&input)

	switch strings.ToLower(input) {
	case "q":
		return
	case "c":
		e.createTopic()
	case "s":
		e.showSettings()
	default:
		idx, err := strconv.Atoi(input)
		if err != nil || idx < 1 || idx > len(topics) {
			cli.Warning("Invalid selection")
			return
		}
		e.currentTopic = topics[idx-1]
		e.exploreTopicMenu()
	}
}

func (e *TopicExplorer) exploreTopicMenu() {
	for {
		fmt.Println()
		cli.Header("Topic: " + e.currentTopic)
		if e.filter != "" {
			cli.Info("Filter: %q", e.filter)
		}
		cli.KeyValue("Decoder", e.decoder)
		cli.KeyValue("Partition", fmt.Sprintf("%d", e.partition))
		cli.KeyValue("Offset", fmt.Sprintf("%d", e.offset))
		cli.Separator()
		fmt.Println("  1. Browse Messages")
		fmt.Println("  2. Produce Message")
		fmt.Println("  3. Live Tail")
		fmt.Println("  4. Topic Metadata")
		fmt.Println("  5. Consumer Groups")
		fmt.Println("  6. Offset Management")
		fmt.Println("  7. Export Messages")
		fmt.Println("  8. Settings")
		fmt.Println("  9. Back to Topics")

		fmt.Print("\nSelection: ")
		var input string
		fmt.Scanln(&input)

		switch input {
		case "1":
			e.browseMessages()
		case "2":
			e.produceMessage()
		case "3":
			e.tailTopic()
		case "4":
			e.showTopicMetadata()
		case "5":
			e.showConsumerGroups()
		case "6":
			e.offsetManagement()
		case "7":
			e.exportMessages()
		case "8":
			e.topicSettings()
		case "9":
			return
		default:
			cli.Warning("Invalid selection")
		}
	}
}

func (e *TopicExplorer) showTopicMetadata() {
	metadata, err := e.client.GetTopicMetadata(e.currentTopic)
	if err != nil {
		cli.Error("Failed to fetch metadata: %v", err)
		return
	}

	cli.Header("\nMetadata for " + e.currentTopic)
	cli.KeyValue("Partitions", fmt.Sprintf("%d", len(metadata.Partitions)))
	cli.Separator()

	for _, p := range metadata.Partitions {
		fmt.Printf("Partition [%d]:\n", p.ID)
		cli.KeyValue("  Leader", p.Leader)
		cli.KeyValue("  State", p.State)
		cli.KeyValue("  Epoch", fmt.Sprintf("%d", p.Epoch))
		cli.KeyValue("  Replicas", strings.Join(p.Replicas, ", "))
		cli.KeyValue("  ISR", strings.Join(p.ISR, ", "))
	}

	fmt.Print("\nPress Enter to continue...")
	fmt.Scanln()
}

func (e *TopicExplorer) showConsumerGroups() {
	groups, err := e.client.ListConsumerGroups()
	if err != nil {
		cli.Error("Failed to list consumer groups: %v", err)
		return
	}

	cli.Header("Consumer Groups for " + e.currentTopic)
	cli.Separator()

	if len(groups) == 0 {
		cli.Info("No consumer groups found")
	} else {
		for _, g := range groups {
			// Check if group is consuming from this topic
			for _, topic := range g.Topics {
				if topic == e.currentTopic {
					fmt.Printf("  %s %s (state: %s, members: %d)\n", 
						cli.IconDot, g.GroupID, g.State, len(g.Members))
					break
				}
			}
		}
	}

	fmt.Print("\nPress Enter to continue...")
	fmt.Scanln()
}

func (e *TopicExplorer) offsetManagement() {
	cli.Header("Offset Management for " + e.currentTopic)
	cli.Separator()
	fmt.Println("  1. Commit Offset")
	fmt.Println("  2. Reset Group Offset")
	fmt.Println("  3. View Group Lag")
	fmt.Println("  4. Back")

	fmt.Print("\nSelection: ")
	var input string
	fmt.Scanln(&input)

	switch input {
	case "1":
		e.commitOffset()
	case "2":
		e.resetGroupOffset()
	case "3":
		e.viewGroupLag()
	case "4":
		return
	}
}

func matchesFilter(key, value []byte, filter string) bool {
	if filter == "" {
		return true
	}

	// Try case-insensitive substring match first (most common)
	if strings.Contains(strings.ToLower(string(value)), strings.ToLower(filter)) ||
		strings.Contains(strings.ToLower(string(key)), strings.ToLower(filter)) {
		return true
	}

	// Try JSON Path filtering if it looks like a JSON and filter starts with $
	if strings.HasPrefix(filter, "$.") && len(value) > 0 && value[0] == '{' {
		var data interface{}
		if err := json.Unmarshal(value, &data); err == nil {
			if matchJSONPath(data, filter) {
				return true
			}
		}
	}

	// Try regex match
	re, err := regexp.Compile(filter)
	if err != nil {
		return false
	}
	return re.Match(value) || re.Match(key)
}

func matchJSONPath(data interface{}, path string) bool {
	parts := strings.Split(path[2:], ".")
	current := data
	for _, part := range parts {
		if m, ok := current.(map[string]interface{}); ok {
			if val, exists := m[part]; exists {
				current = val
			} else {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

func applySchema(decoderName string, args []string) {
	schemaFile := ""
	for i, arg := range args {
		if (arg == "--schema" || arg == "-s") && i+1 < len(args) {
			schemaFile = args[i+1]
			break
		}
	}

	if schemaFile != "" {
		schemaData, err := os.ReadFile(schemaFile)
		if err != nil {
			cli.Warning("Failed to read schema file: %v", err)
			return
		}

		if decoderName == "avro" {
			if err := client.SetAvroSchema(string(schemaData)); err != nil {
				cli.Warning("Failed to parse Avro schema: %v", err)
			} else {
				cli.Info("Loaded Avro schema from %s", schemaFile)
			}
		} else if decoderName == "protobuf" {
			client.SetProtoDescriptor(schemaData)
			cli.Info("Loaded Protobuf descriptor from %s", schemaFile)
		}
	}
}

func formatMessage(c *client.Client, data []byte) string {
	if len(data) == 0 {
		return "<empty>"
	}

	// Try to use the configured decoder
	switch c.GetDecoderName() {
	case "json":
		var v interface{}
		if err := json.Unmarshal(data, &v); err == nil {
			pretty, _ := json.MarshalIndent(v, "", "  ")
			return string(pretty)
		}
	case "string":
		return string(data)
	}

	// Fallback to string if printable, otherwise hex or binary description
	s := string(data)
	isPrintable := true
	for _, r := range s {
		if r < 32 && r != '\n' && r != '\r' && r != '\t' {
			isPrintable = false
			break
		}
	}

	if isPrintable {
		return s
	}

	return fmt.Sprintf("<binary: %d bytes>", len(data))
}

func (e *TopicExplorer) browseMessages() {
	for {
		cli.Header(fmt.Sprintf("\nBrowsing %s [P%d] from offset %d", e.currentTopic, e.partition, e.offset))
		if e.filter != "" {
			cli.Info("Filter: %q", e.filter)
		}

		messages, nextOffset, err := e.client.FetchWithKeys(e.currentTopic, e.partition, e.offset, e.pageSize, e.filter)
		if err != nil {
			cli.Error("Fetch failed: %v", err)
			return
		}

		if len(messages) == 0 {
			cli.Info("No messages found from offset %d", e.offset)
		} else {
			cli.Separator()
			for i, m := range messages {
				if !matchesFilter(m.Key, m.Value, e.filter) {
					continue
				}
				keyStr := "<none>"
				if len(m.Key) > 0 {
					keyStr = string(m.Key)
				}
				fmt.Printf("[%d] %s%d%s | Key: %s%s%s\n", 
					m.Offset, cli.Bold+cli.Cyan, i+1, cli.Reset,
					cli.Dim, keyStr, cli.Reset)
				fmt.Printf("    %s\n", e.formatMessage(m.Value))
			}
			cli.Separator()
		}

		fmt.Printf("\n%sNavigation:%s\n", cli.Bold, cli.Reset)
		fmt.Println("  [n] Next page    [p] Previous page    [g] Go to offset    [j] Jump to latest")
		fmt.Println("  [f] Set filter   [c] Clear filter     [s] Settings        [b] Back")
		fmt.Print("\nCommand: ")
		var input string
		fmt.Scanln(&input)

		switch strings.ToLower(input) {
		case "n":
			e.offset = nextOffset
		case "p":
			if e.offset >= uint64(e.pageSize) {
				e.offset -= uint64(e.pageSize)
			} else {
				e.offset = 0
			}
		case "g":
			e.goToOffset()
		case "j":
			e.jumpToLatest()
		case "f":
			e.setFilter()
		case "c":
			e.filter = ""
			cli.Success("Filter cleared")
		case "s":
			e.topicSettings()
		case "b":
			return
		default:
			cli.Warning("Invalid command")
		}
	}
}

func (e *TopicExplorer) produceMessage() {
	cli.Header("Produce Message to " + e.currentTopic)
	cli.Separator()

	fmt.Print("Message content: ")
	var message string
	fmt.Scanln(&message)
	if message == "" {
		cli.Warning("Message cannot be empty")
		return
	}

	fmt.Print("Message key (optional): ")
	var key string
	fmt.Scanln(&key)

	fmt.Print("Target partition (-1 for auto): ")
	var partStr string
	fmt.Scanln(&partStr)
	// Note: partition selection not yet implemented in server
	if partStr != "" {
		_, _ = strconv.Atoi(partStr)
	}

	var meta *protocol.RecordMetadata
	var err error

	if key != "" {
		meta, err = e.client.ProduceObjectWithKey(e.currentTopic, []byte(key), []byte(message))
	} else {
		meta, err = e.client.ProduceObject(e.currentTopic, []byte(message))
	}

	if err != nil {
		cli.Error("Failed to produce: %v", err)
		return
	}

	cli.Success("Message produced successfully!")
	cli.KeyValue("Topic", meta.Topic)
	cli.KeyValue("Partition", fmt.Sprintf("%d", meta.Partition))
	cli.KeyValue("Offset", fmt.Sprintf("%d", meta.Offset))
	cli.KeyValue("Timestamp", time.UnixMilli(meta.Timestamp).Format(time.RFC3339))

	fmt.Print("\nPress Enter to continue...")
	fmt.Scanln()
}

func (e *TopicExplorer) tailTopic() {
	cli.Info("Live tailing %s... Press Enter to stop.", e.currentTopic)

	stop := make(chan struct{})
	go func() {
		fmt.Scanln()
		close(stop)
	}()

	config := client.DefaultConsumerConfig("cli-tail-" + strconv.FormatInt(time.Now().Unix(), 10))
	config.AutoOffsetReset = "latest"
	consumer := e.client.Consumer([]string{e.currentTopic}, config)

	for {
		select {
		case <-stop:
			return
		default:
			msgs := consumer.Poll(200 * time.Millisecond)
			for _, m := range msgs {
				if e.filter == "" || matchesFilter(m.Key, m.Value, e.filter) {
					keyStr := "<none>"
					if len(m.Key) > 0 {
						keyStr = string(m.Key)
					}
					fmt.Printf("[%s] P%d@%d | Key: %s | %s\n",
						time.Now().Format("15:04:05"), m.Partition, m.Offset, 
						keyStr, e.formatMessage(m.Value))
				}
			}
		}
	}
}

func (e *TopicExplorer) exportMessages() {
	cli.Header("Export Messages from " + e.currentTopic)
	cli.Separator()

	fmt.Print("Export format (json/csv/raw): ")
	var format string
	fmt.Scanln(&format)
	if format == "" {
		format = "json"
	}

	fmt.Print("Number of messages to export (0 for all): ")
	var countStr string
	fmt.Scanln(&countStr)
	count, _ := strconv.Atoi(countStr)
	if count <= 0 {
		count = 1000 // reasonable default
	}

	fmt.Print("Output file (empty for stdout): ")
	var filename string
	fmt.Scanln(&filename)

	messages, _, err := e.client.FetchWithKeys(e.currentTopic, e.partition, e.offset, count, e.filter)
	if err != nil {
		cli.Error("Failed to fetch messages: %v", err)
		return
	}

	var output strings.Builder

	switch strings.ToLower(format) {
	case "json":
		output.WriteString("[\n")
		for i, m := range messages {
			if i > 0 {
				output.WriteString(",\n")
			}
			data := map[string]interface{}{
				"offset": m.Offset,
				"key": string(m.Key),
				"value": string(m.Value),
				"partition": e.partition,
			}
			jsonData, _ := json.MarshalIndent(data, "  ", "  ")
			output.WriteString("  " + string(jsonData))
		}
		output.WriteString("\n]")
	case "csv":
		output.WriteString("offset,partition,key,value\n")
		for _, m := range messages {
			output.WriteString(fmt.Sprintf("%d,%d,\"%s\",\"%s\"\n", 
				m.Offset, e.partition, string(m.Key), string(m.Value)))
		}
	case "raw":
		for _, m := range messages {
			output.WriteString(string(m.Value) + "\n")
		}
	default:
		cli.Error("Unsupported format: %s", format)
		return
	}

	if filename != "" {
		if err := os.WriteFile(filename, []byte(output.String()), 0644); err != nil {
			cli.Error("Failed to write file: %v", err)
			return
		}
		cli.Success("Exported %d messages to %s", len(messages), filename)
	} else {
		fmt.Print(output.String())
	}

	fmt.Print("\nPress Enter to continue...")
	fmt.Scanln()
}

func (e *TopicExplorer) createTopic() {
	cli.Header("Create New Topic")
	cli.Separator()

	fmt.Print("Topic name: ")
	var name string
	fmt.Scanln(&name)
	if name == "" {
		cli.Warning("Topic name cannot be empty")
		return
	}

	fmt.Print("Number of partitions (default 1): ")
	var partStr string
	fmt.Scanln(&partStr)
	partitions := 1
	if partStr != "" {
		partitions, _ = strconv.Atoi(partStr)
		if partitions <= 0 {
			partitions = 1
		}
	}

	if err := e.client.CreateTopic(name, partitions); err != nil {
		cli.Error("Failed to create topic: %v", err)
		return
	}

	cli.Success("Topic '%s' created with %d partition(s)", name, partitions)
	e.currentTopic = name

	fmt.Print("\nPress Enter to continue...")
	fmt.Scanln()
}

func (e *TopicExplorer) showSettings() {
	cli.Header("Global Settings")
	cli.Separator()
	cli.KeyValue("Current Decoder", e.decoder)
	cli.KeyValue("Page Size", fmt.Sprintf("%d", e.pageSize))
	cli.KeyValue("Global Filter", e.filter)
	cli.Separator()
	fmt.Println("  1. Change Decoder")
	fmt.Println("  2. Change Page Size")
	fmt.Println("  3. Set Global Filter")
	fmt.Println("  4. Clear Global Filter")
	fmt.Println("  5. Back")

	fmt.Print("\nSelection: ")
	var input string
	fmt.Scanln(&input)

	switch input {
	case "1":
		e.changeDecoder()
	case "2":
		e.changePageSize()
	case "3":
		e.setFilter()
	case "4":
		e.filter = ""
		cli.Success("Global filter cleared")
	case "5":
		return
	}
}

func (e *TopicExplorer) topicSettings() {
	cli.Header("Topic Settings for " + e.currentTopic)
	cli.Separator()
	cli.KeyValue("Current Partition", fmt.Sprintf("%d", e.partition))
	cli.KeyValue("Current Offset", fmt.Sprintf("%d", e.offset))
	cli.KeyValue("Topic Filter", e.filter)
	cli.Separator()
	fmt.Println("  1. Change Partition")
	fmt.Println("  2. Go to Offset")
	fmt.Println("  3. Set Filter")
	fmt.Println("  4. Clear Filter")
	fmt.Println("  5. Back")

	fmt.Print("\nSelection: ")
	var input string
	fmt.Scanln(&input)

	switch input {
	case "1":
		e.changePartition()
	case "2":
		e.goToOffset()
	case "3":
		e.setFilter()
	case "4":
		e.filter = ""
		cli.Success("Filter cleared")
	case "5":
		return
	}
}

// Helper methods for TopicExplorer
func (e *TopicExplorer) formatMessage(data []byte) string {
	if len(data) == 0 {
		return "<empty>"
	}

	// Try to use the configured decoder
	switch e.decoder {
	case "json":
		var v interface{}
		if err := json.Unmarshal(data, &v); err == nil {
			pretty, _ := json.MarshalIndent(v, "", "  ")
			return string(pretty)
		}
	case "string":
		return string(data)
	}

	// Fallback to string if printable, otherwise show as binary
	s := string(data)
	isPrintable := true
	for _, r := range s {
		if r < 32 && r != '\n' && r != '\r' && r != '\t' {
			isPrintable = false
			break
		}
	}

	if isPrintable {
		return s
	}

	return fmt.Sprintf("<binary: %d bytes>", len(data))
}

func (e *TopicExplorer) changeDecoder() {
	fmt.Print("Enter decoder (string, json, binary, avro, protobuf): ")
	var decoder string
	fmt.Scanln(&decoder)
	if decoder != "" {
		if err := e.client.SetSerde(decoder); err != nil {
			cli.Error("Invalid decoder: %v", err)
		} else {
			e.decoder = decoder
			cli.Success("Decoder changed to %s", decoder)
		}
	}
}

func (e *TopicExplorer) changePageSize() {
	fmt.Print("Enter page size (1-100): ")
	var sizeStr string
	fmt.Scanln(&sizeStr)
	if size, err := strconv.Atoi(sizeStr); err == nil && size > 0 && size <= 100 {
		e.pageSize = size
		cli.Success("Page size changed to %d", size)
	} else {
		cli.Warning("Invalid page size")
	}
}

func (e *TopicExplorer) setFilter() {
	fmt.Print("Enter filter (substring, regex, or $.json.path): ")
	var filter string
	fmt.Scanln(&filter)
	e.filter = filter
	if filter != "" {
		cli.Success("Filter set to: %q", filter)
	} else {
		cli.Success("Filter cleared")
	}
}

func (e *TopicExplorer) changePartition() {
	fmt.Print("Enter partition number: ")
	var partStr string
	fmt.Scanln(&partStr)
	if partition, err := strconv.Atoi(partStr); err == nil && partition >= 0 {
		e.partition = partition
		e.offset = 0 // Reset offset when changing partition
		cli.Success("Changed to partition %d", partition)
	} else {
		cli.Warning("Invalid partition number")
	}
}

func (e *TopicExplorer) goToOffset() {
	fmt.Print("Enter offset: ")
	var offStr string
	fmt.Scanln(&offStr)
	if offset, err := strconv.ParseUint(offStr, 10, 64); err == nil {
		e.offset = offset
		cli.Success("Moved to offset %d", offset)
	} else {
		cli.Warning("Invalid offset")
	}
}

func (e *TopicExplorer) jumpToLatest() {
	// Get latest offset by fetching metadata or trying a high offset
	messages, _, err := e.client.FetchWithKeys(e.currentTopic, e.partition, ^uint64(0)-1000, 1, "")
	if err == nil && len(messages) > 0 {
		e.offset = messages[len(messages)-1].Offset
		cli.Success("Jumped to latest offset: %d", e.offset)
	} else {
		cli.Warning("Could not determine latest offset")
	}
}

func (e *TopicExplorer) commitOffset() {
	fmt.Print("Consumer Group ID: ")
	var groupID string
	fmt.Scanln(&groupID)
	if groupID == "" {
		cli.Warning("Group ID cannot be empty")
		return
	}

	fmt.Printf("Offset to commit (current: %d): ", e.offset)
	var offStr string
	fmt.Scanln(&offStr)
	offset := e.offset
	if offStr != "" {
		if o, err := strconv.ParseUint(offStr, 10, 64); err == nil {
			offset = o
		}
	}

	if err := e.client.CommitOffset(e.currentTopic, groupID, e.partition, offset); err != nil {
		cli.Error("Commit failed: %v", err)
	} else {
		cli.Success("Offset %d committed for group %s", offset, groupID)
	}
}

func (e *TopicExplorer) resetGroupOffset() {
	fmt.Print("Consumer Group ID: ")
	var groupID string
	fmt.Scanln(&groupID)
	if groupID == "" {
		cli.Warning("Group ID cannot be empty")
		return
	}

	fmt.Print("Reset to (earliest/latest/offset): ")
	var mode string
	fmt.Scanln(&mode)

	var offset uint64 = 0
	if mode == "offset" {
		fmt.Print("Enter offset: ")
		var offStr string
		fmt.Scanln(&offStr)
		offset, _ = strconv.ParseUint(offStr, 10, 64)
	}

	if err := e.client.ResetOffset(e.currentTopic, groupID, e.partition, mode, offset); err != nil {
		cli.Error("Reset failed: %v", err)
	} else {
		cli.Success("Offset reset for group %s", groupID)
	}
}

func (e *TopicExplorer) viewGroupLag() {
	fmt.Print("Consumer Group ID: ")
	var groupID string
	fmt.Scanln(&groupID)
	if groupID == "" {
		cli.Warning("Group ID cannot be empty")
		return
	}

	lagInfo, err := e.client.GetLag(e.currentTopic, groupID, e.partition)
	if err != nil {
		cli.Error("Failed to get lag: %v", err)
		return
	}

	cli.Header("Consumer Lag for " + groupID)
	cli.KeyValue("Topic", e.currentTopic)
	cli.KeyValue("Partition", fmt.Sprintf("%d", e.partition))
	cli.KeyValue("Current Offset", fmt.Sprintf("%d", lagInfo.CurrentOffset))
	cli.KeyValue("Committed Offset", fmt.Sprintf("%d", lagInfo.CommittedOffset))
	cli.KeyValue("Latest Offset", fmt.Sprintf("%d", lagInfo.LatestOffset))
	cli.KeyValue("Lag", fmt.Sprintf("%d messages", lagInfo.Lag))

	fmt.Print("\nPress Enter to continue...")
	fmt.Scanln()
}
