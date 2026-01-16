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
	groups           Manage consumer groups
	cluster          View cluster information
	health           Check server health
	benchmark        Run performance benchmarks

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
*/
package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"
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
		cmdTransaction(args)
	case "health":
		cmdHealth(args)
	case "admin":
		cmdAdmin(args)
	case "cluster":
		cmdCluster(args)
	case "groups":
		cmdGroups(args)
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
		"--admin-addr": true,
		"--admin-user": true,
		"--admin-pass": true,
		"--admin-ca-cert": true,
		// Health endpoint options
		"--health-addr": true,
		"--health-ca-cert": true,
	}

	// Known global option flags (boolean)
	globalBool := map[string]bool{
		// Binary protocol TLS
		"-T": true, "--tls": true,
		"-k": true, "--insecure": true, "--tls-insecure": true,
		// Admin API TLS
		"--admin-tls": true,
		"--admin-insecure": true,
		// Health endpoint TLS
		"--health-tls": true,
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
		"--partition": true,
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

	fmt.Println(cli.Bold + "Usage:" + cli.Reset + " flymq-cli [options] <command> [args]")
	fmt.Println()

	// Commands
	fmt.Println(cli.Bold + cli.Cyan + "COMMANDS" + cli.Reset)
	fmt.Println()

	fmt.Println(cli.Dim + "  Messaging" + cli.Reset)
	fmt.Println("    " + cli.Green + "produce" + cli.Reset + " <topic> <msg>    Send a message")
	fmt.Println("    " + cli.Green + "consume" + cli.Reset + " <topic>          Fetch messages (batch)")
	fmt.Println("    " + cli.Green + "subscribe" + cli.Reset + " <topic>        Stream messages (like tail -f)")
	fmt.Println()

	fmt.Println(cli.Dim + "  Topics" + cli.Reset)
	fmt.Println("    " + cli.Green + "create" + cli.Reset + " <topic>           Create a topic")
	fmt.Println("    " + cli.Green + "delete" + cli.Reset + " <topic>           Delete a topic")
	fmt.Println("    " + cli.Green + "topics" + cli.Reset + "                   List topics")
	fmt.Println("    " + cli.Green + "info" + cli.Reset + " <topic>             Topic details")
	fmt.Println()

	fmt.Println(cli.Dim + "  Consumer Groups" + cli.Reset)
	fmt.Println("    " + cli.Green + "groups list" + cli.Reset + "              List groups")
	fmt.Println("    " + cli.Green + "groups describe" + cli.Reset + " <grp>    Group details")
	fmt.Println("    " + cli.Green + "groups lag" + cli.Reset + " <grp>         Consumer lag")
	fmt.Println()

	fmt.Println(cli.Dim + "  Advanced" + cli.Reset)
	fmt.Println("    " + cli.Yellow + "produce-delayed" + cli.Reset + "          Delayed delivery")
	fmt.Println("    " + cli.Yellow + "produce-ttl" + cli.Reset + "              Message with TTL")
	fmt.Println("    " + cli.Yellow + "txn" + cli.Reset + "                      Transactional produce")
	fmt.Println("    " + cli.Yellow + "schema" + cli.Reset + "                   Schema registry")
	fmt.Println("    " + cli.Yellow + "dlq" + cli.Reset + "                      Dead letter queue")
	fmt.Println()

	fmt.Println(cli.Dim + "  Cluster & Admin" + cli.Reset)
	fmt.Println("    " + cli.Cyan + "cluster" + cli.Reset + "                  Cluster operations")
	fmt.Println("    " + cli.Cyan + "admin" + cli.Reset + "                    Admin API")
	fmt.Println("    " + cli.Cyan + "health" + cli.Reset + "                   Health checks")
	fmt.Println()

	fmt.Println(cli.Dim + "  Security" + cli.Reset)
	fmt.Println("    " + cli.Magenta + "auth" + cli.Reset + "                     Authenticate")
	fmt.Println("    " + cli.Magenta + "whoami" + cli.Reset + "                   Current user")
	fmt.Println("    " + cli.Magenta + "users" + cli.Reset + "/" + cli.Magenta + "roles" + cli.Reset + "/" + cli.Magenta + "acl" + cli.Reset + "        Access control")
	fmt.Println()

	// Options
	fmt.Println(cli.Bold + cli.Cyan + "OPTIONS" + cli.Reset)
	fmt.Println()
	fmt.Println("    " + cli.Dim + "-a, --addr" + cli.Reset + " <host:port>   Server address (default: localhost:9092)")
	fmt.Println("    " + cli.Dim + "-u, --username" + cli.Reset + " <user>    Username")
	fmt.Println("    " + cli.Dim + "-P, --password" + cli.Reset + " <pass>    Password")
	fmt.Println("    " + cli.Dim + "-T, --tls" + cli.Reset + "                Enable TLS")
	fmt.Println("    " + cli.Dim + "-k, --insecure" + cli.Reset + "           Skip TLS verification")
	fmt.Println()

	// Examples
	fmt.Println(cli.Bold + cli.Cyan + "EXAMPLES" + cli.Reset)
	fmt.Println()
	fmt.Println(cli.Dim + "    # Quick start" + cli.Reset)
	fmt.Println("    flymq-cli create my-topic")
	fmt.Println("    flymq-cli produce my-topic \"Hello!\"")
	fmt.Println("    flymq-cli subscribe my-topic")
	fmt.Println()
	fmt.Println(cli.Dim + "    # With authentication" + cli.Reset)
	fmt.Println("    flymq-cli --addr server:9092 -u admin -P secret topics")
	fmt.Println()

	fmt.Println(cli.Green + "Tip:" + cli.Reset + " Run 'flymq-cli <command> --help' for command details.")
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
	return ""
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
	return ""
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
	partition := -1 // -1 means auto-select

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
				partition = v
			}
			i++
		case strings.HasPrefix(args[i], "--partition="):
			if v, err := strconv.Atoi(strings.TrimPrefix(args[i], "--partition=")); err == nil {
				partition = v
			}
		}
	}

	c := connect(args)
	defer c.Close()

	// Use ProduceWithMetadata to get full RecordMetadata
	// Note: partition flag is parsed but currently the server handles partitioning
	// The partition will be returned in RecordMetadata
	_ = partition // Reserved for future use when explicit partition routing is needed

	var meta *protocol.RecordMetadata
	var err error
	if len(key) > 0 {
		meta, err = c.ProduceWithKey(topic, key, []byte(message))
	} else {
		meta, err = c.Produce(topic, []byte(message))
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
		}
	}

	c := connect(args)
	defer c.Close()

	messages, nextOffset, err := c.FetchWithKeys(topic, partition, offset, count)
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
		cli.Separator()
	}
	for _, msg := range messages {
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
		messages, nextOffset, err := c.FetchWithKeys(topic, partition, offset, 10)
		if err != nil {
			if !quiet {
				cli.Error("Fetch error: %v", err)
			}
			time.Sleep(time.Second)
			continue
		}

		for _, msg := range messages {
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

func cmdProduceDelayed(args []string) {
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
	if subCmd == "" {
		cli.Error("Usage: flymq-cli dlq <list|replay|purge|stats> <topic> [options]")
		fmt.Println()
		fmt.Println("Subcommands:")
		fmt.Println("  list <topic>    List messages in DLQ")
		fmt.Println("  replay <topic>  Replay messages from DLQ")
		fmt.Println("  purge <topic>   Purge all messages from DLQ")
		fmt.Println("  stats <topic>   Show DLQ statistics")
		os.Exit(1)
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
	default:
		cli.Error("Unknown DLQ subcommand: %s", subCmd)
		os.Exit(1)
	}
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
	if subCmd == "" {
		cli.Error("Usage: flymq-cli schema <register|list|validate|delete|produce> [options]")
		fmt.Println()
		fmt.Println("Subcommands:")
		fmt.Println("  register <name> <type> <schema>  Register a new schema")
		fmt.Println("  list                             List all registered schemas")
		fmt.Println("  validate <name> <message>        Validate a message against a schema")
		fmt.Println("  delete <name>                    Delete a schema")
		fmt.Println("  produce <topic> <schema> <msg>   Produce with schema validation")
		os.Exit(1)
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

func cmdTransaction(args []string) {
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
	if subCmd == "" {
		// Default to showing overall health
		cmdHealthStatus(args)
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
		fmt.Println()
		fmt.Println("Subcommands:")
		fmt.Println("  status    Show overall health status")
		fmt.Println("  live      Check liveness probe")
		fmt.Println("  ready     Check readiness probe")
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
	if subCmd == "" {
		cli.Error("Usage: flymq-cli admin <cluster|topics|groups|schemas> [options]")
		fmt.Println()
		fmt.Println("Subcommands:")
		fmt.Println("  cluster   Show cluster information")
		fmt.Println("  topics    List topics via Admin API")
		fmt.Println("  groups    List consumer groups")
		fmt.Println("  schemas   List schemas via Admin API")
		os.Exit(1)
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

// ============================================================================
// Cluster Commands
// ============================================================================

func cmdCluster(args []string) {
	subCmd, subArgs := extractSubcommand(args)
	if subCmd == "" {
		cli.Error("Usage: flymq-cli cluster <status|members|info|join|leave> [options]")
		fmt.Println()
		fmt.Println("Subcommands:")
		fmt.Println("  status    Show cluster status and health")
		fmt.Println("  members   List all cluster members")
		fmt.Println("  info      Show detailed cluster information")
		fmt.Println("  join      Join a cluster (requires --peer)")
		fmt.Println("  leave     Leave the cluster gracefully")
		os.Exit(1)
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
// Consumer Group Commands
// ============================================================================

func cmdGroups(args []string) {
	subCmd, subArgs := extractSubcommand(args)
	if subCmd == "" {
		cli.Error("Usage: flymq-cli groups <list|describe|reset-offsets|lag|delete> [options]")
		fmt.Println()
		fmt.Println("Subcommands:")
		fmt.Println("  list                          List all consumer groups")
		fmt.Println("  describe <group-id>           Describe a consumer group")
		fmt.Println("  reset-offsets <group-id>      Reset offsets for a consumer group")
		fmt.Println("  lag <group-id>                Show consumer lag for a group")
		fmt.Println("  delete <group-id>             Delete a consumer group")
		os.Exit(1)
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
	if len(args) == 0 {
		printUsersUsage()
		os.Exit(1)
	}

	subcmd := args[0]
	subargs := args[1:]

	switch subcmd {
	case "list":
		cmdUsersList(subargs)
	case "create":
		cmdUsersCreate(subargs)
	case "delete":
		cmdUsersDelete(subargs)
	case "update":
		cmdUsersUpdate(subargs)
	case "get":
		cmdUsersGet(subargs)
	case "passwd":
		cmdUsersPasswd(subargs)
	default:
		cli.Error("Unknown users subcommand: %s", subcmd)
		printUsersUsage()
		os.Exit(1)
	}
}

func printUsersUsage() {
	cli.Header("User Management Commands:")
	fmt.Println("  users list                    List all users")
	fmt.Println("  users create <user> <pass>    Create a new user")
	fmt.Println("  users delete <user>           Delete a user")
	fmt.Println("  users update <user>           Update user roles/status")
	fmt.Println("  users get <user>              Get user details")
	fmt.Println("  users passwd <user>           Change user password")
	fmt.Println()
	cli.Header("Options:")
	fmt.Println("  --roles <role1,role2>         Roles for create/update")
	fmt.Println("  --enabled <true|false>        Enable/disable user")
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
	if len(args) == 0 {
		printACLUsage()
		os.Exit(1)
	}

	subcmd := args[0]
	subargs := args[1:]

	switch subcmd {
	case "list":
		cmdACLList(subargs)
	case "get":
		cmdACLGet(subargs)
	case "set":
		cmdACLSet(subargs)
	case "delete":
		cmdACLDelete(subargs)
	default:
		cli.Error("Unknown acl subcommand: %s", subcmd)
		printACLUsage()
		os.Exit(1)
	}
}

func printACLUsage() {
	cli.Header("ACL Management Commands:")
	fmt.Println("  acl list                      List all topic ACLs")
	fmt.Println("  acl get <topic>               Get ACL for a topic")
	fmt.Println("  acl set <topic>               Set ACL for a topic")
	fmt.Println("  acl delete <topic>            Delete ACL for a topic")
	fmt.Println()
	cli.Header("Options:")
	fmt.Println("  --public                      Make topic public")
	fmt.Println("  --users <user1,user2>         Allowed users")
	fmt.Println("  --roles <role1,role2>         Allowed roles")
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
	if len(args) == 0 {
		printRolesUsage()
		return
	}

	switch args[0] {
	case "list":
		cmdRolesList(args[1:])
	case "get":
		cmdRolesGet(args[1:])
	case "help", "--help", "-h":
		printRolesUsage()
	default:
		cli.Error("Unknown roles subcommand: %s", args[0])
		printRolesUsage()
		os.Exit(1)
	}
}

func printRolesUsage() {
	cli.Header("Role Commands:")
	fmt.Println("  roles list                    List all available roles")
	fmt.Println("  roles get <role>              Get role details")
	fmt.Println()
	cli.Header("Description:")
	fmt.Println("  Roles define sets of permissions that can be assigned to users.")
	fmt.Println("  Built-in roles: admin, producer, consumer, guest")
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

// ============================================================================
// Command Help Functions
// ============================================================================

func printProduceHelp() {
	cli.Header("produce - Produce a message to a topic")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  flymq-cli produce <topic> <message> [options]")
	fmt.Println()
	fmt.Println("Arguments:")
	fmt.Println("  <topic>     Target topic name")
	fmt.Println("  <message>   Message content (string)")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  --key, -k <key>         Message key for partition routing")
	fmt.Println("                          Messages with the same key go to the same partition")
	fmt.Println("  --partition, -p <n>     Target partition number (overrides key-based routing)")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  # Simple message")
	fmt.Println("  flymq-cli produce my-topic \"Hello World\"")
	fmt.Println()
	fmt.Println("  # Message with key (ensures ordering for same key)")
	fmt.Println("  flymq-cli produce orders '{\"id\": 1}' --key user-123")
	fmt.Println("  flymq-cli produce orders '{\"id\": 2}' --key user-123  # Same partition")
	fmt.Println()
	fmt.Println("  # Explicit partition")
	fmt.Println("  flymq-cli produce events \"data\" --partition 2")
	fmt.Println()
}

func printConsumeHelp() {
	cli.Header("consume - Fetch messages from a topic (one-time batch read)")
	fmt.Println()
	fmt.Println("  This command fetches a batch of messages WITHOUT tracking your position.")
	fmt.Println("  Use this for debugging or one-time reads. For production, use 'subscribe'.")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  flymq-cli consume <topic> [options]")
	fmt.Println()
	fmt.Println("Arguments:")
	fmt.Println("  <topic>     Topic to consume from")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  --offset, -o <n>        Starting offset (default: 0)")
	fmt.Println("  --count, -n <n>         Number of messages to fetch (default: 10)")
	fmt.Println("  --partition, -p <n>     Partition to consume from (default: 0)")
	fmt.Println("  --show-key, -k          Display message keys in output")
	fmt.Println("  --quiet, -q             Suppress headers and info messages")
	fmt.Println("  --raw, -r               Raw output: just message content")
	fmt.Println("  --no-offset             Hide offset prefix in output")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  # Read first 10 messages")
	fmt.Println("  flymq-cli consume my-topic")
	fmt.Println()
	fmt.Println("  # Read 50 messages starting at offset 100")
	fmt.Println("  flymq-cli consume my-topic --offset 100 --count 50")
	fmt.Println()
	fmt.Println("  # Show message keys")
	fmt.Println("  flymq-cli consume orders --show-key")
	fmt.Println()
	fmt.Println("  # Raw output for piping")
	fmt.Println("  flymq-cli consume my-topic --raw | grep 'error'")
	fmt.Println()
	fmt.Println("See also:")
	fmt.Println("  subscribe   Continuous streaming with offset tracking (recommended)")
	fmt.Println("  groups      Manage consumer groups and offsets")
	fmt.Println()
}

func printSubscribeHelp() {
	cli.Header("subscribe - Stream messages with automatic offset tracking")
	fmt.Println()
	fmt.Println("  This command continuously streams messages and tracks your position.")
	fmt.Println("  If you restart, it resumes from where you left off. Use this for production.")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  flymq-cli subscribe <topic> [options]")
	fmt.Println()
	fmt.Println("Arguments:")
	fmt.Println("  <topic>     Topic to subscribe to")
	fmt.Println()
	fmt.Println("Consumer Group Options:")
	fmt.Println("  --group, -g <id>        Consumer group ID (default: 'default')")
	fmt.Println("                          Messages are tracked per group. Use a unique name")
	fmt.Println("                          for each application that consumes from this topic.")
	fmt.Println()
	fmt.Println("Starting Position:")
	fmt.Println("  --from-beginning        Start from the first message (for new groups)")
	fmt.Println("  --from-latest           Start from new messages only (default for new groups)")
	fmt.Println("  --from, -f <mode>       Explicit mode: earliest, latest, committed")
	fmt.Println("                          Note: Existing groups always resume from committed offset")
	fmt.Println()
	fmt.Println("Other Options:")
	fmt.Println("  --partition, -p <n>     Partition number (default: 0)")
	fmt.Println("  --show-key, -k          Display message keys in output")
	fmt.Println("  --show-lag, -l          Show consumer lag periodically")
	fmt.Println("  --quiet, -q             Suppress headers and info messages")
	fmt.Println("  --raw, -r               Raw output: just message content")
	fmt.Println("  --no-timestamp          Hide timestamp in output")
	fmt.Println("  --no-offset             Hide offset in output")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  # Stream new messages (like tail -f)")
	fmt.Println("  flymq-cli subscribe my-topic")
	fmt.Println()
	fmt.Println("  # Process all messages from beginning with tracking")
	fmt.Println("  flymq-cli subscribe my-topic --group my-app --from-beginning")
	fmt.Println()
	fmt.Println("  # Resume from where you left off (after restart)")
	fmt.Println("  flymq-cli subscribe my-topic --group my-app")
	fmt.Println()
	fmt.Println("  # Monitor lag while consuming")
	fmt.Println("  flymq-cli subscribe my-topic --group my-app --show-lag")
	fmt.Println()
	fmt.Println("How Consumer Groups Work:")
	fmt.Println("  - Each group tracks its own position independently")
	fmt.Println("  - Multiple apps with different groups each see ALL messages")
	fmt.Println("  - Multiple consumers with SAME group share the workload")
	fmt.Println("  - Offsets are committed automatically after processing")
	fmt.Println()
	fmt.Println("See also:")
	fmt.Println("  consume     One-time batch read (no tracking)")
	fmt.Println("  groups      Manage consumer groups and offsets")
	fmt.Println()
}

func printCreateHelp() {
	cli.Header("create - Create a new topic")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  flymq-cli create <topic> [options]")
	fmt.Println()
	fmt.Println("Arguments:")
	fmt.Println("  <topic>     Name of the topic to create")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  --partitions, -p <n>    Number of partitions (default: 1)")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  # Create topic with 1 partition")
	fmt.Println("  flymq-cli create my-topic")
	fmt.Println()
	fmt.Println("  # Create topic with 4 partitions")
	fmt.Println("  flymq-cli create orders --partitions 4")
	fmt.Println()
}
