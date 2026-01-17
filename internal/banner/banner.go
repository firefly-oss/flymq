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
Package banner provides the startup banner display for FlyMQ.

OVERVIEW:
=========
Displays an ASCII art banner with version information when
the server or CLI starts. Uses ANSI escape codes for colors.

USAGE:
======

	banner.Print()           // Print to stdout
	banner.PrintTo(writer)   // Print to custom writer
	banner.PrintServerWithConfig(cfg)  // Print server banner with configuration

The banner text is embedded at compile time from banner.txt.
*/
package banner

import (
	_ "embed"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	"flymq/internal/config"
)

//go:embed banner.txt
var bannerText string

// ANSI escape codes for terminal text formatting.
const (
	AnsiRed    = "\033[31m"
	AnsiGreen  = "\033[32m"
	AnsiYellow = "\033[33m"
	AnsiCyan   = "\033[36m"
	AnsiReset  = "\033[0m"
	AnsiBold   = "\033[1m"
	AnsiDim    = "\033[2m"
)

// Version information
const (
	Version   = "1.26.9"
	Copyright = "Copyright (c) 2026 Firefly Software Solutions Inc."
	License   = "Licensed under Apache License 2.0"
)

// GetBanner returns the raw ASCII banner text.
func GetBanner() string {
	return bannerText
}

// GetBannerLines returns the banner as individual lines.
func GetBannerLines() []string {
	return strings.Split(strings.TrimRight(bannerText, "\n"), "\n")
}

// Print displays the startup banner with version and copyright information.
func Print() {
	fmt.Println()
	fmt.Println(AnsiCyan + AnsiBold)
	for _, line := range GetBannerLines() {
		fmt.Println("  " + line)
	}
	fmt.Println(AnsiReset)
	fmt.Println(AnsiGreen + AnsiBold + "  FlyMQ" + AnsiReset + " " + AnsiDim + "v" + Version + AnsiReset)
	fmt.Println(AnsiDim + "  High-Performance Message Queue" + AnsiReset)
	fmt.Println()
	fmt.Println(AnsiDim + "  " + Copyright + AnsiReset)
	fmt.Println()
}

// PrintTo writes the banner to the specified writer.
func PrintTo(w io.Writer) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, AnsiCyan+AnsiBold)
	for _, line := range GetBannerLines() {
		fmt.Fprintln(w, "  "+line)
	}
	fmt.Fprintln(w, AnsiReset)
	fmt.Fprintln(w, AnsiGreen+AnsiBold+"  FlyMQ"+AnsiReset+" "+AnsiDim+"v"+Version+AnsiReset)
	fmt.Fprintln(w, AnsiDim+"  High-Performance Message Queue"+AnsiReset)
	fmt.Fprintln(w)
	fmt.Fprintln(w, AnsiDim+"  "+Copyright+AnsiReset)
	fmt.Fprintln(w)
}

// PrintCompact prints a compact version of the banner.
func PrintCompact() {
	fmt.Println(AnsiCyan + AnsiBold + "FlyMQ" + AnsiReset + " v" + Version)
}

// PrintServer prints the banner suitable for server startup.
func PrintServer() {
	fmt.Println()
	fmt.Println(AnsiCyan + AnsiBold)
	for _, line := range GetBannerLines() {
		fmt.Println("  " + line)
	}
	fmt.Println(AnsiReset)
	fmt.Println(AnsiGreen + AnsiBold + "  FlyMQ Server" + AnsiReset + " " + AnsiDim + "v" + Version + AnsiReset)
	fmt.Println(AnsiDim + "  Starting..." + AnsiReset)
	fmt.Println()
}

// PrintCLI prints the banner suitable for CLI startup.
func PrintCLI() {
	fmt.Println()
	fmt.Println(AnsiCyan + AnsiBold)
	for _, line := range GetBannerLines() {
		fmt.Println("  " + line)
	}
	fmt.Println(AnsiReset)
	fmt.Println(AnsiGreen + AnsiBold + "  FlyMQ CLI" + AnsiReset + " " + AnsiDim + "v" + Version + AnsiReset)
	fmt.Println()
}

// PrintServerWithConfig prints the server banner with comprehensive configuration display.
// This provides a clear overview of all settings including cluster, replication, and security.
func PrintServerWithConfig(cfg *config.Config) {
	PrintServerWithConfigTo(os.Stdout, cfg)
}

// PrintServerWithConfigTo writes the server banner with configuration to the specified writer.
func PrintServerWithConfigTo(w io.Writer, cfg *config.Config) {
	// Print ASCII banner
	fmt.Fprintln(w)
	fmt.Fprintln(w, AnsiCyan+AnsiBold)
	for _, line := range GetBannerLines() {
		fmt.Fprintln(w, "  "+line)
	}
	fmt.Fprintln(w, AnsiReset)
	fmt.Fprintln(w, AnsiGreen+AnsiBold+"  FlyMQ Server"+AnsiReset+" "+AnsiDim+"v"+Version+AnsiReset)
	fmt.Fprintln(w, AnsiDim+"  High-Performance Message Queue"+AnsiReset)
	fmt.Fprintln(w)

	// Configuration source
	printConfigSource(w, cfg)

	// Print compact configuration
	printCompactConfig(w, cfg)

	// Footer
	fmt.Fprintln(w, AnsiDim+"  "+Copyright+AnsiReset)
	fmt.Fprintln(w)

	// Log separator
	printLogSeparator(w)
}

// PrintLogSeparator prints a visual separator before logs start.
func PrintLogSeparator() {
	printLogSeparator(os.Stdout)
}

func printLogSeparator(w io.Writer) {
	const lineWidth = 78
	arrow := "v"
	text := " LOGS START HERE "
	padding := (lineWidth - len(text) - 4) / 2 // 4 for arrows on each side
	if padding < 0 {
		padding = 0
	}
	line := strings.Repeat("-", padding)
	fmt.Fprintf(w, "  %s%s %s%s%s %s%s%s\n",
		AnsiYellow, arrow+arrow+line,
		AnsiBold, text, AnsiReset+AnsiYellow,
		line+arrow+arrow, AnsiReset, "")
	fmt.Fprintln(w)
}

// Helper functions for configuration display

func printConfigSource(w io.Writer, cfg *config.Config) {
	fmt.Fprint(w, "  "+AnsiDim+"Config: "+AnsiReset)
	if cfg.ConfigFile != "" {
		fmt.Fprintln(w, AnsiYellow+cfg.ConfigFile+AnsiReset)
	} else {
		fmt.Fprintln(w, AnsiDim+"defaults + environment"+AnsiReset)
	}
	fmt.Fprintln(w)
}

func printCompactConfig(w io.Writer, cfg *config.Config) {
	// Line width for separator
	const lineWidth = 78

	// === NETWORK & CLUSTER ===
	printSectionHeader(w, "Server", lineWidth)

	// Row 1: Basic network info
	col1 := fmtKV("Listen", AnsiGreen+cfg.BindAddr+AnsiReset)
	col2 := fmtKV("Node", cfg.NodeID)
	col3 := fmtKV("Log", cfg.LogLevel)
	printRow3(w, col1, col2, col3)

	// Row 2: Storage
	col1 = fmtKV("Data", cfg.DataDir)
	col2 = fmtKV("Segment", formatBytes(cfg.SegmentBytes))
	col3 = fmtKV("Retention", formatBytes(cfg.Retention))
	printRow3(w, col1, col2, col3)

	fmt.Fprintln(w)

	// === CLUSTER MODE ===
	printSectionHeader(w, "Cluster", lineWidth)
	printClusterInfo(w, cfg)

	fmt.Fprintln(w)

	// === SECURITY ===
	printSectionHeader(w, "Security", lineWidth)
	printSecurityInfo(w, cfg)

	fmt.Fprintln(w)

	// === FEATURES (compact single line) ===
	printSectionHeader(w, "Features", lineWidth)
	printFeaturesInfo(w, cfg)

	fmt.Fprintln(w)

	// === OBSERVABILITY ===
	printSectionHeader(w, "Endpoints", lineWidth)
	printEndpointsInfo(w, cfg)

	fmt.Fprintln(w)

	// === PERFORMANCE ===
	printSectionHeader(w, "Performance", lineWidth)
	printPerformanceInfo(w, cfg)

	fmt.Fprintln(w)
}

func printSectionHeader(w io.Writer, title string, width int) {
	titleLen := len(title) + 4 // "[ title ]"
	leftPad := 2
	rightPad := width - leftPad - titleLen
	if rightPad < 0 {
		rightPad = 0
	}
	fmt.Fprintf(w, "  %s[ %s%s%s ]%s%s\n",
		AnsiDim+strings.Repeat("-", leftPad),
		AnsiReset+AnsiCyan+AnsiBold, title, AnsiReset+AnsiDim,
		strings.Repeat("-", rightPad),
		AnsiReset)
}

func fmtKV(key, value string) string {
	return fmt.Sprintf("%s%s:%s %s", AnsiDim, key, AnsiReset, value)
}

func fmtEnabled(name string, enabled bool) string {
	if enabled {
		return AnsiGreen + name + AnsiReset
	}
	return AnsiDim + name + AnsiReset
}

func fmtDisabled(name string) string {
	return AnsiDim + name + AnsiReset
}

func printRow3(w io.Writer, col1, col2, col3 string) {
	fmt.Fprintf(w, "  %-32s %-26s %s\n", col1, col2, col3)
}

func printRow2(w io.Writer, col1, col2 string) {
	fmt.Fprintf(w, "  %-40s %s\n", col1, col2)
}

func formatBytes(bytes int64) string {
	if bytes == 0 {
		return "unlimited"
	}
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.0f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func printClusterInfo(w io.Writer, cfg *config.Config) {
	isClusterMode := cfg.ClusterAddr != ""
	hasPeers := len(cfg.Peers) > 0

	if !isClusterMode {
		// Standalone mode
		col1 := fmtKV("Mode", AnsiYellow+"standalone"+AnsiReset)
		col2 := AnsiDim + "(set FLYMQ_CLUSTER_ADDR to enable clustering)" + AnsiReset
		printRow2(w, col1, col2)
		return
	}

	// Cluster mode
	var modeStr string
	if hasPeers {
		modeStr = AnsiGreen + "cluster" + AnsiReset
	} else {
		modeStr = AnsiYellow + "cluster (bootstrap)" + AnsiReset
	}

	col1 := fmtKV("Mode", modeStr)
	col2 := fmtKV("Cluster", cfg.ClusterAddr)
	var col3 string
	if hasPeers {
		col3 = fmtKV("Peers", fmt.Sprintf("%d", len(cfg.Peers)))
	} else {
		col3 = fmtKV("Peers", AnsiDim+"none"+AnsiReset)
	}
	printRow3(w, col1, col2, col3)

	// Partition/Replication row
	col1 = fmtKV("Partitions", fmt.Sprintf("%d", cfg.Partition.DefaultPartitions))
	col2 = fmtKV("Replicas", fmt.Sprintf("%d", cfg.Partition.DefaultReplicationFactor))
	col3 = fmtKV("Strategy", cfg.Partition.DistributionStrategy)
	printRow3(w, col1, col2, col3)

	// Auto-rebalance
	if cfg.Partition.AutoRebalanceEnabled {
		col1 = fmtKV("Rebalance", AnsiGreen+"auto"+AnsiReset)
		col2 = fmtKV("Interval", fmt.Sprintf("%ds", cfg.Partition.AutoRebalanceInterval))
		col3 = fmtKV("Threshold", fmt.Sprintf("%.0f%%", cfg.Partition.RebalanceThreshold*100))
		printRow3(w, col1, col2, col3)
	}
}

func printSecurityInfo(w io.Writer, cfg *config.Config) {
	// Build security status items
	var items []string

	// TLS
	if cfg.Security.TLSEnabled {
		items = append(items, fmtEnabled("TLS", true))
	} else {
		items = append(items, fmtKV("TLS", AnsiYellow+"off"+AnsiReset))
	}

	// Encryption
	if cfg.Security.EncryptionEnabled {
		items = append(items, fmtEnabled("Encryption", true))
	} else {
		items = append(items, fmtDisabled("Encryption"))
	}

	// Auth
	if cfg.Auth.Enabled {
		authStr := "Auth"
		if cfg.Auth.RBACEnabled {
			authStr = "Auth+RBAC"
		}
		items = append(items, fmtEnabled(authStr, true))
	} else {
		items = append(items, fmtKV("Auth", AnsiYellow+"off"+AnsiReset))
	}

	// Print in columns
	if len(items) >= 3 {
		printRow3(w, items[0], items[1], items[2])
	}

	// Additional auth details if enabled
	if cfg.Auth.Enabled {
		col1 := fmtKV("Admin", cfg.Auth.AdminUsername)
		var col2 string
		if cfg.Auth.AllowAnonymous {
			col2 = fmtKV("Anonymous", AnsiYellow+"allowed"+AnsiReset)
		} else {
			col2 = fmtKV("Anonymous", "denied")
		}
		col3 := fmtKV("Public Topics", fmt.Sprintf("%v", cfg.Auth.DefaultPublic))
		printRow3(w, col1, col2, col3)
	}
}

func printFeaturesInfo(w io.Writer, cfg *config.Config) {
	// Build feature list as compact items
	var enabled []string
	var disabled []string

	if cfg.Schema.Enabled {
		enabled = append(enabled, "Schema")
	} else {
		disabled = append(disabled, "Schema")
	}

	if cfg.DLQ.Enabled {
		enabled = append(enabled, fmt.Sprintf("DLQ(%d retries)", cfg.DLQ.MaxRetries))
	} else {
		disabled = append(disabled, "DLQ")
	}

	if cfg.TTL.DefaultTTL > 0 {
		enabled = append(enabled, fmt.Sprintf("TTL(%ds)", cfg.TTL.DefaultTTL))
	}

	if cfg.Delayed.Enabled {
		enabled = append(enabled, "Delayed")
	} else {
		disabled = append(disabled, "Delayed")
	}

	if cfg.Transaction.Enabled {
		enabled = append(enabled, "Transactions")
	} else {
		disabled = append(disabled, "Transactions")
	}

	if cfg.Audit.Enabled {
		enabled = append(enabled, "Audit")
	} else {
		disabled = append(disabled, "Audit")
	}

	if cfg.Observability.Tracing.Enabled {
		enabled = append(enabled, "Tracing")
	} else {
		disabled = append(disabled, "Tracing")
	}

	if cfg.Observability.Metrics.Enabled {
		enabled = append(enabled, "Metrics")
	} else {
		disabled = append(disabled, "Metrics")
	}

	if cfg.Observability.Health.Enabled {
		enabled = append(enabled, "Health")
	} else {
		disabled = append(disabled, "Health")
	}

	if cfg.Observability.Admin.Enabled {
		enabled = append(enabled, "Admin")
	} else {
		disabled = append(disabled, "Admin")
	}

	if cfg.Discovery.Enabled {
		enabled = append(enabled, "mDNS")
	} else {
		disabled = append(disabled, "mDNS")
	}

	// Print enabled features
	if len(enabled) > 0 {
		fmt.Fprintf(w, "  %sEnabled:%s  %s%s%s\n", AnsiDim, AnsiReset, AnsiGreen, strings.Join(enabled, ", "), AnsiReset)
	}
	if len(disabled) > 0 {
		fmt.Fprintf(w, "  %sDisabled:%s %s\n", AnsiDim, AnsiReset, AnsiDim+strings.Join(disabled, ", ")+AnsiReset)
	}
}

func printEndpointsInfo(w io.Writer, cfg *config.Config) {
	// Collect all endpoints
	var endpoints []string

	endpoints = append(endpoints, fmtKV("Client", AnsiGreen+cfg.BindAddr+AnsiReset))

	if cfg.Observability.Health.Enabled {
		endpoints = append(endpoints, fmtKV("Health", cfg.Observability.Health.Addr))
	}

	if cfg.Observability.Metrics.Enabled {
		endpoints = append(endpoints, fmtKV("Metrics", cfg.Observability.Metrics.Addr))
	}

	if cfg.Observability.Admin.Enabled {
		endpoints = append(endpoints, fmtKV("Admin", cfg.Observability.Admin.Addr))
	}

	if cfg.Observability.Tracing.Enabled {
		endpoints = append(endpoints, fmtKV("Tracing", cfg.Observability.Tracing.Endpoint))
	}

	// Print in rows of 3
	for i := 0; i < len(endpoints); i += 3 {
		col1 := endpoints[i]
		col2 := ""
		col3 := ""
		if i+1 < len(endpoints) {
			col2 = endpoints[i+1]
		}
		if i+2 < len(endpoints) {
			col3 = endpoints[i+2]
		}
		printRow3(w, col1, col2, col3)
	}
}

func printPerformanceInfo(w io.Writer, cfg *config.Config) {
	// Row 1: Acks and sync settings
	acksStr := cfg.Performance.Acks
	switch cfg.Performance.Acks {
	case "all":
		acksStr = "all (safest)"
	case "leader":
		acksStr = "leader (balanced)"
	case "none":
		acksStr = "none (fastest)"
	}
	col1 := fmtKV("Acks", acksStr)
	col2 := fmtKV("Sync", fmt.Sprintf("%dms/%d msgs", cfg.Performance.SyncIntervalMs, cfg.Performance.SyncBatchSize))
	col3 := fmtKV("Workers", fmt.Sprintf("%d", cfg.Performance.NumIOWorkers))
	printRow3(w, col1, col2, col3)

	// Row 2: I/O features
	var ioFeatures []string
	if cfg.Performance.AsyncIO {
		ioFeatures = append(ioFeatures, "AsyncIO")
	}
	if cfg.Performance.ZeroCopy {
		ioFeatures = append(ioFeatures, "ZeroCopy")
	}
	if cfg.Performance.BinaryProtocol {
		ioFeatures = append(ioFeatures, "Binary")
	}

	col1 = fmtKV("I/O", strings.Join(ioFeatures, "+"))
	col2 = fmtKV("Buffer", formatBytes(int64(cfg.Performance.WriteBufferSize)))

	// Compression
	if cfg.Performance.Compression != "" && cfg.Performance.Compression != "none" {
		col3 = fmtKV("Compress", fmt.Sprintf("%s(L%d)", strings.ToUpper(cfg.Performance.Compression), cfg.Performance.CompressionLevel))
	} else {
		col3 = fmtKV("Compress", AnsiDim+"off"+AnsiReset)
	}
	printRow3(w, col1, col2, col3)

	// Row 3: System
	col1 = fmtKV("CPUs", fmt.Sprintf("%d", runtime.NumCPU()))
	col2 = fmtKV("GOMAXPROCS", fmt.Sprintf("%d", runtime.GOMAXPROCS(0)))
	col3 = ""
	printRow3(w, col1, col2, col3)
}
