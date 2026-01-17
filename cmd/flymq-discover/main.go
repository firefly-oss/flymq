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
flymq-discover - FlyMQ Node Discovery Tool

This tool discovers FlyMQ nodes on the local network using mDNS (Bonjour/Avahi).
It can be used by install.sh to find existing cluster nodes for joining.

Usage:
    flymq-discover                    # Discover nodes (5 second timeout)
    flymq-discover --timeout 10       # Custom timeout in seconds
    flymq-discover --json             # Output as JSON
    flymq-discover --quiet            # Only output addresses (for scripting)
*/
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"flymq/internal/banner"
	"flymq/internal/cluster"
	"flymq/pkg/cli"
)

func main() {
	timeout := flag.Int("timeout", 5, "Discovery timeout in seconds")
	jsonOutput := flag.Bool("json", false, "Output as JSON")
	quiet := flag.Bool("quiet", false, "Only output cluster addresses (for scripting)")
	help := flag.Bool("help", false, "Show help")
	version := flag.Bool("version", false, "Show version information")
	flag.BoolVar(help, "h", false, "Show help")
	flag.BoolVar(version, "v", false, "Show version information")

	flag.Parse()

	if *help {
		printUsage()
		os.Exit(0)
	}

	if *version {
		printVersion()
		os.Exit(0)
	}

	// Suppress mDNS library logging (it logs IPv6 errors that are not critical)
	log.SetOutput(io.Discard)

	// Show banner and welcome message unless in quiet/json mode
	if !*quiet && !*jsonOutput {
		printBanner()
	}

	// Create discovery service (not advertising, just discovering)
	discovery := cluster.NewDiscoveryService(cluster.DiscoveryConfig{
		NodeID:  "discover-client",
		Enabled: false, // Don't advertise, just discover
	})

	// Show scanning message unless in quiet mode
	if !*quiet && !*jsonOutput {
		cli.Info("Scanning for FlyMQ nodes on the network (timeout: %ds)...", *timeout)
		fmt.Println()
	}

	// Discover nodes
	nodes, err := discovery.DiscoverNodes(time.Duration(*timeout) * time.Second)
	if err != nil {
		if !*quiet {
			cli.Error("Discovery failed: %v", err)
		}
		os.Exit(1)
	}

	if len(nodes) == 0 {
		if !*quiet && !*jsonOutput {
			cli.Warning("No FlyMQ nodes found on the network.")
			fmt.Println()
			fmt.Println(cli.Bold + cli.Cyan + "TROUBLESHOOTING" + cli.Reset)
			fmt.Println()
			fmt.Println(cli.Dim + "  Common issues:" + cli.Reset)
			fmt.Println("    " + cli.Yellow + "•" + cli.Reset + " FlyMQ nodes are not running with discovery enabled")
			fmt.Println("    " + cli.Yellow + "•" + cli.Reset + " mDNS/Bonjour is blocked by firewall (UDP port 5353)")
			fmt.Println("    " + cli.Yellow + "•" + cli.Reset + " Nodes are on a different network segment")
			fmt.Println()
			fmt.Println(cli.Dim + "  Try:" + cli.Reset)
			fmt.Println("    " + cli.Green + "flymq-discover --timeout 10" + cli.Reset + "   # Increase timeout")
			fmt.Println()
		}
		os.Exit(0)
	}

	if *jsonOutput {
		outputJSON(nodes)
	} else if *quiet {
		outputQuiet(nodes)
	} else {
		outputHuman(nodes)
	}
}

func printBanner() {
	fmt.Println()
	fmt.Println(cli.Cyan + cli.Bold)
	for _, line := range banner.GetBannerLines() {
		fmt.Println("  " + line)
	}
	fmt.Println(cli.Reset)
	fmt.Println(cli.Green + cli.Bold + "  FlyMQ Discover" + cli.Reset + " " + cli.Dim + "v" + banner.Version + cli.Reset)
	fmt.Println(cli.Dim + "  Network Node Discovery Tool" + cli.Reset)
	fmt.Println()
}

func printVersion() {
	fmt.Println()
	fmt.Println(cli.Cyan + cli.Bold + "  FlyMQ Discover" + cli.Reset + " " + cli.Dim + "v" + banner.Version + cli.Reset)
	fmt.Println(cli.Dim + "  Network Node Discovery Tool" + cli.Reset)
	fmt.Println()
	fmt.Println(cli.Dim + "  " + banner.Copyright + cli.Reset)
	fmt.Println()
}

func printUsage() {
	// Print banner
	fmt.Println()
	fmt.Println(cli.Cyan + cli.Bold)
	for _, line := range banner.GetBannerLines() {
		fmt.Println("  " + line)
	}
	fmt.Println(cli.Reset)
	fmt.Println(cli.Green + cli.Bold + "  FlyMQ Discover" + cli.Reset + " " + cli.Dim + "v" + banner.Version + cli.Reset)
	fmt.Println(cli.Dim + "  Network Node Discovery Tool" + cli.Reset)
	fmt.Println()

	// Description
	fmt.Println(cli.Dim + "  Discovers FlyMQ nodes on the local network using mDNS (Bonjour/Avahi)." + cli.Reset)
	fmt.Println(cli.Dim + "  Useful for finding existing cluster nodes to join." + cli.Reset)
	fmt.Println()

	// Usage
	fmt.Println(cli.Bold + "Usage:" + cli.Reset + " flymq-discover [options]")
	fmt.Println()

	// Options
	fmt.Println(cli.Bold + cli.Cyan + "OPTIONS" + cli.Reset)
	fmt.Println()
	fmt.Println("    " + cli.Green + "--timeout" + cli.Reset + " <seconds>   Discovery timeout (default: 5)")
	fmt.Println("    " + cli.Green + "--json" + cli.Reset + "               Output results as JSON")
	fmt.Println("    " + cli.Green + "--quiet" + cli.Reset + ", " + cli.Green + "-q" + cli.Reset + "          Only output addresses (for scripting)")
	fmt.Println("    " + cli.Green + "--version" + cli.Reset + ", " + cli.Green + "-v" + cli.Reset + "        Show version information")
	fmt.Println("    " + cli.Green + "--help" + cli.Reset + ", " + cli.Green + "-h" + cli.Reset + "           Show this help message")
	fmt.Println()

	// Examples
	fmt.Println(cli.Bold + cli.Cyan + "EXAMPLES" + cli.Reset)
	fmt.Println()
	fmt.Println(cli.Dim + "    # Discover nodes with default timeout" + cli.Reset)
	fmt.Println("    flymq-discover")
	fmt.Println()
	fmt.Println(cli.Dim + "    # Increase timeout for slower networks" + cli.Reset)
	fmt.Println("    flymq-discover --timeout 10")
	fmt.Println()
	fmt.Println(cli.Dim + "    # Get JSON output for automation" + cli.Reset)
	fmt.Println("    flymq-discover --json")
	fmt.Println()
	fmt.Println(cli.Dim + "    # Get just addresses for scripting" + cli.Reset)
	fmt.Println("    flymq-discover --quiet")
	fmt.Println()
	fmt.Println(cli.Dim + "    # Use in install script to find cluster" + cli.Reset)
	fmt.Println("    PEERS=$(flymq-discover --quiet)")
	fmt.Println()

	// Network requirements
	fmt.Println(cli.Bold + cli.Cyan + "NETWORK REQUIREMENTS" + cli.Reset)
	fmt.Println()
	fmt.Println("    " + cli.Yellow + "•" + cli.Reset + " mDNS uses UDP port 5353 (multicast)")
	fmt.Println("    " + cli.Yellow + "•" + cli.Reset + " Nodes must be on the same network segment")
	fmt.Println("    " + cli.Yellow + "•" + cli.Reset + " Firewalls must allow mDNS traffic")
	fmt.Println()
}

func outputJSON(nodes []*cluster.DiscoveredNode) {
	type nodeOutput struct {
		NodeID      string `json:"node_id"`
		ClusterID   string `json:"cluster_id,omitempty"`
		ClusterAddr string `json:"cluster_addr"`
		ClientAddr  string `json:"client_addr,omitempty"`
		Version     string `json:"version,omitempty"`
	}

	output := make([]nodeOutput, len(nodes))
	for i, n := range nodes {
		output[i] = nodeOutput{
			NodeID:      n.NodeID,
			ClusterID:   n.ClusterID,
			ClusterAddr: n.ClusterAddr,
			ClientAddr:  n.ClientAddr,
			Version:     n.Version,
		}
	}

	data, _ := json.MarshalIndent(output, "", "  ")
	fmt.Println(string(data))
}

func outputQuiet(nodes []*cluster.DiscoveredNode) {
	addrs := make([]string, len(nodes))
	for i, n := range nodes {
		addrs[i] = n.ClusterAddr
	}
	fmt.Println(strings.Join(addrs, ","))
}

func outputHuman(nodes []*cluster.DiscoveredNode) {
	cli.Success("Found %d FlyMQ node(s)", len(nodes))
	fmt.Println()

	for i, n := range nodes {
		// Node header with index and ID
		fmt.Printf("  %s[%d]%s %s%s%s\n",
			cli.Dim, i+1, cli.Reset,
			cli.Bold+cli.Cyan, n.NodeID, cli.Reset)

		// Cluster address (always present)
		fmt.Printf("      %sCluster Address:%s %s%s%s\n",
			cli.Dim, cli.Reset,
			cli.Green, n.ClusterAddr, cli.Reset)

		// Client address (optional)
		if n.ClientAddr != "" {
			fmt.Printf("      %sClient Address:%s  %s\n",
				cli.Dim, cli.Reset, n.ClientAddr)
		}

		// Cluster ID (optional)
		if n.ClusterID != "" {
			fmt.Printf("      %sCluster ID:%s      %s\n",
				cli.Dim, cli.Reset, n.ClusterID)
		}

		// Version (optional)
		if n.Version != "" {
			fmt.Printf("      %sVersion:%s         %s\n",
				cli.Dim, cli.Reset, n.Version)
		}

		fmt.Println()
	}

	// Helpful tip
	fmt.Println(cli.Dim + "  Tip: Use --json for machine-readable output" + cli.Reset)
	fmt.Println()
}

