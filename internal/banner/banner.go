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

The banner text is embedded at compile time from banner.txt.
*/
package banner

import (
	_ "embed"
	"fmt"
	"io"
	"strings"
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
	Version   = "1.26.8"
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
