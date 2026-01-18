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
Package cli provides shared CLI utilities for FlyMQ applications.

COLORS:
=======
ANSI escape codes for terminal text formatting:
- Reset, Bold, Dim, Italic, Underline
- Foreground: Black, Red, Green, Yellow, Blue, Magenta, Cyan, White
- Bright variants: BrightRed, BrightGreen, etc.

ICONS:
======
Unicode icons for CLI output:
- IconSuccess (✓), IconError (✗), IconWarning (⚠)
- IconInfo (ℹ), IconArrow (→), IconDot (●)

USAGE:
======

	fmt.Printf("%s%sSuccess!%s\n", cli.Green, cli.Bold, cli.Reset)
	fmt.Printf("%s Operation completed\n", cli.IconSuccess)

Colors are automatically disabled when output is not a TTY.
*/
package cli

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"unicode/utf8"
)

var ansiRegex = regexp.MustCompile(`\033\[[0-9;]*[a-zA-Z]`)

func visibleLen(s string) int {
	return utf8.RuneCountInString(ansiRegex.ReplaceAllString(s, ""))
}

// ANSI color codes for terminal output.
const (
	Reset     = "\033[0m"
	Bold      = "\033[1m"
	Dim       = "\033[2m"
	Italic    = "\033[3m"
	Underline = "\033[4m"

	// Foreground colors
	Black   = "\033[30m"
	Red     = "\033[31m"
	Green   = "\033[32m"
	Yellow  = "\033[33m"
	Blue    = "\033[34m"
	Magenta = "\033[35m"
	Cyan    = "\033[36m"
	White   = "\033[37m"

	// Bright foreground colors
	BrightBlack  = "\033[90m"
	BrightRed    = "\033[91m"
	BrightGreen  = "\033[92m"
	BrightYellow = "\033[93m"
	BrightBlue   = "\033[94m"
	BrightCyan   = "\033[96m"
	BrightWhite  = "\033[97m"
)

// Icons for CLI output
const (
	IconSuccess = "✓"
	IconError   = "✗"
	IconWarning = "⚠"
	IconInfo    = "ℹ"
	IconArrow   = "→"
	IconDot     = "●"
)

var colorsEnabled = true

func init() {
	if os.Getenv("NO_COLOR") != "" {
		colorsEnabled = false
	}
	if fileInfo, _ := os.Stdout.Stat(); (fileInfo.Mode() & os.ModeCharDevice) == 0 {
		colorsEnabled = false
	}
}

// SetColorsEnabled enables or disables color output.
func SetColorsEnabled(enabled bool) {
	colorsEnabled = enabled
}

func Colorize(color, text string) string {
	if !colorsEnabled {
		return text
	}
	return color + text + Reset
}

// Success prints a success message.
func Success(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Println(Colorize(Green, IconSuccess+" "+msg))
}

// Error prints an error message.
func Error(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintln(os.Stderr, Colorize(Red, IconError+" "+msg))
}

// ErrorWithHint prints an error message with a helpful hint.
func ErrorWithHint(message string, hint string) {
	fmt.Fprintln(os.Stderr, Colorize(Red, IconError+" "+message))
	if hint != "" {
		fmt.Fprintln(os.Stderr, Colorize(Dim, "  "+IconArrow+" Hint: "+hint))
	}
}

// ErrorWithSuggestion prints an error with a suggested command.
func ErrorWithSuggestion(message string, suggestion string) {
	fmt.Fprintln(os.Stderr, Colorize(Red, IconError+" "+message))
	if suggestion != "" {
		fmt.Fprintln(os.Stderr, Colorize(Cyan, "  "+IconArrow+" Try: "+suggestion))
	}
}

// Warning prints a warning message.
func Warning(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Println(Colorize(Yellow, IconWarning+" "+msg))
}

// Info prints an info message.
func Info(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Println(Colorize(Cyan, IconInfo+" "+msg))
}

// Header prints a header/title.
func Header(text string) {
	fmt.Println(Colorize(Bold+Cyan, text))
}

// Section prints a section header with spacing.
func Section(name string) {
	fmt.Println()
	fmt.Println(Colorize(Bold+Cyan, strings.ToUpper(name)))
}

// Command prints a command with its arguments and description.
func Command(name, args, description string) {
	cmdPart := Colorize(Green, name)
	if args != "" {
		cmdPart += " " + Colorize(Dim, args)
	}
	// Use 30 as fixed width for command + args part for alignment
	padding := 30 - visibleLen(cmdPart)
	if padding < 1 {
		padding = 1
	}
	fmt.Printf("  %s%s%s\n", cmdPart, strings.Repeat(" ", padding), description)
}

// Tip prints a tip message (green).
func Tip(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Println(Colorize(Green, "  "+IconArrow+" Tip: "+msg))
}

// Note prints a note message (dimmed).
func Note(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Println(Colorize(Dim, "  Note: "+msg))
}

// KeyValueAligned prints a labeled value with alignment.
func KeyValueAligned(key, value string, width int) {
	padding := width - visibleLen(key)
	if padding < 0 {
		padding = 0
	}
	fmt.Printf("  %s:%s %s\n", Colorize(Dim, key), strings.Repeat(" ", padding), value)
}

// Footer prints a small footer.
func Footer() {
	fmt.Println()
}

// SubCommand prints a subcommand entry with a visual indicator.
func SubCommand(name, args, description string) {
	cmdPart := "  " + Colorize(Green, name)
	if args != "" {
		cmdPart += " " + Colorize(Dim, args)
	}
	padding := 30 - visibleLen(cmdPart)
	if padding < 1 {
		padding = 1
	}
	fmt.Printf("%s%s%s %s\n", cmdPart, strings.Repeat(" ", padding), Colorize(Dim, IconArrow), description)
}

// SubCommandSection prints a dimmed section header for grouping commands.
func SubCommandSection(name string) {
	fmt.Println(Colorize(Dim, "  "+name))
}

// Option prints a CLI option with its description.
func Option(short, long, placeholder, description string) {
	var optPart string
	if short != "" {
		optPart = Colorize(Dim, short) + ", "
	}
	optPart += Colorize(Dim, long)
	if placeholder != "" {
		optPart += " " + Colorize(Dim, "<"+placeholder+">")
	}

	padding := 30 - visibleLen(optPart)
	if padding < 1 {
		padding = 1
	}
	fmt.Printf("  %s%s%s\n", optPart, strings.Repeat(" ", padding), description)
}

// Example prints an example command with a description.
func Example(description, command string) {
	fmt.Println(Colorize(Dim, "    # "+description))
	fmt.Println("    " + command)
	fmt.Println()
}

// KeyValue prints a key-value pair.
func KeyValue(key string, value interface{}) {
	fmt.Printf("  %s: %v\n", Colorize(Dim, key), value)
}

// Separator prints a horizontal line.
func Separator() {
	fmt.Println(Colorize(Dim, "────────────────────────────────────────────────────────────────────────────────"))
}

// SeparatorN prints a horizontal line of specified length.
func SeparatorN(n int) {
	if n <= 0 {
		return
	}
	fmt.Println(Colorize(Dim, strings.Repeat("─", n)))
}

// Table prints a formatted table to the console.
func Table(headers []string, rows [][]string) {
	fmt.Print(TableString(headers, rows))
}

// TableString returns a formatted table as a string.
func TableString(headers []string, rows [][]string) string {
	if len(headers) == 0 {
		return ""
	}

	var sb strings.Builder

	colWidths := make([]int, len(headers))
	for i, h := range headers {
		colWidths[i] = utf8.RuneCountInString(h)
	}

	for _, row := range rows {
		for i, col := range row {
			if i < len(colWidths) {
				l := visibleLen(col)
				if l > colWidths[i] {
					colWidths[i] = l
				}
			}
		}
	}

	// Header
	for i, h := range headers {
		sb.WriteString(Colorize(Bold+Cyan, strings.ToUpper(h)))
		if i < len(headers)-1 {
			padding := colWidths[i] - utf8.RuneCountInString(h) + 3
			sb.WriteString(strings.Repeat(" ", padding))
		}
	}
	sb.WriteString("\n")

	// Separator line
	totalWidth := 0
	for i, w := range colWidths {
		totalWidth += w
		if i < len(colWidths)-1 {
			totalWidth += 3
		}
	}
	sb.WriteString(Colorize(Dim, strings.Repeat("─", totalWidth)))
	sb.WriteString("\n")

	// Data
	for _, row := range rows {
		for i, col := range row {
			if i < len(colWidths) {
				sb.WriteString(col)
				if i < len(row)-1 {
					padding := colWidths[i] - visibleLen(col) + 3
					sb.WriteString(strings.Repeat(" ", padding))
				}
			}
		}
		sb.WriteString("\n")
	}

	return sb.String()
}
