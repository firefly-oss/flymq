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
)

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

func colorize(color, text string) string {
	if !colorsEnabled {
		return text
	}
	return color + text + Reset
}

// Success prints a success message.
func Success(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Println(colorize(Green, IconSuccess+" "+msg))
}

// Error prints an error message.
func Error(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintln(os.Stderr, colorize(Red, IconError+" "+msg))
}

// ErrorWithHint prints an error message with a helpful hint.
func ErrorWithHint(message string, hint string) {
	fmt.Fprintln(os.Stderr, colorize(Red, IconError+" "+message))
	if hint != "" {
		fmt.Fprintln(os.Stderr, colorize(Dim, "  "+IconArrow+" Hint: "+hint))
	}
}

// ErrorWithSuggestion prints an error with a suggested command.
func ErrorWithSuggestion(message string, suggestion string) {
	fmt.Fprintln(os.Stderr, colorize(Red, IconError+" "+message))
	if suggestion != "" {
		fmt.Fprintln(os.Stderr, colorize(Cyan, "  "+IconArrow+" Try: "+suggestion))
	}
}

// Warning prints a warning message.
func Warning(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Println(colorize(Yellow, IconWarning+" "+msg))
}

// Info prints an info message.
func Info(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Println(colorize(Cyan, IconInfo+" "+msg))
}

// Hint prints a hint message (dimmed).
func Hint(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Println(colorize(Dim, "  "+IconArrow+" "+msg))
}

// Header prints a header/title.
func Header(text string) {
	fmt.Println(colorize(Bold+Cyan, text))
}

// KeyValue prints a key-value pair.
func KeyValue(key string, value interface{}) {
	fmt.Printf("  %s: %v\n", colorize(Dim, key), value)
}

// Separator prints a horizontal line.
func Separator() {
	fmt.Println(colorize(Dim, "────────────────────────────────────────"))
}

// Example prints an example command.
func Example(description, command string) {
	fmt.Printf("  %s\n", colorize(Dim, "# "+description))
	fmt.Printf("  %s\n", colorize(Cyan, command))
}
