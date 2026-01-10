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
Package logging provides a structured logging framework for FlyMQ.
It supports multiple log levels, structured fields, and configurable output.
*/
package logging

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Level represents the severity of a log message.
type Level int

const (
	// DEBUG level for detailed debugging information.
	DEBUG Level = iota
	// INFO level for general operational information.
	INFO
	// WARN level for warning conditions.
	WARN
	// ERROR level for error conditions.
	ERROR
)

// String returns the string representation of the log level.
func (l Level) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// ParseLevel parses a string into a Level.
func ParseLevel(s string) Level {
	switch s {
	case "DEBUG", "debug":
		return DEBUG
	case "INFO", "info":
		return INFO
	case "WARN", "warn", "WARNING", "warning":
		return WARN
	case "ERROR", "error":
		return ERROR
	default:
		return INFO
	}
}

// Entry represents a single log entry with all its metadata.
type Entry struct {
	Timestamp time.Time              `json:"timestamp"`
	Level     string                 `json:"level"`
	Component string                 `json:"component"`
	Message   string                 `json:"message"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

// Logger provides structured logging capabilities.
type Logger struct {
	component string
	level     Level
	output    io.Writer
	mu        sync.Mutex
	jsonMode  bool
}

// Config holds logger configuration options.
type Config struct {
	Level    Level
	Output   io.Writer
	JSONMode bool
}

// DefaultConfig returns the default logger configuration.
func DefaultConfig() Config {
	return Config{
		Level:    INFO,
		Output:   os.Stdout,
		JSONMode: false,
	}
}

// globalConfig holds the global logger configuration.
var (
	globalConfig = DefaultConfig()
	globalMu     sync.RWMutex
)

// SetGlobalLevel sets the global log level.
func SetGlobalLevel(level Level) {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalConfig.Level = level
}

// SetGlobalOutput sets the global log output.
func SetGlobalOutput(w io.Writer) {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalConfig.Output = w
}

// SetJSONMode enables or disables JSON output mode.
func SetJSONMode(enabled bool) {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalConfig.JSONMode = enabled
}

// NewLogger creates a new Logger for the specified component.
func NewLogger(component string) *Logger {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return &Logger{
		component: component,
		level:     globalConfig.Level,
		output:    globalConfig.Output,
		jsonMode:  globalConfig.JSONMode,
	}
}

// log writes a log entry at the specified level.
func (l *Logger) log(level Level, msg string, args ...interface{}) {
	// Check if this level should be logged
	globalMu.RLock()
	minLevel := globalConfig.Level
	output := globalConfig.Output
	jsonMode := globalConfig.JSONMode
	globalMu.RUnlock()

	if level < minLevel {
		return
	}

	// Build the entry
	entry := Entry{
		Timestamp: time.Now().UTC(),
		Level:     level.String(),
		Component: l.component,
		Message:   msg,
	}

	// Parse key-value pairs from args
	if len(args) > 0 {
		entry.Fields = make(map[string]interface{})
		for i := 0; i < len(args)-1; i += 2 {
			key, ok := args[i].(string)
			if !ok {
				key = fmt.Sprintf("arg%d", i)
			}
			entry.Fields[key] = args[i+1]
		}
		// Handle odd number of args
		if len(args)%2 != 0 {
			entry.Fields["extra"] = args[len(args)-1]
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if jsonMode {
		l.writeJSON(output, entry)
	} else {
		l.writeText(output, entry)
	}
}

// writeJSON writes the entry in JSON format.
func (l *Logger) writeJSON(w io.Writer, entry Entry) {
	data, err := json.Marshal(entry)
	if err != nil {
		fmt.Fprintf(w, "ERROR: failed to marshal log entry: %v\n", err)
		return
	}
	fmt.Fprintln(w, string(data))
}

// writeText writes the entry in human-readable text format.
func (l *Logger) writeText(w io.Writer, entry Entry) {
	// Format: 2006-01-02T15:04:05.000Z [LEVEL] [component] message key=value ...
	timestamp := entry.Timestamp.Format("2006-01-02T15:04:05.000Z")

	// Color codes for different levels
	var levelColor string
	switch entry.Level {
	case "DEBUG":
		levelColor = "\033[36m" // Cyan
	case "INFO":
		levelColor = "\033[32m" // Green
	case "WARN":
		levelColor = "\033[33m" // Yellow
	case "ERROR":
		levelColor = "\033[31m" // Red
	default:
		levelColor = "\033[0m"
	}
	resetColor := "\033[0m"

	// Build the log line
	line := fmt.Sprintf("%s %s[%-5s]%s [%s] %s",
		timestamp, levelColor, entry.Level, resetColor, entry.Component, entry.Message)

	// Append fields
	if len(entry.Fields) > 0 {
		for k, v := range entry.Fields {
			line += fmt.Sprintf(" %s=%v", k, v)
		}
	}

	fmt.Fprintln(w, line)
}

// Debug logs a message at DEBUG level.
func (l *Logger) Debug(msg string, args ...interface{}) {
	l.log(DEBUG, msg, args...)
}

// Info logs a message at INFO level.
func (l *Logger) Info(msg string, args ...interface{}) {
	l.log(INFO, msg, args...)
}

// Warn logs a message at WARN level.
func (l *Logger) Warn(msg string, args ...interface{}) {
	l.log(WARN, msg, args...)
}

// Error logs a message at ERROR level.
func (l *Logger) Error(msg string, args ...interface{}) {
	l.log(ERROR, msg, args...)
}
