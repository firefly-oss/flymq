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

package logging

import (
	"bytes"
	"strings"
	"testing"
)

func TestLevelString(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{DEBUG, "DEBUG"},
		{INFO, "INFO"},
		{WARN, "WARN"},
		{ERROR, "ERROR"},
		{Level(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		if got := tt.level.String(); got != tt.expected {
			t.Errorf("Level(%d).String() = %s, want %s", tt.level, got, tt.expected)
		}
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected Level
	}{
		{"DEBUG", DEBUG},
		{"debug", DEBUG},
		{"INFO", INFO},
		{"info", INFO},
		{"WARN", WARN},
		{"warn", WARN},
		{"WARNING", WARN},
		{"warning", WARN},
		{"ERROR", ERROR},
		{"error", ERROR},
		{"unknown", INFO}, // default
	}

	for _, tt := range tests {
		if got := ParseLevel(tt.input); got != tt.expected {
			t.Errorf("ParseLevel(%s) = %d, want %d", tt.input, got, tt.expected)
		}
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Level != INFO {
		t.Errorf("Expected default level INFO, got %d", cfg.Level)
	}
	if cfg.JSONMode {
		t.Error("Expected JSONMode to be false by default")
	}
}

func TestNewLogger(t *testing.T) {
	logger := NewLogger("test-component")
	if logger == nil {
		t.Fatal("Expected non-nil logger")
	}
	if logger.component != "test-component" {
		t.Errorf("Expected component 'test-component', got %s", logger.component)
	}
}

func TestLoggerOutput(t *testing.T) {
	var buf bytes.Buffer
	SetGlobalOutput(&buf)
	SetGlobalLevel(DEBUG)
	defer func() {
		SetGlobalLevel(INFO)
	}()

	logger := NewLogger("test")
	logger.Info("test message", "key", "value")

	output := buf.String()
	if !strings.Contains(output, "test message") {
		t.Errorf("Expected output to contain 'test message', got: %s", output)
	}
	if !strings.Contains(output, "[test]") {
		t.Errorf("Expected output to contain '[test]', got: %s", output)
	}
	if !strings.Contains(output, "key=value") {
		t.Errorf("Expected output to contain 'key=value', got: %s", output)
	}
}

func TestLoggerLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	SetGlobalOutput(&buf)
	SetGlobalLevel(WARN)
	defer func() {
		SetGlobalLevel(INFO)
	}()

	logger := NewLogger("test")
	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")

	output := buf.String()
	if strings.Contains(output, "debug message") {
		t.Error("Debug message should be filtered")
	}
	if strings.Contains(output, "info message") {
		t.Error("Info message should be filtered")
	}
	if !strings.Contains(output, "warn message") {
		t.Error("Warn message should be present")
	}
	if !strings.Contains(output, "error message") {
		t.Error("Error message should be present")
	}
}

func TestLoggerJSONMode(t *testing.T) {
	var buf bytes.Buffer
	SetGlobalOutput(&buf)
	SetGlobalLevel(INFO)
	SetJSONMode(true)
	defer func() {
		SetJSONMode(false)
	}()

	logger := NewLogger("test")
	logger.Info("json test", "foo", "bar")

	output := buf.String()
	if !strings.Contains(output, `"message":"json test"`) {
		t.Errorf("Expected JSON output with message field, got: %s", output)
	}
	if !strings.Contains(output, `"component":"test"`) {
		t.Errorf("Expected JSON output with component field, got: %s", output)
	}
}

func TestLoggerAllLevels(t *testing.T) {
	var buf bytes.Buffer
	SetGlobalOutput(&buf)
	SetGlobalLevel(DEBUG)
	defer func() {
		SetGlobalLevel(INFO)
	}()

	logger := NewLogger("test")
	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Error("error")

	output := buf.String()
	if !strings.Contains(output, "DEBUG") {
		t.Error("Expected DEBUG in output")
	}
	if !strings.Contains(output, "INFO") {
		t.Error("Expected INFO in output")
	}
	if !strings.Contains(output, "WARN") {
		t.Error("Expected WARN in output")
	}
	if !strings.Contains(output, "ERROR") {
		t.Error("Expected ERROR in output")
	}
}
