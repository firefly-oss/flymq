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

package cli

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestColorConstants(t *testing.T) {
	// Verify ANSI codes are defined
	if Reset == "" {
		t.Error("Expected non-empty Reset")
	}
	if Bold == "" {
		t.Error("Expected non-empty Bold")
	}
	if Red == "" {
		t.Error("Expected non-empty Red")
	}
	if Green == "" {
		t.Error("Expected non-empty Green")
	}
	if Yellow == "" {
		t.Error("Expected non-empty Yellow")
	}
	if Blue == "" {
		t.Error("Expected non-empty Blue")
	}
	if Cyan == "" {
		t.Error("Expected non-empty Cyan")
	}
}

func TestIconConstants(t *testing.T) {
	if IconSuccess == "" {
		t.Error("Expected non-empty IconSuccess")
	}
	if IconError == "" {
		t.Error("Expected non-empty IconError")
	}
	if IconWarning == "" {
		t.Error("Expected non-empty IconWarning")
	}
	if IconInfo == "" {
		t.Error("Expected non-empty IconInfo")
	}
	if IconArrow == "" {
		t.Error("Expected non-empty IconArrow")
	}
	if IconDot == "" {
		t.Error("Expected non-empty IconDot")
	}
}

func TestSetColorsEnabled(t *testing.T) {
	// Save original state
	original := colorsEnabled

	SetColorsEnabled(false)
	if colorsEnabled {
		t.Error("Expected colors to be disabled")
	}

	SetColorsEnabled(true)
	if !colorsEnabled {
		t.Error("Expected colors to be enabled")
	}

	// Restore original state
	colorsEnabled = original
}

func TestColorize(t *testing.T) {
	// Test with colors enabled
	SetColorsEnabled(true)
	result := colorize(Red, "test")
	if !strings.Contains(result, "test") {
		t.Error("Expected result to contain 'test'")
	}
	if !strings.Contains(result, Red) {
		t.Error("Expected result to contain Red color code")
	}
	if !strings.Contains(result, Reset) {
		t.Error("Expected result to contain Reset code")
	}

	// Test with colors disabled
	SetColorsEnabled(false)
	result = colorize(Red, "test")
	if result != "test" {
		t.Errorf("Expected 'test' without colors, got '%s'", result)
	}

	// Restore
	SetColorsEnabled(true)
}

func captureOutput(f func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	f()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	return buf.String()
}

func TestSuccess(t *testing.T) {
	SetColorsEnabled(false)
	output := captureOutput(func() {
		Success("test message %s", "arg")
	})

	if !strings.Contains(output, "test message arg") {
		t.Errorf("Expected output to contain 'test message arg', got '%s'", output)
	}
	if !strings.Contains(output, IconSuccess) {
		t.Errorf("Expected output to contain success icon, got '%s'", output)
	}
}

func TestInfo(t *testing.T) {
	SetColorsEnabled(false)
	output := captureOutput(func() {
		Info("info message")
	})

	if !strings.Contains(output, "info message") {
		t.Errorf("Expected output to contain 'info message', got '%s'", output)
	}
	if !strings.Contains(output, IconInfo) {
		t.Errorf("Expected output to contain info icon, got '%s'", output)
	}
}

func TestWarning(t *testing.T) {
	SetColorsEnabled(false)
	output := captureOutput(func() {
		Warning("warning message")
	})

	if !strings.Contains(output, "warning message") {
		t.Errorf("Expected output to contain 'warning message', got '%s'", output)
	}
}
