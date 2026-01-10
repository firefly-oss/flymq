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

package banner

import (
	"bytes"
	"strings"
	"testing"
)

func TestGetBanner(t *testing.T) {
	banner := GetBanner()
	if banner == "" {
		t.Error("Expected non-empty banner")
	}
}

func TestGetBannerLines(t *testing.T) {
	lines := GetBannerLines()
	if len(lines) == 0 {
		t.Error("Expected at least one line in banner")
	}
}

func TestPrintTo(t *testing.T) {
	var buf bytes.Buffer
	PrintTo(&buf)

	output := buf.String()
	if output == "" {
		t.Error("Expected non-empty output")
	}

	// Check for version
	if !strings.Contains(output, Version) {
		t.Errorf("Expected output to contain version %s", Version)
	}

	// Check for copyright
	if !strings.Contains(output, "Copyright") {
		t.Error("Expected output to contain copyright")
	}
}

func TestVersionConstant(t *testing.T) {
	if Version == "" {
		t.Error("Expected non-empty version")
	}
}

func TestCopyrightConstant(t *testing.T) {
	if Copyright == "" {
		t.Error("Expected non-empty copyright")
	}
	if !strings.Contains(Copyright, "Firefly") {
		t.Error("Expected copyright to contain 'Firefly'")
	}
}

func TestLicenseConstant(t *testing.T) {
	if License == "" {
		t.Error("Expected non-empty license")
	}
	if !strings.Contains(License, "Apache") {
		t.Error("Expected license to contain 'Apache'")
	}
}

func TestAnsiConstants(t *testing.T) {
	// Verify ANSI codes are defined
	if AnsiRed == "" {
		t.Error("Expected non-empty AnsiRed")
	}
	if AnsiGreen == "" {
		t.Error("Expected non-empty AnsiGreen")
	}
	if AnsiYellow == "" {
		t.Error("Expected non-empty AnsiYellow")
	}
	if AnsiCyan == "" {
		t.Error("Expected non-empty AnsiCyan")
	}
	if AnsiReset == "" {
		t.Error("Expected non-empty AnsiReset")
	}
	if AnsiBold == "" {
		t.Error("Expected non-empty AnsiBold")
	}
	if AnsiDim == "" {
		t.Error("Expected non-empty AnsiDim")
	}
}
