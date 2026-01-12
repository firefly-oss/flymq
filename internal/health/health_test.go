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

package health

import (
	"errors"
	"testing"
)

func TestNewChecker(t *testing.T) {
	checker := NewChecker("1.0.0")
	if checker == nil {
		t.Fatal("Expected non-nil checker")
	}
}

func TestRegisterCheck(t *testing.T) {
	checker := NewChecker("1.0.0")

	checker.RegisterCheck("test", func() CheckResult {
		return CheckResult{Status: StatusHealthy}
	})

	response := checker.RunChecks()
	if len(response.Checks) != 1 {
		t.Errorf("Expected 1 check, got %d", len(response.Checks))
	}
}

func TestRunChecksAllHealthy(t *testing.T) {
	checker := NewChecker("1.0.0")

	checker.RegisterCheck("check1", func() CheckResult {
		return CheckResult{Status: StatusHealthy}
	})
	checker.RegisterCheck("check2", func() CheckResult {
		return CheckResult{Status: StatusHealthy}
	})

	response := checker.RunChecks()
	if response.Status != StatusHealthy {
		t.Errorf("Expected status healthy, got %s", response.Status)
	}
}

func TestRunChecksWithUnhealthy(t *testing.T) {
	checker := NewChecker("1.0.0")

	checker.RegisterCheck("healthy", func() CheckResult {
		return CheckResult{Status: StatusHealthy}
	})
	checker.RegisterCheck("unhealthy", func() CheckResult {
		return CheckResult{Status: StatusUnhealthy, Message: "service down"}
	})

	response := checker.RunChecks()
	if response.Status != StatusUnhealthy {
		t.Errorf("Expected status unhealthy, got %s", response.Status)
	}
}

func TestRunChecksWithDegraded(t *testing.T) {
	checker := NewChecker("1.0.0")

	checker.RegisterCheck("healthy", func() CheckResult {
		return CheckResult{Status: StatusHealthy}
	})
	checker.RegisterCheck("degraded", func() CheckResult {
		return CheckResult{Status: StatusDegraded, Message: "high latency"}
	})

	response := checker.RunChecks()
	if response.Status != StatusDegraded {
		t.Errorf("Expected status degraded, got %s", response.Status)
	}
}

func TestIsHealthy(t *testing.T) {
	checker := NewChecker("1.0.0")

	checker.RegisterCheck("check", func() CheckResult {
		return CheckResult{Status: StatusHealthy}
	})

	if !checker.IsHealthy() {
		t.Error("Expected IsHealthy to return true")
	}

	checker.RegisterCheck("bad", func() CheckResult {
		return CheckResult{Status: StatusUnhealthy}
	})

	if checker.IsHealthy() {
		t.Error("Expected IsHealthy to return false")
	}
}

func TestStorageCheck(t *testing.T) {
	// Healthy storage
	check := StorageCheck(func() error { return nil })
	result := check()
	if result.Status != StatusHealthy {
		t.Errorf("Expected healthy, got %s", result.Status)
	}

	// Unhealthy storage
	check = StorageCheck(func() error { return errors.New("disk full") })
	result = check()
	if result.Status != StatusUnhealthy {
		t.Errorf("Expected unhealthy, got %s", result.Status)
	}
}

func TestClusterCheck(t *testing.T) {
	// Sufficient nodes
	check := ClusterCheck(3, func() int { return 5 })
	result := check()
	if result.Status != StatusHealthy {
		t.Errorf("Expected healthy, got %s", result.Status)
	}

	// Insufficient nodes
	check = ClusterCheck(3, func() int { return 2 })
	result = check()
	if result.Status != StatusDegraded {
		t.Errorf("Expected degraded, got %s", result.Status)
	}
}

func TestMemoryCheck(t *testing.T) {
	// Normal usage
	check := MemoryCheck(80.0, func() float64 { return 50.0 })
	result := check()
	if result.Status != StatusHealthy {
		t.Errorf("Expected healthy, got %s", result.Status)
	}

	// High usage
	check = MemoryCheck(80.0, func() float64 { return 90.0 })
	result = check()
	if result.Status != StatusDegraded {
		t.Errorf("Expected degraded, got %s", result.Status)
	}
}

func TestDiskCheck(t *testing.T) {
	// Normal usage
	check := DiskCheck(90.0, func() float64 { return 50.0 })
	result := check()
	if result.Status != StatusHealthy {
		t.Errorf("Expected healthy, got %s", result.Status)
	}

	// High usage
	check = DiskCheck(90.0, func() float64 { return 95.0 })
	result = check()
	if result.Status != StatusDegraded {
		t.Errorf("Expected degraded, got %s", result.Status)
	}
}
