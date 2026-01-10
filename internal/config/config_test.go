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

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.BindAddr != ":9092" {
		t.Errorf("Expected BindAddr :9092, got %s", cfg.BindAddr)
	}
	if cfg.ClusterAddr != ":9093" {
		t.Errorf("Expected ClusterAddr :9093, got %s", cfg.ClusterAddr)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("Expected LogLevel info, got %s", cfg.LogLevel)
	}
	if cfg.SegmentBytes != 1024*1024*64 {
		t.Errorf("Expected SegmentBytes 64MB, got %d", cfg.SegmentBytes)
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr bool
	}{
		{
			name:    "valid default config",
			modify:  func(c *Config) {},
			wantErr: false,
		},
		{
			name:    "missing bind_addr",
			modify:  func(c *Config) { c.BindAddr = "" },
			wantErr: true,
		},
		{
			name:    "missing data_dir",
			modify:  func(c *Config) { c.DataDir = "" },
			wantErr: true,
		},
		{
			name: "TLS enabled without cert",
			modify: func(c *Config) {
				c.Security.TLSEnabled = true
				c.Security.TLSKeyFile = "/path/to/key"
			},
			wantErr: true,
		},
		{
			name: "TLS enabled without key",
			modify: func(c *Config) {
				c.Security.TLSEnabled = true
				c.Security.TLSCertFile = "/path/to/cert"
			},
			wantErr: true,
		},
		{
			name: "encryption enabled without key",
			modify: func(c *Config) {
				c.Security.EncryptionEnabled = true
			},
			wantErr: true,
		},
		{
			name: "encryption key wrong length",
			modify: func(c *Config) {
				c.Security.EncryptionEnabled = true
				c.Security.EncryptionKey = "tooshort"
			},
			wantErr: true,
		},
		{
			name: "invalid schema validation mode",
			modify: func(c *Config) {
				c.Schema.Enabled = true
				c.Schema.Validation = "invalid"
			},
			wantErr: true,
		},
		{
			name: "negative DLQ max retries",
			modify: func(c *Config) {
				c.DLQ.Enabled = true
				c.DLQ.MaxRetries = -1
			},
			wantErr: true,
		},
		{
			name: "negative TTL",
			modify: func(c *Config) {
				c.TTL.DefaultTTL = -1
			},
			wantErr: true,
		},
		{
			name: "invalid tracing sample rate",
			modify: func(c *Config) {
				c.Observability.Tracing.Enabled = true
				c.Observability.Tracing.SampleRate = 1.5
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestIsTLSEnabled(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.IsTLSEnabled() {
		t.Error("Expected TLS to be disabled by default")
	}

	cfg.Security.TLSEnabled = true
	cfg.Security.TLSCertFile = "/path/to/cert"
	cfg.Security.TLSKeyFile = "/path/to/key"
	if !cfg.IsTLSEnabled() {
		t.Error("Expected TLS to be enabled")
	}
}

func TestIsEncryptionEnabled(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.IsEncryptionEnabled() {
		t.Error("Expected encryption to be disabled by default")
	}

	cfg.Security.EncryptionEnabled = true
	cfg.Security.EncryptionKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if !cfg.IsEncryptionEnabled() {
		t.Error("Expected encryption to be enabled")
	}
}

func TestLoadFromFile(t *testing.T) {
	dir := t.TempDir()
	configFile := filepath.Join(dir, "config.json")

	configJSON := `{
		"bind_addr": ":8080",
		"data_dir": "/tmp/test",
		"log_level": "debug"
	}`

	if err := os.WriteFile(configFile, []byte(configJSON), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	mgr := &Manager{config: DefaultConfig()}
	if err := mgr.LoadFromFile(configFile); err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	cfg := mgr.Get()
	if cfg.BindAddr != ":8080" {
		t.Errorf("Expected BindAddr :8080, got %s", cfg.BindAddr)
	}
	if cfg.DataDir != "/tmp/test" {
		t.Errorf("Expected DataDir /tmp/test, got %s", cfg.DataDir)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("Expected LogLevel debug, got %s", cfg.LogLevel)
	}
}

func TestLoadFromEnv(t *testing.T) {
	// Save original env vars
	origBindAddr := os.Getenv(EnvBindAddr)
	origLogLevel := os.Getenv(EnvLogLevel)
	defer func() {
		os.Setenv(EnvBindAddr, origBindAddr)
		os.Setenv(EnvLogLevel, origLogLevel)
	}()

	os.Setenv(EnvBindAddr, ":7777")
	os.Setenv(EnvLogLevel, "warn")

	mgr := &Manager{config: DefaultConfig()}
	mgr.LoadFromEnv()

	cfg := mgr.Get()
	if cfg.BindAddr != ":7777" {
		t.Errorf("Expected BindAddr :7777, got %s", cfg.BindAddr)
	}
	if cfg.LogLevel != "warn" {
		t.Errorf("Expected LogLevel warn, got %s", cfg.LogLevel)
	}
}

func TestManagerGetSet(t *testing.T) {
	mgr := &Manager{config: DefaultConfig()}

	cfg := mgr.Get()
	cfg.BindAddr = ":1234"
	mgr.Set(cfg)

	newCfg := mgr.Get()
	if newCfg.BindAddr != ":1234" {
		t.Errorf("Expected BindAddr :1234, got %s", newCfg.BindAddr)
	}
}

func TestGlobalManager(t *testing.T) {
	mgr := Global()
	if mgr == nil {
		t.Fatal("Expected non-nil global manager")
	}
}

func TestGetDefaultDataDir(t *testing.T) {
	dir := GetDefaultDataDir()
	if dir == "" {
		t.Error("Expected non-empty default data dir")
	}
}
