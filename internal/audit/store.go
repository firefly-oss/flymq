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

package audit

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// FileStore implements Store using append-only log files.
type FileStore struct {
	dir           string
	currentFile   *os.File
	currentDate   string
	mu            sync.RWMutex
	nodeID        string
	maxFileSize   int64
	retentionDays int
}

// FileStoreConfig contains configuration for FileStore.
type FileStoreConfig struct {
	Dir           string // Directory for audit logs
	NodeID        string // Node ID for cluster identification
	MaxFileSize   int64  // Maximum file size before rotation (default: 100MB)
	RetentionDays int    // Days to retain audit logs (default: 90)
}

// NewFileStore creates a new file-based audit store.
func NewFileStore(cfg FileStoreConfig) (*FileStore, error) {
	if cfg.Dir == "" {
		cfg.Dir = "data/audit"
	}
	if cfg.MaxFileSize == 0 {
		cfg.MaxFileSize = 100 * 1024 * 1024 // 100MB
	}
	if cfg.RetentionDays == 0 {
		cfg.RetentionDays = 90
	}

	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create audit directory: %w", err)
	}

	store := &FileStore{
		dir:           cfg.Dir,
		nodeID:        cfg.NodeID,
		maxFileSize:   cfg.MaxFileSize,
		retentionDays: cfg.RetentionDays,
	}

	return store, nil
}

// Record stores a new audit event.
func (s *FileStore) Record(event *Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Generate ID if not set
	if event.ID == "" {
		event.ID = uuid.New().String()
	}

	// Set timestamp if not set
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	// Set node ID
	if event.NodeID == "" {
		event.NodeID = s.nodeID
	}

	// Ensure we have the right file open
	if err := s.ensureFile(); err != nil {
		return err
	}

	// Encode event as JSON
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Write with newline
	if _, err := s.currentFile.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write event: %w", err)
	}

	// Sync to disk
	return s.currentFile.Sync()
}

// ensureFile ensures the current file is open and valid.
func (s *FileStore) ensureFile() error {
	today := time.Now().UTC().Format("2006-01-02")

	// Check if we need a new file (date change or size limit)
	needNewFile := s.currentFile == nil || s.currentDate != today

	if !needNewFile && s.currentFile != nil {
		info, err := s.currentFile.Stat()
		if err == nil && info.Size() >= s.maxFileSize {
			needNewFile = true
		}
	}

	if needNewFile {
		if s.currentFile != nil {
			s.currentFile.Close()
		}

		filename := s.getFilename(today)
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open audit file: %w", err)
		}

		s.currentFile = file
		s.currentDate = today
	}

	return nil
}

// getFilename returns the filename for a given date.
func (s *FileStore) getFilename(date string) string {
	return filepath.Join(s.dir, fmt.Sprintf("audit-%s.log", date))
}

// Close closes the store.
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.currentFile != nil {
		return s.currentFile.Close()
	}
	return nil
}

// Query retrieves events matching the filter.
func (s *FileStore) Query(filter *QueryFilter) (*QueryResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if filter == nil {
		filter = &QueryFilter{}
	}
	if filter.Limit == 0 {
		filter.Limit = 100
	}

	// Get list of files to scan
	files, err := s.getFilesToScan(filter)
	if err != nil {
		return nil, err
	}

	var allEvents []Event
	for _, file := range files {
		events, err := s.readEventsFromFile(file, filter)
		if err != nil {
			continue // Skip files that can't be read
		}
		allEvents = append(allEvents, events...)
	}

	// Sort by timestamp descending (newest first)
	sort.Slice(allEvents, func(i, j int) bool {
		return allEvents[i].Timestamp.After(allEvents[j].Timestamp)
	})

	totalCount := len(allEvents)

	// Apply pagination
	start := filter.Offset
	if start > len(allEvents) {
		start = len(allEvents)
	}
	end := start + filter.Limit
	if end > len(allEvents) {
		end = len(allEvents)
	}

	return &QueryResult{
		Events:     allEvents[start:end],
		TotalCount: totalCount,
		HasMore:    end < totalCount,
	}, nil
}

// getFilesToScan returns the list of audit files to scan based on the filter.
func (s *FileStore) getFilesToScan(filter *QueryFilter) ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read audit directory: %w", err)
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "audit-") {
			continue
		}
		files = append(files, filepath.Join(s.dir, entry.Name()))
	}

	// Sort files by name (which includes date) in reverse order
	sort.Sort(sort.Reverse(sort.StringSlice(files)))
	return files, nil
}

// readEventsFromFile reads and filters events from a single file.
func (s *FileStore) readEventsFromFile(filename string, filter *QueryFilter) ([]Event, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var events []Event
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB buffer

	for scanner.Scan() {
		var event Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			continue // Skip malformed lines
		}

		if s.matchesFilter(&event, filter) {
			events = append(events, event)
		}
	}

	return events, scanner.Err()
}

// matchesFilter checks if an event matches the query filter.
func (s *FileStore) matchesFilter(event *Event, filter *QueryFilter) bool {
	// Time range filter
	if filter.StartTime != nil && event.Timestamp.Before(*filter.StartTime) {
		return false
	}
	if filter.EndTime != nil && event.Timestamp.After(*filter.EndTime) {
		return false
	}

	// Event type filter
	if len(filter.EventTypes) > 0 {
		found := false
		for _, t := range filter.EventTypes {
			if event.Type == t {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// User filter
	if filter.User != "" && event.User != filter.User {
		return false
	}

	// Resource filter
	if filter.Resource != "" && !strings.Contains(event.Resource, filter.Resource) {
		return false
	}

	// Result filter
	if filter.Result != "" && event.Result != filter.Result {
		return false
	}

	// Full-text search
	if filter.Search != "" {
		searchLower := strings.ToLower(filter.Search)
		found := strings.Contains(strings.ToLower(event.Action), searchLower) ||
			strings.Contains(strings.ToLower(event.Resource), searchLower)
		if !found {
			for _, v := range event.Details {
				if strings.Contains(strings.ToLower(v), searchLower) {
					found = true
					break
				}
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// Export exports events to the specified format.
func (s *FileStore) Export(filter *QueryFilter, format ExportFormat) ([]byte, error) {
	// Get all matching events (no pagination for export)
	exportFilter := *filter
	exportFilter.Limit = 0
	exportFilter.Offset = 0

	result, err := s.Query(&exportFilter)
	if err != nil {
		return nil, err
	}

	switch format {
	case ExportJSON:
		return json.MarshalIndent(result.Events, "", "  ")
	case ExportCSV:
		return s.exportCSV(result.Events)
	default:
		return nil, fmt.Errorf("unsupported export format: %s", format)
	}
}

// exportCSV exports events to CSV format.
func (s *FileStore) exportCSV(events []Event) ([]byte, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	// Write header
	header := []string{"ID", "Timestamp", "Type", "User", "ClientIP", "Resource", "Action", "Result", "NodeID", "Details"}
	if err := writer.Write(header); err != nil {
		return nil, err
	}

	// Write events
	for _, event := range events {
		details, _ := json.Marshal(event.Details)
		row := []string{
			event.ID,
			event.Timestamp.Format(time.RFC3339),
			string(event.Type),
			event.User,
			event.ClientIP,
			event.Resource,
			event.Action,
			event.Result,
			event.NodeID,
			string(details),
		}
		if err := writer.Write(row); err != nil {
			return nil, err
		}
	}

	writer.Flush()
	return buf.Bytes(), writer.Error()
}

