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

package broker

import (
	"testing"
)

func TestNewConsumerGroupManager(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewConsumerGroupManager(dir)
	if err != nil {
		t.Fatalf("NewConsumerGroupManager failed: %v", err)
	}
	if mgr == nil {
		t.Fatal("Expected non-nil manager")
	}
}

func TestGetOrCreateGroup(t *testing.T) {
	dir := t.TempDir()
	mgr, _ := NewConsumerGroupManager(dir)

	group := mgr.GetOrCreateGroup("test-topic", "test-group")
	if group == nil {
		t.Fatal("Expected non-nil group")
	}
	if group.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got %s", group.Topic)
	}
	if group.GroupID != "test-group" {
		t.Errorf("Expected group ID 'test-group', got %s", group.GroupID)
	}

	// Get same group again
	group2 := mgr.GetOrCreateGroup("test-topic", "test-group")
	if group != group2 {
		t.Error("Expected same group instance")
	}
}

func TestCommitAndGetOffset(t *testing.T) {
	dir := t.TempDir()
	mgr, _ := NewConsumerGroupManager(dir)

	changed, err := mgr.CommitOffset("test-topic", "test-group", 0, 100)
	if err != nil {
		t.Fatalf("CommitOffset failed: %v", err)
	}
	if !changed {
		t.Error("Expected changed to be true for first commit")
	}

	offset, exists := mgr.GetCommittedOffset("test-topic", "test-group", 0)
	if !exists {
		t.Error("Expected offset to exist")
	}
	if offset != 100 {
		t.Errorf("Expected offset 100, got %d", offset)
	}

	// Commit same offset again - should not change
	changed, err = mgr.CommitOffset("test-topic", "test-group", 0, 100)
	if err != nil {
		t.Fatalf("CommitOffset failed: %v", err)
	}
	if changed {
		t.Error("Expected changed to be false for same offset")
	}

	// Commit different offset - should change
	changed, err = mgr.CommitOffset("test-topic", "test-group", 0, 101)
	if err != nil {
		t.Fatalf("CommitOffset failed: %v", err)
	}
	if !changed {
		t.Error("Expected changed to be true for new offset")
	}
}

func TestGetNonExistentOffset(t *testing.T) {
	dir := t.TempDir()
	mgr, _ := NewConsumerGroupManager(dir)

	_, exists := mgr.GetCommittedOffset("non-existent", "non-existent", 0)
	if exists {
		t.Error("Expected offset to not exist")
	}
}

func TestMultiplePartitionOffsets(t *testing.T) {
	dir := t.TempDir()
	mgr, _ := NewConsumerGroupManager(dir)

	// Commit offsets for multiple partitions
	_, _ = mgr.CommitOffset("test-topic", "test-group", 0, 100)
	_, _ = mgr.CommitOffset("test-topic", "test-group", 1, 200)
	_, _ = mgr.CommitOffset("test-topic", "test-group", 2, 300)

	tests := []struct {
		partition int
		expected  uint64
	}{
		{0, 100},
		{1, 200},
		{2, 300},
	}

	for _, tt := range tests {
		offset, exists := mgr.GetCommittedOffset("test-topic", "test-group", tt.partition)
		if !exists {
			t.Errorf("Partition %d: expected offset to exist", tt.partition)
		}
		if offset != tt.expected {
			t.Errorf("Partition %d: expected %d, got %d", tt.partition, tt.expected, offset)
		}
	}
}

func TestMultipleGroups(t *testing.T) {
	dir := t.TempDir()
	mgr, _ := NewConsumerGroupManager(dir)

	// Commit offsets for different groups
	_, _ = mgr.CommitOffset("test-topic", "group-a", 0, 100)
	_, _ = mgr.CommitOffset("test-topic", "group-b", 0, 200)

	offsetA, _ := mgr.GetCommittedOffset("test-topic", "group-a", 0)
	offsetB, _ := mgr.GetCommittedOffset("test-topic", "group-b", 0)

	if offsetA != 100 {
		t.Errorf("Group A: expected 100, got %d", offsetA)
	}
	if offsetB != 200 {
		t.Errorf("Group B: expected 200, got %d", offsetB)
	}
}

func TestOffsetPersistence(t *testing.T) {
	dir := t.TempDir()

	// Create manager and commit offset
	mgr1, _ := NewConsumerGroupManager(dir)
	_, _ = mgr1.CommitOffset("test-topic", "test-group", 0, 500)

	// Create new manager (simulating restart)
	mgr2, err := NewConsumerGroupManager(dir)
	if err != nil {
		t.Fatalf("NewConsumerGroupManager failed: %v", err)
	}

	offset, exists := mgr2.GetCommittedOffset("test-topic", "test-group", 0)
	if !exists {
		t.Error("Expected offset to exist after reload")
	}
	if offset != 500 {
		t.Errorf("Expected offset 500 after reload, got %d", offset)
	}
}
