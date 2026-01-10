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
Consumer Group Management for FlyMQ.

CONSUMER GROUPS:
================
A consumer group is a set of consumers that cooperatively consume messages
from a topic. Key concepts:

1. LOAD BALANCING: Partitions are distributed among consumers in the group
2. FAULT TOLERANCE: If a consumer fails, its partitions are reassigned
3. OFFSET TRACKING: Each group tracks its progress independently

OFFSET MANAGEMENT:
==================
- Offsets are stored per (topic, group, partition) tuple
- Consumers commit offsets after processing messages
- On restart, consumers resume from the last committed offset
- Offsets are persisted to disk as JSON files

STORAGE:
========
Offsets are stored in: {DataDir}/__consumer_offsets/{topic}_{group}.json

Example file content:

	{
	  "group_id": "my-group",
	  "topic": "orders",
	  "offsets": {
	    "0": 1234,
	    "1": 5678
	  }
	}
*/
package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ConsumerGroup tracks the state of a group of consumers for a topic.
// Each group maintains independent offsets for each partition.
type ConsumerGroup struct {
	GroupID string         `json:"group_id"` // Unique group identifier
	Topic   string         `json:"topic"`    // Topic being consumed
	Offsets map[int]uint64 `json:"offsets"`  // partition ID -> committed offset
	mu      sync.RWMutex   // Protects offset updates
}

// ConsumerGroupManager manages all consumer groups.
type ConsumerGroupManager struct {
	dataDir string
	groups  map[string]*ConsumerGroup // key: "topic:group_id"
	mu      sync.RWMutex
}

// NewConsumerGroupManager creates a new consumer group manager.
func NewConsumerGroupManager(dataDir string) (*ConsumerGroupManager, error) {
	mgr := &ConsumerGroupManager{
		dataDir: filepath.Join(dataDir, "__consumer_offsets"),
		groups:  make(map[string]*ConsumerGroup),
	}
	if err := os.MkdirAll(mgr.dataDir, 0755); err != nil {
		return nil, err
	}
	if err := mgr.loadGroups(); err != nil {
		return nil, err
	}
	return mgr, nil
}

func (m *ConsumerGroupManager) loadGroups() error {
	entries, err := os.ReadDir(m.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			data, err := os.ReadFile(filepath.Join(m.dataDir, entry.Name()))
			if err != nil {
				continue
			}
			var group ConsumerGroup
			if err := json.Unmarshal(data, &group); err != nil {
				continue
			}
			key := fmt.Sprintf("%s:%s", group.Topic, group.GroupID)
			m.groups[key] = &group
		}
	}
	return nil
}

// GetOrCreateGroup gets or creates a consumer group.
func (m *ConsumerGroupManager) GetOrCreateGroup(topic, groupID string) *ConsumerGroup {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s", topic, groupID)
	if group, exists := m.groups[key]; exists {
		return group
	}

	group := &ConsumerGroup{
		GroupID: groupID,
		Topic:   topic,
		Offsets: make(map[int]uint64),
	}
	m.groups[key] = group
	return group
}

// GetCommittedOffset returns the committed offset for a partition.
func (m *ConsumerGroupManager) GetCommittedOffset(topic, groupID string, partition int) (uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", topic, groupID)
	group, exists := m.groups[key]
	if !exists {
		return 0, false
	}

	group.mu.RLock()
	defer group.mu.RUnlock()
	offset, exists := group.Offsets[partition]
	return offset, exists
}

// CommitOffset commits an offset for a consumer group.
func (m *ConsumerGroupManager) CommitOffset(topic, groupID string, partition int, offset uint64) error {
	group := m.GetOrCreateGroup(topic, groupID)

	group.mu.Lock()
	group.Offsets[partition] = offset
	group.mu.Unlock()

	return m.persistGroup(group)
}

func (m *ConsumerGroupManager) persistGroup(group *ConsumerGroup) error {
	group.mu.RLock()
	data, err := json.MarshalIndent(group, "", "  ")
	group.mu.RUnlock()
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("%s_%s.json", group.Topic, group.GroupID)
	return os.WriteFile(filepath.Join(m.dataDir, filename), data, 0644)
}

// ListGroups returns all consumer groups.
func (m *ConsumerGroupManager) ListGroups() []*ConsumerGroup {
	m.mu.RLock()
	defer m.mu.RUnlock()

	groups := make([]*ConsumerGroup, 0, len(m.groups))
	for _, group := range m.groups {
		groups = append(groups, group)
	}
	return groups
}

// GetGroup returns a consumer group by topic and group ID.
func (m *ConsumerGroupManager) GetGroup(topic, groupID string) (*ConsumerGroup, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", topic, groupID)
	group, exists := m.groups[key]
	return group, exists
}

// DeleteGroup removes a consumer group.
func (m *ConsumerGroupManager) DeleteGroup(topic, groupID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s", topic, groupID)
	if _, exists := m.groups[key]; !exists {
		return fmt.Errorf("consumer group not found: %s:%s", topic, groupID)
	}

	delete(m.groups, key)

	// Remove the persisted file
	filename := fmt.Sprintf("%s_%s.json", topic, groupID)
	filePath := filepath.Join(m.dataDir, filename)
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
