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
Cluster Membership Management using Gossip Protocol (SWIM).

OVERVIEW:
=========
Membership uses a gossip-based protocol for failure detection and
member discovery. Based on SWIM (Scalable Weakly-consistent
Infection-style Process Group Membership Protocol).

MEMBER STATES:
==============
- ALIVE: Member is healthy and responding
- SUSPECT: Member missed heartbeats, may be failing
- DEAD: Member confirmed failed, will be removed
- LEFT: Member gracefully left the cluster

FAILURE DETECTION:
==================
1. Each node periodically pings random members
2. If no response, node asks others to ping (indirect probe)
3. If still no response, member marked SUSPECT
4. After timeout, SUSPECT becomes DEAD

GOSSIP PROTOCOL:
================
State changes are disseminated via piggybacked gossip messages.
Each message includes recent state changes, spreading exponentially.
*/
package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"flymq/internal/logging"
)

// MemberStatus represents the status of a cluster member.
type MemberStatus int

const (
	MemberAlive MemberStatus = iota
	MemberSuspect
	MemberDead
	MemberLeft
)

func (s MemberStatus) String() string {
	switch s {
	case MemberAlive:
		return "alive"
	case MemberSuspect:
		return "suspect"
	case MemberDead:
		return "dead"
	case MemberLeft:
		return "left"
	default:
		return "unknown"
	}
}

// Member represents a cluster member with full metadata.
type Member struct {
	ID          string            `json:"id"`
	Address     string            `json:"address"`
	ClusterAddr string            `json:"cluster_addr"`
	AdminAddr   string            `json:"admin_addr,omitempty"`
	Status      MemberStatus      `json:"status"`
	RaftState   string            `json:"raft_state"` // "follower", "candidate", "leader"
	Metadata    map[string]string `json:"metadata"`
	JoinedAt    time.Time         `json:"joined_at"`
	LastSeen    time.Time         `json:"last_seen"`
	Stats       *MemberStats      `json:"stats,omitempty"`
}

// MemberStats contains runtime statistics for a cluster member.
type MemberStats struct {
	MemoryUsedMB     float64 `json:"memory_used_mb"`
	MemoryAllocMB    float64 `json:"memory_alloc_mb"`
	MemorySysMB      float64 `json:"memory_sys_mb"`
	Goroutines       int     `json:"goroutines"`
	NumGC            uint32  `json:"num_gc"`
	TopicCount       int     `json:"topic_count"`
	PartitionCount   int     `json:"partition_count"`
	MessagesReceived int64   `json:"messages_received"`
	MessagesSent     int64   `json:"messages_sent"`
	BytesReceived    int64   `json:"bytes_received"`
	BytesSent        int64   `json:"bytes_sent"`
}

// MembershipConfig holds configuration for membership management.
type MembershipConfig struct {
	NodeID         string
	Address        string // Bind address for client connections
	ClusterAddr    string // Bind address for cluster traffic
	AdvertiseAddr  string // Advertised cluster address (what other nodes use to reach us)
	DataDir        string
	GossipInterval time.Duration
	SuspectTimeout time.Duration
	DeadTimeout    time.Duration
	Peers          []string
}

// DefaultMembershipConfig returns default membership configuration.
func DefaultMembershipConfig() MembershipConfig {
	return MembershipConfig{
		GossipInterval: 1 * time.Second,
		SuspectTimeout: 5 * time.Second,
		DeadTimeout:    30 * time.Second,
	}
}

// MembershipManager manages cluster membership.
type MembershipManager struct {
	mu      sync.RWMutex
	config  MembershipConfig
	members map[string]*Member
	self    *Member
	logger  *logging.Logger

	// Event channels
	joinCh  chan *Member
	leaveCh chan *Member

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewMembershipManager creates a new membership manager.
func NewMembershipManager(config MembershipConfig) (*MembershipManager, error) {
	// Use advertise address if available, otherwise fall back to cluster address
	advertiseAddr := config.AdvertiseAddr
	if advertiseAddr == "" {
		advertiseAddr = config.ClusterAddr
	}

	self := &Member{
		ID:          config.NodeID,
		Address:     config.Address,
		ClusterAddr: advertiseAddr, // Use advertise address for cluster communication
		Status:      MemberAlive,
		Metadata:    make(map[string]string),
		JoinedAt:    time.Now(),
		LastSeen:    time.Now(),
	}

	m := &MembershipManager{
		config:  config,
		members: make(map[string]*Member),
		self:    self,
		joinCh:  make(chan *Member, 100),
		leaveCh: make(chan *Member, 100),
		stopCh:  make(chan struct{}),
		logger:  logging.NewLogger("membership"),
	}

	// Add self to members
	m.members[self.ID] = self

	// Add peers as initial members
	// Note: For static peer configuration (like Docker Compose), peers are identified by their address.
	// In production deployments with dynamic discovery, we'd use a seed node handshake protocol
	// to exchange node IDs. For now, we use the peer address as a temporary ID until
	// proper discovery updates it.
	for _, peer := range config.Peers {
		// Normalize peer address for consistent handling
		peer = normalizePeerAddress(peer)

		// Skip if peer is our own address (bind or advertise)
		if peer == config.ClusterAddr || peer == advertiseAddr || peer == normalizePeerAddress(config.ClusterAddr) || peer == normalizePeerAddress(advertiseAddr) {
			continue
		}
		if _, exists := m.members[peer]; !exists {
			m.members[peer] = &Member{
				ID:          peer, // Temporary ID until we get node info exchange
				Address:     peer,
				ClusterAddr: peer,
				Status:      MemberAlive, // Assume alive initially for static peers
				JoinedAt:    time.Now(),
				LastSeen:    time.Now(),
			}
			m.logger.Info("Added peer to membership", "peer", peer)
		}
	}

	// Load persisted membership
	if err := m.loadMembership(); err != nil {
		m.logger.Warn("Failed to load membership", "error", err)
	}

	return m, nil
}

// Start begins the membership manager.
func (m *MembershipManager) Start() error {
	m.logger.Info("Starting membership manager", "node_id", m.config.NodeID)

	m.wg.Add(1)
	go m.healthCheckLoop()

	return nil
}

// Stop stops the membership manager.
func (m *MembershipManager) Stop() error {
	close(m.stopCh)
	m.wg.Wait()
	return m.saveMembership()
}

// Join adds a new member to the cluster.
func (m *MembershipManager) Join(member *Member) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.members[member.ID]; exists {
		// Update existing member
		m.members[member.ID].Status = MemberAlive
		m.members[member.ID].LastSeen = time.Now()
		return nil
	}

	member.JoinedAt = time.Now()
	member.LastSeen = time.Now()
	member.Status = MemberAlive
	m.members[member.ID] = member

	m.logger.Info("Member joined", "member_id", member.ID, "address", member.Address)

	// Notify listeners
	select {
	case m.joinCh <- member:
	default:
	}

	return m.saveMembership()
}

// Leave removes a member from the cluster.
func (m *MembershipManager) Leave(memberID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	member, exists := m.members[memberID]
	if !exists {
		return fmt.Errorf("member not found: %s", memberID)
	}

	member.Status = MemberLeft
	m.logger.Info("Member left", "member_id", memberID)

	// Notify listeners
	select {
	case m.leaveCh <- member:
	default:
	}

	return m.saveMembership()
}

// GetMember returns a member by ID.
func (m *MembershipManager) GetMember(id string) (*Member, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	member, ok := m.members[id]
	return member, ok
}

// GetMembers returns all active members.
func (m *MembershipManager) GetMembers() []*Member {
	m.mu.RLock()
	defer m.mu.RUnlock()

	members := make([]*Member, 0, len(m.members))
	for _, member := range m.members {
		if member.Status == MemberAlive {
			members = append(members, member)
		}
	}
	return members
}

// GetAllMembers returns all members regardless of status.
func (m *MembershipManager) GetAllMembers() []*Member {
	m.mu.RLock()
	defer m.mu.RUnlock()

	members := make([]*Member, 0, len(m.members))
	for _, member := range m.members {
		members = append(members, member)
	}
	return members
}

// Self returns this node's member info.
func (m *MembershipManager) Self() *Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.self
}

// UpdateLastSeen updates the last seen time for a member.
// It tries to find the member by ID first, then by address if not found.
// If the member is not found and the address looks valid, it adds the member.
func (m *MembershipManager) UpdateLastSeen(memberIDOrAddr string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	// Try to find by ID first
	if member, ok := m.members[memberIDOrAddr]; ok {
		member.LastSeen = now
		if member.Status == MemberSuspect {
			member.Status = MemberAlive
			m.logger.Debug("Member recovered from suspect", "member_id", memberIDOrAddr)
		}
		return
	}

	// Try to find by address (for peers added by address before we know their ID)
	for _, member := range m.members {
		if member.Address == memberIDOrAddr || member.ClusterAddr == memberIDOrAddr {
			member.LastSeen = now
			if member.Status == MemberSuspect {
				member.Status = MemberAlive
				m.logger.Debug("Member recovered from suspect", "member_id", member.ID, "address", memberIDOrAddr)
			}
			return
		}
	}

	// If we couldn't find the member but it looks like a valid address,
	// add it as a new member (discovered through Raft communication)
	if isValidAddress(memberIDOrAddr) && memberIDOrAddr != m.self.ClusterAddr {
		newMember := &Member{
			ID:          memberIDOrAddr, // Use address as temporary ID
			Address:     memberIDOrAddr,
			ClusterAddr: memberIDOrAddr,
			Status:      MemberAlive,
			Metadata:    make(map[string]string),
			JoinedAt:    now,
			LastSeen:    now,
		}
		m.members[memberIDOrAddr] = newMember
		m.logger.Info("Discovered new member via Raft", "address", memberIDOrAddr)

		// Notify listeners
		select {
		case m.joinCh <- newMember:
		default:
		}
		return
	}

	// If we couldn't find the member, log it for debugging
	m.logger.Debug("UpdateLastSeen: member not found", "lookup", memberIDOrAddr)
}

// isValidAddress checks if a string looks like a valid host:port address.
func isValidAddress(addr string) bool {
	if addr == "" {
		return false
	}
	// Check for host:port format
	lastColon := strings.LastIndex(addr, ":")
	if lastColon == -1 || lastColon == len(addr)-1 {
		return false
	}
	// Check that port is numeric
	port := addr[lastColon+1:]
	for _, c := range port {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// JoinEvents returns a channel for join events.
func (m *MembershipManager) JoinEvents() <-chan *Member {
	return m.joinCh
}

// LeaveEvents returns a channel for leave events.
func (m *MembershipManager) LeaveEvents() <-chan *Member {
	return m.leaveCh
}

func (m *MembershipManager) healthCheckLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.checkMemberHealth()
		}
	}
}

func (m *MembershipManager) checkMemberHealth() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for id, member := range m.members {
		if id == m.self.ID {
			continue
		}

		timeSinceLastSeen := now.Sub(member.LastSeen)

		switch member.Status {
		case MemberAlive:
			if timeSinceLastSeen > m.config.SuspectTimeout {
				member.Status = MemberSuspect
				m.logger.Warn("Member suspected", "member_id", id, "last_seen", member.LastSeen)
			}
		case MemberSuspect:
			if timeSinceLastSeen > m.config.DeadTimeout {
				member.Status = MemberDead
				m.logger.Warn("Member declared dead", "member_id", id, "last_seen", member.LastSeen)
				select {
				case m.leaveCh <- member:
				default:
				}
			}
		}
	}
}

func (m *MembershipManager) loadMembership() error {
	memberFile := filepath.Join(m.config.DataDir, "membership.json")
	data, err := os.ReadFile(memberFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var members []*Member
	if err := json.Unmarshal(data, &members); err != nil {
		return err
	}

	for _, member := range members {
		if member.ID != m.self.ID {
			member.Status = MemberSuspect // Mark as suspect until we hear from them
			m.members[member.ID] = member
		}
	}

	return nil
}

func (m *MembershipManager) saveMembership() error {
	if err := os.MkdirAll(m.config.DataDir, 0755); err != nil {
		return err
	}

	members := make([]*Member, 0, len(m.members))
	for _, member := range m.members {
		members = append(members, member)
	}

	data, err := json.MarshalIndent(members, "", "  ")
	if err != nil {
		return err
	}

	memberFile := filepath.Join(m.config.DataDir, "membership.json")
	return os.WriteFile(memberFile, data, 0644)
}

// normalizePeerAddress converts localhost to 127.0.0.1 for consistent addressing
func normalizePeerAddress(peer string) string {
	if len(peer) >= 9 && peer[:9] == "localhost" {
		return "127.0.0.1" + peer[9:]
	}
	return peer
}
