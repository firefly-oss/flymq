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
Raft Consensus Implementation for FlyMQ.

RAFT OVERVIEW:
==============
Raft is a consensus algorithm that ensures all nodes agree on the same
sequence of commands. It's used for cluster metadata coordination.

NODE STATES:
============
- FOLLOWER: Passive, responds to leader requests
- CANDIDATE: Seeking election as leader
- LEADER: Handles all client requests, replicates to followers

LEADER ELECTION:
================
1. Follower times out waiting for heartbeat
2. Becomes candidate, increments term, votes for self
3. Requests votes from other nodes
4. Wins if receives majority of votes
5. Becomes leader and starts sending heartbeats

LOG REPLICATION:
================
1. Leader receives command from client
2. Appends to local log
3. Sends AppendEntries to followers
4. Waits for majority acknowledgment
5. Commits entry and applies to state machine

SAFETY GUARANTEES:
==================
- Election Safety: At most one leader per term
- Leader Append-Only: Leader never overwrites log entries
- Log Matching: If logs match at an index, they match for all prior
- Leader Completeness: Committed entries appear in future leaders' logs
*/
package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"flymq/internal/logging"
)

// NodeState represents the state of a Raft node.
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Term    uint64          `json:"term"`
	Index   uint64          `json:"index"`
	Command json.RawMessage `json:"command"`
}

// RaftConfig holds configuration for the Raft node.
type RaftConfig struct {
	NodeID            string
	ClusterAddr       string // Address to bind for cluster traffic
	AdvertiseAddr     string // Address to advertise to other nodes (used in Raft messages)
	ClientAddr        string // Client-facing address (for client failover)
	DataDir           string
	Peers             []string
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	// AsyncReplication enables async replication mode for better throughput.
	// When true, Apply() returns immediately after local append, replication happens in background.
	// This trades durability for performance (similar to Kafka's acks=1).
	AsyncReplication bool
	// ReplicationTimeout is the timeout for waiting for replication acknowledgments.
	ReplicationTimeout time.Duration
}

// DefaultRaftConfig returns default Raft configuration.
func DefaultRaftConfig() RaftConfig {
	return RaftConfig{
		ElectionTimeout:    150 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		AsyncReplication:   false,
		ReplicationTimeout: 5 * time.Second,
	}
}

// StatsCollector provides local node stats.
type StatsCollector func() *NodeStats

// RaftNode implements the Raft consensus algorithm.
type RaftNode struct {
	mu sync.RWMutex

	// Persistent state
	currentTerm uint64
	votedFor    string
	log         []LogEntry

	// Volatile state
	commitIndex      uint64
	lastApplied      uint64
	state            NodeState
	leaderID         string // Track the current leader ID
	leaderClientAddr string // Track the leader's client-facing address for failover

	// Leader state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Peer stats (collected from AppendEntries responses)
	peerStats map[string]*NodeStats

	// Configuration
	config RaftConfig
	peers  map[string]*Peer

	// Channels
	applyCh  chan LogEntry
	stopCh   chan struct{}
	voteCh   chan *VoteRequest
	appendCh chan *AppendEntriesRequest

	// Timers
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// Transport
	transport Transport

	// Stats collector callback
	statsCollector StatsCollector

	// Peer activity callback - called when we receive a message from a peer
	peerActivityCallback func(peerID string)

	logger *logging.Logger
}

// VoteRequest represents a RequestVote RPC.
type VoteRequest struct {
	Term          uint64 `json:"term"`
	CandidateID   string `json:"candidate_id"`
	CandidateAddr string `json:"candidate_addr,omitempty"` // Candidate's advertised cluster address
	LastLogIndex  uint64 `json:"last_log_index"`
	LastLogTerm   uint64 `json:"last_log_term"`
}

// VoteResponse represents a RequestVote RPC response.
type VoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	VoterID     string `json:"voter_id,omitempty"`   // Voter's node ID
	VoterAddr   string `json:"voter_addr,omitempty"` // Voter's advertised cluster address
}

// AppendEntriesRequest represents an AppendEntries RPC.
type AppendEntriesRequest struct {
	Term             uint64     `json:"term"`
	LeaderID         string     `json:"leader_id"`
	LeaderAddr       string     `json:"leader_addr,omitempty"`        // Leader's cluster address for membership tracking
	LeaderClientAddr string     `json:"leader_client_addr,omitempty"` // Leader's client-facing address for client failover
	PrevLogIndex     uint64     `json:"prev_log_index"`
	PrevLogTerm      uint64     `json:"prev_log_term"`
	Entries          []LogEntry `json:"entries"`
	LeaderCommit     uint64     `json:"leader_commit"`
}

// AppendEntriesResponse represents an AppendEntries RPC response.
type AppendEntriesResponse struct {
	Term          uint64     `json:"term"`
	Success       bool       `json:"success"`
	NodeStats     *NodeStats `json:"node_stats,omitempty"`
	ResponderID   string     `json:"responder_id,omitempty"`   // Responder's node ID
	ResponderAddr string     `json:"responder_addr,omitempty"` // Responder's advertised cluster address
}

// NodeStats contains runtime statistics for a node.
type NodeStats struct {
	NodeID           string  `json:"node_id"`
	Address          string  `json:"address"`
	ClusterAddr      string  `json:"cluster_addr"`
	RaftState        string  `json:"raft_state"`
	MemoryUsedMB     float64 `json:"memory_used_mb"`
	MemoryAllocMB    float64 `json:"memory_alloc_mb"`
	MemorySysMB      float64 `json:"memory_sys_mb"`
	Goroutines       int     `json:"goroutines"`
	NumGC            uint32  `json:"num_gc"`
	TopicCount       int     `json:"topic_count"`
	PartitionCount   int     `json:"partition_count"`
	MessagesReceived int64   `json:"messages_received"`
	MessagesSent     int64   `json:"messages_sent"`
	Uptime           string  `json:"uptime"`
}

// Peer represents a remote Raft node.
type Peer struct {
	ID      string
	Address string
}

// Transport defines the interface for Raft RPC communication.
type Transport interface {
	SendVoteRequest(peer string, req *VoteRequest) (*VoteResponse, error)
	SendAppendEntries(peer string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	SetVoteHandler(handler func(*VoteRequest) *VoteResponse)
	SetAppendHandler(handler func(*AppendEntriesRequest) *AppendEntriesResponse)
	Start() error
	Stop() error
}

// NewRaftNode creates a new Raft node.
func NewRaftNode(config RaftConfig, transport Transport, applyCh chan LogEntry) (*RaftNode, error) {
	if config.ElectionTimeout == 0 {
		config.ElectionTimeout = 150 * time.Millisecond
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 50 * time.Millisecond
	}
	if config.ReplicationTimeout == 0 {
		config.ReplicationTimeout = 5 * time.Second
	}

	node := &RaftNode{
		config:     config,
		transport:  transport,
		applyCh:    applyCh,
		stopCh:     make(chan struct{}),
		voteCh:     make(chan *VoteRequest, 100),
		appendCh:   make(chan *AppendEntriesRequest, 100),
		state:      Follower,
		peers:      make(map[string]*Peer),
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
		peerStats:  make(map[string]*NodeStats),
		logger:     logging.NewLogger("raft"),
	}

	// Initialize peers (exclude self)
	for _, addr := range config.Peers {
		// Skip if the peer address matches our own node ID or cluster address
		if addr == config.NodeID {
			continue
		}
		node.peers[addr] = &Peer{ID: addr, Address: addr}
	}

	// Load persistent state
	if err := node.loadState(); err != nil {
		return nil, fmt.Errorf("failed to load raft state: %w", err)
	}

	// Register transport handlers
	transport.SetVoteHandler(node.handleVoteRequest)
	transport.SetAppendHandler(node.handleAppendEntries)

	return node, nil
}

// Start begins the Raft node operation.
func (n *RaftNode) Start(ctx context.Context) error {
	n.logger.Info("Starting Raft node", "node_id", n.config.NodeID, "state", n.state.String())

	if err := n.transport.Start(); err != nil {
		return fmt.Errorf("failed to start transport: %w", err)
	}

	// Start election timer
	n.resetElectionTimer()

	go n.run(ctx)
	return nil
}

// Stop gracefully stops the Raft node.
func (n *RaftNode) Stop() error {
	close(n.stopCh)
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	return n.transport.Stop()
}

// State returns the current state of the node.
func (n *RaftNode) State() NodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

// IsLeader returns true if this node is the leader.
func (n *RaftNode) IsLeader() bool {
	return n.State() == Leader
}

// IsSingleNode returns true if this is a single-node cluster (no peers).
func (n *RaftNode) IsSingleNode() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.peers) == 0
}

// Term returns the current term.
func (n *RaftNode) Term() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}

// LeaderID returns the current leader ID (empty if unknown).
func (n *RaftNode) LeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.state == Leader {
		return n.config.NodeID
	}
	return n.leaderID
}

// LeaderClientAddr returns the leader's client-facing address for client failover.
// Returns empty string if we are the leader or if the leader's address is unknown.
func (n *RaftNode) LeaderClientAddr() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.state == Leader {
		return n.config.ClientAddr
	}
	return n.leaderClientAddr
}

// CurrentTerm returns the current Raft term.
func (n *RaftNode) CurrentTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}

// CommitIndex returns the current commit index.
func (n *RaftNode) CommitIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.commitIndex
}

// LastApplied returns the last applied log index.
func (n *RaftNode) LastApplied() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastApplied
}

// LogLength returns the number of entries in the log.
func (n *RaftNode) LogLength() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.log)
}

// SetStatsCollector sets the callback for collecting local node stats.
func (n *RaftNode) SetStatsCollector(collector StatsCollector) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.statsCollector = collector
}

// SetPeerActivityCallback sets the callback for peer activity notifications.
// This is called when we receive any message from a peer (heartbeat, vote, etc).
func (n *RaftNode) SetPeerActivityCallback(callback func(peerID string)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peerActivityCallback = callback
}

// notifyPeerActivity calls the peer activity callback if set.
func (n *RaftNode) notifyPeerActivity(peerID string) {
	n.mu.RLock()
	callback := n.peerActivityCallback
	n.mu.RUnlock()
	if callback != nil {
		callback(peerID)
	}
}

// GetPeerStats returns stats for all known peers.
func (n *RaftNode) GetPeerStats() map[string]*NodeStats {
	n.mu.RLock()
	defer n.mu.RUnlock()
	// Return a copy
	result := make(map[string]*NodeStats, len(n.peerStats))
	for k, v := range n.peerStats {
		result[k] = v
	}
	return result
}

// updatePeerStats stores stats received from a peer.
func (n *RaftNode) updatePeerStats(peerID string, stats *NodeStats) {
	if stats == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peerStats[peerID] = stats
}

// getLocalStats returns local node stats using the stats collector.
func (n *RaftNode) getLocalStats() *NodeStats {
	n.mu.RLock()
	collector := n.statsCollector
	n.mu.RUnlock()
	if collector != nil {
		return collector()
	}
	return nil
}

func (n *RaftNode) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		default:
		}

		switch n.State() {
		case Follower:
			n.runFollower(ctx)
		case Candidate:
			n.runCandidate(ctx)
		case Leader:
			n.runLeader(ctx)
		}
	}
}

func (n *RaftNode) runFollower(ctx context.Context) {
	n.logger.Debug("Running as follower", "term", n.currentTerm)
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case <-n.electionTimer.C:
			n.logger.Info("Election timeout, becoming candidate")
			n.mu.Lock()
			n.state = Candidate
			n.mu.Unlock()
			return
		case req := <-n.voteCh:
			n.handleVoteRequest(req)
		case req := <-n.appendCh:
			n.handleAppendEntries(req)
			n.resetElectionTimer()
		}
	}
}

func (n *RaftNode) runCandidate(ctx context.Context) {
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.config.NodeID
	currentTerm := n.currentTerm
	numPeers := len(n.peers)
	n.mu.Unlock()

	// Cluster size = peers + self
	// Majority = floor(clusterSize/2) + 1
	clusterSize := numPeers + 1
	votesNeeded := clusterSize/2 + 1

	n.logger.Info("Starting election", "term", currentTerm, "cluster_size", clusterSize, "needed", votesNeeded)
	n.resetElectionTimer()

	// Vote for self
	votes := 1

	// If we already have enough votes (single-node cluster with no peers), become leader immediately
	if numPeers == 0 && votes >= votesNeeded {
		n.mu.Lock()
		n.state = Leader
		n.mu.Unlock()
		n.logger.Info("Won election (single-node), becoming leader", "term", currentTerm, "votes", votes)
		return
	}

	// Request votes from peers
	n.mu.RLock()
	voteCh := make(chan bool, numPeers)
	for _, peer := range n.peers {
		go func(p *Peer) {
			resp, err := n.transport.SendVoteRequest(p.Address, &VoteRequest{
				Term:          currentTerm,
				CandidateID:   n.config.NodeID,
				CandidateAddr: n.config.AdvertiseAddr,
				LastLogIndex:  n.lastLogIndex(),
				LastLogTerm:   n.lastLogTerm(),
			})
			if err != nil {
				n.logger.Debug("Failed to send vote request", "peer", p.Address, "error", err)
				voteCh <- false
				return
			}

			// Notify peer activity from vote response (for membership tracking)
			if resp.VoterAddr != "" {
				n.notifyPeerActivity(resp.VoterAddr)
			}

			if resp.Term > currentTerm {
				n.mu.Lock()
				n.currentTerm = resp.Term
				n.state = Follower
				n.votedFor = ""
				n.mu.Unlock()
			}
			voteCh <- resp.VoteGranted
		}(peer)
	}
	n.mu.RUnlock()

	// Collect votes with timeout
	votesReceived := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case <-n.electionTimer.C:
			n.logger.Debug("Election timeout, restarting election", "votes", votes, "needed", votesNeeded)
			return
		case granted := <-voteCh:
			votesReceived++
			if granted {
				votes++
				n.logger.Debug("Received vote", "votes", votes, "needed", votesNeeded)
				if votes >= votesNeeded {
					n.mu.Lock()
					n.state = Leader
					n.mu.Unlock()
					n.logger.Info("Won election, becoming leader", "term", currentTerm, "votes", votes)
					return
				}
			}
			// If we've received all responses and still don't have enough votes, restart election
			if votesReceived >= numPeers && votes < votesNeeded {
				n.logger.Debug("Lost election, not enough votes", "votes", votes, "needed", votesNeeded)
				return
			}
		case req := <-n.appendCh:
			if req.Term >= currentTerm {
				n.mu.Lock()
				n.state = Follower
				n.mu.Unlock()
				n.handleAppendEntries(req)
				return
			}
		}
	}
}

func (n *RaftNode) runLeader(ctx context.Context) {
	n.logger.Info("Running as leader", "term", n.currentTerm)

	// Initialize leader state
	n.mu.Lock()
	n.leaderID = n.config.NodeID // We are the leader
	for _, peer := range n.peers {
		n.nextIndex[peer.ID] = n.lastLogIndexLocked() + 1
		n.matchIndex[peer.ID] = 0
	}
	n.mu.Unlock()

	// Start heartbeat timer
	n.heartbeatTimer = time.NewTimer(n.config.HeartbeatInterval)
	n.logger.Debug("Leader heartbeat timer started", "interval", n.config.HeartbeatInterval)

	for {
		// Check if we're still the leader (could change via vote request handler)
		n.mu.RLock()
		stillLeader := n.state == Leader
		n.mu.RUnlock()
		if !stillLeader {
			n.logger.Info("No longer leader, stepping down")
			return
		}

		select {
		case <-ctx.Done():
			n.logger.Debug("Leader: context done")
			return
		case <-n.stopCh:
			n.logger.Debug("Leader: stop channel closed")
			return
		case <-n.heartbeatTimer.C:
			n.logger.Debug("Leader: heartbeat timer fired")
			// Double check we're still leader before sending heartbeats
			if n.State() == Leader {
				n.sendHeartbeats()
			} else {
				n.logger.Info("No longer leader, not sending heartbeats")
				return
			}
			n.heartbeatTimer.Reset(n.config.HeartbeatInterval)
		case req := <-n.appendCh:
			n.logger.Debug("Leader: received appendCh message", "term", req.Term)
			if req.Term > n.currentTerm {
				n.mu.Lock()
				n.currentTerm = req.Term
				n.state = Follower
				n.votedFor = ""
				n.mu.Unlock()
				return
			}
		}
	}
}

func (n *RaftNode) sendHeartbeats() {
	n.mu.RLock()
	term := n.currentTerm
	commitIndex := n.commitIndex
	logLen := uint64(len(n.log))
	peerCount := len(n.peers)
	n.mu.RUnlock()

	if peerCount == 0 {
		return // No peers to send heartbeats to
	}

	for _, peer := range n.peers {
		go func(p *Peer) {
			// Get peer-specific info while holding lock
			n.mu.RLock()
			nextIdx := n.nextIndex[p.ID]
			if nextIdx == 0 {
				nextIdx = 1 // Initialize if not set
			}

			// Calculate prevLogIndex and prevLogTerm for this peer
			prevLogIndex := nextIdx - 1
			var prevLogTerm uint64
			if prevLogIndex > 0 && prevLogIndex <= uint64(len(n.log)) {
				prevLogTerm = n.log[prevLogIndex-1].Term
			}

			// ONLY copy the entries this peer needs (not the full log!)
			// This is the critical memory optimization
			var entries []LogEntry
			if nextIdx <= logLen && nextIdx <= uint64(len(n.log)) {
				// Limit batch size to prevent large allocations
				maxBatch := uint64(100)
				endIdx := uint64(len(n.log))
				if endIdx-nextIdx+1 > maxBatch {
					endIdx = nextIdx + maxBatch - 1
				}
				entries = make([]LogEntry, endIdx-nextIdx+1)
				copy(entries, n.log[nextIdx-1:endIdx])
			}
			n.mu.RUnlock()

			req := &AppendEntriesRequest{
				Term:             term,
				LeaderID:         n.config.NodeID,
				LeaderAddr:       n.config.AdvertiseAddr,
				LeaderClientAddr: n.config.ClientAddr,
				PrevLogIndex:     prevLogIndex,
				PrevLogTerm:      prevLogTerm,
				Entries:          entries,
				LeaderCommit:     commitIndex,
			}
			n.logger.Debug("Sending AppendEntries", "to", p.Address, "leaderClientAddr", n.config.ClientAddr)

			resp, err := n.transport.SendAppendEntries(p.Address, req)
			if err != nil {
				return
			}

			// Notify peer activity from response (for membership tracking)
			if resp.ResponderAddr != "" {
				n.notifyPeerActivity(resp.ResponderAddr)
			}

			// Collect peer stats from response
			if resp.NodeStats != nil {
				n.updatePeerStats(p.ID, resp.NodeStats)
			}

			n.mu.Lock()
			if resp.Success && len(entries) > 0 {
				n.nextIndex[p.ID] = nextIdx + uint64(len(entries))
				n.matchIndex[p.ID] = n.nextIndex[p.ID] - 1
			} else if !resp.Success {
				// Peer rejected, decrement nextIndex to try earlier entries
				if n.nextIndex[p.ID] > 1 {
					n.nextIndex[p.ID]--
				}
			}
			n.mu.Unlock()
		}(peer)
	}
}

func (n *RaftNode) handleVoteRequest(req *VoteRequest) *VoteResponse {
	// Notify that we received activity from this peer (for membership tracking)
	// Use CandidateAddr if available for proper address tracking
	if req.CandidateAddr != "" {
		n.notifyPeerActivity(req.CandidateAddr)
	} else {
		n.notifyPeerActivity(req.CandidateID)
	}

	n.logger.Debug("Handling VoteRequest", "from", req.CandidateID, "addr", req.CandidateAddr)
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &VoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
		VoterID:     n.config.NodeID,
		VoterAddr:   n.config.AdvertiseAddr,
	}

	if req.Term < n.currentTerm {
		return resp
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.state = Follower
		n.votedFor = ""
	}

	// Check if we can vote for this candidate
	if (n.votedFor == "" || n.votedFor == req.CandidateID) &&
		n.isLogUpToDate(req.LastLogIndex, req.LastLogTerm) {
		n.votedFor = req.CandidateID
		resp.VoteGranted = true
		n.resetElectionTimer()
	}

	return resp
}

func (n *RaftNode) handleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	// Notify that we received activity from this peer (for membership tracking)
	// Use LeaderAddr if available, otherwise fall back to LeaderID
	if req.LeaderAddr != "" {
		n.notifyPeerActivity(req.LeaderAddr)
	} else {
		n.notifyPeerActivity(req.LeaderID)
	}

	n.mu.Lock()

	n.logger.Debug("Handling AppendEntries", "from", req.LeaderID, "reqTerm", req.Term, "currentTerm", n.currentTerm, "state", n.state.String())

	resp := &AppendEntriesResponse{
		Term:          n.currentTerm,
		Success:       false,
		ResponderID:   n.config.NodeID,
		ResponderAddr: n.config.AdvertiseAddr,
	}

	// Reject requests from older terms
	if req.Term < n.currentTerm {
		n.logger.Debug("Rejecting AppendEntries: old term")
		n.mu.Unlock()
		return resp
	}

	// If request has higher term, step down and update
	if req.Term > n.currentTerm {
		n.logger.Info("Stepping down: higher term received", "newTerm", req.Term, "newLeader", req.LeaderID, "leaderClientAddr", req.LeaderClientAddr)
		n.currentTerm = req.Term
		n.votedFor = ""
		n.state = Follower
		n.leaderID = req.LeaderID
		n.leaderClientAddr = req.LeaderClientAddr
	} else if n.state != Leader {
		// Only update leaderID if we're not the leader
		// If we're the leader and receive same-term AppendEntries, ignore it
		// (this can happen with delayed/duplicate messages)
		if n.leaderID != req.LeaderID {
			n.logger.Info("Updating leaderID", "from", n.leaderID, "to", req.LeaderID, "leaderClientAddr", req.LeaderClientAddr)
		}
		n.leaderID = req.LeaderID
		n.leaderClientAddr = req.LeaderClientAddr
	} else {
		// We're the leader with same term - this shouldn't happen normally
		// but could occur with network delays. Reject it.
		n.logger.Debug("Rejecting AppendEntries: we are leader")
		n.mu.Unlock()
		return resp
	}

	// Check log consistency
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > uint64(len(n.log)) {
			n.mu.Unlock()
			return resp
		}
		if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			n.mu.Unlock()
			return resp
		}
	}

	// Append new entries
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + uint64(i) + 1
		if idx <= uint64(len(n.log)) {
			if n.log[idx-1].Term != entry.Term {
				n.log = n.log[:idx-1]
				n.log = append(n.log, entry)
			}
		} else {
			n.log = append(n.log, entry)
		}
	}

	// Update commit index
	oldCommitIndex := n.commitIndex
	if req.LeaderCommit > n.commitIndex {
		lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
		if req.LeaderCommit < lastNewIndex {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}
		n.logger.Info("Commit index updated", "from", oldCommitIndex, "to", n.commitIndex)
	}

	resp.Success = true

	// Reset election timer since we received valid heartbeat from leader
	n.resetElectionTimer()

	// Apply newly committed entries (must be done after releasing lock)
	if n.commitIndex > oldCommitIndex {
		go n.applyCommittedEntries()
	}

	// Release lock BEFORE collecting stats to avoid deadlock
	// (stats collector may call back into raft methods that need the lock)
	n.mu.Unlock()

	// Collect local stats for the response (outside lock)
	resp.NodeStats = n.getLocalStatsUnlocked()

	return resp
}

// getLocalStatsUnlocked returns local node stats without holding the lock.
func (n *RaftNode) getLocalStatsUnlocked() *NodeStats {
	if n.statsCollector != nil {
		return n.statsCollector()
	}
	return nil
}

// Apply applies a command to the state machine.
// This adds the entry to log, replicates to followers, and waits for commit.
// If AsyncReplication is enabled, returns immediately after local append.
func (n *RaftNode) Apply(command []byte) error {
	if !n.IsLeader() {
		return fmt.Errorf("not the leader")
	}

	n.mu.Lock()
	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   uint64(len(n.log)) + 1,
		Command: command,
	}
	n.log = append(n.log, entry)
	entryIndex := entry.Index
	numPeers := len(n.peers)
	n.mu.Unlock()

	// Fast path for single-node cluster: no replication needed
	if numPeers == 0 {
		n.mu.Lock()
		if entryIndex > n.commitIndex {
			n.commitIndex = entryIndex
			go n.applyCommittedEntries()
		}
		n.mu.Unlock()
		return nil
	}

	// Async replication mode: return immediately, replicate in background
	if n.config.AsyncReplication {
		go n.replicateEntryAsync(entryIndex)
		return nil
	}

	// Synchronous replication: wait for majority
	return n.replicateEntrySync(entryIndex)
}

// replicateEntryAsync replicates an entry to followers in the background
func (n *RaftNode) replicateEntryAsync(entryIndex uint64) {
	n.mu.RLock()
	numPeers := len(n.peers)
	n.mu.RUnlock()

	if numPeers == 0 {
		return
	}

	successCh := make(chan bool, numPeers)
	n.startReplication(successCh)

	// Wait for responses in background and update commit index
	go func() {
		successCount := 1 // Count self
		needed := (numPeers+1)/2 + 1
		timeout := time.After(n.config.ReplicationTimeout)

		for i := 0; i < numPeers; i++ {
			select {
			case success := <-successCh:
				if success {
					successCount++
					if successCount >= needed {
						n.mu.Lock()
						if entryIndex > n.commitIndex {
							n.commitIndex = entryIndex
							go n.applyCommittedEntries()
						}
						n.mu.Unlock()
						return
					}
				}
			case <-timeout:
				return
			}
		}

		// Check if we got majority after all responses
		if successCount >= needed {
			n.mu.Lock()
			if entryIndex > n.commitIndex {
				n.commitIndex = entryIndex
				go n.applyCommittedEntries()
			}
			n.mu.Unlock()
		}
	}()
}

// replicateEntrySync replicates an entry and waits for majority acknowledgment
func (n *RaftNode) replicateEntrySync(entryIndex uint64) error {
	n.mu.RLock()
	numPeers := len(n.peers)
	n.mu.RUnlock()

	successCh := make(chan bool, numPeers)
	n.startReplication(successCh)

	// Wait for majority (including self)
	successCount := 1 // Count self
	needed := (numPeers+1)/2 + 1
	timeout := time.After(n.config.ReplicationTimeout)

	for i := 0; i < numPeers; i++ {
		select {
		case success := <-successCh:
			if success {
				successCount++
				if successCount >= needed {
					// Majority achieved, commit the entry
					n.mu.Lock()
					if entryIndex > n.commitIndex {
						n.commitIndex = entryIndex
						go n.applyCommittedEntries()
					}
					n.mu.Unlock()
					return nil
				}
			}
		case <-timeout:
			return fmt.Errorf("timeout waiting for replication")
		}
	}

	// Check if we got majority after all responses
	if successCount >= needed {
		n.mu.Lock()
		if entryIndex > n.commitIndex {
			n.commitIndex = entryIndex
			go n.applyCommittedEntries()
		}
		n.mu.Unlock()
		return nil
	}

	return fmt.Errorf("failed to replicate to majority")
}

// startReplication starts replicating to all peers
func (n *RaftNode) startReplication(successCh chan bool) {
	n.mu.RLock()
	peers := make([]*Peer, 0, len(n.peers))
	for _, p := range n.peers {
		peers = append(peers, p)
	}
	n.mu.RUnlock()

	for _, peer := range peers {
		go func(p *Peer) {
			n.mu.RLock()
			term := n.currentTerm
			commitIndex := n.commitIndex
			nextIdx := n.nextIndex[p.ID]
			prevLogIndex := nextIdx - 1
			var prevLogTerm uint64
			if prevLogIndex > 0 && prevLogIndex <= uint64(len(n.log)) {
				prevLogTerm = n.log[prevLogIndex-1].Term
			}
			// ONLY copy the entries this peer needs (not the full log!)
			var entries []LogEntry
			if nextIdx <= uint64(len(n.log)) {
				// Limit batch size to prevent large allocations
				maxBatch := uint64(100)
				endIdx := uint64(len(n.log))
				if endIdx-nextIdx+1 > maxBatch {
					endIdx = nextIdx + maxBatch - 1
				}
				entries = make([]LogEntry, endIdx-nextIdx+1)
				copy(entries, n.log[nextIdx-1:endIdx])
			}
			n.mu.RUnlock()

			req := &AppendEntriesRequest{
				Term:             term,
				LeaderID:         n.config.NodeID,
				LeaderAddr:       n.config.AdvertiseAddr,
				LeaderClientAddr: n.config.ClientAddr,
				PrevLogIndex:     prevLogIndex,
				PrevLogTerm:      prevLogTerm,
				Entries:          entries,
				LeaderCommit:     commitIndex,
			}

			resp, err := n.transport.SendAppendEntries(p.Address, req)
			if err != nil {
				successCh <- false
				return
			}

			// Notify peer activity from response (for membership tracking)
			if resp.ResponderAddr != "" {
				n.notifyPeerActivity(resp.ResponderAddr)
			}

			n.mu.Lock()
			if resp.Success {
				n.nextIndex[p.ID] = nextIdx + uint64(len(entries))
				n.matchIndex[p.ID] = n.nextIndex[p.ID] - 1
			} else {
				if n.nextIndex[p.ID] > 1 {
					n.nextIndex[p.ID]--
				}
			}
			n.mu.Unlock()
			successCh <- resp.Success
		}(peer)
	}
}

// applyCommittedEntries applies all committed but not yet applied entries.
func (n *RaftNode) applyCommittedEntries() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		if n.lastApplied <= uint64(len(n.log)) {
			entry := n.log[n.lastApplied-1]
			// Debug only to reduce log spam during high-throughput operations
			if n.lastApplied%1000 == 0 {
				n.logger.Debug("Applying entries", "current", n.lastApplied, "target", n.commitIndex)
			}
			// Send to apply channel (non-blocking)
			select {
			case n.applyCh <- entry:
				// Clear the command bytes to allow GC after apply
				n.log[n.lastApplied-1].Command = nil
			default:
				n.logger.Warn("Apply channel full, dropping entry", "index", entry.Index)
			}
		}
	}

	// Compact log to prevent unbounded growth
	// Keep only entries that haven't been applied to all peers
	n.maybeCompactLog()
}

// maybeCompactLog removes old log entries to prevent memory exhaustion.
// Called with n.mu already held.
func (n *RaftNode) maybeCompactLog() {
	// Only compact if log is large enough
	if len(n.log) < 1000 {
		return
	}

	// Find the minimum matched index across all peers
	// We can only remove entries that all peers have received
	minMatchIdx := n.lastApplied
	for _, matchIdx := range n.matchIndex {
		if matchIdx < minMatchIdx {
			minMatchIdx = matchIdx
		}
	}

	// Keep a buffer of entries for new/slow peers
	keepFromIdx := uint64(0)
	if minMatchIdx > 100 {
		keepFromIdx = minMatchIdx - 100
	}

	if keepFromIdx == 0 {
		return // Nothing to compact
	}

	// Create new log slice without old entries
	newLog := make([]LogEntry, uint64(len(n.log))-keepFromIdx)
	copy(newLog, n.log[keepFromIdx:])
	n.log = newLog

	// Adjust indices to account for removed entries
	// Note: This is a simplified compaction. A production implementation
	// would need to track a log offset for proper index calculation.
	n.logger.Debug("Compacted log", "removed", keepFromIdx, "remaining", len(n.log))
}

func (n *RaftNode) replicateLog() {
	n.mu.RLock()
	term := n.currentTerm
	commitIndex := n.commitIndex
	n.mu.RUnlock()

	for _, peer := range n.peers {
		go func(p *Peer) {
			n.mu.RLock()
			nextIdx := n.nextIndex[p.ID]
			prevLogIndex := nextIdx - 1
			var prevLogTerm uint64
			if prevLogIndex > 0 && prevLogIndex <= uint64(len(n.log)) {
				prevLogTerm = n.log[prevLogIndex-1].Term
			}
			var entries []LogEntry
			if nextIdx <= uint64(len(n.log)) {
				entries = n.log[nextIdx-1:]
			}
			n.mu.RUnlock()

			req := &AppendEntriesRequest{
				Term:             term,
				LeaderID:         n.config.NodeID,
				LeaderAddr:       n.config.AdvertiseAddr,
				LeaderClientAddr: n.config.ClientAddr,
				PrevLogIndex:     prevLogIndex,
				PrevLogTerm:      prevLogTerm,
				Entries:          entries,
				LeaderCommit:     commitIndex,
			}

			resp, err := n.transport.SendAppendEntries(p.Address, req)
			if err != nil {
				return
			}

			// Notify peer activity from response (for membership tracking)
			if resp.ResponderAddr != "" {
				n.notifyPeerActivity(resp.ResponderAddr)
			}

			n.mu.Lock()
			if resp.Success {
				n.nextIndex[p.ID] = nextIdx + uint64(len(entries))
				n.matchIndex[p.ID] = n.nextIndex[p.ID] - 1
			} else {
				if n.nextIndex[p.ID] > 1 {
					n.nextIndex[p.ID]--
				}
			}
			n.mu.Unlock()
		}(peer)
	}
}

func (n *RaftNode) resetElectionTimer() {
	timeout := n.config.ElectionTimeout + time.Duration(rand.Int63n(int64(n.config.ElectionTimeout)))
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(timeout)
	} else {
		n.electionTimer.Reset(timeout)
	}
}

func (n *RaftNode) lastLogIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastLogIndexLocked()
}

func (n *RaftNode) lastLogIndexLocked() uint64 {
	return uint64(len(n.log))
}

func (n *RaftNode) lastLogTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastLogTermLocked()
}

func (n *RaftNode) lastLogTermLocked() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *RaftNode) isLogUpToDate(lastLogIndex, lastLogTerm uint64) bool {
	myLastTerm := n.lastLogTermLocked()
	myLastIndex := n.lastLogIndexLocked()

	if lastLogTerm != myLastTerm {
		return lastLogTerm > myLastTerm
	}
	return lastLogIndex >= myLastIndex
}

func (n *RaftNode) loadState() error {
	stateFile := filepath.Join(n.config.DataDir, "raft_state.json")
	data, err := os.ReadFile(stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var state struct {
		CurrentTerm uint64     `json:"current_term"`
		VotedFor    string     `json:"voted_for"`
		Log         []LogEntry `json:"log"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor
	n.log = state.Log
	return nil
}

func (n *RaftNode) saveState() error {
	if err := os.MkdirAll(n.config.DataDir, 0755); err != nil {
		return err
	}

	state := struct {
		CurrentTerm uint64     `json:"current_term"`
		VotedFor    string     `json:"voted_for"`
		Log         []LogEntry `json:"log"`
	}{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	// Use atomic write: write to temp file, sync, then rename
	// This ensures the state file is never corrupted even if the server crashes
	stateFile := filepath.Join(n.config.DataDir, "raft_state.json")
	tempFile := stateFile + ".tmp"

	f, err := os.Create(tempFile)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tempFile)
		return err
	}

	// Sync to ensure data is on disk before rename
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tempFile)
		return err
	}

	if err := f.Close(); err != nil {
		os.Remove(tempFile)
		return err
	}

	// Atomic rename - this is atomic on POSIX systems
	return os.Rename(tempFile, stateFile)
}
