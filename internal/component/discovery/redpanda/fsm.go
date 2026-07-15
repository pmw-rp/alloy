package redpanda

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// assignment records which collector currently owns a broker, and which
// Redpanda cluster that broker belongs to (used by the allocator to avoid
// assigning two brokers from the same cluster to one collector when
// avoidable).
type assignment struct {
	Collector string `json:"collector"`
	ClusterID string `json:"cluster_id"`
}

type commandOp string

const (
	opAssign   commandOp = "assign"
	opUnassign commandOp = "unassign"
)

// command is a single Raft log entry: either assign a broker to a collector,
// or unassign a broker that's no longer tracked.
type command struct {
	Op        commandOp `json:"op"`
	BrokerID  string    `json:"broker_id"`
	ClusterID string    `json:"cluster_id,omitempty"`
	Collector string    `json:"collector,omitempty"`
}

// fsm is the Raft state machine holding the current broker->collector
// assignment. Only the current leader proposes commands (see allocate.go
// and raftnode.go); every replica, leader or follower, reads the committed
// result to decide which brokers it's responsible for scraping.
type fsm struct {
	mu          sync.RWMutex
	assignments map[string]assignment // brokerID -> assignment
}

func newFSM() *fsm {
	return &fsm{assignments: make(map[string]assignment)}
}

// snapshotState returns a defensive copy of the current assignment map.
func (f *fsm) snapshotState() map[string]assignment {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]assignment, len(f.assignments))
	for k, v := range f.assignments {
		out[k] = v
	}
	return out
}

// collectorOf returns the collector currently assigned to brokerID, if any.
func (f *fsm) collectorOf(brokerID string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	a, ok := f.assignments[brokerID]
	if !ok {
		return "", false
	}
	return a.Collector, true
}

// Apply implements raft.FSM.
func (f *fsm) Apply(log *raft.Log) any {
	var cmd command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("decoding command: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	switch cmd.Op {
	case opAssign:
		f.assignments[cmd.BrokerID] = assignment{Collector: cmd.Collector, ClusterID: cmd.ClusterID}
	case opUnassign:
		delete(f.assignments, cmd.BrokerID)
	default:
		return fmt.Errorf("unknown command op %q", cmd.Op)
	}
	return nil
}

// Snapshot implements raft.FSM.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{assignments: f.snapshotState()}, nil
}

// Restore implements raft.FSM.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var assignments map[string]assignment
	if err := json.NewDecoder(rc).Decode(&assignments); err != nil {
		return fmt.Errorf("decoding snapshot: %w", err)
	}
	if assignments == nil {
		assignments = make(map[string]assignment)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.assignments = assignments
	return nil
}

type fsmSnapshot struct {
	assignments map[string]assignment
}

// Persist implements raft.FSMSnapshot.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.assignments); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release implements raft.FSMSnapshot.
func (s *fsmSnapshot) Release() {}
