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
	// opSetEpoch records this Raft group's incarnation identity — a random
	// value minted once, by whichever node bootstraps a fresh cluster
	// (either a genuinely first-ever install, or a legitimate recovery
	// after every replica lost its state simultaneously — see
	// raftnode.go's decideBootstrap). It's proposed as the very first
	// command any fresh cluster ever commits, before anything else, and
	// never changes for the lifetime of that incarnation. Named "epoch,"
	// not "cluster ID," to avoid colliding with command.ClusterID below,
	// which identifies a Redpanda cluster a broker belongs to — a
	// completely unrelated concept.
	opSetEpoch commandOp = "set_epoch"
)

// command is a single Raft log entry: assign a broker to a collector,
// unassign a broker that's no longer tracked, or record this Raft group's
// epoch (see opSetEpoch).
type command struct {
	Op        commandOp `json:"op"`
	BrokerID  string    `json:"broker_id"`
	ClusterID string    `json:"cluster_id,omitempty"`
	Collector string    `json:"collector,omitempty"`
	Epoch     string    `json:"epoch,omitempty"` // only meaningful for opSetEpoch
}

// fsm is the Raft state machine holding the current broker->collector
// assignment. Only the current leader proposes commands (see allocate.go
// and raftnode.go); every replica, leader or follower, applies the
// committed result to its own copy via Raft replication.
//
// onChange fires after every successful Apply, on every replica — not just
// the leader. This matters: a follower's own reconcile() never runs (it's
// leader-only), so without this hook a follower would only ever republish
// its exported targets from Update(), and could sit on a stale assignment
// indefinitely after the leader reassigns something out from under it.
type fsm struct {
	mu          sync.RWMutex
	assignments map[string]assignment // brokerID -> assignment
	epoch       string                // this Raft group's incarnation identity — see opSetEpoch
	onChange    func()
}

func newFSM(onChange func()) *fsm {
	return &fsm{assignments: make(map[string]assignment), onChange: onChange}
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

// currentEpoch returns this Raft group's incarnation identity, or "" if no
// opSetEpoch command has been applied yet (momentarily, right after
// BootstrapCluster but before the bootstrapper's own first Apply commits).
func (f *fsm) currentEpoch() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.epoch
}

// Apply implements raft.FSM.
func (f *fsm) Apply(log *raft.Log) any {
	var cmd command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("decoding command: %w", err)
	}

	f.mu.Lock()
	switch cmd.Op {
	case opAssign:
		f.assignments[cmd.BrokerID] = assignment{Collector: cmd.Collector, ClusterID: cmd.ClusterID}
	case opUnassign:
		delete(f.assignments, cmd.BrokerID)
	case opSetEpoch:
		// Idempotent-but-immutable: only the bootstrapper ever proposes
		// this, exactly once, so a second attempt should never happen in
		// practice — but Apply must never fail based on FSM-internal
		// state, so silently ignore rather than erroring if it somehow did.
		if f.epoch == "" {
			f.epoch = cmd.Epoch
		}
	default:
		f.mu.Unlock()
		return fmt.Errorf("unknown command op %q", cmd.Op)
	}
	f.mu.Unlock()

	// Must fire after releasing f.mu: onChange ultimately calls back into
	// publishTargets(), which reads FSM state via collectorOf() — calling it
	// while still holding f.mu (a non-reentrant RWMutex) would deadlock.
	if f.onChange != nil {
		f.onChange()
	}
	return nil
}

// Snapshot implements raft.FSM.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	epoch := f.epoch
	f.mu.RUnlock()
	return &fsmSnapshot{assignments: f.snapshotState(), epoch: epoch}, nil
}

// fsmSnapshotData is the on-disk/wire format for a snapshot — bundles the
// epoch alongside the assignments so a replica restoring from a snapshot
// (rather than replaying the log from scratch) still learns the cluster's
// incarnation identity.
type fsmSnapshotData struct {
	Assignments map[string]assignment `json:"assignments"`
	Epoch       string                `json:"epoch,omitempty"`
}

// Restore implements raft.FSM.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var data fsmSnapshotData
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return fmt.Errorf("decoding snapshot: %w", err)
	}
	if data.Assignments == nil {
		data.Assignments = make(map[string]assignment)
	}
	f.mu.Lock()
	f.assignments = data.Assignments
	f.epoch = data.Epoch
	f.mu.Unlock()

	if f.onChange != nil {
		f.onChange()
	}
	return nil
}

type fsmSnapshot struct {
	assignments map[string]assignment
	epoch       string
}

// Persist implements raft.FSMSnapshot.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data := fsmSnapshotData{Assignments: s.assignments, Epoch: s.epoch}
	if err := json.NewEncoder(sink).Encode(data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release implements raft.FSMSnapshot.
func (s *fsmSnapshot) Release() {}
