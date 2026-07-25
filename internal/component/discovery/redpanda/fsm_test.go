package redpanda

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

type fakeSink struct {
	bytes.Buffer
	canceled bool
}

func (s *fakeSink) ID() string    { return "fake" }
func (s *fakeSink) Cancel() error { s.canceled = true; return nil }
func (s *fakeSink) Close() error  { return nil }

func TestFSM_ApplyAssignUnassign(t *testing.T) {
	f := newFSM(nil)

	applyCommand(t, f, command{Op: opAssign, BrokerID: "b1", ClusterID: "c1", Collector: "collector-0"})

	collector, ok := f.collectorOf("b1")
	if !ok || collector != "collector-0" {
		t.Fatalf("expected b1 assigned to collector-0, got %q (ok=%v)", collector, ok)
	}

	applyCommand(t, f, command{Op: opUnassign, BrokerID: "b1"})

	if _, ok := f.collectorOf("b1"); ok {
		t.Fatalf("expected b1 to be unassigned")
	}
}

func TestFSM_SetEpochIsImmutable(t *testing.T) {
	f := newFSM(nil)
	if got := f.currentEpoch(); got != "" {
		t.Fatalf("expected empty epoch before any opSetEpoch, got %q", got)
	}

	applyCommand(t, f, command{Op: opSetEpoch, Epoch: "epoch-1"})
	if got := f.currentEpoch(); got != "epoch-1" {
		t.Fatalf("expected epoch-1, got %q", got)
	}

	// A second attempt (which should never happen in practice — only the
	// bootstrapper ever proposes this, exactly once) must not change it.
	applyCommand(t, f, command{Op: opSetEpoch, Epoch: "epoch-2"})
	if got := f.currentEpoch(); got != "epoch-1" {
		t.Fatalf("expected epoch to remain epoch-1 after a second set attempt, got %q", got)
	}
}

func TestFSM_UnknownOp(t *testing.T) {
	f := newFSM(nil)
	res := f.Apply(logFor(t, command{Op: "bogus", BrokerID: "b1"}))
	if res == nil {
		t.Fatalf("expected an error result for an unknown op")
	}
	if _, ok := res.(error); !ok {
		t.Fatalf("expected Apply to return an error, got %T", res)
	}
}

func TestFSM_SnapshotRestore(t *testing.T) {
	f := newFSM(nil)
	applyCommand(t, f, command{Op: opSetEpoch, Epoch: "epoch-1"})
	applyCommand(t, f, command{Op: opAssign, BrokerID: "b1", ClusterID: "c1", Collector: "collector-0"})
	applyCommand(t, f, command{Op: opAssign, BrokerID: "b2", ClusterID: "c1", Collector: "collector-1"})

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	sink := &fakeSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if sink.canceled {
		t.Fatalf("sink was canceled unexpectedly")
	}

	restored := newFSM(nil)
	if err := restored.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if c, ok := restored.collectorOf("b1"); !ok || c != "collector-0" {
		t.Fatalf("expected restored b1 -> collector-0, got %q (ok=%v)", c, ok)
	}
	if c, ok := restored.collectorOf("b2"); !ok || c != "collector-1" {
		t.Fatalf("expected restored b2 -> collector-1, got %q (ok=%v)", c, ok)
	}
	if got := restored.currentEpoch(); got != "epoch-1" {
		t.Fatalf("expected restored epoch to be epoch-1, got %q", got)
	}
}

func applyCommand(t *testing.T, f *fsm, cmd command) {
	t.Helper()
	res := f.Apply(logFor(t, cmd))
	if err, ok := res.(error); ok {
		t.Fatalf("Apply(%+v) returned error: %v", cmd, err)
	}
}

func logFor(t *testing.T, cmd command) *raft.Log {
	t.Helper()
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshaling command: %v", err)
	}
	return &raft.Log{Data: data}
}
