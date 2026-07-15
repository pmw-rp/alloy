package redpanda

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// testNode is a Raft peer built on in-memory stores/transport, used to
// exercise the same bootstrap-then-AddVoter sequence raftnode.go uses in
// production without needing real TCP ports or disk.
type testNode struct {
	id        string
	raft      *raft.Raft
	fsm       *fsm
	transport *raft.InmemTransport
}

func newTestNode(t *testing.T, id string) *testNode {
	t.Helper()

	addr, transport := raft.NewInmemTransport(raft.ServerAddress(id))

	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(id)
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond

	f := newFSM()
	logStore := raft.NewInmemStore()
	snapshots := raft.NewInmemSnapshotStore()

	r, err := raft.NewRaft(conf, f, logStore, logStore, snapshots, transport)
	if err != nil {
		t.Fatalf("NewRaft(%s): %v", id, err)
	}

	_ = addr
	return &testNode{id: id, raft: r, fsm: f, transport: transport}
}

// connectAll wires every node's transport to every other node's, as
// raft.NewInmemTransport requires for in-process tests (there's no real
// network to route through).
func connectAll(nodes []*testNode) {
	for _, a := range nodes {
		for _, b := range nodes {
			if a.id == b.id {
				continue
			}
			a.transport.Connect(raft.ServerAddress(b.id), b.transport)
		}
	}
}

func awaitLeader(t *testing.T, nodes []*testNode, timeout time.Duration) *testNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*testNode
		for _, n := range nodes {
			if n.raft.State() == raft.Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) > 1 {
			t.Fatalf("split-brain: multiple nodes think they're leader: %v", namesOf(leaders))
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %s", timeout)
	return nil
}

func namesOf(nodes []*testNode) []string {
	names := make([]string, len(nodes))
	for i, n := range nodes {
		names[i] = n.id
	}
	return names
}

// TestRaftBootstrap_SingleBootstrapperThenAddVoter exercises exactly the
// pattern raftnode.go uses in production: one deterministically-chosen node
// bootstraps a single-voter configuration containing only itself, and every
// other node is added as a voter by that leader afterward. This is the
// pattern the research into hashicorp/raft's bootstrap semantics said is
// required — every node independently calling BootstrapCluster would
// split-brain instead of safely no-op.
func TestRaftBootstrap_SingleBootstrapperThenAddVoter(t *testing.T) {
	ids := []string{"node-a", "node-b", "node-c"}
	nodes := make([]*testNode, len(ids))
	for i, id := range ids {
		nodes[i] = newTestNode(t, id)
	}
	connectAll(nodes)

	// Only node-a (the deterministic bootstrapper, lowest-sorted name)
	// bootstraps, containing only itself.
	bootstrapper := nodes[0]
	err := bootstrapper.raft.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{{ID: raft.ServerID(bootstrapper.id), Address: raft.ServerAddress(bootstrapper.id)}},
	}).Error()
	if err != nil {
		t.Fatalf("BootstrapCluster: %v", err)
	}

	leader := awaitLeader(t, nodes, 2*time.Second)
	if leader.id != bootstrapper.id {
		t.Fatalf("expected %s to be leader, got %s", bootstrapper.id, leader.id)
	}

	// Leader adds every other node as a voter.
	for _, n := range nodes {
		if n.id == leader.id {
			continue
		}
		if err := leader.raft.AddVoter(raft.ServerID(n.id), raft.ServerAddress(n.id), 0, time.Second).Error(); err != nil {
			t.Fatalf("AddVoter(%s): %v", n.id, err)
		}
	}

	// Still exactly one leader after the membership changes.
	leader = awaitLeader(t, nodes, 2*time.Second)

	// A command applied on the leader must propagate to every follower's FSM.
	cmd := command{Op: opAssign, BrokerID: "b1", ClusterID: "c1", Collector: "collector-x"}
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := leader.raft.Apply(data, time.Second).Error(); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		allSee := true
		for _, n := range nodes {
			if c, ok := n.fsm.collectorOf("b1"); !ok || c != "collector-x" {
				allSee = false
			}
		}
		if allSee {
			break
		}
		if time.Now().After(deadline) {
			for _, n := range nodes {
				c, ok := n.fsm.collectorOf("b1")
				t.Logf("node %s: collector=%q ok=%v", n.id, c, ok)
			}
			t.Fatalf("command did not propagate to all nodes within timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestRaftBootstrap_SimultaneousBootstrapWouldSplitBrain documents, rather
// than asserts a fix for, the hazard the package doc comment and raftnode.go
// explicitly avoid: if every node calls BootstrapCluster independently, each
// with an empty log, each succeeds and becomes its own single-node leader.
// This is why production code picks exactly one deterministic bootstrapper
// instead.
func TestRaftBootstrap_SimultaneousBootstrapWouldSplitBrain(t *testing.T) {
	ids := []string{"node-a", "node-b"}
	nodes := make([]*testNode, len(ids))
	for i, id := range ids {
		nodes[i] = newTestNode(t, id)
	}
	connectAll(nodes)

	for _, n := range nodes {
		err := n.raft.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{{ID: raft.ServerID(n.id), Address: raft.ServerAddress(n.id)}},
		}).Error()
		if err != nil {
			t.Fatalf("BootstrapCluster(%s): %v", n.id, err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	leaderCount := 0
	for _, n := range nodes {
		if n.raft.State() == raft.Leader {
			leaderCount++
		}
	}
	if leaderCount != len(nodes) {
		t.Fatalf("expected every independently-bootstrapped node to be its own leader (split-brain), got %d/%d leaders", leaderCount, len(nodes))
	}
}
