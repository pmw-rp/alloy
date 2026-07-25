package redpanda

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/grafana/ckit/peer"
	"github.com/hashicorp/raft"
)

func TestStatefulSetNameFromPodName(t *testing.T) {
	cases := []struct {
		podName string
		want    string
	}{
		{"collector-collector-alloy-0", "collector-collector-alloy"},
		{"collector-collector-alloy-12", "collector-collector-alloy"},
		{"foo", "foo"},         // no hyphen at all: unchanged
		{"foo-bar", "foo-bar"}, // suffix isn't numeric: unchanged
		{"foo-", "foo-"},       // empty suffix: unchanged
		{"a-b-c-3", "a-b-c"},   // only the last segment is stripped
	}
	for _, c := range cases {
		if got := statefulSetNameFromPodName(c.podName); got != c.want {
			t.Errorf("statefulSetNameFromPodName(%q) = %q, want %q", c.podName, got, c.want)
		}
	}
}

func TestRaftAdvertiseDomainFor(t *testing.T) {
	t.Run("no namespace file: falls back to empty (IP-based addressing)", func(t *testing.T) {
		orig := podNamespaceFile
		podNamespaceFile = filepath.Join(t.TempDir(), "does-not-exist")
		defer func() { podNamespaceFile = orig }()

		if got := raftAdvertiseDomainFor("collector-collector-alloy-0"); got != "" {
			t.Fatalf("expected empty domain when namespace file is missing, got %q", got)
		}
	})

	t.Run("namespace file present: derives the headless service DNS suffix", func(t *testing.T) {
		orig := podNamespaceFile
		nsFile := filepath.Join(t.TempDir(), "namespace")
		if err := os.WriteFile(nsFile, []byte("collector\n"), 0o644); err != nil {
			t.Fatalf("writing fixture namespace file: %v", err)
		}
		podNamespaceFile = nsFile
		defer func() { podNamespaceFile = orig }()

		got := raftAdvertiseDomainFor("collector-collector-alloy-0")
		want := "collector-collector-alloy.collector.svc.cluster.local"
		if got != want {
			t.Fatalf("raftAdvertiseDomainFor(...) = %q, want %q", got, want)
		}
	})
}

func peersNamed(names ...string) []peer.Peer {
	peers := make([]peer.Peer, len(names))
	for i, n := range names {
		peers[i] = peer.Peer{Name: n}
	}
	return peers
}

// TestHasReachableQuorum_EmptyConfiguration covers a bare, not-yet-added
// node — no configured voters at all is ordinary startup, not a
// quorum-loss condition, so this must not false-trigger checkQuorumLoss.
func TestHasReachableQuorum_EmptyConfiguration(t *testing.T) {
	tn := newTestNode(t, "node-a")
	n := &raftNode{raft: tn.raft, self: "node-a"}

	has, err := n.hasReachableQuorum(nil)
	if err != nil {
		t.Fatalf("hasReachableQuorum: %v", err)
	}
	if !has {
		t.Fatalf("expected an unconfigured node to report quorum reachable")
	}
}

// TestHasReachableQuorum_MajorityLive covers steady state: every
// configured voter is currently visible via gossip.
func TestHasReachableQuorum_MajorityLive(t *testing.T) {
	tn := newTestNode(t, "node-a")
	bootstrapFiveServers(t, tn)
	n := &raftNode{raft: tn.raft, self: "node-a"}

	has, err := n.hasReachableQuorum(peersNamed("node-a", "node-b", "node-c", "node-d", "node-e"))
	if err != nil {
		t.Fatalf("hasReachableQuorum: %v", err)
	}
	if !has {
		t.Fatalf("expected quorum reachable when every configured voter is live")
	}
}

// TestHasReachableQuorum_ExactlyAtBoundary covers the boundary: exactly a
// majority (3 of 5) live must still count as reachable.
func TestHasReachableQuorum_ExactlyAtBoundary(t *testing.T) {
	tn := newTestNode(t, "node-a")
	bootstrapFiveServers(t, tn)
	n := &raftNode{raft: tn.raft, self: "node-a"}

	has, err := n.hasReachableQuorum(peersNamed("node-a", "node-b", "node-c"))
	if err != nil {
		t.Fatalf("hasReachableQuorum: %v", err)
	}
	if !has {
		t.Fatalf("expected exactly a majority (3 of 5) to count as quorum reachable")
	}
}

// TestHasReachableQuorum_MajorityLost is the regression test for the real
// bug found in live testing: losing more voters at once than the old
// configuration's fault tolerance (5 replicas down to 2 survivors here)
// leaves a structurally unreachable quorum — this must report false so
// checkQuorumLoss can eventually self-restart the replica.
func TestHasReachableQuorum_MajorityLost(t *testing.T) {
	tn := newTestNode(t, "node-a")
	bootstrapFiveServers(t, tn)
	n := &raftNode{raft: tn.raft, self: "node-a"}

	has, err := n.hasReachableQuorum(peersNamed("node-a", "node-b"))
	if err != nil {
		t.Fatalf("hasReachableQuorum: %v", err)
	}
	if has {
		t.Fatalf("expected quorum unreachable with only 2 of 5 configured voters live")
	}
}

// bootstrapFiveServers bootstraps tn with a 5-voter configuration
// (node-a..node-e) — only node-a is a real, running testNode; the other
// four are never connected. That's fine here: hasReachableQuorum only
// reads the locally-stored configuration, which BootstrapCluster writes
// purely locally with no RPCs to the other listed servers.
func bootstrapFiveServers(t *testing.T, tn *testNode) {
	t.Helper()
	err := tn.raft.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{ID: "node-a", Address: "node-a"},
			{ID: "node-b", Address: "node-b"},
			{ID: "node-c", Address: "node-c"},
			{ID: "node-d", Address: "node-d"},
			{ID: "node-e", Address: "node-e"},
		},
	}).Error()
	if err != nil {
		t.Fatalf("BootstrapCluster: %v", err)
	}
	// GetConfiguration must reflect the bootstrapped configuration
	// promptly — this is a local, synchronous log write, not an RPC, but
	// give it a moment in case raft.NewRaft's own startup goroutines are
	// still initializing.
	deadline := time.Now().Add(2 * time.Second)
	for {
		future := tn.raft.GetConfiguration()
		if err := future.Error(); err == nil && len(future.Configuration().Servers) == 5 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("configuration never reflected the 5 bootstrapped servers")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRaftAddressFor_PrefersDNSNameOverIP(t *testing.T) {
	p := peer.Peer{Name: "collector-collector-alloy-1", Addr: "10.0.0.5:12345"}

	got := raftAddressFor(p, "collector-collector-alloy.collector.svc.cluster.local", 9700)
	want := "collector-collector-alloy-1.collector-collector-alloy.collector.svc.cluster.local:9700"
	if string(got) != want {
		t.Fatalf("raftAddressFor with a domain = %q, want %q", got, want)
	}

	// With no domain, falls back to the peer's IP — this is what goes stale
	// across a pod restart, which is exactly why the domain-based path above
	// exists.
	got = raftAddressFor(p, "", 9700)
	want = "10.0.0.5:9700"
	if string(got) != want {
		t.Fatalf("raftAddressFor with no domain = %q, want %q", got, want)
	}
}
