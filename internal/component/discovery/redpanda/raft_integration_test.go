package redpanda

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/ckit/peer"
	"github.com/hashicorp/raft"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

// testNode is a Raft peer built on in-memory stores/transport, used to
// exercise the same bootstrap-then-AddVoter sequence raftnode.go uses in
// production without needing real TCP ports or disk.
type testNode struct {
	id            string
	raft          *raft.Raft
	fsm           *fsm
	transport     *raft.InmemTransport
	onChangeCount atomic.Int64
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

	tn := &testNode{id: id, transport: transport}
	f := newFSM(func() { tn.onChangeCount.Add(1) })
	tn.fsm = f
	logStore := raft.NewInmemStore()
	snapshots := raft.NewInmemSnapshotStore()

	r, err := raft.NewRaft(conf, f, logStore, logStore, snapshots, transport)
	if err != nil {
		t.Fatalf("NewRaft(%s): %v", id, err)
	}
	tn.raft = r

	_ = addr
	return tn
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

	// Regression test for a real bug found in live testing: a follower's own
	// reconcile() never runs (it's leader-only), so republishing exported
	// targets can't be driven from there. Every node — leader and followers
	// alike — must have its onChange fired by Apply, or a follower would
	// sit on stale exports indefinitely after the leader reassigns
	// something out from under it.
	for _, n := range nodes {
		if n.onChangeCount.Load() == 0 {
			t.Fatalf("node %s never had its FSM onChange callback fire; a follower with this bug would never republish its targets after an assignment change", n.id)
		}
	}
}

// TestReconcileMembership_RemovesVoterRequestingLeave is the core
// regression test for graceful scale-down: a voter marked as leaving must
// be removed even though it's still fully gossip-visible — the whole
// point being that a pod blocked in its own preStop hook (see redpanda.go's
// handleLeave) stays gossip-visible for as long as it's waiting to be
// removed, so waiting for gossip to notice it's gone would never resolve.
// A second reconcile pass, with the same peer list and the same voter
// still marked leaving, must not re-add it — no flapping while the
// preStop hook is still polling for its own removal.
func TestReconcileMembership_RemovesVoterRequestingLeave(t *testing.T) {
	ids := []string{"node-a", "node-b", "node-c"}
	nodes := make([]*testNode, len(ids))
	for i, id := range ids {
		nodes[i] = newTestNode(t, id)
	}
	connectAll(nodes)

	bootstrapper := nodes[0]
	if err := bootstrapper.raft.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{{ID: raft.ServerID(bootstrapper.id), Address: raft.ServerAddress(bootstrapper.id)}},
	}).Error(); err != nil {
		t.Fatalf("BootstrapCluster: %v", err)
	}
	leader := awaitLeader(t, nodes, 2*time.Second)
	for _, n := range nodes {
		if n.id == leader.id {
			continue
		}
		if err := leader.raft.AddVoter(raft.ServerID(n.id), raft.ServerAddress(n.id), 0, time.Second).Error(); err != nil {
			t.Fatalf("AddVoter(%s): %v", n.id, err)
		}
	}
	leader = awaitLeader(t, nodes, 2*time.Second)

	n := &raftNode{raft: leader.raft, self: leader.id}
	peers := []peer.Peer{{Name: "node-a"}, {Name: "node-b"}, {Name: "node-c"}}
	raftAddr := func(p peer.Peer) raft.ServerAddress { return raft.ServerAddress(p.Name) }
	leaving := map[string]bool{"node-b": true}

	if err := n.reconcileMembership(peers, raftAddr, leaving); err != nil {
		t.Fatalf("reconcileMembership: %v", err)
	}
	assertVoters := func(want int) []string {
		voters, err := n.currentVoters()
		if err != nil {
			t.Fatalf("currentVoters: %v", err)
		}
		for _, v := range voters {
			if v == "node-b" {
				t.Fatalf("expected node-b to be removed as a voter despite still being gossip-visible, got voters=%v", voters)
			}
		}
		if len(voters) != want {
			t.Fatalf("expected %d remaining voters, got %v", want, voters)
		}
		return voters
	}
	assertVoters(2)

	// A second pass, still gossip-visible and still marked leaving, must
	// not flap node-b back in.
	if err := n.reconcileMembership(peers, raftAddr, leaving); err != nil {
		t.Fatalf("reconcileMembership (second pass): %v", err)
	}
	assertVoters(2)
}

// TestReconcileMembership_RemovesSelfLastWhenLeaderIsAlsoLeaving is the
// regression test for the real bug found live: a bulk departure large
// enough that the leader was frequently among the departing set needed
// one election *per removed voter* to fully drain, since removing self
// mid-loop caused an immediate step-down that aborted the rest of that
// pass with a "not leader" error. Here the leader and one other voter are
// both marked leaving; a single reconcileMembership call must remove both
// — proving self-removal is deferred until after every other pending
// removal in the same pass has already succeeded, not interleaved with
// them in configuration order.
func TestReconcileMembership_RemovesSelfLastWhenLeaderIsAlsoLeaving(t *testing.T) {
	ids := []string{"node-a", "node-b", "node-c", "node-d"}
	nodes := make([]*testNode, len(ids))
	for i, id := range ids {
		nodes[i] = newTestNode(t, id)
	}
	connectAll(nodes)

	if err := nodes[0].raft.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{{ID: raft.ServerID(nodes[0].id), Address: raft.ServerAddress(nodes[0].id)}},
	}).Error(); err != nil {
		t.Fatalf("BootstrapCluster: %v", err)
	}
	leader := awaitLeader(t, nodes, 2*time.Second)
	for _, n := range nodes {
		if n.id == leader.id {
			continue
		}
		if err := leader.raft.AddVoter(raft.ServerID(n.id), raft.ServerAddress(n.id), 0, time.Second).Error(); err != nil {
			t.Fatalf("AddVoter(%s): %v", n.id, err)
		}
	}
	leader = awaitLeader(t, nodes, 2*time.Second)

	n := &raftNode{raft: leader.raft, self: leader.id}
	peers := make([]peer.Peer, len(ids))
	for i, id := range ids {
		peers[i] = peer.Peer{Name: id}
	}
	raftAddr := func(p peer.Peer) raft.ServerAddress { return raft.ServerAddress(p.Name) }

	// The leader itself, plus one other voter, are both leaving.
	var other string
	for _, id := range ids {
		if id != leader.id {
			other = id
			break
		}
	}
	leaving := map[string]bool{leader.id: true, other: true}

	if err := n.reconcileMembership(peers, raftAddr, leaving); err != nil {
		t.Fatalf("reconcileMembership: %v", err)
	}

	voters, err := n.currentVoters()
	if err != nil {
		t.Fatalf("currentVoters: %v", err)
	}
	if len(voters) != 2 {
		t.Fatalf("expected both the leader and the other departing voter removed in a single pass, got %v", voters)
	}
	for _, v := range voters {
		if v == leader.id || v == other {
			t.Fatalf("expected both %s and %s removed, got voters=%v", leader.id, other, voters)
		}
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

// discardLogger is a *slog.Logger that drops everything — decideBootstrap
// and decideBootstrapLocked log operator-visible warnings on paths tests
// below deliberately exercise, and a real test log sink would just be noise.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// shrinkBootstrapElectionTimings speeds up the bootstrap-election Lease's
// acquire/renew/retry cycle for the duration of a test — the production
// defaults (matching client-go's own documented values) involve real wall
// time (15s lease duration, 2s retries) that would make every test using
// decideBootstrap slow. Restores the originals on cleanup.
func shrinkBootstrapElectionTimings(t *testing.T) {
	t.Helper()
	origDuration, origRenew, origRetry, origTimeout := bootstrapElectionLeaseDuration, bootstrapElectionRenewDeadline, bootstrapElectionRetryPeriod, bootstrapElectionTimeout
	bootstrapElectionLeaseDuration = 300 * time.Millisecond
	bootstrapElectionRenewDeadline = 200 * time.Millisecond
	bootstrapElectionRetryPeriod = 20 * time.Millisecond
	bootstrapElectionTimeout = 5 * time.Second
	t.Cleanup(func() {
		bootstrapElectionLeaseDuration, bootstrapElectionRenewDeadline, bootstrapElectionRetryPeriod, bootstrapElectionTimeout = origDuration, origRenew, origRetry, origTimeout
	})
}

// TestDecideBootstrapLocked_FreshInstall covers a genuinely fresh install:
// no marker exists yet, so this node is unconditionally the bootstrapper —
// it must create the marker with a fresh epoch and eagerly record its own
// hasState annotation, without waiting for redpanda.go's periodic refresh.
func TestDecideBootstrapLocked_FreshInstall(t *testing.T) {
	clientset := fake.NewClientset(podWithHasState("collector-collector-alloy-0", nil))

	shouldBootstrap, epoch, err := decideBootstrapLocked(context.Background(), clientset, "collector", "collector-collector-alloy-0", discardLogger())
	if err != nil {
		t.Fatalf("decideBootstrapLocked: %v", err)
	}
	if !shouldBootstrap || epoch == "" {
		t.Fatalf("expected a fresh install to bootstrap with a non-empty epoch, got shouldBootstrap=%v epoch=%q", shouldBootstrap, epoch)
	}

	cm, err := clientset.CoreV1().ConfigMaps("collector").Get(context.Background(), "collector-collector-alloy-raft-bootstrapped", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get marker: %v", err)
	}
	if cm.Data["epoch"] != epoch {
		t.Fatalf("expected marker epoch to be %q, got %q", epoch, cm.Data["epoch"])
	}

	pod, err := clientset.CoreV1().Pods("collector").Get(context.Background(), "collector-collector-alloy-0", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get pod: %v", err)
	}
	if pod.Annotations[hasStateAnnotation] != "true" {
		t.Fatalf("expected hasState annotation to be eagerly set to true, got %q", pod.Annotations[hasStateAnnotation])
	}
}

// TestDecideBootstrapLocked_PeerHasState is the case that must block a
// reclaim: a real cluster still exists somewhere, evidenced by a sibling
// pod's own annotation, even though the marker looks like it could be
// claimed.
func TestDecideBootstrapLocked_PeerHasState(t *testing.T) {
	clientset := fake.NewClientset(
		podWithHasState("collector-collector-alloy-0", nil),
		podWithHasState("collector-collector-alloy-1", strPtr("true")),
	)
	if _, err := clientset.CoreV1().ConfigMaps("collector").Create(context.Background(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "collector-collector-alloy-raft-bootstrapped"},
		Data:       map[string]string{"bootstrappedBy": "collector-collector-alloy-1", "epoch": "epoch-old"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seeding marker: %v", err)
	}

	shouldBootstrap, epoch, err := decideBootstrapLocked(context.Background(), clientset, "collector", "collector-collector-alloy-0", discardLogger())
	if err != nil {
		t.Fatalf("decideBootstrapLocked: %v", err)
	}
	if shouldBootstrap || epoch != "" {
		t.Fatalf("expected no bootstrap while a sibling reports state, got shouldBootstrap=%v epoch=%q", shouldBootstrap, epoch)
	}

	cm, err := clientset.CoreV1().ConfigMaps("collector").Get(context.Background(), "collector-collector-alloy-raft-bootstrapped", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get marker: %v", err)
	}
	if cm.Data["epoch"] != "epoch-old" {
		t.Fatalf("expected marker to be untouched, got epoch %q", cm.Data["epoch"])
	}
}

// TestDecideBootstrapLocked_RecoversFromTotalStateLoss is the "entire fleet
// lost all state simultaneously" path: the marker predates this node's
// restart, but no sibling currently claims to have state, so it's safe to
// reclaim the marker with a fresh epoch — and, same as a fresh install,
// this node's own hasState annotation must be set eagerly.
func TestDecideBootstrapLocked_RecoversFromTotalStateLoss(t *testing.T) {
	clientset := fake.NewClientset(
		podWithHasState("collector-collector-alloy-0", nil),
		podWithHasState("collector-collector-alloy-1", nil),
	)
	if _, err := clientset.CoreV1().ConfigMaps("collector").Create(context.Background(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "collector-collector-alloy-raft-bootstrapped"},
		Data:       map[string]string{"bootstrappedBy": "collector-collector-alloy-1", "epoch": "epoch-old"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seeding marker: %v", err)
	}

	shouldBootstrap, epoch, err := decideBootstrapLocked(context.Background(), clientset, "collector", "collector-collector-alloy-0", discardLogger())
	if err != nil {
		t.Fatalf("decideBootstrapLocked: %v", err)
	}
	if !shouldBootstrap || epoch == "" || epoch == "epoch-old" {
		t.Fatalf("expected a fresh epoch and shouldBootstrap=true, got shouldBootstrap=%v epoch=%q", shouldBootstrap, epoch)
	}

	cm, err := clientset.CoreV1().ConfigMaps("collector").Get(context.Background(), "collector-collector-alloy-raft-bootstrapped", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get marker: %v", err)
	}
	if cm.Data["epoch"] != epoch {
		t.Fatalf("expected marker epoch to be updated to %q, got %q", epoch, cm.Data["epoch"])
	}

	pod, err := clientset.CoreV1().Pods("collector").Get(context.Background(), "collector-collector-alloy-0", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get pod: %v", err)
	}
	if pod.Annotations[hasStateAnnotation] != "true" {
		t.Fatalf("expected hasState annotation to be eagerly set to true, got %q", pod.Annotations[hasStateAnnotation])
	}
}

// TestDecideBootstrap_SerializesConcurrentCallers is the regression test
// for the real gap found in live testing: a full-fleet restart had every
// replica independently read the marker, see no sibling with state, and
// each successfully write back its own epoch in turn — a
// resourceVersion-conditioned Update only rejects a write based on stale
// *data* it already read, not a second, equally fresh read that arrives
// after the first write commits, so it did not prevent this. Gating the
// whole decision behind the bootstrap-election Lease should: at most one
// of these concurrent callers may ever be inside the decision at once, so
// each subsequent caller sees the previous winner's already-true
// hasStateAnnotation and correctly backs off instead of reclaiming again.
func TestDecideBootstrap_SerializesConcurrentCallers(t *testing.T) {
	shrinkBootstrapElectionTimings(t)

	const attempts = 10
	pods := make([]runtime.Object, attempts)
	for i := 0; i < attempts; i++ {
		pods[i] = podWithHasState(fmt.Sprintf("collector-collector-alloy-%d", i), nil)
	}
	clientset := fake.NewClientset(pods...)

	type result struct {
		shouldBootstrap bool
		epoch           string
		err             error
	}
	results := make(chan result, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			shouldBootstrap, epoch, err := decideBootstrap(context.Background(), clientset, "collector", fmt.Sprintf("collector-collector-alloy-%d", i), discardLogger())
			results <- result{shouldBootstrap, epoch, err}
		}(i)
	}
	wg.Wait()
	close(results)

	winners := 0
	var winningEpoch string
	for r := range results {
		if r.err != nil {
			t.Fatalf("decideBootstrap: %v", r.err)
		}
		if r.shouldBootstrap {
			winners++
			winningEpoch = r.epoch
			if r.epoch == "" {
				t.Fatalf("expected the winner to have a non-empty epoch")
			}
		} else if r.epoch != "" {
			t.Fatalf("expected a non-bootstrapping caller to have an empty epoch, got %q", r.epoch)
		}
	}
	if winners != 1 {
		t.Fatalf("expected exactly 1 winner among %d concurrent callers, got %d", attempts, winners)
	}

	cm, err := clientset.CoreV1().ConfigMaps("collector").Get(context.Background(), "collector-collector-alloy-raft-bootstrapped", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get marker: %v", err)
	}
	if cm.Data["epoch"] != winningEpoch {
		t.Fatalf("expected marker epoch %q to match the sole winner's epoch %q", cm.Data["epoch"], winningEpoch)
	}
}

func podWithHasState(name string, hasState *string) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "collector"}}
	if hasState != nil {
		pod.Annotations = map[string]string{hasStateAnnotation: *hasState}
	}
	return pod
}

func strPtr(s string) *string { return &s }

// TestAnyPeerHasState_NoSiblings covers a genuinely fresh install: no other
// pods of this StatefulSet exist yet at all, so there's nothing to report
// state, and the caller must not mistake that for reassurance.
func TestAnyPeerHasState_NoSiblings(t *testing.T) {
	clientset := fake.NewClientset(podWithHasState("collector-collector-alloy-0", nil))

	has, err := anyPeerHasState(context.Background(), clientset, "collector", "collector-collector-alloy-0")
	if err != nil {
		t.Fatalf("anyPeerHasState: %v", err)
	}
	if has {
		t.Fatalf("expected no peer state when this is the only pod")
	}
}

// TestAnyPeerHasState_SiblingWithState is the case that must block a
// reclaim: a real cluster still exists somewhere, evidenced by a sibling
// pod's own annotation, even though the marker looked claimable.
func TestAnyPeerHasState_SiblingWithState(t *testing.T) {
	clientset := fake.NewClientset(
		podWithHasState("collector-collector-alloy-0", nil),
		podWithHasState("collector-collector-alloy-1", strPtr("true")),
	)

	has, err := anyPeerHasState(context.Background(), clientset, "collector", "collector-collector-alloy-0")
	if err != nil {
		t.Fatalf("anyPeerHasState: %v", err)
	}
	if !has {
		t.Fatalf("expected sibling's hasState=true annotation to be reported")
	}
}

// TestAnyPeerHasState_MissingAnnotationTreatedAsFalse verifies a missing
// annotation is never treated as reassurance that state exists — only an
// explicit "true" counts.
func TestAnyPeerHasState_MissingAnnotationTreatedAsFalse(t *testing.T) {
	clientset := fake.NewClientset(
		podWithHasState("collector-collector-alloy-0", nil),
		podWithHasState("collector-collector-alloy-1", nil),
		podWithHasState("collector-collector-alloy-2", strPtr("false")),
	)

	has, err := anyPeerHasState(context.Background(), clientset, "collector", "collector-collector-alloy-0")
	if err != nil {
		t.Fatalf("anyPeerHasState: %v", err)
	}
	if has {
		t.Fatalf("expected missing/false annotations to report no peer state")
	}
}

// TestAnyPeerHasState_IgnoresOtherStatefulSetsAndSelf verifies the sibling
// check is scoped to same-StatefulSet pods only, and never counts the
// caller's own pod as a "peer".
func TestAnyPeerHasState_IgnoresOtherStatefulSetsAndSelf(t *testing.T) {
	clientset := fake.NewClientset(
		podWithHasState("collector-collector-alloy-0", strPtr("true")), // self
		podWithHasState("other-app-0", strPtr("true")),                 // different StatefulSet
	)

	has, err := anyPeerHasState(context.Background(), clientset, "collector", "collector-collector-alloy-0")
	if err != nil {
		t.Fatalf("anyPeerHasState: %v", err)
	}
	if has {
		t.Fatalf("expected self and unrelated StatefulSet pods to be excluded")
	}
}

// TestSetHasStateAnnotation_RoundTrip verifies the annotation actually lands
// on the pod object and can flip both directions — the false->true
// transition matters as much as true, since a restarting node that gets
// caught up as a voter needs its annotation to stop reporting stale.
func TestSetHasStateAnnotation_RoundTrip(t *testing.T) {
	clientset := fake.NewClientset(podWithHasState("collector-collector-alloy-0", nil))

	if err := setHasStateAnnotation(context.Background(), clientset, "collector", "collector-collector-alloy-0", true); err != nil {
		t.Fatalf("setHasStateAnnotation(true): %v", err)
	}
	pod, err := clientset.CoreV1().Pods("collector").Get(context.Background(), "collector-collector-alloy-0", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get pod: %v", err)
	}
	if pod.Annotations[hasStateAnnotation] != "true" {
		t.Fatalf("expected annotation to be true, got %q", pod.Annotations[hasStateAnnotation])
	}

	if err := setHasStateAnnotation(context.Background(), clientset, "collector", "collector-collector-alloy-0", false); err != nil {
		t.Fatalf("setHasStateAnnotation(false): %v", err)
	}
	pod, err = clientset.CoreV1().Pods("collector").Get(context.Background(), "collector-collector-alloy-0", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get pod: %v", err)
	}
	if pod.Annotations[hasStateAnnotation] != "false" {
		t.Fatalf("expected annotation to be false, got %q", pod.Annotations[hasStateAnnotation])
	}
}

func podWithAnnotation(name, key, value string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   "collector",
			Annotations: map[string]string{key: value},
		},
	}
}

// TestRequestGracefulRemoval_RoundTrip verifies the preStop handler's
// write actually lands on the pod object under raftLeavingAnnotation —
// see votersRequestingRemoval, the reader side.
func TestRequestGracefulRemoval_RoundTrip(t *testing.T) {
	clientset := fake.NewClientset(podWithHasState("collector-collector-alloy-0", nil))

	if err := requestGracefulRemoval(context.Background(), clientset, "collector", "collector-collector-alloy-0"); err != nil {
		t.Fatalf("requestGracefulRemoval: %v", err)
	}
	pod, err := clientset.CoreV1().Pods("collector").Get(context.Background(), "collector-collector-alloy-0", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get pod: %v", err)
	}
	if pod.Annotations[raftLeavingAnnotation] != "true" {
		t.Fatalf("expected raftLeavingAnnotation to be true, got %q", pod.Annotations[raftLeavingAnnotation])
	}
}

// TestVotersRequestingRemoval_ScopesAndIncludesSelf covers the read side:
// only same-StatefulSet pods count, missing/false annotations don't, and —
// unlike anyPeerHasState — self is deliberately included, since a leader
// that's also the one leaving needs to see its own request.
func TestVotersRequestingRemoval_ScopesAndIncludesSelf(t *testing.T) {
	clientset := fake.NewClientset(
		podWithAnnotation("collector-collector-alloy-0", raftLeavingAnnotation, "true"), // self, leaving
		podWithAnnotation("collector-collector-alloy-1", raftLeavingAnnotation, "false"),
		podWithHasState("collector-collector-alloy-2", nil),             // no annotation at all
		podWithAnnotation("other-app-0", raftLeavingAnnotation, "true"), // different StatefulSet
	)

	leaving, err := votersRequestingRemoval(context.Background(), clientset, "collector", "collector-collector-alloy-0")
	if err != nil {
		t.Fatalf("votersRequestingRemoval: %v", err)
	}
	if len(leaving) != 1 || !leaving["collector-collector-alloy-0"] {
		t.Fatalf("expected exactly self reported as leaving, got %v", leaving)
	}
}

// TestRaftNode_HasState verifies the redpanda.go-facing accessor a
// restarting replica uses to decide what to report in its own
// hasStateAnnotation: true only once this node actually appears in its own
// Raft configuration, not merely once a raft.Raft instance exists.
func TestRaftNode_HasState(t *testing.T) {
	tn := newTestNode(t, "node-a")
	n := &raftNode{raft: tn.raft, fsm: tn.fsm, self: "node-a"}

	if n.hasState() {
		t.Fatalf("expected hasState=false before bootstrap")
	}

	if err := tn.raft.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{{ID: raft.ServerID(tn.id), Address: raft.ServerAddress(tn.id)}},
	}).Error(); err != nil {
		t.Fatalf("BootstrapCluster: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for !n.hasState() {
		if time.Now().After(deadline) {
			t.Fatalf("expected hasState=true after bootstrapping with self as the only voter")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestAdmissionState_RoundTrip verifies writeAdmissionState's write lands
// in the shared admission-state ConfigMap under podName's own key, sorted,
// and that readAdmissionState recovers the same set back out — see
// redpanda.go's seedAdmissionGate, the reader side's only caller.
// Deliberately a ConfigMap, not a pod annotation like
// hasStateAnnotation/raftLeavingAnnotation — see admissionStateConfigMapSuffix
// for why: a pod annotation doesn't survive a full StatefulSet pod
// recreation, confirmed live, only a container restart within the same
// still-existing pod.
func TestAdmissionState_RoundTrip(t *testing.T) {
	clientset := fake.NewClientset(podWithHasState("collector-collector-alloy-0", nil))

	if err := writeAdmissionState(context.Background(), clientset, "collector", "collector-collector-alloy-0", []string{"b/1", "a/0"}); err != nil {
		t.Fatalf("writeAdmissionState: %v", err)
	}
	cm, err := clientset.CoreV1().ConfigMaps("collector").Get(context.Background(), "collector-collector-alloy-admission-state", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get configmap: %v", err)
	}
	if got := cm.Data["collector-collector-alloy-0"]; got != "a/0,b/1" {
		t.Fatalf("expected sorted value %q, got %q", "a/0,b/1", got)
	}

	restored, err := readAdmissionState(context.Background(), clientset, "collector", "collector-collector-alloy-0")
	if err != nil {
		t.Fatalf("readAdmissionState: %v", err)
	}
	if len(restored) != 2 || restored[0] != "a/0" || restored[1] != "b/1" {
		t.Fatalf("expected [a/0 b/1], got %v", restored)
	}
}

// TestAdmissionState_PreservesOtherPodsKeys verifies writeAdmissionState
// only touches its own key in the shared ConfigMap — every replica writes
// the same object, so a naive overwrite would clobber siblings' state.
func TestAdmissionState_PreservesOtherPodsKeys(t *testing.T) {
	clientset := fake.NewClientset(podWithHasState("collector-collector-alloy-0", nil))

	if err := writeAdmissionState(context.Background(), clientset, "collector", "collector-collector-alloy-0", []string{"a/0"}); err != nil {
		t.Fatalf("writeAdmissionState (pod 0): %v", err)
	}
	if err := writeAdmissionState(context.Background(), clientset, "collector", "collector-collector-alloy-1", []string{"a/1"}); err != nil {
		t.Fatalf("writeAdmissionState (pod 1): %v", err)
	}

	restored0, err := readAdmissionState(context.Background(), clientset, "collector", "collector-collector-alloy-0")
	if err != nil {
		t.Fatalf("readAdmissionState (pod 0): %v", err)
	}
	if len(restored0) != 1 || restored0[0] != "a/0" {
		t.Fatalf("expected pod 0's own state [a/0] to survive pod 1's write, got %v", restored0)
	}
}

// TestReadAdmissionState_NoConfigMapReturnsNilNotError covers the
// genuinely-fresh-StatefulSet / admission-control-just-enabled case: no
// prior state is not an error condition.
func TestReadAdmissionState_NoConfigMapReturnsNilNotError(t *testing.T) {
	clientset := fake.NewClientset(podWithHasState("collector-collector-alloy-0", nil))

	restored, err := readAdmissionState(context.Background(), clientset, "collector", "collector-collector-alloy-0")
	if err != nil {
		t.Fatalf("readAdmissionState: %v", err)
	}
	if restored != nil {
		t.Fatalf("expected nil, got %v", restored)
	}
}
