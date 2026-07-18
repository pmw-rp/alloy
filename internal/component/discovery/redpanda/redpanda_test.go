package redpanda

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/ckit/peer"
	"github.com/grafana/ckit/shard"
	"github.com/hashicorp/raft"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/grafana/alloy/internal/component"
	"github.com/grafana/alloy/internal/component/discovery"
)

type fakeCluster struct {
	ready bool
	peers []peer.Peer
}

func (f fakeCluster) Lookup(_ shard.Key, _ int, _ shard.Op) ([]peer.Peer, error) { return f.peers, nil }
func (f fakeCluster) Peers() []peer.Peer                                         { return f.peers }
func (f fakeCluster) Ready() bool                                                { return f.ready }

func TestPublishTargets_NotReadyPublishesNothing(t *testing.T) {
	var published *Exports
	c := &Component{
		opts: component.Options{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			OnStateChange: func(e component.Exports) {
				exp := e.(Exports)
				published = &exp
			},
		},
		clustering: true,
		cluster:    fakeCluster{ready: false, peers: []peer.Peer{{Name: "self", Self: true}}},
		uuidCache:  map[string]string{},
		tracked:    map[string]trackedPod{},
	}

	c.publishTargets()

	if published == nil {
		t.Fatalf("expected OnStateChange to be called even when not ready")
	}
	if len(published.Targets) != 0 {
		t.Fatalf("expected no targets while cluster is not ready, got %+v", published.Targets)
	}
}

// TestNew_SucceedsBeforeSelfAppearsInPeers is a regression test for a real
// crash: New() used to look itself up in cluster.Peers() synchronously and
// fail the whole component build if it wasn't there yet. But the cluster
// service only registers this node as a peer once its own Run() has
// started the underlying gossip node — a separate lifecycle phase with no
// ordering guarantee relative to component construction. On perfectly
// ordinary startup timing (not just misconfiguration), New() could see an
// empty peer list and crash-loop the component. New() must succeed
// regardless; only Run() is allowed to wait.
func TestNew_SucceedsBeforeSelfAppearsInPeers(t *testing.T) {
	var published *Exports
	opts := component.Options{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnStateChange: func(e component.Exports) {
			exp := e.(Exports)
			published = &exp
		},
		GetServiceData: func(name string) (any, error) {
			return fakeCluster{ready: true, peers: nil}, nil // no peers yet, not even self
		},
	}

	c, err := New(opts, Arguments{Clustering: true})
	if err != nil {
		t.Fatalf("New() must not fail just because self isn't in cluster.Peers() yet, got: %v", err)
	}
	if c == nil {
		t.Fatalf("expected a non-nil component")
	}
	if published == nil {
		t.Fatalf("expected Update (called from New) to publish something, even if empty")
	}
	if len(published.Targets) != 0 {
		t.Fatalf("expected no targets before the raft node exists, got %+v", published.Targets)
	}
}

// TestPublishTargets_FiltersToAdminPortOnly is a regression test for the
// "one broker exports 4 targets" observation from live testing:
// discovery.kubernetes (role: pod) emits one target per declared container
// port, plus one anonymous no-port target for the init container (which has
// zero declared ports), all sharing the same namespace/pod identity. Only
// the admin port actually serves Redpanda's metrics endpoint, so every
// other variant — including the no-port one, which an earlier version of
// this filter incorrectly let through — should be dropped.
func TestPublishTargets_FiltersToAdminPortOnly(t *testing.T) {
	f := newFSM(nil)
	cmd := command{Op: opAssign, BrokerID: "ns/pod-a", ClusterID: "cluster-1", Collector: "me"}
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if res := f.Apply(&raft.Log{Data: data}); res != nil {
		t.Fatalf("Apply: %v", res)
	}

	makeTarget := func(port string) discovery.Target {
		m := map[string]string{
			"__meta_kubernetes_namespace": "ns",
			"__meta_kubernetes_pod_name":  "pod-a",
			"__meta_kubernetes_pod_ip":    "10.0.0.1",
		}
		if port != "" {
			m["__meta_kubernetes_pod_container_port_number"] = port
		}
		return discovery.NewTargetFromMap(m)
	}

	var published *Exports
	c := &Component{
		opts: component.Options{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			OnStateChange: func(e component.Exports) {
				exp := e.(Exports)
				published = &exp
			},
		},
		clustering: true,
		cluster:    fakeCluster{ready: true, peers: []peer.Peer{{Name: "me", Self: true}}},
		node:       &raftNode{fsm: f, self: "me"},
		uuidCache:  map[string]string{"ns/pod-a": "cluster-1"},
		tracked:    map[string]trackedPod{},
		lastTargets: []discovery.Target{
			makeTarget("9644"),  // admin port — should be kept
			makeTarget("9092"),  // Kafka protocol port — should be dropped
			makeTarget("33145"), // RPC port — should be dropped
			makeTarget(""),      // init container, no declared ports — should be dropped too
		},
		adminPort: 9644,
	}

	c.publishTargets()

	if published == nil {
		t.Fatalf("expected OnStateChange to be called")
	}
	if len(published.Targets) != 1 {
		t.Fatalf("expected exactly 1 target (the admin port), got %d: %+v", len(published.Targets), published.Targets)
	}
	if port, _ := published.Targets[0].Get("__meta_kubernetes_pod_container_port_number"); port != "9644" {
		t.Fatalf("expected the surviving target to be the admin port, got port %q", port)
	}
}

// TestNew_OrdinalMode_SkipsClusterService is a regression test for the
// default (Clustering = false) mode's independence from Alloy's native
// clustering: New() must succeed even when opts.GetServiceData is nil,
// since ordinal mode never calls it at all — unlike Clustering = true,
// there's no cluster service dependency to satisfy.
func TestNew_OrdinalMode_SkipsClusterService(t *testing.T) {
	var published *Exports
	opts := component.Options{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnStateChange: func(e component.Exports) {
			exp := e.(Exports)
			published = &exp
		},
		// Deliberately no GetServiceData — calling it would panic (nil func).
	}

	c, err := New(opts, Arguments{})
	if err != nil {
		t.Fatalf("New() must not require the cluster service in ordinal mode, got: %v", err)
	}
	if c == nil {
		t.Fatalf("expected a non-nil component")
	}
	if published == nil {
		t.Fatalf("expected Update (called from New) to publish something, even if empty")
	}
}

func ordinalTarget(namespace, podName, port string) discovery.Target {
	m := map[string]string{
		"__meta_kubernetes_namespace": namespace,
		"__meta_kubernetes_pod_name":  podName,
		"__meta_kubernetes_pod_ip":    "10.0.0.1",
	}
	if port != "" {
		m["__meta_kubernetes_pod_container_port_number"] = port
	}
	return discovery.NewTargetFromMap(m)
}

// TestPublishTargetsOrdinal_SortsAndModShards is the core regression test
// for the restored default scheme: every tracked pod is sorted by
// (namespace, StatefulSet name, pod ordinal) and labeled with its position
// in that sort, mod NumShards — and, unlike clustering mode, every
// currently-present target is published, none filtered by ownership.
func TestPublishTargetsOrdinal_SortsAndModShards(t *testing.T) {
	targets := []discovery.Target{
		ordinalTarget("a", "redpanda-1", "9644"), // sorts 2nd within namespace a
		ordinalTarget("a", "redpanda-0", "9644"), // sorts 1st within namespace a
		ordinalTarget("b", "redpanda-0", "9644"), // sorts last (namespace b > a)
	}

	var published *Exports
	c := &Component{
		opts: component.Options{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			OnStateChange: func(e component.Exports) {
				exp := e.(Exports)
				published = &exp
			},
		},
		numShards: 2,
		tracked: map[string]trackedPod{
			"a/redpanda-1": newTrackedPod(targets[0]),
			"a/redpanda-0": newTrackedPod(targets[1]),
			"b/redpanda-0": newTrackedPod(targets[2]),
		},
		uuidCache: map[string]string{
			"a/redpanda-1": "cluster-a",
			"a/redpanda-0": "cluster-a",
			"b/redpanda-0": "cluster-b",
		},
		lastTargets: targets,
		adminPort:   9644,
	}

	c.publishTargets()

	if published == nil {
		t.Fatalf("expected OnStateChange to be called")
	}
	if len(published.Targets) != 3 {
		t.Fatalf("expected all 3 tracked targets published unfiltered, got %d: %+v", len(published.Targets), published.Targets)
	}

	want := map[string]string{
		"a/redpanda-0": "0", // position 0, mod 2 = 0
		"a/redpanda-1": "1", // position 1, mod 2 = 1
		"b/redpanda-0": "0", // position 2, mod 2 = 0
	}
	for _, target := range published.Targets {
		ns, _ := target.Get("__meta_kubernetes_namespace")
		pod, _ := target.Get("__meta_kubernetes_pod_name")
		key := ns + "/" + pod
		got, _ := target.Get(labelPodOrdinal)
		if got != want[key] {
			t.Errorf("%s: expected %s=%q, got %q", key, labelPodOrdinal, want[key], got)
		}
	}
}

// TestPublishTargetsOrdinal_NoNumShardsUsesRawPosition verifies the
// NumShards=0 (unset) fallback: the raw sort position is used, not a
// modded value — matching the original scheme's documented default.
func TestPublishTargetsOrdinal_NoNumShardsUsesRawPosition(t *testing.T) {
	targets := []discovery.Target{
		ordinalTarget("a", "redpanda-0", "9644"),
		ordinalTarget("a", "redpanda-1", "9644"),
	}

	var published *Exports
	c := &Component{
		opts: component.Options{
			Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
			OnStateChange: func(e component.Exports) { exp := e.(Exports); published = &exp },
		},
		tracked: map[string]trackedPod{
			"a/redpanda-0": newTrackedPod(targets[0]),
			"a/redpanda-1": newTrackedPod(targets[1]),
		},
		uuidCache:   map[string]string{"a/redpanda-0": "cluster-a", "a/redpanda-1": "cluster-a"},
		lastTargets: targets,
		adminPort:   9644,
	}

	c.publishTargets()

	want := map[string]string{"a/redpanda-0": "0", "a/redpanda-1": "1"}
	for _, target := range published.Targets {
		pod, _ := target.Get("__meta_kubernetes_pod_name")
		got, _ := target.Get(labelPodOrdinal)
		if got != want["a/"+pod] {
			t.Errorf("%s: expected %s=%q, got %q", pod, labelPodOrdinal, want["a/"+pod], got)
		}
	}
}

// TestPublishTargetsOrdinal_FiltersToAdminPortOnly verifies the admin-port
// filter applies in ordinal mode too — a deliberate improvement over the
// original (pre-Raft) version of this scheme, which lacked it.
func TestPublishTargetsOrdinal_FiltersToAdminPortOnly(t *testing.T) {
	wrongPort := ordinalTarget("a", "redpanda-0", "9092")
	rightPort := ordinalTarget("a", "redpanda-0", "9644")

	var published *Exports
	c := &Component{
		opts: component.Options{
			Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
			OnStateChange: func(e component.Exports) { exp := e.(Exports); published = &exp },
		},
		tracked:     map[string]trackedPod{"a/redpanda-0": newTrackedPod(rightPort)},
		uuidCache:   map[string]string{"a/redpanda-0": "cluster-a"},
		lastTargets: []discovery.Target{wrongPort, rightPort},
		adminPort:   9644,
	}

	c.publishTargets()

	if len(published.Targets) != 1 {
		t.Fatalf("expected exactly 1 target (the admin port), got %d: %+v", len(published.Targets), published.Targets)
	}
	if port, _ := published.Targets[0].Get("__meta_kubernetes_pod_container_port_number"); port != "9644" {
		t.Fatalf("expected the surviving target to be the admin port, got port %q", port)
	}
}

// TestRun_OrdinalMode_IsNoOp verifies Run() in ordinal mode does none of
// the clustering-mode Raft-node setup and simply blocks until ctx is
// canceled, matching the original (pre-Raft) component's Run().
func TestRun_OrdinalMode_IsNoOp(t *testing.T) {
	c := &Component{opts: component.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if err := c.Run(ctx); err != nil {
		t.Fatalf("expected Run() to return nil once ctx is done, got: %v", err)
	}
}

// TestHandleLeave_NonClusteringModeIsNoOp verifies the preStop handler
// never blocks pod termination in the default (non-clustering) scheme,
// where there's no raft voter to remove in the first place.
func TestHandleLeave_NonClusteringModeIsNoOp(t *testing.T) {
	c := &Component{opts: component.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}

	w := httptest.NewRecorder()
	c.handleLeave(w, httptest.NewRequest(http.MethodGet, "/leave", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// TestHandleLeave_NoNodeIsNoOp covers clustering mode before the raft node
// has been built yet (see waitAndBuildRaftNode) — there's nothing to
// remove, and blocking an ordinary early-lifecycle pod restart on this
// would be wrong.
func TestHandleLeave_NoNodeIsNoOp(t *testing.T) {
	c := &Component{
		opts:       component.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
		clustering: true,
	}

	w := httptest.NewRecorder()
	c.handleLeave(w, httptest.NewRequest(http.MethodGet, "/leave", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// threeVoterCluster builds a genuine 3-voter cluster (bootstrap + AddVoter,
// same pattern as TestReconcileMembership_RemovesVoterRequestingLeave) so
// removing one voter leaves a well-defined 2-voter majority behind —
// unlike removing the sole voter from a 1-node cluster, which is a
// degenerate edge case that doesn't behave like a real multi-replica
// scale-down. Node IDs deliberately use the real <StatefulSet>-<ordinal>
// pod-name convention (not "node-a" style), since votersRequestingRemoval
// scopes siblings by parsing that convention out of the name — a mismatch
// there would silently exclude every "sibling" and defeat the point of a
// test exercising it. Returns the elected leader's underlying testNode (so
// the test can call RemoveServer on it directly, simulating the real
// leader's reconcile() noticing a leave request) and a *raftNode wrapping
// "collector-collector-alloy-1".
func threeVoterCluster(t *testing.T) (leader *testNode, middle *raftNode) {
	t.Helper()
	ids := []string{"collector-collector-alloy-0", "collector-collector-alloy-1", "collector-collector-alloy-2"}
	nodes := make([]*testNode, len(ids))
	for i, id := range ids {
		nodes[i] = newTestNode(t, id)
	}
	connectAll(nodes)

	if err := nodes[0].raft.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{{ID: raft.ServerID(ids[0]), Address: raft.ServerAddress(ids[0])}},
	}).Error(); err != nil {
		t.Fatalf("BootstrapCluster: %v", err)
	}
	l := awaitLeader(t, nodes, 2*time.Second)
	for _, n := range nodes {
		if n.id == l.id {
			continue
		}
		if err := l.raft.AddVoter(raft.ServerID(n.id), raft.ServerAddress(n.id), 0, time.Second).Error(); err != nil {
			t.Fatalf("AddVoter(%s): %v", n.id, err)
		}
	}
	l = awaitLeader(t, nodes, 2*time.Second)

	var mid *testNode
	for _, n := range nodes {
		if n.id == "collector-collector-alloy-1" {
			mid = n
		}
	}
	return l, &raftNode{raft: mid.raft, self: "collector-collector-alloy-1"}
}

// TestHandleLeave_SucceedsWhenRemovedPromptly is the core regression test
// for the preStop mechanism: once the (simulated) leader removes this
// replica as a voter, the handler must notice via its own hasState() poll
// and return well before gracefulLeaveTimeout — and it must have actually
// requested the removal via the pod annotation reconcile() relies on.
func TestHandleLeave_SucceedsWhenRemovedPromptly(t *testing.T) {
	orig := gracefulLeaveTimeout
	defer func() { gracefulLeaveTimeout = orig }()
	gracefulLeaveTimeout = 3 * time.Second

	leader, node := threeVoterCluster(t)
	clientset := fake.NewClientset(podWithHasState("collector-collector-alloy-1", nil))
	c := &Component{
		opts:         component.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
		clustering:   true,
		node:         node,
		k8sClient:    clientset,
		k8sNamespace: "collector",
	}

	// Simulates the leader noticing the leave request and removing this
	// replica as a voter partway through the handler's poll loop.
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = leader.raft.RemoveServer(raft.ServerID("collector-collector-alloy-1"), 0, time.Second).Error()
	}()

	w := httptest.NewRecorder()
	start := time.Now()
	c.handleLeave(w, httptest.NewRequest(http.MethodGet, "/leave", nil))
	elapsed := time.Since(start)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if elapsed >= gracefulLeaveTimeout {
		t.Fatalf("expected handleLeave to return promptly once removed, took %s", elapsed)
	}

	pod, err := clientset.CoreV1().Pods("collector").Get(context.Background(), "collector-collector-alloy-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get pod: %v", err)
	}
	if pod.Annotations[raftLeavingAnnotation] != "true" {
		t.Fatalf("expected raftLeavingAnnotation to be set, got %q", pod.Annotations[raftLeavingAnnotation])
	}
}

// TestHandleLeave_TimesOutIfNeverRemoved verifies the handler gives up
// after gracefulLeaveTimeout rather than hanging forever — if a leader
// never processes the leave request (e.g. it's also gone), a stuck
// preStop hook would otherwise wedge the whole pod's termination.
func TestHandleLeave_TimesOutIfNeverRemoved(t *testing.T) {
	orig := gracefulLeaveTimeout
	defer func() { gracefulLeaveTimeout = orig }()
	gracefulLeaveTimeout = 300 * time.Millisecond

	_, node := threeVoterCluster(t)
	clientset := fake.NewClientset(podWithHasState("collector-collector-alloy-1", nil))
	c := &Component{
		opts:         component.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
		clustering:   true,
		node:         node,
		k8sClient:    clientset,
		k8sNamespace: "collector",
	}
	// Deliberately never removed as a voter.

	w := httptest.NewRecorder()
	c.handleLeave(w, httptest.NewRequest(http.MethodGet, "/leave", nil))

	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected 504, got %d", w.Code)
	}
}

// TestGracefulLeaveDeadline_ScalesWithSimultaneousDepartures is a pure
// unit test of the scaling formula: one departure gets the base budget,
// each additional simultaneous one adds gracefulLeaveTimeoutPerExtra, and
// the total never exceeds gracefulLeaveTimeoutMax.
func TestGracefulLeaveDeadline_ScalesWithSimultaneousDepartures(t *testing.T) {
	origBase, origExtra, origMax := gracefulLeaveTimeout, gracefulLeaveTimeoutPerExtra, gracefulLeaveTimeoutMax
	defer func() {
		gracefulLeaveTimeout, gracefulLeaveTimeoutPerExtra, gracefulLeaveTimeoutMax = origBase, origExtra, origMax
	}()
	gracefulLeaveTimeout = 20 * time.Second
	gracefulLeaveTimeoutPerExtra = 5 * time.Second
	gracefulLeaveTimeoutMax = 90 * time.Second

	cases := []struct {
		numLeaving int
		want       time.Duration
	}{
		{0, 20 * time.Second},  // never observed in practice (self always counts), but must not underflow
		{1, 20 * time.Second},  // the common case: only this replica leaving
		{3, 30 * time.Second},  // confirmed-safe live: 20 + 2*5
		{9, 60 * time.Second},  // the bulk case that motivated this: 20 + 8*5
		{50, 90 * time.Second}, // capped rather than growing unboundedly
	}
	for _, c := range cases {
		if got := gracefulLeaveDeadline(c.numLeaving); got != c.want {
			t.Errorf("gracefulLeaveDeadline(%d) = %s, want %s", c.numLeaving, got, c.want)
		}
	}
}

// TestHandleLeave_ScalesTimeoutWithSimultaneousDepartures is the
// regression test for the real bug found live: a 10-to-1 scale-down let
// several preStop hooks time out on a fixed budget before the leader
// worked through all 9 departures. Here, two other voters are already
// marked leaving when this replica's handler starts, so its effective
// deadline must be scaled up accordingly — removal arrives after the
// unscaled base timeout would have given up, but before the scaled one.
func TestHandleLeave_ScalesTimeoutWithSimultaneousDepartures(t *testing.T) {
	origBase, origExtra, origMax := gracefulLeaveTimeout, gracefulLeaveTimeoutPerExtra, gracefulLeaveTimeoutMax
	defer func() {
		gracefulLeaveTimeout, gracefulLeaveTimeoutPerExtra, gracefulLeaveTimeoutMax = origBase, origExtra, origMax
	}()
	gracefulLeaveTimeout = 200 * time.Millisecond
	gracefulLeaveTimeoutPerExtra = 200 * time.Millisecond
	gracefulLeaveTimeoutMax = 5 * time.Second

	leader, node := threeVoterCluster(t)
	// ordinals 0 and 2 are already leaving before ordinal 1's handler even
	// starts — three simultaneous departures, so ordinal 1's own deadline
	// should be the base plus two extras (600ms), not just the base (200ms).
	clientset := fake.NewClientset(
		podWithAnnotation("collector-collector-alloy-0", raftLeavingAnnotation, "true"),
		podWithHasState("collector-collector-alloy-1", nil),
		podWithAnnotation("collector-collector-alloy-2", raftLeavingAnnotation, "true"),
	)
	c := &Component{
		opts:         component.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
		clustering:   true,
		node:         node,
		k8sClient:    clientset,
		k8sNamespace: "collector",
	}

	// Removed at 400ms: after the unscaled 200ms base would have expired,
	// but before the scaled 600ms deadline.
	go func() {
		time.Sleep(400 * time.Millisecond)
		_ = leader.raft.RemoveServer(raft.ServerID("collector-collector-alloy-1"), 0, time.Second).Error()
	}()

	w := httptest.NewRecorder()
	c.handleLeave(w, httptest.NewRequest(http.MethodGet, "/leave", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (removed within the scaled deadline), got %d", w.Code)
	}
}

// TestCheckQuorumLoss_HealthyNeverTriggers verifies the common case: every
// configured voter is live, so checkQuorumLoss must never even start the
// grace-period clock, let alone restart the process.
func TestCheckQuorumLoss_HealthyNeverTriggers(t *testing.T) {
	origExit := exitProcess
	defer func() { exitProcess = origExit }()
	exited := false
	exitProcess = func(int) { exited = true }

	tn := newTestNode(t, "node-a")
	bootstrapFiveServers(t, tn)
	node := &raftNode{raft: tn.raft, self: "node-a"}

	c := &Component{
		opts:    component.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
		cluster: fakeCluster{peers: peersNamed("node-a", "node-b", "node-c", "node-d", "node-e")},
	}

	c.checkQuorumLoss(node)

	if exited {
		t.Fatalf("expected no restart while quorum is fully reachable")
	}
	if !c.quorumLostSince.IsZero() {
		t.Fatalf("expected quorumLostSince to stay zero while healthy")
	}
}

// TestCheckQuorumLoss_TransientBlipResets is a regression test for the
// exact hazard the grace period exists to avoid: a live full-fleet-restart
// test showed a legitimate ~25s leader-election flap from DNS propagation
// lag. Quorum looking unreachable for one tick and then recovering must
// not restart the process, and must reset the tracked start time so a
// later, unrelated blip doesn't inherit stale timing.
func TestCheckQuorumLoss_TransientBlipResets(t *testing.T) {
	origExit := exitProcess
	defer func() { exitProcess = origExit }()
	exited := false
	exitProcess = func(int) { exited = true }

	tn := newTestNode(t, "node-a")
	bootstrapFiveServers(t, tn)
	node := &raftNode{raft: tn.raft, self: "node-a"}

	c := &Component{
		opts:    component.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
		cluster: fakeCluster{peers: peersNamed("node-a", "node-b")}, // only 2 of 5 live
	}

	c.checkQuorumLoss(node)
	if c.quorumLostSince.IsZero() {
		t.Fatalf("expected quorumLostSince to be set once quorum looks unreachable")
	}

	c.cluster = fakeCluster{peers: peersNamed("node-a", "node-b", "node-c", "node-d", "node-e")}
	c.checkQuorumLoss(node)

	if exited {
		t.Fatalf("expected no restart once quorum recovered")
	}
	if !c.quorumLostSince.IsZero() {
		t.Fatalf("expected quorumLostSince to reset once quorum recovered")
	}
}

// TestCheckQuorumLoss_SustainedLossTriggersRestart is the regression test
// for the real deadlock found in live testing: losing more voters at once
// than the old configuration's fault tolerance permanently strands the
// survivors (no leader electable, no RemoveServer committable). Once
// hasReachableQuorum has reported false for longer than quorumLossGrace,
// this replica must restart itself so it can recover via decideBootstrap
// after coming back with no local state.
func TestCheckQuorumLoss_SustainedLossTriggersRestart(t *testing.T) {
	origExit := exitProcess
	defer func() { exitProcess = origExit }()
	var exitCode int
	exited := false
	exitProcess = func(code int) { exited = true; exitCode = code }

	tn := newTestNode(t, "node-a")
	bootstrapFiveServers(t, tn)
	node := &raftNode{raft: tn.raft, self: "node-a"}

	c := &Component{
		opts:            component.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
		cluster:         fakeCluster{peers: peersNamed("node-a", "node-b")}, // only 2 of 5 live
		quorumLostSince: time.Now().Add(-quorumLossGrace - time.Second),
	}

	c.checkQuorumLoss(node)

	if !exited {
		t.Fatalf("expected a restart once quorum loss has persisted past the grace period")
	}
	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}
}

// TestSignalPublish_Coalesces is a regression test for the "flickering
// publish count" observation from live testing: a follower catching up on
// a backlog of commits (or the leader itself, committing many Assign
// commands in one reconcile pass) fired one republish per individual
// commit. signalPublish is the fix: repeated signals collapse into at most
// one pending item, regardless of how many times it's called before
// something drains it.
func TestSignalPublish_Coalesces(t *testing.T) {
	c := &Component{publishSignal: make(chan struct{}, 1)}

	for i := 0; i < 20; i++ {
		c.signalPublish()
	}

	if len(c.publishSignal) != 1 {
		t.Fatalf("expected exactly 1 pending signal after 20 calls, got %d", len(c.publishSignal))
	}

	<-c.publishSignal
	if len(c.publishSignal) != 0 {
		t.Fatalf("expected the channel to be empty after draining")
	}

	// A signal sent after draining must be queued again — this isn't a
	// one-shot latch, it should keep working across multiple bursts.
	c.signalPublish()
	if len(c.publishSignal) != 1 {
		t.Fatalf("expected a fresh signal to be queued after drain, got %d pending", len(c.publishSignal))
	}
}

func clusterUUIDHandler(uuid string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/cluster/uuid" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cluster_uuid":"` + uuid + `"}`))
	}
}

// podAddrOf extracts the host and port from an httptest server's listener
// address, for building probeAdminAPI's podIP/AdminPort inputs directly
// rather than through its full URL (probeAdminAPI builds its own
// scheme://host:port URL internally).
func podAddrOf(t *testing.T, addr string) (host string, port int) {
	t.Helper()
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", addr, err)
	}
	portNum, err := strconv.Atoi(p)
	if err != nil {
		t.Fatalf("parsing port %q: %v", p, err)
	}
	return h, portNum
}

// TestProbeAdminAPI_PlaintextOnlyServerDetectsHTTP covers a Redpanda
// cluster with no TLS on its admin API — probeAdminAPI's https attempt
// must fail cleanly and fall back to http, not error out just because the
// first scheme tried didn't work.
func TestProbeAdminAPI_PlaintextOnlyServerDetectsHTTP(t *testing.T) {
	srv := httptest.NewServer(clusterUUIDHandler("plaintext-uuid"))
	defer srv.Close()
	host, port := podAddrOf(t, srv.Listener.Addr().String())

	uuid, tlsEnabled, err := probeAdminAPI(host, Arguments{AdminPort: port})
	if err != nil {
		t.Fatalf("probeAdminAPI: %v", err)
	}
	if uuid != "plaintext-uuid" {
		t.Fatalf("expected uuid %q, got %q", "plaintext-uuid", uuid)
	}
	if tlsEnabled {
		t.Fatalf("expected tlsEnabled=false for a plaintext-only server")
	}
}

// TestProbeAdminAPI_TLSServerDetectsHTTPS covers a Redpanda cluster with
// TLS enabled (self-signed, matching typical in-cluster certs) — requires
// TLSSkipVerify since the test server's cert isn't in any trust store.
func TestProbeAdminAPI_TLSServerDetectsHTTPS(t *testing.T) {
	srv := httptest.NewTLSServer(clusterUUIDHandler("tls-uuid"))
	defer srv.Close()
	host, port := podAddrOf(t, srv.Listener.Addr().String())

	uuid, tlsEnabled, err := probeAdminAPI(host, Arguments{AdminPort: port, TLSSkipVerify: true})
	if err != nil {
		t.Fatalf("probeAdminAPI: %v", err)
	}
	if uuid != "tls-uuid" {
		t.Fatalf("expected uuid %q, got %q", "tls-uuid", uuid)
	}
	if !tlsEnabled {
		t.Fatalf("expected tlsEnabled=true for a TLS-only server")
	}
}

// TestProbeAdminAPI_TLSServerWithoutSkipVerifyFails covers the case where
// a self-signed cert is rejected (TLSSkipVerify: false) and there's no
// plaintext listener to fall back to either — both attempts should fail,
// not silently succeed via the wrong scheme.
func TestProbeAdminAPI_TLSServerWithoutSkipVerifyFails(t *testing.T) {
	srv := httptest.NewTLSServer(clusterUUIDHandler("tls-uuid"))
	defer srv.Close()
	host, port := podAddrOf(t, srv.Listener.Addr().String())

	_, _, err := probeAdminAPI(host, Arguments{AdminPort: port, TLSSkipVerify: false})
	if err == nil {
		t.Fatalf("expected an error: https should fail cert validation, and http has nothing to fall back to")
	}
}

// TestProbeAdminAPI_NothingListeningFails covers a pod whose admin port
// isn't reachable at all yet (e.g. still starting) — both attempts should
// fail with a combined error, not hang or panic.
func TestProbeAdminAPI_NothingListeningFails(t *testing.T) {
	// Grab a port and immediately release it so nothing is listening.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	host, port := podAddrOf(t, l.Addr().String())
	l.Close()

	_, _, err = probeAdminAPI(host, Arguments{AdminPort: port})
	if err == nil {
		t.Fatalf("expected an error when nothing is listening")
	}
}
