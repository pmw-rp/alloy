// Package redpanda implements the discovery.redpanda component, which
// enriches discovered pod targets with per-pod Redpanda cluster UUIDs by
// querying each pod's admin API. This solves the multi-cluster UUID problem
// where multiple Redpanda clusters share a namespace — each pod gets its own
// cluster_uuid label rather than a single shared value.
//
// UUIDs are cached permanently (they never change for a cluster's lifetime).
// Update blocks until all uncached pods are fetched, so published targets
// always carry the cluster_uuid label.
//
// Which collector replica scrapes which broker is decided one of two ways,
// selected by the Clustering argument:
//
//   - Clustering = false (the default): a static ordinal % NumShards
//     scheme. Every tracked pod (present or recently absent — see
//     StaleTimeout) is sorted by (namespace, StatefulSet name, pod ordinal)
//     and labeled with its position in that sort, mod NumShards if set, under
//     __tmp_pod_ordinal (see publishTargetsOrdinal). This component does not
//     decide ownership itself in this mode — every currently-present target
//     is published unfiltered, and a downstream relabel rule (comparing
//     __tmp_pod_ordinal against this replica's own env("POD_INDEX")) is what
//     actually decides which replica scrapes which pod. NumShards has to be
//     kept manually in sync with the real collector replica count, and the
//     global sort couples every Redpanda cluster's assignment to every other
//     cluster's topology — a broker added to one cluster can reshuffle scrape
//     ownership for brokers in a totally unrelated one. No dependency on
//     Alloy's native clustering at all: Run() is a no-op in this mode.
//   - Clustering = true: an embedded Raft group spanning every replica of
//     this component (one Raft peer per Alloy collector replica, membership
//     driven by Alloy's native gossip clustering via cluster.Peers() — see
//     raftnode.go). Only the current Raft leader proposes assignment changes
//     (see allocate.go); every replica reads the resulting committed state to
//     decide which brokers it owns and filters Exports.Targets to just those
//     — unlike the ordinal scheme, this component decides ownership itself,
//     and nothing downstream needs to relabel-filter by shard.
//
// Migrating a deployment from one mode to the other requires a coordinated,
// all-at-once rollout of every collector replica: there is no safety net that
// mirrors the other mode's assignment until every replica has switched, so a
// rolling restart across the switch can briefly double-scrape or gap brokers
// while old- and new-mode replicas coexist.
//
// Everything below this point applies only when Clustering = true.
//
// Every replica's Raft state is deliberately ephemeral (no persistent
// volume) so a restart never depends on local disk surviving — deciding
// whether it's safe to bootstrap a fresh Raft cluster is instead settled by
// decideBootstrap (raftnode.go), gated behind an exclusive, expiring
// Kubernetes Lease so at most one replica ever makes this decision at a
// time, no matter how many restart at once or how inconsistent their
// gossip views of each other are. A replica that decides against
// bootstrapping (because a real cluster already exists, marked by a
// well-known ConfigMap) just waits to be added as a voter by whoever
// already has.
//
// A full-fleet restart is therefore routine, but is also exactly the
// scenario that can leave that marker pointing at a cluster that no longer
// exists anywhere: if every replica loses its Raft state simultaneously,
// the marker still survives in the Kubernetes API, so nobody can win a
// naive "does the marker already exist" check, yet nobody retains any
// configuration either — a permanent deadlock with no path to recovery.
// Recovering from that without reintroducing split-brain risk needs two
// more pieces, both leader/voter-agnostic and deliberately not based on
// any timeout or elapsed-time heuristic:
//
//   - Every replica publishes, on its own pod object, whether its local
//     Raft store currently has state (see setHasStateAnnotation). A
//     replica deciding whether to bootstrap checks every sibling's
//     annotation (see anyPeerHasState) before ever treating the marker as
//     stale — only if literally none report state does it reclaim the
//     marker, and the winner eagerly publishes its own annotation before
//     releasing the decision lease, so the very next contender (often
//     within milliseconds, during a full-fleet restart) sees it and backs
//     off instead of reclaiming a second time.
//   - Each Raft cluster incarnation mints a random "epoch" (see fsm.go's
//     opSetEpoch) once, at genuine bootstrap, and replicates it into the
//     FSM itself — not just the external ConfigMap — so any future leader,
//     even after an ordinary in-cluster leadership change, inherently knows
//     the cluster's true identity from its own replicated state. The
//     leader periodically heartbeats this epoch into the marker (see
//     refreshEpochHeartbeat), refusing to overwrite it if the marker's
//     recorded epoch doesn't match — a mismatch is strong evidence of a
//     split-brain in progress and gets logged loudly rather than silently
//     resolved by whichever side writes last.
//
// A collector replica that restarts is removed as a Raft voter immediately
// (see raftnode.go's reconcileMembership) so the rest of the fleet doesn't
// have to wait out however long it takes to come back before Raft writes
// can proceed again — with few voters, every write needs (almost) all of
// them. But its brokers' assignment records aren't touched immediately:
// reconcileAssignments leaves them exactly as they are, still pointing at
// the now-absent collector, for a grace period (collectorRejoinGrace below)
// in case the same-named replica returns — since the record was never
// changed, a same-named voter rejoining makes it valid again immediately,
// no command needed at all. Only once that grace period elapses without the
// replica returning are those brokers actually handed to another collector.
//
// That grace period is skipped entirely when the departure is deliberate —
// a graceful scale-down via the preStop hook (see handleLeave below),
// signaled the same way reconcileMembership already learns about it:
// votersRequestingRemoval's leaving set. Confirmed live: without this,
// every ordinary scale-down left a departing replica's brokers unscraped by
// anyone for the full grace period, since nothing distinguished "gone for
// good" from "might come right back" — the two cases the grace period
// exists to tell apart in the first place. A leaving collector's brokers
// are reassigned on the very same reconcile pass that removes it as a
// voter, landing on their new owner before the departing replica has even
// finished shutting down (it's still blocked in its own preStop handler at
// this point), shrinking the gap to at most a brief window of
// double-scraping rather than a guaranteed hole in collection.
//
// Admission control (the AdmissionControl argument, only meaningful when
// Clustering = true) adds an optional, purely local layer between "the FSM
// says I own this broker" and "I've published it in Exports.Targets."
// Raft's job is unchanged — it still only decides ownership. Each replica
// additionally holds its own admissionGate (admission.go), gating which of
// its Raft-assigned brokers are actually admitted into published targets
// based on this replica's own flush-queue health, not admitting everything
// the instant Raft assigns it:
//
//   - Health signal: flushHealthReader scrapes Alloy's own /metrics
//     endpoint in-process (no real TCP round trip — see
//     httpservice.Data's DialFunc) for a configured
//     otelcol.processor.metricsbatcher instance's flushes_in_flight /
//     flushes_capacity gauges, and computes their ratio. A name-based
//     coupling (AdmissionControl.FlushMetricsComponentID): Alloy has no
//     typed reference between unrelated components' Arguments/Exports for
//     this. Confirmed live: flushes_capacity is recorded the moment
//     metricsbatcher starts, but flushes_in_flight only gets its first
//     data point once a flush actually happens — which itself depends on
//     something having been admitted to scrape. Missing in_flight (with
//     capacity present) is therefore treated as "definitely idle" (ratio
//     0), not an error, or a fresh deployment would deadlock permanently:
//     nothing ever admitted because the health check always fails,
//     nothing ever flowing through metricsbatcher to produce that first
//     data point because nothing was ever admitted.
//   - Every reconcile tick (the same one reconcile()/checkQuorumLoss
//     already run on, but unlike reconcile() this runs on every replica,
//     not just the leader — admission is a local capacity decision about
//     *this* replica, not a cluster-wide ownership one): at or above
//     HighWatermark, drop the most-recently-admitted broker; at or below
//     LowWatermark, admit exactly one more from the backlog (assigned but
//     not yet admitted, in a deterministic sorted order); in between, hold
//     steady. One target per tick, deliberately conservative — the
//     alternative (batch or all-at-once admission) converges faster but
//     risks a bigger single-step overshoot back into overload.
//   - The admitted set is persisted into a shared ConfigMap, one key per
//     pod name (admissionStateConfigMapSuffix, raftnode.go), whenever it
//     changes, and restored — intersected with whatever's currently
//     actually assigned, in case ownership moved on while this replica
//     was down — once at startup (seedAdmissionGate). A ConfigMap, not a
//     pod annotation like hasStateAnnotation/raftLeavingAnnotation above:
//     confirmed live, a pod annotation does not survive a full StatefulSet
//     pod recreation (a fresh Pod object with none of the previous
//     incarnation's annotations), only a container restart within the
//     same still-existing pod — and it's exactly a full recreation this
//     needs to survive. This is what lets a routine restart of an
//     already-stable replica resume where it left off instead of always
//     ramping from empty; it does not replace ongoing health checks,
//     which resume immediately and will shrink again within one tick if
//     conditions actually changed while this replica was down.
//   - The gap between assigned and admitted is published as a gauge
//     (discovery_redpanda_admission_gap) for an external HPA/KEDA to scale
//     alloy.replicas on — a persistently nonzero gap is this component's
//     own signal that it can't safely admit everything it's been asked to
//     own, decoupling *detection* (this component's job) from *reaction*
//     (Kubernetes' job).
//
// reconcileMembership's one-at-a-time RemoveServer assumes voters depart
// gradually enough for each removal to commit while the rest of the old
// configuration is still reachable. Losing more voters at once than that
// configuration's fault tolerance (confirmed live: 5 replicas scaled down
// to 2, or 10 down to 3) leaves the survivors permanently below quorum —
// no leader electable, no RemoveServer committable, since both need a
// majority of a configuration that no longer has one. checkQuorumLoss
// detects this on every replica (not just the leader, since a replica
// stuck without one never runs reconcile() at all) by comparing Raft's
// configured voters against cluster.Peers() — a protocol independent of
// Raft, so it keeps working through the deadlock — and, once that's
// stayed structurally impossible for quorumLossGrace, restarts this
// replica's process. Ephemeral storage means it comes back with no local
// state, and the existing decideBootstrap safeguards above — untouched by
// any of this — govern whether recovering from there is actually safe.
package redpanda

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/ckit/peer"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/grafana/alloy/internal/component"
	"github.com/grafana/alloy/internal/component/discovery"
	"github.com/grafana/alloy/internal/featuregate"
	"github.com/grafana/alloy/internal/service/cluster"
	httpservice "github.com/grafana/alloy/internal/service/http"
)

func init() {
	component.Register(component.Registration{
		Name:      "discovery.redpanda",
		Stability: featuregate.StabilityGenerallyAvailable,
		Args:      Arguments{},
		Exports:   Exports{},
		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			return New(opts, args.(Arguments))
		},
	})
}

const (
	defaultAdminPort    = 9644
	defaultRaftBindPort = 9700
	fetchTimeout        = 10 * time.Second
	defaultStaleTimeout = 5 * time.Minute
	reconcileInterval   = 10 * time.Second
	// collectorRejoinGrace is how long a collector's brokers stay assigned
	// to it (untouched, unreassigned) after it drops out of the voter set,
	// before being treated as truly gone and reassigned elsewhere. Long
	// enough to cover an ordinary pod restart (image already cached, no
	// heavy startup work) without the resulting reassignment/rebalance
	// churn, short enough to still react reasonably promptly to a genuine
	// scale-down. Not exposed as an Argument: the tradeoff it's balancing
	// (how long a restart takes vs. how long affected brokers can tolerate
	// going unscraped) doesn't need per-deployment tuning to get right.
	collectorRejoinGrace = 60 * time.Second
	// labelPodOrdinal is the target label publishTargetsOrdinal attaches in
	// non-clustering mode — see the package doc comment.
	labelPodOrdinal = "__tmp_pod_ordinal"
	// labelAdminTLSEnabled is the target label both publishTargetsOrdinal
	// and publishTargetsClustered attach alongside cluster_id, recording
	// whether probeAdminAPI detected this pod's admin API as speaking TLS
	// ("true"/"false") — a downstream relabel rule maps this onto
	// __scheme__ so prometheus.scrape picks the right scheme per target,
	// since a fleet of monitored Redpanda clusters isn't guaranteed to be
	// uniformly TLS or plaintext.
	labelAdminTLSEnabled = "__tmp_admin_tls_enabled"
	// defaultAdmissionHighWatermark/LowWatermark are AdmissionControlArguments'
	// defaults — see the package doc comment's admission control section.
	defaultAdmissionHighWatermark = 0.75
	defaultAdmissionLowWatermark  = 0.5
)

// quorumLossGrace is how long raftNode.hasReachableQuorum must report a
// structurally-unreachable quorum, continuously, before a replica
// concludes it's genuinely stuck (see checkQuorumLoss) rather than mid an
// ordinary election — comfortably above ordinary election/convergence
// noise (a live full-fleet-restart test showed a legitimate ~25s leader
// flap from DNS propagation lag right after mass pod recreation). A var,
// not a const, so tests can shrink it.
var quorumLossGrace = 90 * time.Second

// exitProcess terminates this replica so Kubernetes restarts it fresh —
// see checkQuorumLoss. A var, not a direct os.Exit call, so tests can
// observe the trigger without killing the test process.
var exitProcess = os.Exit

// Arguments configures the discovery.redpanda component.
type Arguments struct {
	Targets   []discovery.Target `alloy:"targets,attr"`
	AdminPort int                `alloy:"admin_port,attr,optional"`
	// TLSSkipVerify skips certificate verification whenever a pod's admin
	// API turns out to speak TLS — see probeAdminAPI's doc comment for why
	// there's no tls_enabled counterpart: whether TLS is in use at all is
	// detected per-pod, not configured.
	TLSSkipVerify bool `alloy:"tls_skip_verify,attr,optional"`
	// StaleTimeout is how long a pod that disappears from discovery holds its
	// assignment/ordinal slot before being evicted. Default: 5m.
	StaleTimeout time.Duration `alloy:"stale_timeout,attr,optional"`
	// Clustering selects the Raft-backed allocation scheme (see the package
	// doc comment) instead of the default static ordinal % NumShards scheme.
	// Default: false.
	Clustering bool `alloy:"clustering,attr,optional"`
	// RaftBindPort is the TCP port this component binds for Raft RPC between
	// collector replicas when Clustering is true. Must be the same across
	// every replica in the fleet, and separate from Alloy's own clustering
	// port. Default: 9700. Ignored when Clustering is false.
	RaftBindPort int `alloy:"raft_bind_port,attr,optional"`
	// NumShards is the number of collector replicas, used only when
	// Clustering is false: each tracked pod's stable sort position is
	// reported mod NumShards rather than raw, so collectors and Redpanda
	// pods don't need to be 1:1. Default: 0 (use the raw position). Ignored
	// when Clustering is true.
	NumShards int `alloy:"num_shards,attr,optional"`
	// AdmissionControl gates how many of this replica's Raft-assigned
	// brokers are actually admitted into published targets, based on local
	// flush-queue health — see the package doc comment's admission control
	// section. Only meaningful when Clustering is true; ignored otherwise.
	AdmissionControl AdmissionControlArguments `alloy:"admission_control,block,optional"`
}

// AdmissionControlArguments configures discovery.redpanda's optional
// admission control layer — see the package doc comment.
type AdmissionControlArguments struct {
	// Enabled turns admission control on. Default: false.
	Enabled bool `alloy:"enabled,attr,optional"`
	// FlushMetricsComponentID names the otelcol.processor.metricsbatcher
	// component instance whose flushes_in_flight/flushes_capacity gauges
	// this component reads to decide whether it's safe to admit more (e.g.
	// "otelcol.processor.metricsbatcher.default"). Required when Enabled.
	FlushMetricsComponentID string `alloy:"flush_metrics_component_id,attr,optional"`
	// HighWatermark is the flushes_in_flight/flushes_capacity ratio at or
	// above which the admission gate drops its most-recently-admitted
	// broker. Default: 0.75.
	HighWatermark float64 `alloy:"high_watermark,attr,optional"`
	// LowWatermark is the ratio at or below which the admission gate
	// admits one more broker from the backlog. Default: 0.5.
	LowWatermark float64 `alloy:"low_watermark,attr,optional"`
}

// Exports holds the enriched targets output.
type Exports struct {
	Targets []discovery.Target `alloy:"targets,attr"`
}

// trackedPod holds the last-seen timestamp for a pod, plus (for
// publishTargetsOrdinal's sort — unused when Clustering is true) the
// StatefulSet name and numeric ordinal parsed from its pod name. Its
// identity (the map key) is namespace/podName; that, plus the resolved
// cluster UUID in uuidCache, is all either allocation mode needs.
type trackedPod struct {
	lastSeen   time.Time
	namespace  string
	stsName    string
	podOrdinal int
}

// newTrackedPod extracts stable sort metadata from a newly-discovered
// target — see publishTargetsOrdinal. Computed unconditionally, regardless
// of Clustering, since it's cheap and harmless to have if unused.
func newTrackedPod(t discovery.Target) trackedPod {
	ns, _ := t.Get("__meta_kubernetes_namespace")
	podName, _ := t.Get("__meta_kubernetes_pod_name")
	stsName, ordinal := parsePodName(podName)
	return trackedPod{namespace: ns, stsName: stsName, podOrdinal: ordinal}
}

// parsePodName splits a StatefulSet pod name (e.g. "redpanda-sandbox-2")
// into the StatefulSet name ("redpanda-sandbox") and numeric ordinal (2) by
// splitting on the last '-'. Falls back to (podName, 0) for non-conforming
// names.
func parsePodName(podName string) (stsName string, ordinal int) {
	idx := strings.LastIndexByte(podName, '-')
	if idx < 0 {
		return podName, 0
	}
	n, err := strconv.Atoi(podName[idx+1:])
	if err != nil {
		return podName, 0
	}
	return podName[:idx], n
}

// Component implements the discovery.redpanda component.
type Component struct {
	opts       component.Options
	clustering bool // see Arguments.Clustering; selects publishTargets' behavior

	// numShards is only meaningful when !clustering — see
	// publishTargetsOrdinal.
	numShards int

	// cluster/node/raftBindPort are only ever populated, and only ever
	// meaningful, when clustering is true.
	cluster      cluster.Cluster
	node         *raftNode
	raftBindPort int

	// k8sClient/k8sNamespace are built once, in waitAndBuildRaftNode, and
	// reused for the lifetime of the component: both the periodic
	// hasState-annotation refresh below and the leader's epoch heartbeat
	// (see reconcile) need the same Kubernetes API access newRaftNode
	// already required for the bootstrap decision.
	k8sClient    kubernetes.Interface
	k8sNamespace string

	// publishSignal coalesces bursts of FSM changes (e.g. a new follower
	// catching up on a backlog of commits, or the leader itself committing
	// many Assign commands in one reconcile pass) into a single republish.
	// Buffered(1) with a non-blocking send: any signals that arrive while
	// one is already pending are dropped, since whenever the pending one is
	// actually drained it reads the FSM's current (freshest) state anyway —
	// nothing is lost, only redundant intermediate republishes are.
	publishSignal chan struct{}

	mut         sync.Mutex
	uuidCache   map[string]string     // key: "namespace/podName" -> cluster_uuid
	tlsCache    map[string]bool       // key: "namespace/podName" -> whether probeAdminAPI detected TLS; always set alongside uuidCache
	tracked     map[string]trackedPod // key -> tracking info (survives pod disappearance)
	lastTargets []discovery.Target    // raw targets from the most recent Update
	adminPort   int                   // resolved (defaulted) AdminPort from the most recent Update

	// danglingCollectorSince tracks, per collector name, how long it's been
	// referenced by an assignment while absent from the voter set — see
	// reconcileAssignments' grace-period handling. Only ever touched from
	// reconcile(), which only ever runs on Run()'s single goroutine, so
	// unlike the fields above this doesn't need mut.
	danglingCollectorSince map[string]time.Time

	// lastReportedHasState is the hasState value most recently written via
	// setHasStateAnnotation, or nil if this process has never written it
	// yet. A pointer, not a plain bool: a pod's annotations can outlive a
	// single container incarnation (e.g. a plain crash/OOM restart without
	// the Pod object itself being recreated), so the very first refresh
	// after Run() starts must always write, even if the freshly-computed
	// value happens to be false — otherwise a stale "true" left over from
	// before this restart would never get corrected, since false already
	// matches Go's zero value. Only ever touched from Run()'s single
	// goroutine.
	lastReportedHasState *bool

	// quorumLostSince tracks how long raftNode.hasReachableQuorum has
	// continuously reported a structurally-unreachable quorum — see
	// checkQuorumLoss. Zero value means "not currently observed as lost".
	// Only ever touched from Run()'s single goroutine.
	quorumLostSince time.Time

	// admissionEnabled/admissionCfg/admissionGate/flushHealth/
	// admissionGapGauge are only ever populated, and only ever meaningful,
	// when clustering is true and Arguments.AdmissionControl.Enabled —
	// see the package doc comment's admission control section. Fixed at
	// construction (New()), not refreshed by Update() — the same
	// convention already used for clustering/raftBindPort above.
	// admissionGate is only ever touched from Run()'s single goroutine;
	// admissionGapGauge is safe for concurrent use by construction
	// (prometheus.Gauge).
	admissionEnabled  bool
	admissionCfg      AdmissionControlArguments
	admissionGate     *admissionGate
	flushHealth       *flushHealthReader
	admissionGapGauge prometheus.Gauge
}

var _ component.Component = (*Component)(nil)
var _ httpservice.Component = (*Component)(nil)

// gracefulLeaveTimeout/gracefulLeaveTimeoutPerExtra/gracefulLeaveTimeoutMax
// size the wait budget handleLeave gives itself — see gracefulLeaveDeadline.
// Comfortably inside terminationGracePeriodSeconds, which the chart sets
// accordingly when Clustering is true — kubelet counts preStop hook time
// against that same budget, not in addition to it. Vars, not consts, so
// tests can shrink them.
var (
	gracefulLeaveTimeout         = 20 * time.Second
	gracefulLeaveTimeoutPerExtra = 5 * time.Second
	gracefulLeaveTimeoutMax      = 90 * time.Second
)

// gracefulLeaveDeadline computes how long handleLeave should wait for this
// replica to be removed as a voter, scaled by numLeaving — how many
// voters (including this one) are currently also asking to leave at the
// same time, from votersRequestingRemoval. A fixed timeout sized for one
// departure isn't enough for a bulk one: the leader can only remove
// voters one at a time, and may itself be among the departing set,
// forcing a leadership handoff partway through. Confirmed live: a 10-to-1
// scale-down let several preStop hooks time out on a fixed 20s budget
// before the leader worked through all 9 departures, stranding the
// survivor below quorum against a configuration that hadn't shrunk to
// match. Capped at gracefulLeaveTimeoutMax — past that point
// checkQuorumLoss's own self-restart recovers just as well, so there's no
// benefit to making a pod hang around in Terminating any longer.
func gracefulLeaveDeadline(numLeaving int) time.Duration {
	extra := numLeaving - 1
	if extra < 0 {
		extra = 0
	}
	d := gracefulLeaveTimeout + time.Duration(extra)*gracefulLeaveTimeoutPerExtra
	if d > gracefulLeaveTimeoutMax {
		return gracefulLeaveTimeoutMax
	}
	return d
}

// Handler implements httpservice.Component: Alloy's HTTP service mounts
// whatever this returns at /api/v0/component/<id>/, so a Kubernetes
// preStop hook (see the chart) can hit
// /api/v0/component/discovery.redpanda.pods/leave to trigger a graceful
// Raft departure before the pod is torn down — see handleLeave.
func (c *Component) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/leave", c.handleLeave)
	return mux
}

// handleLeave implements the preStop side of graceful scale-down: it asks
// the raft leader (via requestGracefulRemoval's pod annotation —
// reconcile checks every voter for this on every pass, removing any
// so-marked immediately rather than waiting for gossip to notice it's
// gone) to remove this replica as a voter, then blocks until that's
// actually happened, or gracefulLeaveDeadline elapses, before responding.
// Kubernetes doesn't send SIGTERM until this handler returns, so — unlike
// relying on podManagementPolicy or scale-down pacing alone — this makes
// voter removal happen one at a time no matter how many replicas an
// operator asks Kubernetes to remove in a single step.
//
// A no-op outside clustering mode, or if the raft node was never built
// (e.g. this replica never found itself in cluster.Peers() — see
// waitAndBuildRaftNode): there's no voter to remove either way, and
// blocking pod termination on something that will never happen would
// just make an ordinary rollout hang.
func (c *Component) handleLeave(w http.ResponseWriter, r *http.Request) {
	if !c.clustering {
		w.WriteHeader(http.StatusOK)
		return
	}
	c.mut.Lock()
	node := c.node
	c.mut.Unlock()
	if node == nil {
		w.WriteHeader(http.StatusOK)
		return
	}

	ctx := r.Context()
	if err := requestGracefulRemoval(ctx, c.k8sClient, c.k8sNamespace, node.self); err != nil {
		c.opts.Logger.Warn("failed to request graceful raft removal; proceeding with shutdown anyway", "err", err)
		w.WriteHeader(http.StatusOK)
		return
	}

	// Count simultaneous departures (including this one, just requested
	// above) to size the wait budget — see gracefulLeaveDeadline. Falls
	// back to the single-replica budget if this fails; that's the
	// pre-existing, already-safe behavior, not a new risk.
	leaving, err := votersRequestingRemoval(ctx, c.k8sClient, c.k8sNamespace, node.self)
	if err != nil {
		c.opts.Logger.Warn("failed to count simultaneous departures; using the single-replica leave timeout", "err", err)
	}
	timeout := gracefulLeaveDeadline(len(leaving))

	deadline := time.Now().Add(timeout)
	for node.hasState() {
		if time.Now().After(deadline) {
			c.opts.Logger.Warn("timed out waiting to be removed as a raft voter; proceeding with shutdown anyway", "timeout", timeout, "simultaneous_departures", len(leaving))
			w.WriteHeader(http.StatusGatewayTimeout)
			return
		}
		select {
		case <-ctx.Done():
			w.WriteHeader(http.StatusOK)
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
	c.opts.Logger.Info("gracefully left the raft cluster before shutdown")
	w.WriteHeader(http.StatusOK)
}

// New creates a new discovery.redpanda component. When args.Clustering is
// true, it deliberately does not wait to find itself among cluster.Peers(),
// let alone build the Raft node, synchronously here: the cluster service
// only registers this node as a peer once its own Run() has started the
// underlying gossip node (internal/service/cluster/cluster.go's
// node.Start()), which is a separate lifecycle phase from component
// construction with no ordering guarantee relative to it. Failing New() on
// that race would crash-loop the component on totally ordinary startup
// timing, not just genuine misconfiguration. That work happens in Run()
// instead, which can afford to wait.
//
// When args.Clustering is false, none of the above applies: this component
// doesn't touch the cluster service at all, so it works whether or not
// Alloy's native clustering happens to be enabled for other reasons.
func New(opts component.Options, args Arguments) (*Component, error) {
	c := &Component{
		opts:                   opts,
		clustering:             args.Clustering,
		numShards:              args.NumShards,
		publishSignal:          make(chan struct{}, 1),
		uuidCache:              make(map[string]string),
		tlsCache:               make(map[string]bool),
		tracked:                make(map[string]trackedPod),
		danglingCollectorSince: make(map[string]time.Time),
	}

	if args.Clustering {
		data, err := opts.GetServiceData(cluster.ServiceName)
		if err != nil {
			return nil, fmt.Errorf("getting cluster service: %w", err)
		}
		c.cluster = data.(cluster.Cluster)

		raftBindPort := args.RaftBindPort
		if raftBindPort == 0 {
			raftBindPort = defaultRaftBindPort
		}
		c.raftBindPort = raftBindPort

		if args.AdmissionControl.Enabled {
			admissionCfg := args.AdmissionControl
			if admissionCfg.HighWatermark == 0 {
				admissionCfg.HighWatermark = defaultAdmissionHighWatermark
			}
			if admissionCfg.LowWatermark == 0 {
				admissionCfg.LowWatermark = defaultAdmissionLowWatermark
			}
			if admissionCfg.FlushMetricsComponentID == "" {
				return nil, fmt.Errorf("admission_control.flush_metrics_component_id is required when admission_control.enabled is true")
			}
			if !(admissionCfg.LowWatermark > 0 && admissionCfg.LowWatermark < admissionCfg.HighWatermark && admissionCfg.HighWatermark <= 1) {
				return nil, fmt.Errorf("admission_control requires 0 < low_watermark < high_watermark <= 1, got low_watermark=%v high_watermark=%v", admissionCfg.LowWatermark, admissionCfg.HighWatermark)
			}

			reader, err := newFlushHealthReader(opts, admissionCfg.FlushMetricsComponentID)
			if err != nil {
				return nil, fmt.Errorf("setting up admission control: %w", err)
			}

			c.admissionGapGauge = prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "discovery_redpanda_admission_gap",
				Help: "Brokers currently assigned to this replica but not yet admitted into published targets (assigned minus admitted). A persistently nonzero value signals this replica cannot safely admit everything it owns.",
			})
			opts.Registerer.MustRegister(c.admissionGapGauge)

			c.admissionEnabled = true
			c.admissionCfg = admissionCfg
			c.admissionGate = newAdmissionGate()
			c.flushHealth = reader
		}
	}

	if err := c.Update(args); err != nil {
		return nil, err
	}
	return c, nil
}

// selfIdentity finds this node's own name and host among the current
// cluster peers. It returns an error if self isn't present yet — a normal,
// expected condition early in startup (see New()'s doc comment), not
// necessarily a misconfiguration.
func selfIdentity(cl cluster.Cluster) (name, host string, err error) {
	for _, p := range cl.Peers() {
		if p.Self {
			h, _, splitErr := net.SplitHostPort(p.Addr)
			if splitErr != nil {
				h = p.Addr
			}
			return p.Name, h, nil
		}
	}
	return "", "", fmt.Errorf("could not find self in cluster peers")
}

// Run implements component.Component. In ordinal mode (Clustering = false)
// all the work happens synchronously in Update, so this just blocks until
// shutdown. In clustering mode, it first waits for this replica to appear
// in cluster.Peers() (see New()'s doc comment for why that can't happen
// synchronously in New()), builds the Raft node once that's possible, and
// then owns the Raft node's reconcile loop: only the current leader acts,
// bridging cluster.Peers() into Raft voter membership and proposing broker
// assignment changes.
func (c *Component) Run(ctx context.Context) error {
	if !c.clustering {
		<-ctx.Done()
		return nil
	}

	node, err := c.waitAndBuildRaftNode(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	c.mut.Lock()
	c.node = node
	c.mut.Unlock()

	if c.admissionEnabled {
		c.seedAdmissionGate(node)
	}

	// Targets may already be waiting (Update() may have run before the node
	// existed) — publish now that ownership can actually be determined.
	c.publishTargets()

	// Record this replica's own hasState immediately, not just on the first
	// tick: a sibling that restarts moments later and finds the bootstrap
	// marker already claimed relies on anyPeerHasState seeing an up-to-date
	// annotation, not one that's up to reconcileInterval stale.
	c.refreshHasStateAnnotation(node)

	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	leaderCh := node.raft.LeaderCh()

	for {
		select {
		case <-ctx.Done():
			return node.raft.Shutdown().Error()
		case <-leaderCh:
			c.reconcile()
		case <-ticker.C:
			c.refreshHasStateAnnotation(node)
			c.checkQuorumLoss(node)
			c.reconcile()
			if c.admissionEnabled {
				c.reconcileAdmission(ctx, node)
			}
		case <-c.publishSignal:
			c.publishTargets()
		}
	}
}

// checkQuorumLoss self-diagnoses the permanent deadlock a bulk loss of
// Raft voters can cause (see the package doc comment): if not enough of
// this replica's configured voters are currently visible via gossip to
// form a majority (raftNode.hasReachableQuorum), no leader can ever be
// elected and no membership change — including RemoveServer for the
// departed voters — can ever commit. That's structural, not transient, so
// once it's persisted for quorumLossGrace this replica restarts itself
// (exitProcess) rather than wait indefinitely: ephemeral storage means it
// comes back with no local state, and decideBootstrap's existing
// Lease/peer-check/epoch safeguards — unaffected by any of this — govern
// whether it's actually safe to recover from there. This function only
// ever decides *whether to restart*; it never decides whether a fresh
// bootstrap is safe.
//
// Runs on every replica, not just the leader: a replica stuck without a
// leader never runs reconcile() at all, so leader-gating this would mean
// nothing ever detects the deadlock from the follower/candidate side.
func (c *Component) checkQuorumLoss(node *raftNode) {
	hasQuorum, err := node.hasReachableQuorum(c.cluster.Peers())
	if err != nil {
		c.opts.Logger.Warn("failed to check raft quorum reachability", "err", err)
		return
	}
	if hasQuorum {
		c.quorumLostSince = time.Time{}
		return
	}
	if c.quorumLostSince.IsZero() {
		c.quorumLostSince = time.Now()
		return
	}
	if time.Since(c.quorumLostSince) < quorumLossGrace {
		return
	}
	c.opts.Logger.Error(
		"raft quorum has been structurally unreachable for too long; restarting this replica so it can recover via a fresh bootstrap decision once its local state is cleared",
		"lost_since", c.quorumLostSince, "grace_period", quorumLossGrace,
	)
	exitProcess(1)
}

// refreshHasStateAnnotation re-checks whether this replica currently has
// valid Raft state (i.e. appears in its own Raft configuration — see
// raftNode.hasState) and, if that differs from what this process last
// successfully recorded, updates this pod's own hasStateAnnotation — see
// raftnode.go's anyPeerHasState, which other restarting replicas rely on
// before ever considering the bootstrap marker stale. Runs on every
// replica unconditionally, not just the leader.
func (c *Component) refreshHasStateAnnotation(node *raftNode) {
	current := node.hasState()
	if c.lastReportedHasState != nil && *c.lastReportedHasState == current {
		return
	}
	if err := setHasStateAnnotation(context.Background(), c.k8sClient, c.k8sNamespace, node.self, current); err != nil {
		c.opts.Logger.Warn("failed to update raft-has-state annotation", "err", err)
		return
	}
	c.lastReportedHasState = &current
}

// signalPublish is the FSM's onChange callback (see fsm.go) — fired after
// every commit this replica applies, on every replica, leader or follower.
// It only queues a republish rather than performing one directly, so a
// burst of many commits (a follower catching up on a backlog, or the
// leader itself committing many Assign commands in one reconcile pass)
// collapses into a single republish once Run()'s loop gets to it, instead
// of one republish per individual commit.
func (c *Component) signalPublish() {
	select {
	case c.publishSignal <- struct{}{}:
	default:
	}
}

// waitAndBuildRaftNode polls cluster.Peers() until this replica appears (see
// New()'s doc comment for why that can't happen synchronously), then builds
// the Raft node. Deciding who's allowed to bootstrap a fresh cluster doesn't
// depend on gossip having converged at all — see newRaftNode's decideBootstrap
// — so unlike an earlier version of this function, there's no need to wait
// for cluster.Ready() here first.
func (c *Component) waitAndBuildRaftNode(ctx context.Context) (*raftNode, error) {
	const pollInterval = time.Second

	var (
		selfName, selfHost string
		err                error
	)
	for {
		selfName, selfHost, err = selfIdentity(c.cluster)
		if err == nil {
			break
		}
		c.opts.Logger.Debug("waiting to find self in cluster peers before starting raft", "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(pollInterval):
		}
	}

	if peers := c.cluster.Peers(); len(peers) == 1 {
		c.opts.Logger.Warn(
			"this replica sees no other cluster peers; if this is meant to be a multi-replica deployment, " +
				"broker-to-collector allocation will not be sharded correctly. Check that Alloy was started " +
				"with --cluster.enabled and the other replicas are reachable. If you intend to run a single " +
				"replica, this warning is expected and can be ignored. Consider setting --cluster.wait-for-size " +
				"to the intended replica count so the cluster service refuses to serve traffic until every " +
				"replica has actually joined, instead of each isolated replica silently assuming it owns everything.",
		)
	}

	namespace, err := podNamespace()
	if err != nil {
		return nil, fmt.Errorf("determining pod namespace: %w", err)
	}
	clientset, err := inClusterClientset()
	if err != nil {
		return nil, fmt.Errorf("building kubernetes client: %w", err)
	}
	c.k8sClient = clientset
	c.k8sNamespace = namespace

	node, err := newRaftNode(
		filepath.Join(c.opts.DataPath, "raft"),
		selfName, selfHost, c.raftBindPort,
		clientset, namespace,
		c.opts.Logger,
		slogWriter{c.opts.Logger},
		c.signalPublish,
	)
	if err != nil {
		return nil, fmt.Errorf("starting raft node: %w", err)
	}
	return node, nil
}

// reconcile is the leader-only work: bridge gossip membership into Raft
// voters, then reconcile broker assignments against the current tracked set.
// A no-op on followers (isLeader() guards every write inside) or if the
// Raft node hasn't been built yet.
func (c *Component) reconcile() {
	c.mut.Lock()
	node := c.node
	c.mut.Unlock()
	if node == nil || !node.isLeader() {
		return
	}

	prevVoters, err := node.currentVoters()
	if err != nil {
		c.opts.Logger.Warn("failed to read raft configuration", "err", err)
		return
	}

	leaving, err := votersRequestingRemoval(context.Background(), c.k8sClient, c.k8sNamespace, node.self)
	if err != nil {
		c.opts.Logger.Warn("failed to check for voters requesting graceful removal", "err", err)
		// Proceed without it rather than skipping the whole reconcile pass
		// — the ordinary gossip-absence removal path below still works.
	}

	peers := c.cluster.Peers()
	if err := node.reconcileMembership(peers, func(p peer.Peer) raft.ServerAddress {
		return raftAddressFor(p, node.advertiseDomain, c.raftBindPort)
	}, leaving); err != nil {
		c.opts.Logger.Warn("failed to reconcile raft membership", "err", err)
	}

	newVoters, err := node.currentVoters()
	if err != nil {
		c.opts.Logger.Warn("failed to read raft configuration", "err", err)
		return
	}
	addedCollectors := diffAdded(prevVoters, newVoters)

	c.mut.Lock()
	totalTracked := len(c.tracked)
	tracked := make(map[string]brokerInfo)
	for key := range c.tracked {
		uuid, ok := c.uuidCache[key]
		if !ok {
			continue
		}
		tracked[key] = brokerInfo{id: key, clusterID: uuid}
	}
	c.mut.Unlock()

	current := node.fsm.snapshotState()
	cmds := reconcileAssignments(time.Now(), current, tracked, newVoters, c.danglingCollectorSince, collectorRejoinGrace, leaving)
	for _, cmd := range cmds {
		if err := node.propose(cmd); err != nil {
			c.opts.Logger.Warn("failed to propose broker assignment", "err", err, "broker", cmd.BrokerID)
		}
	}

	c.refreshEpochHeartbeat(node)

	c.opts.Logger.Info(
		"reconcile pass",
		"voters", len(newVoters),
		"added_collectors", len(addedCollectors),
		"tracked_pods_total", totalTracked,
		"tracked_pods_with_uuid", len(tracked),
		"committed_assignments", len(current),
		"proposed_commands", len(cmds),
	)

	// No explicit publish here even if cmds was non-empty: node.propose
	// above blocks until each command is committed *and* applied to this
	// node's own FSM (that's what raft.Apply's returned future waits for),
	// so onChange/signalPublish has already fired for each one — a
	// republish is already queued via publishSignal, coalesced with
	// whatever else just landed, instead of a second one right here.
}

// refreshEpochHeartbeat is the leader-only half of the epoch mechanism: it
// compares the bootstrap marker's currently-recorded epoch against this
// cluster's own epoch (learned via ordinary Raft replication — see fsm.go's
// opSetEpoch), refreshing the marker's heartbeat timestamp only if they
// still match. A mismatch means something else now claims this cluster's
// identity — possibly a rare, narrowly-scoped race in the recovery path
// (see raftnode.go's decideBootstrap) slipped past its safeguards — and
// blindly overwriting the marker at that point would just silently win a
// last-write-wins fight and erase the evidence, so this logs loudly and
// leaves the marker untouched instead. The heartbeat timestamp itself is
// purely an observability aid for operators inspecting the marker; no
// decision logic anywhere in this package branches on its value or age.
func (c *Component) refreshEpochHeartbeat(node *raftNode) {
	epoch := node.fsm.currentEpoch()
	if epoch == "" {
		// Momentary: right after this node became leader of a fresh
		// bootstrap, before its own opSetEpoch command has committed yet.
		return
	}

	name := statefulSetNameFromPodName(node.self) + bootstrapMarkerSuffix
	ctx := context.Background()
	cm, err := c.k8sClient.CoreV1().ConfigMaps(c.k8sNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		c.opts.Logger.Warn("failed to read bootstrap marker for epoch heartbeat", "err", err)
		return
	}
	if recorded := cm.Data["epoch"]; recorded != epoch {
		c.opts.Logger.Error(
			"bootstrap marker's recorded epoch does not match this cluster's own epoch; refusing to overwrite it — "+
				"this may indicate a split-brain recovery race, investigate before assuming it self-resolved",
			"marker_epoch", recorded, "our_epoch", epoch,
		)
		return
	}

	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data["lastHeartbeat"] = time.Now().UTC().Format(time.RFC3339)
	if _, err := c.k8sClient.CoreV1().ConfigMaps(c.k8sNamespace).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		c.opts.Logger.Warn("failed to refresh bootstrap marker heartbeat", "err", err)
	}
}

// assignedToMe returns the set of broker keys the current Raft-committed
// state assigns to this replica — the admission gate's input backlog, and
// (when admission control is disabled) exactly what publishTargetsClustered
// already publishes unconditionally.
func assignedToMe(node *raftNode) map[string]bool {
	assigned := make(map[string]bool)
	for brokerID, a := range node.fsm.snapshotState() {
		if a.Collector == node.self {
			assigned[brokerID] = true
		}
	}
	return assigned
}

// seedAdmissionGate restores this replica's last-known-good admitted set
// (see admissionStateConfigMapSuffix) intersected with what's currently
// actually assigned — see the package doc comment's admission control
// section. The intersection is only meaningful once this node's own FSM
// has actually caught up via Raft log replication, which right after
// waitAndBuildRaftNode returns hasn't necessarily happened yet (AddVoter
// and catchup is a real network round trip) — confirmed live: seeding
// immediately restored nothing at all, every time, because
// assignedToMe(node) was still completely empty at that exact instant.
// node.hasState() becoming true confirms this replica is a caught-up
// voter, so this waits for that first, bounded by raftTimeout; a
// best-effort wait, not a fatal blocker — proceeding on timeout still
// falls back to the normal ramp, no worse than not waiting at all.
func (c *Component) seedAdmissionGate(node *raftNode) {
	deadline := time.Now().Add(raftTimeout)
	for !node.hasState() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	restored, err := readAdmissionState(context.Background(), c.k8sClient, c.k8sNamespace, node.self)
	if err != nil {
		c.opts.Logger.Warn("failed to read persisted admission state; starting from empty", "err", err)
		return
	}
	assigned := assignedToMe(node)
	c.admissionGate.seed(restored, assigned)
	c.opts.Logger.Info(
		"seeded admission gate from persisted state",
		"restored", len(restored), "kept_after_intersect", len(c.admissionGate.admittedOrder()),
	)
}

// reconcileAdmission is the per-replica, per-tick admission control pass —
// see the package doc comment. Runs on every replica, unlike reconcile()
// (leader-only): admission is a local capacity decision about this
// replica's own flush health, not a cluster-wide ownership decision.
func (c *Component) reconcileAdmission(ctx context.Context, node *raftNode) {
	assigned := assignedToMe(node)

	ratio, err := c.flushHealth.ratio(ctx)
	if err != nil {
		c.opts.Logger.Warn("failed to read flush health signal; holding admission steady this tick", "err", err)
		// Still let reconcile drop anything no longer assigned even
		// without a fresh ratio — that's not a health decision. A ratio
		// strictly between the watermarks makes reconcile grow nothing
		// and shrink nothing, i.e. hold.
		ratio = (c.admissionCfg.LowWatermark + c.admissionCfg.HighWatermark) / 2
	}

	changed := c.admissionGate.reconcile(assigned, ratio, c.admissionCfg)
	admittedCount := len(c.admissionGate.admittedOrder())
	c.admissionGapGauge.Set(float64(len(assigned) - admittedCount))

	if changed {
		if err := writeAdmissionState(ctx, c.k8sClient, c.k8sNamespace, node.self, c.admissionGate.admittedOrder()); err != nil {
			c.opts.Logger.Warn("failed to persist admission state", "err", err)
		}
		c.signalPublish()
	}

	c.opts.Logger.Info(
		"admission control pass",
		"assigned", len(assigned), "admitted", admittedCount, "ratio", ratio, "changed", changed,
	)
}

// Update fetches UUIDs for uncached pods, refreshes pod tracking, and
// publishes the targets this replica currently owns.
func (c *Component) Update(args component.Arguments) error {
	newArgs := args.(Arguments)
	if newArgs.AdminPort == 0 {
		newArgs.AdminPort = defaultAdminPort
	}
	staleTimeout := newArgs.StaleTimeout
	if staleTimeout == 0 {
		staleTimeout = defaultStaleTimeout
	}

	now := time.Now()

	currentTargets := make(map[string]discovery.Target, len(newArgs.Targets))
	for _, t := range newArgs.Targets {
		key := targetKey(t)
		if key != "" {
			currentTargets[key] = t
		}
	}

	c.mut.Lock()
	for key, t := range currentTargets {
		pod, exists := c.tracked[key]
		if !exists {
			pod = newTrackedPod(t)
		}
		pod.lastSeen = now
		c.tracked[key] = pod
	}
	for key, pod := range c.tracked {
		if now.Sub(pod.lastSeen) > staleTimeout {
			delete(c.tracked, key)
			delete(c.uuidCache, key)
			delete(c.tlsCache, key)
			c.opts.Logger.Debug("evicted stale pod", "key", key)
		}
	}
	c.lastTargets = newArgs.Targets
	c.adminPort = newArgs.AdminPort
	c.mut.Unlock()

	// Fetch UUIDs for currently-present uncached pods.
	type fetchJob struct {
		key   string
		podIP string
	}
	var jobs []fetchJob
	for key, t := range currentTargets {
		c.mut.Lock()
		_, cached := c.uuidCache[key]
		c.mut.Unlock()
		if cached {
			continue
		}
		podIP, ok := t.Get("__meta_kubernetes_pod_ip")
		if !ok || podIP == "" {
			c.opts.Logger.Warn("target has no pod IP, cannot fetch UUID", "key", key)
			continue
		}
		jobs = append(jobs, fetchJob{key: key, podIP: podIP})
	}

	if len(jobs) > 0 {
		var wg sync.WaitGroup
		for _, job := range jobs {
			wg.Add(1)
			go func(j fetchJob) {
				defer wg.Done()
				uuid, tlsEnabled, err := probeAdminAPI(j.podIP, newArgs)
				if err != nil {
					c.opts.Logger.Warn("failed to fetch cluster UUID", "key", j.key, "pod_ip", j.podIP, "err", err)
					return
				}
				c.opts.Logger.Debug("cached cluster UUID", "key", j.key, "uuid", uuid, "tls_enabled", tlsEnabled)
				c.mut.Lock()
				c.uuidCache[j.key] = uuid
				c.tlsCache[j.key] = tlsEnabled
				c.mut.Unlock()
			}(job)
		}
		wg.Wait()
	}

	c.mut.Lock()
	resolvedUUIDs := len(c.uuidCache)
	c.mut.Unlock()
	c.opts.Logger.Info(
		"update pass",
		"raw_targets", len(newArgs.Targets),
		"tracked_pods", len(currentTargets),
		"uuid_fetch_jobs", len(jobs),
		"resolved_uuids_total", resolvedUUIDs,
	)

	c.publishTargets()
	return nil
}

// publishTargets recomputes Exports from the current tracked/UUID state and
// publishes it, via whichever allocation mode Arguments.Clustering selected
// — see the package doc comment. Called from Update (after a discovery
// refresh) and, in clustering mode, from reconcile (after an assignment
// change that happened independently of any discovery refresh).
func (c *Component) publishTargets() {
	if !c.clustering {
		c.publishTargetsOrdinal()
		return
	}
	c.publishTargetsClustered()
}

// publishTargetsOrdinal implements the default, non-clustering allocation
// scheme: sort every tracked pod by (namespace, StatefulSet name, pod
// ordinal), label each currently-present target with its position in that
// sort (mod NumShards, if set) under labelPodOrdinal, and publish every one
// of them unfiltered — see the package doc comment for why this component
// doesn't decide ownership itself in this mode; a downstream relabel rule
// does. Admin-port filtering is applied here too even though the original
// (pre-Raft) version of this scheme lacked it: excluding non-metrics-port
// targets is a general correctness fix, not something that should depend
// on which allocation mode is active.
func (c *Component) publishTargetsOrdinal() {
	c.mut.Lock()
	rawTargets := c.lastTargets
	adminPort := c.adminPort
	numShards := c.numShards
	tracked := make(map[string]trackedPod, len(c.tracked))
	for k, v := range c.tracked {
		tracked[k] = v
	}
	uuidCache := make(map[string]string, len(c.uuidCache))
	for k, v := range c.uuidCache {
		uuidCache[k] = v
	}
	tlsCache := make(map[string]bool, len(c.tlsCache))
	for k, v := range c.tlsCache {
		tlsCache[k] = v
	}
	c.mut.Unlock()
	if adminPort == 0 {
		adminPort = defaultAdminPort
	}
	adminPortStr := strconv.Itoa(adminPort)

	type entry struct {
		key string
		trackedPod
	}
	sorted := make([]entry, 0, len(tracked))
	for key, pod := range tracked {
		sorted = append(sorted, entry{key: key, trackedPod: pod})
	}
	sort.Slice(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		if a.namespace != b.namespace {
			return a.namespace < b.namespace
		}
		if a.stsName != b.stsName {
			return a.stsName < b.stsName
		}
		return a.podOrdinal < b.podOrdinal
	})

	ordinals := make(map[string]int, len(sorted))
	for i, e := range sorted {
		if numShards > 0 {
			ordinals[e.key] = i % numShards
		} else {
			ordinals[e.key] = i
		}
	}

	var noUUID, noOrdinal, wrongPort int
	enriched := make([]discovery.Target, 0, len(rawTargets))
	for _, t := range rawTargets {
		key := targetKey(t)
		if key == "" {
			enriched = append(enriched, t)
			continue
		}
		if port, _ := t.Get("__meta_kubernetes_pod_container_port_number"); port != adminPortStr {
			wrongPort++
			continue
		}
		uuid, hasUUID := uuidCache[key]
		tlsEnabled := tlsCache[key]
		if !hasUUID {
			noUUID++
			c.opts.Logger.Warn("excluding target with unknown cluster UUID", "key", key)
			continue
		}
		ordinal, ok := ordinals[key]
		if !ok {
			noOrdinal++
			continue
		}
		builder := discovery.NewTargetBuilderFrom(t)
		builder.Set("cluster_id", uuid)
		builder.Set(labelAdminTLSEnabled, strconv.FormatBool(tlsEnabled))
		builder.Set(labelPodOrdinal, strconv.Itoa(ordinal))
		enriched = append(enriched, builder.Target())
	}

	c.opts.Logger.Info(
		"publishing targets (ordinal mode)",
		"raw_targets", len(rawTargets),
		"excluded_wrong_port", wrongPort,
		"excluded_no_uuid", noUUID,
		"excluded_no_ordinal", noOrdinal,
		"published", len(enriched),
	)

	c.opts.OnStateChange(Exports{Targets: enriched})
}

// publishTargetsClustered implements the Clustering = true allocation
// scheme, filtering to just the brokers the current Raft-committed state
// assigns to this replica.
func (c *Component) publishTargetsClustered() {
	// If the operator configured --cluster.wait-for-size and the cluster
	// hasn't reached it yet, this replica must not assume it's safe to
	// scrape anything — the same rule discovery.DistributedTargets and
	// other clustering-aware components follow. Without this check, a
	// deployment that forgot --cluster.enabled entirely would silently have
	// every replica believe it's the only one and scrape every broker N
	// times over, since a lone, ungossiped node is indistinguishable from a
	// genuinely single-replica deployment from inside this component.
	c.mut.Lock()
	node := c.node
	rawTargets := c.lastTargets
	adminPort := c.adminPort
	uuidCache := make(map[string]string, len(c.uuidCache))
	for k, v := range c.uuidCache {
		uuidCache[k] = v
	}
	tlsCache := make(map[string]bool, len(c.tlsCache))
	for k, v := range c.tlsCache {
		tlsCache[k] = v
	}
	c.mut.Unlock()
	if adminPort == 0 {
		adminPort = defaultAdminPort
	}
	adminPortStr := strconv.Itoa(adminPort)

	var admitted map[string]bool
	if c.admissionEnabled {
		admitted = c.admissionGate.admittedSet()
	}

	// node is nil until Run() finds this replica in cluster.Peers() and
	// builds the Raft node (see New()'s doc comment for why that can't
	// happen synchronously). Until then, and whenever the cluster isn't
	// Ready() (e.g. --cluster.wait-for-size not yet met), this replica must
	// not assume it's safe to scrape anything — the same rule
	// discovery.DistributedTargets and other clustering-aware components
	// follow. Without this, a deployment that forgot --cluster.enabled
	// entirely would have every replica believe it's the only one and
	// scrape every broker N times over, since a lone, ungossiped node is
	// indistinguishable from a genuinely single-replica deployment from
	// inside this component.
	if node == nil || !c.cluster.Ready() {
		c.opts.Logger.Debug("cluster not ready, publishing no targets")
		c.opts.OnStateChange(Exports{Targets: nil})
		return
	}

	var noUUID, notAssignedToMe, wrongPort, notAdmitted int
	enriched := make([]discovery.Target, 0, len(rawTargets))
	for _, t := range rawTargets {
		key := targetKey(t)
		if key == "" {
			enriched = append(enriched, t)
			continue
		}
		// discovery.kubernetes (role: pod) emits one target per declared
		// container port, all sharing this same namespace/pod identity —
		// plus one anonymous "bare IP, no port" target for any container
		// with zero declared ports at all (e.g. an init container), which
		// upstream's own docs say the user is expected to relabel a port
		// onto before it's scrapable at all. Only the admin port actually
		// serves Redpanda's Prometheus metrics endpoint, so a target is
		// worth exporting only if it explicitly matches it — a missing
		// port label doesn't get a pass, it gets excluded like any other
		// non-admin-port target.
		if port, _ := t.Get("__meta_kubernetes_pod_container_port_number"); port != adminPortStr {
			wrongPort++
			continue
		}
		uuid, hasUUID := uuidCache[key]
		if !hasUUID {
			noUUID++
			c.opts.Logger.Warn("excluding target with unknown cluster UUID", "key", key)
			continue
		}
		collector, has := node.fsm.collectorOf(key)
		if !has || collector != node.self {
			notAssignedToMe++
			continue // not assigned to this replica
		}
		if c.admissionEnabled && !admitted[key] {
			notAdmitted++
			continue // assigned to this replica, but not yet (or no longer) admitted — see admission.go
		}
		builder := discovery.NewTargetBuilderFrom(t)
		builder.Set("cluster_id", uuid)
		builder.Set(labelAdminTLSEnabled, strconv.FormatBool(tlsCache[key]))
		enriched = append(enriched, builder.Target())
	}

	c.opts.Logger.Info(
		"publishing targets",
		"raw_targets", len(rawTargets),
		"excluded_wrong_port", wrongPort,
		"excluded_no_uuid", noUUID,
		"excluded_not_assigned_to_me", notAssignedToMe,
		"excluded_not_admitted", notAdmitted,
		"published", len(enriched),
		"self", node.self,
	)

	c.opts.OnStateChange(Exports{Targets: enriched})
}

func targetKey(t discovery.Target) string {
	ns, hasNs := t.Get("__meta_kubernetes_namespace")
	pod, hasPod := t.Get("__meta_kubernetes_pod_name")
	if !hasNs || !hasPod || ns == "" || pod == "" {
		return ""
	}
	return ns + "/" + pod
}

type clusterUUIDResponse struct {
	ClusterUUID string `json:"cluster_uuid"`
}

// probeAdminAPI determines a pod's cluster_uuid and, along the way, whether
// its admin API speaks TLS at all — rather than trusting a single fleet-wide
// Arguments.TLSEnabled flag, which forces every monitored Redpanda cluster to
// use the same scheme. Real deployments aren't guaranteed to be uniform (a
// freshly added cluster might be plaintext while others already have TLS
// enabled), and getting that one flag wrong for even one cluster silently
// blocks its brokers from ever resolving a UUID — and therefore from ever
// being scraped, since Update excludes anything without one.
//
// Tries https first (respecting TLSSkipVerify for certificate validation),
// then falls back to http. Both attempts run against the exact same
// request path/port — only the scheme (and, for https, TLS verification)
// differs — so whichever one actually gets a valid cluster_uuid response
// back is a reliable signal of which scheme this pod's admin API uses, not
// a guess. The result is cached permanently alongside the UUID itself (see
// Component.tlsCache) and republished on every target as labelAdminTLSEnabled,
// for a downstream relabel rule to route each target's actual scrape at the
// right scheme — see the chart's relabel rules.
func probeAdminAPI(podIP string, args Arguments) (uuid string, tlsEnabled bool, err error) {
	uuid, httpsErr := fetchClusterUUID(podIP, args.AdminPort, "https", args.TLSSkipVerify)
	if httpsErr == nil {
		return uuid, true, nil
	}
	uuid, httpErr := fetchClusterUUID(podIP, args.AdminPort, "http", false)
	if httpErr == nil {
		return uuid, false, nil
	}
	return "", false, fmt.Errorf("probing admin API via https: %s; via http: %s", httpsErr, httpErr)
}

func fetchClusterUUID(podIP string, adminPort int, scheme string, tlsSkipVerify bool) (string, error) {
	url := fmt.Sprintf("%s://%s:%d/v1/cluster/uuid", scheme, podIP, adminPort)

	ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("building request: %w", err)
	}

	transport := http.DefaultTransport
	if scheme == "https" && tlsSkipVerify {
		transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // user-configured
		}
	}

	resp, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		return "", fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading response: %w", err)
	}

	var result clusterUUIDResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("parsing response: %w", err)
	}

	if result.ClusterUUID == "" {
		return "", fmt.Errorf("empty cluster_uuid in response from %s", url)
	}

	return result.ClusterUUID, nil
}

// slogWriter adapts component.Options.Logger to the io.Writer Raft wants
// for its internal logging.
type slogWriter struct {
	log interface{ Debug(msg string, args ...any) }
}

func (w slogWriter) Write(p []byte) (int, error) {
	w.log.Debug(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
