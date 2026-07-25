package redpanda

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/ckit/peer"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	raftTimeout    = 10 * time.Second
	raftMaxPool    = 3
	snapshotRetain = 2

	// bootstrapMarkerSuffix names the well-known ConfigMap (appended to the
	// StatefulSet name) that records the current cluster incarnation's
	// epoch — see decideBootstrap.
	bootstrapMarkerSuffix = "-raft-bootstrapped"

	// bootstrapElectionLeaseSuffix names the Lease used to serialize the
	// "should I bootstrap a fresh raft cluster right now" decision across
	// every replica that currently lacks local raft state — see
	// decideBootstrap. Without this, two replicas that each independently
	// read the bootstrap marker, found no sibling with state, and wrote
	// back a fresh epoch could each succeed in turn: a resourceVersion-
	// conditioned Update only rejects a write based on stale *data* it
	// already read, not a second, equally fresh read that happens to
	// arrive after the first write commits. Confirmed live: a full-fleet
	// restart produced three independently "reclaimed" epochs before
	// Raft's own term-safety converged them back into one cluster. Gating
	// the whole decision behind exclusive lease ownership makes at most
	// one replica ever perform it at a time, so the check-then-write
	// inside is race-free by construction, not by luck.
	bootstrapElectionLeaseSuffix = "-raft-bootstrap-election"

	// hasStateAnnotation records, on a pod's own object, whether its local
	// Raft store currently has state — see setHasStateAnnotation and
	// anyPeerHasState.
	hasStateAnnotation = "redpanda.alloy.grafana.com/raft-has-state"

	// raftLeavingAnnotation records, on a pod's own object, that it's asked
	// to be gracefully removed as a Raft voter before it terminates — see
	// requestGracefulRemoval, votersRequestingRemoval, and redpanda.go's
	// preStop HTTP handler. Unlike hasStateAnnotation (a passive fact any
	// replica can read about a sibling), this is a request: the leader
	// removes any voter marked this way immediately, without waiting for
	// gossip to notice it's actually gone, so a pod blocked in its own
	// preStop hook has somewhere to make progress toward.
	raftLeavingAnnotation = "redpanda.alloy.grafana.com/raft-leaving"
)

// The bootstrap-election lease's timing. Deliberately vars, not consts, so
// tests can shrink them — a real acquire/renew/retry cycle takes real wall
// time, and the production defaults (matching client-go's own documented
// "core clients default to" values) would make every test slow.
var (
	bootstrapElectionLeaseDuration = 15 * time.Second
	bootstrapElectionRenewDeadline = 10 * time.Second
	bootstrapElectionRetryPeriod   = 2 * time.Second
	// bootstrapElectionTimeout bounds how long a node will wait to win the
	// bootstrap-election lease at all, in case of a real problem (RBAC
	// misconfigured, API server unreachable) rather than merely losing the
	// race to a sibling — leaderelection.LeaderElector.Run retries
	// indefinitely on its own otherwise.
	bootstrapElectionTimeout = 30 * time.Second
)

// podNamespaceFile is where Kubernetes mounts a pod's own namespace via its
// service account token — present in virtually any pod with no extra chart
// configuration needed. A var, not a const, so tests can point it at a
// fixture file instead of stubbing the filesystem.
var podNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// raftNode wraps the single Raft peer embedded inside this component
// instance. Every Alloy collector replica runs one discovery.redpanda
// instance, so one raftNode per replica, all part of the same Raft group.
type raftNode struct {
	raft            *raft.Raft
	fsm             *fsm
	self            string // this node's own peer name, from cluster.Peers()
	advertiseDomain string // see raftAdvertiseDomainFor; "" means fall back to IP-based addressing
}

// newRaftNode constructs the embedded Raft peer. If this node has no prior
// Raft state, it doesn't guess whether that's because no real cluster exists
// yet or because its own disk was simply never persisted — see
// decideBootstrap — before concluding it's safe to bootstrap a fresh
// single-voter cluster.
//
// selfName/selfHost identify this node; raftBindPort is a TCP port dedicated
// to Raft RPC, separate from Alloy's own gossip port (peer.Peer.Addr is the
// gossip address, not usable directly here). onChange is invoked after every
// commit this replica applies, whether or not it's the leader — see fsm.go.
// logger is used for operator-visible warnings about the bootstrap decision
// itself; logOutput is Raft's own internal (debug-level) logging. clientset
// and namespace are passed in (rather than built here) so the caller can
// reuse the same client for the periodic hasState-annotation refresh and the
// leader's epoch heartbeat — see redpanda.go.
func newRaftNode(dataDir, selfName, selfHost string, raftBindPort int, clientset kubernetes.Interface, namespace string, logger *slog.Logger, logOutput io.Writer, onChange func()) (*raftNode, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating raft data dir: %w", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("opening raft log store: %w", err)
	}

	snapshotDir := filepath.Join(dataDir, "snapshots")
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating raft snapshot dir: %w", err)
	}
	snapshots, err := raft.NewFileSnapshotStore(snapshotDir, snapshotRetain, logOutput)
	if err != nil {
		return nil, fmt.Errorf("opening raft snapshot store: %w", err)
	}

	advertiseDomain := raftAdvertiseDomainFor(selfName)

	// raft.NewTCPTransport's advertise parameter must be a genuine
	// *net.TCPAddr — it's used for more than String() internally, and
	// rejects any other net.Addr implementation with "local address is not
	// a TCP address". The DNS name (when available) is advertised instead
	// via the bootstrap Configuration's Address field below, which is just
	// an unvalidated raft.ServerAddress string — that's the field remote
	// peers and AddVoter actually dial, so the transport's own advertise
	// address only needs to be *some* valid local TCP address, never a DNS
	// name.
	bindAddr := fmt.Sprintf("0.0.0.0:%d", raftBindPort)
	advertiseAddr := fmt.Sprintf("%s:%d", selfHost, raftBindPort)
	advertise, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("resolving raft advertise address %q: %w", advertiseAddr, err)
	}
	transport, err := raft.NewTCPTransport(bindAddr, advertise, raftMaxPool, raftTimeout, logOutput)
	if err != nil {
		return nil, fmt.Errorf("creating raft transport: %w", err)
	}

	// Bootstrap must happen before raft.NewRaft, and only from exactly one
	// node with an empty log — see the package doc comment for why every
	// node calling BootstrapCluster independently would split-brain rather
	// than safely no-op.
	hasState, err := raft.HasExistingState(logStore, logStore, snapshots)
	if err != nil {
		return nil, fmt.Errorf("checking for existing raft state: %w", err)
	}

	var shouldBootstrap bool
	var epoch string
	if !hasState {
		// Empty local state doesn't necessarily mean no real cluster exists
		// — it might just mean this node's disk was never persisted (no
		// PVC) while it's still a valid, continuously-configured voter
		// elsewhere. Rather than guess from local disk state or elapsed
		// time, decideBootstrap settles this deterministically: at most one
		// replica ever performs the decision at a time (see
		// bootstrapElectionLeaseSuffix), so there's no window for two
		// replicas to each conclude independently that it's safe to
		// bootstrap.
		var err error
		shouldBootstrap, epoch, err = decideBootstrap(context.Background(), clientset, namespace, selfName, logger)
		if err != nil {
			return nil, fmt.Errorf("deciding whether to bootstrap raft cluster: %w", err)
		}
	}

	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(selfName)

	if shouldBootstrap {
		bootstrapConf := raft.Configuration{
			Servers: []raft.Server{{
				ID:      raft.ServerID(selfName),
				Address: raftAddressFor(peer.Peer{Name: selfName, Addr: selfHost}, advertiseDomain, raftBindPort),
			}},
		}
		if err := raft.BootstrapCluster(conf, logStore, logStore, snapshots, transport, bootstrapConf); err != nil {
			return nil, fmt.Errorf("bootstrapping raft cluster: %w", err)
		}
	}

	f := newFSM(onChange)
	r, err := raft.NewRaft(conf, f, logStore, logStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("starting raft: %w", err)
	}

	if shouldBootstrap {
		if err := recordEpoch(r, epoch); err != nil {
			return nil, fmt.Errorf("recording raft epoch: %w", err)
		}
	}

	return &raftNode{raft: r, fsm: f, self: selfName, advertiseDomain: advertiseDomain}, nil
}

// recordEpoch waits for this freshly-bootstrapped, single-voter cluster to
// elect itself leader — which happens automatically, but not synchronously
// with raft.NewRaft returning, so this polls rather than using
// raft.LeaderCh() (easy to miss a notification on that channel if the
// listener attaches even slightly late) — then proposes epoch as the very
// first command this incarnation of the cluster ever commits, before
// anything else.
func recordEpoch(r *raft.Raft, epoch string) error {
	deadline := time.Now().Add(raftTimeout)
	for r.State() != raft.Leader {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for self-elected leadership after bootstrap")
		}
		time.Sleep(10 * time.Millisecond)
	}
	data, err := json.Marshal(command{Op: opSetEpoch, Epoch: epoch})
	if err != nil {
		return fmt.Errorf("encoding epoch command: %w", err)
	}
	return r.Apply(data, raftTimeout).Error()
}

// decideBootstrap decides, exactly once and without racing any other
// replica, whether this node should bootstrap a fresh Raft cluster right
// now. It first acquires the bootstrap-election Lease — exclusive,
// expiring, so a crash mid-decision doesn't wedge every future restart —
// and only once holding it runs decideBootstrapLocked. See
// bootstrapElectionLeaseSuffix for why a lease, not another CAS, is what
// actually makes this race-free.
//
// Returns shouldBootstrap=true and a freshly-minted epoch only when it's
// this node's job to bootstrap. false means either a real cluster already
// exists elsewhere, or (see anyPeerHasState) it might, and this node
// should just wait to be added as a voter.
func decideBootstrap(ctx context.Context, clientset kubernetes.Interface, namespace, selfName string, logger *slog.Logger) (bool, string, error) {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      statefulSetNameFromPodName(selfName) + bootstrapElectionLeaseSuffix,
			Namespace: namespace,
		},
		Client:     clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: selfName},
	}

	electCtx, cancel := context.WithTimeout(ctx, bootstrapElectionTimeout)
	defer cancel()

	var (
		acquired        bool
		shouldBootstrap bool
		epoch           string
		decideErr       error
	)

	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   bootstrapElectionLeaseDuration,
		RenewDeadline:   bootstrapElectionRenewDeadline,
		RetryPeriod:     bootstrapElectionRetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leCtx context.Context) {
				acquired = true
				shouldBootstrap, epoch, decideErr = decideBootstrapLocked(leCtx, clientset, namespace, selfName, logger)
				// Release the lease immediately once decided, rather than
				// holding it for the rest of LeaseDuration — the next
				// contender (if any) only needs to wait out an ordinary
				// restart's decision, not a full lease term.
				cancel()
			},
			OnStoppedLeading: func() {},
		},
	})
	if err != nil {
		return false, "", fmt.Errorf("creating bootstrap-election leader elector: %w", err)
	}

	le.Run(electCtx)

	if !acquired {
		return false, "", fmt.Errorf("timed out after %s waiting to win the bootstrap-election lease", bootstrapElectionTimeout)
	}
	return shouldBootstrap, epoch, decideErr
}

// decideBootstrapLocked is decideBootstrap's actual decision logic,
// called only once this node exclusively holds the bootstrap-election
// lease. Because exclusivity is guaranteed by the lease rather than by a
// resourceVersion CAS on the marker itself, this can plainly Get, then
// Create or Update the marker with no further coordination — no other
// caller can be inside this function at the same time.
//
// Two cases produce shouldBootstrap=true: the marker doesn't exist at all
// (a genuinely fresh install), or it exists but no currently-existing
// sibling pod claims to have Raft state (see anyPeerHasState) — the
// "entire fleet lost all state simultaneously" recovery path. Both cases
// eagerly set this node's own hasStateAnnotation to true before returning,
// not just relying on redpanda.go's periodic refresh: that refresh only
// runs once this node's own Run() loop starts, well after this function
// returns, and the very next lease-holder (right behind this one, often
// within milliseconds during a full-fleet restart) needs to see it
// immediately — otherwise it would see the same "no sibling has state"
// evidence and reclaim the marker all over again.
func decideBootstrapLocked(ctx context.Context, clientset kubernetes.Interface, namespace, selfName string, logger *slog.Logger) (bool, string, error) {
	name := statefulSetNameFromPodName(selfName) + bootstrapMarkerSuffix
	epoch := uuid.NewString()

	cm, err := clientset.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := clientset.CoreV1().ConfigMaps(namespace).Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Data: map[string]string{
				"bootstrappedBy": selfName,
				"epoch":          epoch,
			},
		}, metav1.CreateOptions{}); err != nil {
			return false, "", fmt.Errorf("creating bootstrap marker configmap %q: %w", name, err)
		}
		if err := setHasStateAnnotation(ctx, clientset, namespace, selfName, true); err != nil {
			logger.Warn("failed to eagerly set hasState annotation after a fresh bootstrap", "err", err)
		}
		return true, epoch, nil
	}
	if err != nil {
		return false, "", fmt.Errorf("reading bootstrap marker configmap %q: %w", name, err)
	}

	// The marker already exists — normally that just means a real cluster
	// exists and this node should wait to be added, same as always. But if
	// literally every replica lost its Raft state at once (routine now
	// that storage is ephemeral — any full-fleet restart does this),
	// nobody will ever be able to add anyone, and the marker alone can't
	// tell the difference. Before ever treating it as stale, check whether
	// any currently-existing sibling pod claims to actually have state.
	hasPeerState, err := anyPeerHasState(ctx, clientset, namespace, selfName)
	if err != nil {
		// Fail safe: if we can't verify, assume a peer might have state
		// rather than risk a false reclaim.
		logger.Warn("failed to check sibling pods for existing raft state; conservatively assuming a real cluster exists", "err", err)
		hasPeerState = true
	}
	if hasPeerState {
		return false, "", nil
	}

	logger.Warn("no sibling pod reports existing raft state; reclaiming the bootstrap marker and starting a fresh raft cluster", "epoch", epoch)
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data["bootstrappedBy"] = selfName
	cm.Data["epoch"] = epoch
	if _, err := clientset.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		return false, "", fmt.Errorf("reclaiming bootstrap marker configmap %q: %w", name, err)
	}
	if err := setHasStateAnnotation(ctx, clientset, namespace, selfName, true); err != nil {
		logger.Warn("failed to eagerly set hasState annotation after reclaiming the bootstrap marker", "err", err)
	}
	return true, epoch, nil
}

// anyPeerHasState reports whether any currently-existing sibling pod (same
// StatefulSet, identified by name convention — see statefulSetNameFromPodName
// — not by any chart-provided label, so this needs no extra configuration)
// other than selfName claims, via its own hasStateAnnotation, to have valid
// Raft state right now. decideBootstrapLocked checks this before ever
// concluding the bootstrap marker might be stale. A missing or
// unparseable annotation is treated the same as "false" — evidence of
// nothing, never treated as reassurance that it's safe to proceed.
func anyPeerHasState(ctx context.Context, clientset kubernetes.Interface, namespace, selfName string) (bool, error) {
	statefulSetName := statefulSetNameFromPodName(selfName)
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("listing pods in namespace %q: %w", namespace, err)
	}
	for _, pod := range pods.Items {
		if pod.Name == selfName {
			continue
		}
		if statefulSetNameFromPodName(pod.Name) != statefulSetName {
			continue
		}
		if pod.Annotations[hasStateAnnotation] == "true" {
			return true, nil
		}
	}
	return false, nil
}

// setHasStateAnnotation records, on this pod's own object, whether its
// local Raft store currently has state — see anyPeerHasState. Called once
// at startup with whatever hasState was just observed, and again later
// whenever it changes (a restarting node that gets added as a voter
// transitions from false to true asynchronously, well after startup) — see
// redpanda.go's periodic refresh.
func setHasStateAnnotation(ctx context.Context, clientset kubernetes.Interface, namespace, podName string, hasState bool) error {
	return setPodAnnotation(ctx, clientset, namespace, podName, hasStateAnnotation, strconv.FormatBool(hasState))
}

// requestGracefulRemoval marks this pod's own object as asking to be
// removed as a Raft voter before it terminates — see raftLeavingAnnotation
// and redpanda.go's preStop HTTP handler, the only caller.
func requestGracefulRemoval(ctx context.Context, clientset kubernetes.Interface, namespace, podName string) error {
	return setPodAnnotation(ctx, clientset, namespace, podName, raftLeavingAnnotation, "true")
}

// votersRequestingRemoval returns the names of any currently-existing pod
// (same StatefulSet as self, identified by name convention, matching
// anyPeerHasState's scoping) whose raftLeavingAnnotation is "true" —
// including self, deliberately: if this replica is both the leader and
// the one leaving, its own reconcile pass needs to see its own request.
// reconcileMembership removes every voter in the returned set immediately,
// without waiting for gossip to notice it's gone.
func votersRequestingRemoval(ctx context.Context, clientset kubernetes.Interface, namespace, selfName string) (map[string]bool, error) {
	statefulSetName := statefulSetNameFromPodName(selfName)
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing pods in namespace %q: %w", namespace, err)
	}
	leaving := make(map[string]bool)
	for _, pod := range pods.Items {
		if statefulSetNameFromPodName(pod.Name) != statefulSetName {
			continue
		}
		if pod.Annotations[raftLeavingAnnotation] == "true" {
			leaving[pod.Name] = true
		}
	}
	return leaving, nil
}

// setPodAnnotation patches a single annotation onto this pod's own object
// — the shared plumbing behind setHasStateAnnotation and
// requestGracefulRemoval.
func setPodAnnotation(ctx context.Context, clientset kubernetes.Interface, namespace, podName, key, value string) error {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{
				key: value,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("encoding annotation patch: %w", err)
	}
	_, err = clientset.CoreV1().Pods(namespace).Patch(ctx, podName, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("patching %s annotation on pod %q: %w", key, podName, err)
	}
	return nil
}

// inClusterClientset builds a Kubernetes clientset using the pod's own
// service account — the same in-cluster credentials discovery.kubernetes
// itself relies on, no extra configuration needed.
func inClusterClientset() (kubernetes.Interface, error) {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("building in-cluster kubernetes config: %w", err)
	}
	return kubernetes.NewForConfig(restConfig)
}

func (n *raftNode) isLeader() bool {
	return n.raft.State() == raft.Leader
}

func (n *raftNode) propose(cmd command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("encoding command: %w", err)
	}
	return n.raft.Apply(data, raftTimeout).Error()
}

// currentVoters returns the names of the current Raft voters.
func (n *raftNode) currentVoters() ([]string, error) {
	future := n.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("getting raft configuration: %w", err)
	}
	config := future.Configuration()
	voters := make([]string, 0, len(config.Servers))
	for _, srv := range config.Servers {
		voters = append(voters, string(srv.ID))
	}
	return voters, nil
}

// hasState reports whether this node currently appears in its own Raft
// configuration — i.e. whether it's a fully-caught-up member of a real
// cluster right now, as opposed to a bare, unconfigured instance still
// waiting to be added as a voter. Used by redpanda.go to keep this pod's
// own hasStateAnnotation accurate — see anyPeerHasState.
func (n *raftNode) hasState() bool {
	voters, err := n.currentVoters()
	if err != nil {
		return false
	}
	for _, v := range voters {
		if v == n.self {
			return true
		}
	}
	return false
}

// hasReachableQuorum reports whether enough of this node's *configured*
// Raft voters are currently visible via gossip (livePeers, from
// cluster.Peers() — a protocol completely independent of Raft, so it
// keeps working even during a Raft-level deadlock) to still form a
// majority. This needs no Raft-internal debug state and works regardless
// of this node's own Raft role (leader, candidate, or follower).
//
// If this ever reports false and stays false, the implication is
// structural, not transient: no leader can be elected and no membership
// change — including RemoveServer for the departed voters — can ever
// commit, because both require a majority of the *configured* voter set,
// which no longer exists. Confirmed live: losing more voters at once than
// the old configuration's fault tolerance (e.g. 5 replicas down to 2, or
// 10 down to 3) leaves the survivors permanently stuck retrying elections
// nobody can win. See redpanda.go's checkQuorumLoss for what a replica
// does once it's sure this is the case.
//
// An empty configuration (no servers at all) returns true — that's not a
// quorum-loss condition, just a bare node still waiting to be added as a
// voter for the first time, which is ordinary during startup.
func (n *raftNode) hasReachableQuorum(livePeers []peer.Peer) (bool, error) {
	future := n.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return false, fmt.Errorf("getting raft configuration: %w", err)
	}
	config := future.Configuration()
	if len(config.Servers) == 0 {
		return true, nil
	}

	live := make(map[raft.ServerID]bool, len(livePeers))
	for _, p := range livePeers {
		live[raft.ServerID(p.Name)] = true
	}

	reachable := 0
	for _, srv := range config.Servers {
		if live[srv.ID] {
			reachable++
		}
	}
	return reachable >= (len(config.Servers)/2)+1, nil
}

// reconcileMembership adds any gossip peer that isn't yet a Raft voter, and
// removes any Raft voter that's no longer a gossip peer — or that's still
// gossip-visible but has asked to leave (leaving, from
// votersRequestingRemoval: pod names with raftLeavingAnnotation set,
// checked here rather than waiting for gossip to notice the pod is
// actually gone, since a pod blocked in its own preStop hook — see
// redpanda.go's Handler — stays gossip-visible the entire time it's
// waiting to be removed). A voter that's leaving is also never re-added
// even if it's still a gossip peer, so a slow preStop hook doesn't flap
// between removed and re-added on successive reconcile passes.
// AddVoter/RemoveServer are only accepted when called on the current
// leader — Raft forwards configuration changes through the leader's log —
// so this is a no-op on followers by construction, not something this
// function needs to check itself beyond the isLeader() guard.
//
// If the leader itself needs removing (it's leaving too, or somehow
// dropped from gossipPeers), that removal is deliberately done last, only
// after every other pending removal in this pass has already succeeded —
// removing self any earlier causes an almost-immediate step-down (Raft
// steps a leader down once it commits a config change excluding itself),
// which would abort the rest of this pass with a "not leader" error and
// leave everything else still queued for whichever replica wins the next
// election. Confirmed live: a bulk departure large enough that the leader
// was frequently among the departing set needed one election *per voter*
// to fully drain, chaining past even a generously-scaled timeout. Ordering
// self-removal last means one leadership term can clear an entire batch
// of other departures before finally stepping down, needing at most one
// additional election overall — not one per removed voter.
func (n *raftNode) reconcileMembership(peers []peer.Peer, raftAddr func(peer.Peer) raft.ServerAddress, leaving map[string]bool) error {
	if !n.isLeader() {
		return nil
	}

	future := n.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return fmt.Errorf("getting raft configuration: %w", err)
	}
	config := future.Configuration()

	currentVoters := make(map[raft.ServerID]bool, len(config.Servers))
	for _, srv := range config.Servers {
		currentVoters[srv.ID] = true
	}

	gossipPeers := make(map[raft.ServerID]bool, len(peers))
	for _, p := range peers {
		gossipPeers[raft.ServerID(p.Name)] = true
	}

	for _, p := range peers {
		id := raft.ServerID(p.Name)
		if currentVoters[id] || leaving[p.Name] {
			continue
		}
		if err := n.raft.AddVoter(id, raftAddr(p), 0, raftTimeout).Error(); err != nil {
			return fmt.Errorf("adding voter %s: %w", p.Name, err)
		}
	}

	removeSelf := false
	for _, srv := range config.Servers {
		if gossipPeers[srv.ID] && !leaving[string(srv.ID)] {
			continue
		}
		if string(srv.ID) == n.self {
			removeSelf = true
			continue
		}
		if err := n.raft.RemoveServer(srv.ID, 0, raftTimeout).Error(); err != nil {
			return fmt.Errorf("removing server %s: %w", srv.ID, err)
		}
	}
	if removeSelf {
		if err := n.raft.RemoveServer(raft.ServerID(n.self), 0, raftTimeout).Error(); err != nil {
			return fmt.Errorf("removing server %s: %w", n.self, err)
		}
	}
	return nil
}

// podNamespace reads this pod's own namespace from the standard
// service-account mount every pod gets automatically, with zero extra chart
// configuration needed.
func podNamespace() (string, error) {
	nsBytes, err := os.ReadFile(podNamespaceFile)
	if err != nil {
		return "", fmt.Errorf("reading pod namespace file %q: %w", podNamespaceFile, err)
	}
	namespace := strings.TrimSpace(string(nsBytes))
	if namespace == "" {
		return "", fmt.Errorf("pod namespace file %q is empty", podNamespaceFile)
	}
	return namespace, nil
}

// raftAdvertiseDomainFor derives the DNS suffix for this pod's stable
// per-replica Raft address — <governing-headless-Service-name>.<namespace>.
// svc.cluster.local. The Service name is recovered by stripping selfName's
// trailing "-<ordinal>" (a StatefulSet pod name is always
// <StatefulSet-name>-<N>, and a StatefulSet's governing Service must be
// headless and, by the convention this component assumes, share the
// StatefulSet's name — which it does in this chart, since discovery.redpanda
// already requires Kubernetes and a StatefulSet to mean anything).
//
// Returns "" if the namespace can't be determined (e.g. not actually
// running in a Kubernetes pod), which callers treat as "fall back to
// IP-based addressing" — see raftAddressFor.
func raftAdvertiseDomainFor(selfName string) string {
	namespace, err := podNamespace()
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%s.%s.svc.cluster.local", statefulSetNameFromPodName(selfName), namespace)
}

// statefulSetNameFromPodName strips a StatefulSet pod name's trailing
// "-<ordinal>" to recover the StatefulSet's own name. Falls back to the
// full name unchanged if it doesn't look like a StatefulSet pod name.
func statefulSetNameFromPodName(podName string) string {
	idx := strings.LastIndexByte(podName, '-')
	if idx < 0 {
		return podName
	}
	if _, err := strconv.Atoi(podName[idx+1:]); err != nil {
		return podName
	}
	return podName[:idx]
}

// raftAddressFor derives a peer's Raft RPC address. If advertiseDomain is
// set (see raftAdvertiseDomainFor), it builds a stable per-pod DNS name —
// <peer-name>.<domain>:<port> — that keeps working across that pod's
// restarts, since Kubernetes' headless-service DNS repoints the same name
// at whatever pod currently holds that identity, and Go's own dialer
// re-resolves DNS on every connection attempt rather than caching an IP.
// Without it, this falls back to deriving from the peer's gossip address
// (its current pod IP), which is simple but goes stale the moment that pod
// is recreated — this is exactly what caused a real deadlock in testing: a
// leader's RemoveServer/AddVoter can't fix a stale address for an existing
// voter name, and it can't even run without a leader in the first place, so
// an ordinary restart of a minority of voters could permanently wedge the
// group.
func raftAddressFor(p peer.Peer, advertiseDomain string, raftBindPort int) raft.ServerAddress {
	if advertiseDomain != "" {
		return raft.ServerAddress(fmt.Sprintf("%s.%s:%d", p.Name, advertiseDomain, raftBindPort))
	}
	host, _, err := net.SplitHostPort(p.Addr)
	if err != nil {
		host = p.Addr
	}
	return raft.ServerAddress(fmt.Sprintf("%s:%d", host, raftBindPort))
}

// diffAdded returns the names present in after but not before.
func diffAdded(before, after []string) []string {
	beforeSet := make(map[string]bool, len(before))
	for _, b := range before {
		beforeSet[b] = true
	}
	var added []string
	for _, a := range after {
		if !beforeSet[a] {
			added = append(added, a)
		}
	}
	return added
}
