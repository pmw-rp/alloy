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
// Which collector replica scrapes which broker is decided by an embedded
// Raft group spanning every replica of this component (one Raft peer per
// Alloy collector replica, membership driven by Alloy's native gossip
// clustering via cluster.Peers() — see raftnode.go). Only the current Raft
// leader proposes assignment changes (see allocate.go); every replica reads
// the resulting committed state to decide which brokers it owns. This
// replaced an earlier static ordinal/NumShards scheme: that scheme required
// NumShards to be kept manually in sync with the real collector replica
// count, and its global sort coupled every Redpanda cluster's assignment to
// every other cluster's topology. There is no toggle back to that scheme —
// Raft-backed allocation is unconditional, including for single-replica
// deployments (which simply run a degenerate single-voter Raft group).
//
// Upgrading an existing deployment from the old scheme requires a
// coordinated, all-at-once rollout of every collector replica: there is no
// safety net that mirrors the old assignment until every replica has
// migrated, so a rolling restart can briefly double-scrape or gap brokers
// while old- and new-code replicas coexist.
package redpanda

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/grafana/ckit/peer"
	"github.com/hashicorp/raft"

	"github.com/grafana/alloy/internal/component"
	"github.com/grafana/alloy/internal/component/discovery"
	"github.com/grafana/alloy/internal/featuregate"
	"github.com/grafana/alloy/internal/service/cluster"
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
)

// Arguments configures the discovery.redpanda component.
type Arguments struct {
	Targets       []discovery.Target `alloy:"targets,attr"`
	AdminPort     int                `alloy:"admin_port,attr,optional"`
	TLSEnabled    bool               `alloy:"tls_enabled,attr,optional"`
	TLSSkipVerify bool               `alloy:"tls_skip_verify,attr,optional"`
	// StaleTimeout is how long a pod that disappears from discovery holds its
	// assignment before being evicted. Default: 5m.
	StaleTimeout time.Duration `alloy:"stale_timeout,attr,optional"`
	// RaftBindPort is the TCP port this component binds for Raft RPC between
	// collector replicas. Must be the same across every replica in the
	// fleet, and separate from Alloy's own clustering port. Default: 9700.
	RaftBindPort int `alloy:"raft_bind_port,attr,optional"`
}

// Exports holds the enriched targets output.
type Exports struct {
	Targets []discovery.Target `alloy:"targets,attr"`
}

// trackedPod holds the last-seen timestamp for a pod. Its identity (the map
// key) is namespace/podName; that's all the allocator needs beyond the
// resolved cluster UUID, which lives in uuidCache.
type trackedPod struct {
	lastSeen time.Time
}

// Component implements the discovery.redpanda component.
type Component struct {
	opts         component.Options
	cluster      cluster.Cluster
	node         *raftNode
	raftBindPort int

	mut         sync.Mutex
	uuidCache   map[string]string     // key: "namespace/podName" -> cluster_uuid
	tracked     map[string]trackedPod // key -> tracking info (survives pod disappearance)
	lastTargets []discovery.Target    // raw targets from the most recent Update
}

var _ component.Component = (*Component)(nil)

// New creates a new discovery.redpanda component.
func New(opts component.Options, args Arguments) (*Component, error) {
	data, err := opts.GetServiceData(cluster.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("getting cluster service: %w", err)
	}
	clusterSvc := data.(cluster.Cluster)

	selfName, selfHost, err := selfIdentity(clusterSvc)
	if err != nil {
		return nil, err
	}

	if peers := clusterSvc.Peers(); len(peers) == 1 {
		opts.Logger.Warn(
			"this replica sees no other cluster peers; if this is meant to be a multi-replica deployment, " +
				"broker-to-collector allocation will not be sharded correctly. Check that Alloy was started " +
				"with --cluster.enabled and the other replicas are reachable. If you intend to run a single " +
				"replica, this warning is expected and can be ignored. Consider setting --cluster.wait-for-size " +
				"to the intended replica count so the cluster service refuses to serve traffic until every " +
				"replica has actually joined, instead of each isolated replica silently assuming it owns everything.",
		)
	}

	raftBindPort := args.RaftBindPort
	if raftBindPort == 0 {
		raftBindPort = defaultRaftBindPort
	}

	node, err := newRaftNode(
		filepath.Join(opts.DataPath, "raft"),
		selfName, selfHost, raftBindPort,
		clusterSvc.Peers(),
		slogWriter{opts.Logger},
	)
	if err != nil {
		return nil, fmt.Errorf("starting raft node: %w", err)
	}

	c := &Component{
		opts:         opts,
		cluster:      clusterSvc,
		node:         node,
		raftBindPort: raftBindPort,
		uuidCache:    make(map[string]string),
		tracked:      make(map[string]trackedPod),
	}
	if err := c.Update(args); err != nil {
		return nil, err
	}
	return c, nil
}

// selfIdentity finds this node's own name and host among the current
// cluster peers.
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

// Run implements component.Component. It owns the Raft node's reconcile
// loop: only the current leader acts, bridging cluster.Peers() into Raft
// voter membership and proposing broker assignment changes.
func (c *Component) Run(ctx context.Context) error {
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	leaderCh := c.node.raft.LeaderCh()

	for {
		select {
		case <-ctx.Done():
			return c.node.raft.Shutdown().Error()
		case <-leaderCh:
			c.reconcile()
		case <-ticker.C:
			c.reconcile()
		}
	}
}

// reconcile is the leader-only work: bridge gossip membership into Raft
// voters, then reconcile broker assignments against the current tracked set.
// A no-op on followers (isLeader() guards every write inside).
func (c *Component) reconcile() {
	if !c.node.isLeader() {
		return
	}

	prevVoters, err := c.node.currentVoters()
	if err != nil {
		c.opts.Logger.Warn("failed to read raft configuration", "err", err)
		return
	}

	peers := c.cluster.Peers()
	if err := c.node.reconcileMembership(peers, func(p peer.Peer) raft.ServerAddress {
		return raftAddrFor(p, c.raftBindPort)
	}); err != nil {
		c.opts.Logger.Warn("failed to reconcile raft membership", "err", err)
	}

	newVoters, err := c.node.currentVoters()
	if err != nil {
		c.opts.Logger.Warn("failed to read raft configuration", "err", err)
		return
	}
	addedCollectors := diffAdded(prevVoters, newVoters)

	c.mut.Lock()
	tracked := make(map[string]brokerInfo)
	for key := range c.tracked {
		uuid, ok := c.uuidCache[key]
		if !ok {
			continue
		}
		tracked[key] = brokerInfo{id: key, clusterID: uuid}
	}
	c.mut.Unlock()

	current := c.node.fsm.snapshotState()
	cmds := reconcileAssignments(current, tracked, newVoters, addedCollectors)
	for _, cmd := range cmds {
		if err := c.node.propose(cmd); err != nil {
			c.opts.Logger.Warn("failed to propose broker assignment", "err", err, "broker", cmd.BrokerID)
		}
	}

	if len(cmds) > 0 {
		c.publishTargets()
	}
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
	for key := range currentTargets {
		pod, exists := c.tracked[key]
		if !exists {
			pod = trackedPod{}
		}
		pod.lastSeen = now
		c.tracked[key] = pod
	}
	for key, pod := range c.tracked {
		if now.Sub(pod.lastSeen) > staleTimeout {
			delete(c.tracked, key)
			delete(c.uuidCache, key)
			c.opts.Logger.Debug("evicted stale pod", "key", key)
		}
	}
	c.lastTargets = newArgs.Targets
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
				uuid, err := fetchClusterUUID(j.podIP, newArgs)
				if err != nil {
					c.opts.Logger.Warn("failed to fetch cluster UUID", "key", j.key, "pod_ip", j.podIP, "err", err)
					return
				}
				c.opts.Logger.Debug("cached cluster UUID", "key", j.key, "uuid", uuid)
				c.mut.Lock()
				c.uuidCache[j.key] = uuid
				c.mut.Unlock()
			}(job)
		}
		wg.Wait()
	}

	c.publishTargets()
	return nil
}

// publishTargets recomputes Exports from the current tracked/UUID state and
// the current Raft-committed assignment, and publishes it. Called from
// Update (after a discovery refresh) and from reconcile (after an
// assignment change that happened independently of any discovery refresh).
func (c *Component) publishTargets() {
	// If the operator configured --cluster.wait-for-size and the cluster
	// hasn't reached it yet, this replica must not assume it's safe to
	// scrape anything — the same rule discovery.DistributedTargets and
	// other clustering-aware components follow. Without this check, a
	// deployment that forgot --cluster.enabled entirely would silently have
	// every replica believe it's the only one and scrape every broker N
	// times over, since a lone, ungossiped node is indistinguishable from a
	// genuinely single-replica deployment from inside this component.
	if !c.cluster.Ready() {
		c.opts.Logger.Debug("cluster not ready, publishing no targets")
		c.opts.OnStateChange(Exports{Targets: nil})
		return
	}

	c.mut.Lock()
	rawTargets := c.lastTargets
	uuidCache := make(map[string]string, len(c.uuidCache))
	for k, v := range c.uuidCache {
		uuidCache[k] = v
	}
	c.mut.Unlock()

	enriched := make([]discovery.Target, 0, len(rawTargets))
	for _, t := range rawTargets {
		key := targetKey(t)
		if key == "" {
			enriched = append(enriched, t)
			continue
		}
		uuid, hasUUID := uuidCache[key]
		if !hasUUID {
			c.opts.Logger.Warn("excluding target with unknown cluster UUID", "key", key)
			continue
		}
		collector, has := c.node.fsm.collectorOf(key)
		if !has || collector != c.node.self {
			continue // not assigned to this replica
		}
		builder := discovery.NewTargetBuilderFrom(t)
		builder.Set("cluster_id", uuid)
		enriched = append(enriched, builder.Target())
	}

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

func fetchClusterUUID(podIP string, args Arguments) (string, error) {
	scheme := "http"
	if args.TLSEnabled {
		scheme = "https"
	}

	url := fmt.Sprintf("%s://%s:%d/v1/cluster/uuid", scheme, podIP, args.AdminPort)

	ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("building request: %w", err)
	}

	transport := http.DefaultTransport
	if args.TLSEnabled && args.TLSSkipVerify {
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
