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
// Each target also receives a __tmp_pod_ordinal label: a stable integer (0..N-1)
// assigned by sorting all known pods by (namespace, statefulset-name, pod-ordinal).
// Collector pods use this label with env("POD_INDEX") to shard scrape targets
// without relying on gossip-based clustering. Pods that temporarily disappear
// (e.g. during rolling restarts) hold their ordinal slot for StaleTimeout
// (default 5m) to prevent renumbering.
package redpanda

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/alloy/internal/component"
	"github.com/grafana/alloy/internal/component/discovery"
	"github.com/grafana/alloy/internal/featuregate"
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
	fetchTimeout        = 10 * time.Second
	defaultStaleTimeout = 5 * time.Minute
	labelPodOrdinal     = "__tmp_pod_ordinal"
)

// Arguments configures the discovery.redpanda_uuid component.
type Arguments struct {
	Targets       []discovery.Target `alloy:"targets,attr"`
	AdminPort     int                `alloy:"admin_port,attr,optional"`
	TLSEnabled    bool               `alloy:"tls_enabled,attr,optional"`
	TLSSkipVerify bool               `alloy:"tls_skip_verify,attr,optional"`
	// StaleTimeout is how long a pod that disappears from discovery holds its
	// ordinal slot before being evicted and triggering renumbering. Default: 5m.
	StaleTimeout time.Duration `alloy:"stale_timeout,attr,optional"`
	// NumShards is the number of collector pods (replicas). When set, the label
	// value is ordinal % NumShards rather than the raw ordinal, so collectors
	// and Redpanda pods don't need to be 1:1. Default: 0 (use raw ordinal).
	NumShards int `alloy:"num_shards,attr,optional"`
}

// Exports holds the enriched targets output.
type Exports struct {
	Targets []discovery.Target `alloy:"targets,attr"`
}

// trackedPod holds the sort metadata and last-seen timestamp for a pod.
type trackedPod struct {
	namespace  string
	stsName    string
	podOrdinal int
	lastSeen   time.Time
}

// Component implements the discovery.redpanda_uuid component.
type Component struct {
	opts      component.Options
	mut       sync.Mutex
	uuidCache map[string]string     // key: "namespace/podName" → cluster_uuid
	tracked   map[string]trackedPod // key → tracking info (survives pod disappearance)
}

var _ component.Component = (*Component)(nil)

// New creates a new discovery.redpanda_uuid component.
func New(opts component.Options, args Arguments) (*Component, error) {
	c := &Component{
		opts:      opts,
		uuidCache: make(map[string]string),
		tracked:   make(map[string]trackedPod),
	}
	if err := c.Update(args); err != nil {
		return nil, err
	}
	return c, nil
}

// Run implements component.Component. All work happens in Update.
func (c *Component) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Update fetches UUIDs for uncached pods, assigns stable ordinals sorted by
// (namespace, statefulset-name, pod-ordinal), and publishes enriched targets.
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

	// Index current targets by key.
	currentTargets := make(map[string]discovery.Target, len(newArgs.Targets))
	for _, t := range newArgs.Targets {
		key := targetKey(t)
		if key != "" {
			currentTargets[key] = t
		}
	}

	c.mut.Lock()

	// Mark currently-present pods as seen; register new pods with their sort metadata.
	for key, t := range currentTargets {
		pod, exists := c.tracked[key]
		if !exists {
			pod = newTrackedPod(t)
		}
		pod.lastSeen = now
		c.tracked[key] = pod
	}

	// Evict pods absent longer than staleTimeout.
	for key, pod := range c.tracked {
		if now.Sub(pod.lastSeen) > staleTimeout {
			delete(c.tracked, key)
			delete(c.uuidCache, key)
			c.opts.Logger.Debug("evicted stale pod", "key", key)
		}
	}

	// Sort all tracked pods (present + recently absent) and assign stable ordinals.
	type entry struct {
		key string
		trackedPod
	}
	sorted := make([]entry, 0, len(c.tracked))
	for key, pod := range c.tracked {
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
		if newArgs.NumShards > 0 {
			ordinals[e.key] = i % newArgs.NumShards
		} else {
			ordinals[e.key] = i
		}
	}

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

	// Build enriched output: currently-present pods only, with cluster_id and ordinal.
	enriched := make([]discovery.Target, 0, len(newArgs.Targets))
	for _, t := range newArgs.Targets {
		key := targetKey(t)
		if key == "" {
			enriched = append(enriched, t)
			continue
		}
		c.mut.Lock()
		uuid, hasUUID := c.uuidCache[key]
		c.mut.Unlock()
		if !hasUUID {
			c.opts.Logger.Warn("excluding target with unknown cluster UUID", "key", key)
			continue
		}
		ordinal, ok := ordinals[key]
		if !ok {
			continue
		}
		builder := discovery.NewTargetBuilderFrom(t)
		builder.Set("cluster_id", uuid)
		builder.Set(labelPodOrdinal, strconv.Itoa(ordinal))
		enriched = append(enriched, builder.Target())
	}

	c.opts.OnStateChange(Exports{Targets: enriched})
	return nil
}

// newTrackedPod extracts stable sort metadata from a newly-discovered target.
func newTrackedPod(t discovery.Target) trackedPod {
	ns, _ := t.Get("__meta_kubernetes_namespace")
	podName, _ := t.Get("__meta_kubernetes_pod_name")
	stsName, ordinal := parsePodName(podName)
	return trackedPod{
		namespace:  ns,
		stsName:    stsName,
		podOrdinal: ordinal,
	}
}

// parsePodName splits a StatefulSet pod name (e.g. "redpanda-sandbox-2") into
// the StatefulSet name ("redpanda-sandbox") and numeric ordinal (2) by
// splitting on the last '-'. Falls back to (podName, 0) for non-conforming names.
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
