package redpanda

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/grafana/alloy/internal/component"
	httpservice "github.com/grafana/alloy/internal/service/http"
)

// admissionFlushMetricInFlight/Capacity name the self-instrumentation
// gauges otelcol.processor.metricsbatcher exposes for its bounded flush
// concurrency (see that component's processor.go). Alloy has no native
// channel for one component to read another's metrics directly — only the
// combined Prometheus registry every component's own metrics already flow
// into — so flushHealthReader reads them the same way any external
// scraper would: an HTTP GET against Alloy's own /metrics.
const (
	admissionFlushMetricInFlight = "otelcol_processor_metricsbatcher_flushes_in_flight"
	admissionFlushMetricCapacity = "otelcol_processor_metricsbatcher_flushes_capacity"
	admissionComponentIDLabel    = "component_id"
)

// flushHealthReader reads a configured otelcol.processor.metricsbatcher
// instance's flush concurrency gauges by scraping Alloy's own /metrics
// endpoint in-process. httpservice.Data's DialFunc/MemoryListenAddr avoid
// an actual TCP round trip — the same in-memory-dial mechanism
// prometheus.exporter.* components use to expose themselves for scraping
// (see internal/component/prometheus/exporter/exporter.go), used here in
// the opposite direction to consume another component's metrics instead of
// publishing this component's own.
type flushHealthReader struct {
	client      *http.Client
	metricsURL  string
	componentID string
}

// newFlushHealthReader builds a reader for componentID — the component ID
// of the otelcol.processor.metricsbatcher instance to watch (e.g.
// "otelcol.processor.metricsbatcher.default"), a name-based coupling since
// Alloy has no typed reference between unrelated components' Arguments/
// Exports for this.
func newFlushHealthReader(opts component.Options, componentID string) (*flushHealthReader, error) {
	data, err := opts.GetServiceData(httpservice.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("getting http service data: %w", err)
	}
	httpData := data.(httpservice.Data)
	return &flushHealthReader{
		client: &http.Client{
			Transport: &http.Transport{DialContext: httpData.DialFunc},
			Timeout:   5 * time.Second,
		},
		metricsURL:  "http://" + httpData.MemoryListenAddr + "/metrics",
		componentID: componentID,
	}, nil
}

// ratio returns flushes_in_flight / flushes_capacity for the configured
// component, or an error if either series can't be found yet (e.g. a
// flush_metrics_component_id typo, or that component hasn't published its
// first data point).
func (r *flushHealthReader) ratio(ctx context.Context) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.metricsURL, nil)
	if err != nil {
		return 0, fmt.Errorf("building metrics request: %w", err)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("scraping %s: %w", r.metricsURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status %d scraping %s", resp.StatusCode, r.metricsURL)
	}

	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("parsing metrics response: %w", err)
	}

	inFlight, ok := gaugeValueForComponent(families, admissionFlushMetricInFlight, r.componentID)
	if !ok {
		return 0, fmt.Errorf("metric %q not found for component_id %q", admissionFlushMetricInFlight, r.componentID)
	}
	capacity, ok := gaugeValueForComponent(families, admissionFlushMetricCapacity, r.componentID)
	if !ok {
		return 0, fmt.Errorf("metric %q not found for component_id %q", admissionFlushMetricCapacity, r.componentID)
	}
	if capacity <= 0 {
		return 0, fmt.Errorf("metric %q reported non-positive value %v", admissionFlushMetricCapacity, capacity)
	}
	return inFlight / capacity, nil
}

// gaugeValueForComponent finds the sample in families for the metric
// family name, scoped to the sample whose component_id label equals
// componentID — the same scoping otelcol.processor.metricsbatcher's own
// self-instrumentation attaches (via Alloy's per-component metric
// wrapping), needed here because /metrics contains every component's
// metrics mixed together.
func gaugeValueForComponent(families map[string]*dto.MetricFamily, name, componentID string) (float64, bool) {
	mf, ok := families[name]
	if !ok {
		return 0, false
	}
	for _, m := range mf.GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == admissionComponentIDLabel && l.GetValue() == componentID {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// admissionGate tracks, for this replica, which of its Raft-assigned
// brokers are actually admitted into published targets — see the package
// doc comment's admission control section. seed()/reconcile() are only
// ever called from Run()'s single goroutine, but admittedOrder()/
// admittedSet() are also called from publishTargetsClustered(), which can
// run from Update()'s goroutine concurrently with Run()'s (the same reason
// Component itself has c.mut) — so this type needs its own lock rather
// than relying on single-goroutine access.
type admissionGate struct {
	mu       sync.Mutex
	admitted []string        // ordered oldest-first; shrink pops from the end (most-recently-admitted first)
	inSet    map[string]bool // admitted, mirrored as a set for O(1) membership checks
}

func newAdmissionGate() *admissionGate {
	return &admissionGate{inSet: make(map[string]bool)}
}

// seed restores a previously-admitted set (e.g. from persisted pod
// annotation state) intersected with what's currently actually assigned,
// so a routine restart of an already-stable replica resumes where it left
// off instead of always ramping from empty. Only meaningful before the
// gate has done any of its own reconciling; call at most once, right after
// construction.
func (g *admissionGate) seed(restored []string, assigned map[string]bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.admitted = nil
	g.inSet = make(map[string]bool)
	for _, id := range restored {
		if assigned[id] {
			g.admitted = append(g.admitted, id)
			g.inSet[id] = true
		}
	}
}

// admittedOrder returns a defensive copy of the currently admitted set, in
// admission order (oldest first) — used both to publish targets and to
// persist state.
func (g *admissionGate) admittedOrder() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]string, len(g.admitted))
	copy(out, g.admitted)
	return out
}

// admittedSet returns a defensive copy of the currently admitted set, as a
// set, for membership checks against published targets.
func (g *admissionGate) admittedSet() map[string]bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]bool, len(g.inSet))
	for k := range g.inSet {
		out[k] = true
	}
	return out
}

// reconcile applies one gate decision for the current tick: unconditionally
// drop anything no longer assigned (not a health decision, just a fact),
// then — depending on ratio against the configured watermarks — shrink the
// most-recently-admitted entry, grow by admitting one backlog entry (in a
// deterministic, sorted order, so behavior is reproducible and
// debuggable), or hold steady in between. Returns whether the admitted set
// actually changed.
func (g *admissionGate) reconcile(assigned map[string]bool, ratio float64, cfg AdmissionControlArguments) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	changed := false

	kept := make([]string, 0, len(g.admitted))
	for _, id := range g.admitted {
		if assigned[id] {
			kept = append(kept, id)
		} else {
			delete(g.inSet, id)
			changed = true
		}
	}
	g.admitted = kept

	switch {
	case ratio >= cfg.HighWatermark:
		if n := len(g.admitted); n > 0 {
			drop := g.admitted[n-1]
			g.admitted = g.admitted[:n-1]
			delete(g.inSet, drop)
			changed = true
		}
	case ratio <= cfg.LowWatermark:
		var candidates []string
		for id := range assigned {
			if !g.inSet[id] {
				candidates = append(candidates, id)
			}
		}
		if len(candidates) > 0 {
			sort.Strings(candidates)
			next := candidates[0]
			g.admitted = append(g.admitted, next)
			g.inSet[next] = true
			changed = true
		}
	}
	return changed
}
