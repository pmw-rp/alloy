package redpanda

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testCfg(low, high float64) AdmissionControlArguments {
	return AdmissionControlArguments{Enabled: true, LowWatermark: low, HighWatermark: high}
}

func TestAdmissionGate_GrowsOneAtATimeWhenHealthy(t *testing.T) {
	g := newAdmissionGate()
	assigned := map[string]bool{"a/0": true, "a/1": true, "a/2": true}
	cfg := testCfg(0.5, 0.75)

	changed := g.reconcile(assigned, 0.1, cfg)
	require.True(t, changed)
	assert.Len(t, g.admittedOrder(), 1)

	changed = g.reconcile(assigned, 0.1, cfg)
	require.True(t, changed)
	assert.Len(t, g.admittedOrder(), 2)

	changed = g.reconcile(assigned, 0.1, cfg)
	require.True(t, changed)
	assert.ElementsMatch(t, []string{"a/0", "a/1", "a/2"}, g.admittedOrder())

	// Nothing left to admit — further healthy ticks are no-ops.
	changed = g.reconcile(assigned, 0.1, cfg)
	assert.False(t, changed)
}

func TestAdmissionGate_GrowsInDeterministicSortedOrder(t *testing.T) {
	g := newAdmissionGate()
	assigned := map[string]bool{"z/9": true, "a/0": true, "m/5": true}
	cfg := testCfg(0.5, 0.75)

	g.reconcile(assigned, 0.1, cfg)
	assert.Equal(t, []string{"a/0"}, g.admittedOrder())

	g.reconcile(assigned, 0.1, cfg)
	assert.Equal(t, []string{"a/0", "m/5"}, g.admittedOrder())

	g.reconcile(assigned, 0.1, cfg)
	assert.Equal(t, []string{"a/0", "m/5", "z/9"}, g.admittedOrder())
}

func TestAdmissionGate_ShrinksMostRecentlyAdmittedFirst(t *testing.T) {
	g := newAdmissionGate()
	assigned := map[string]bool{"a/0": true, "a/1": true, "a/2": true}
	cfg := testCfg(0.5, 0.75)

	// Ramp up to fully admitted.
	for i := 0; i < 3; i++ {
		g.reconcile(assigned, 0.1, cfg)
	}
	require.ElementsMatch(t, []string{"a/0", "a/1", "a/2"}, g.admittedOrder())

	// Overloaded: drop the most recently admitted (a/2), not the oldest.
	changed := g.reconcile(assigned, 0.9, cfg)
	require.True(t, changed)
	assert.Equal(t, []string{"a/0", "a/1"}, g.admittedOrder())

	changed = g.reconcile(assigned, 0.9, cfg)
	require.True(t, changed)
	assert.Equal(t, []string{"a/0"}, g.admittedOrder())

	changed = g.reconcile(assigned, 0.9, cfg)
	require.True(t, changed)
	assert.Empty(t, g.admittedOrder())

	// Nothing left to drop — further overloaded ticks are no-ops.
	changed = g.reconcile(assigned, 0.9, cfg)
	assert.False(t, changed)
}

func TestAdmissionGate_HoldsSteadyBetweenWatermarks(t *testing.T) {
	g := newAdmissionGate()
	assigned := map[string]bool{"a/0": true, "a/1": true}
	cfg := testCfg(0.5, 0.75)

	g.reconcile(assigned, 0.1, cfg)
	require.Equal(t, []string{"a/0"}, g.admittedOrder())

	changed := g.reconcile(assigned, 0.6, cfg) // strictly between 0.5 and 0.75
	assert.False(t, changed)
	assert.Equal(t, []string{"a/0"}, g.admittedOrder())
}

func TestAdmissionGate_DropsNoLongerAssignedRegardlessOfHealth(t *testing.T) {
	g := newAdmissionGate()
	assigned := map[string]bool{"a/0": true, "a/1": true}
	cfg := testCfg(0.5, 0.75)

	g.reconcile(assigned, 0.1, cfg)
	g.reconcile(assigned, 0.1, cfg)
	require.ElementsMatch(t, []string{"a/0", "a/1"}, g.admittedOrder())

	// a/0 no longer assigned (ownership moved elsewhere) — dropped even
	// though health is comfortably healthy (would otherwise only grow).
	changed := g.reconcile(map[string]bool{"a/1": true}, 0.1, cfg)
	require.True(t, changed)
	assert.Equal(t, []string{"a/1"}, g.admittedOrder())
}

func TestAdmissionGate_Seed_IntersectsWithCurrentlyAssigned(t *testing.T) {
	g := newAdmissionGate()
	restored := []string{"a/0", "a/1", "a/2"}
	assigned := map[string]bool{"a/0": true, "a/2": true} // a/1 moved away while this replica was down

	g.seed(restored, assigned)

	assert.ElementsMatch(t, []string{"a/0", "a/2"}, g.admittedOrder())
	assert.True(t, g.admittedSet()["a/0"])
	assert.False(t, g.admittedSet()["a/1"])
}

func TestAdmissionGate_SeedThenReconcile_RestoresWithoutReramping(t *testing.T) {
	g := newAdmissionGate()
	assigned := map[string]bool{"a/0": true, "a/1": true, "a/2": true}
	cfg := testCfg(0.5, 0.75)

	g.seed([]string{"a/0", "a/1", "a/2"}, assigned)
	require.Len(t, g.admittedOrder(), 3)

	// A healthy tick right after restoring shouldn't need to grow anything
	// further — restoration already got there in one step, not three.
	changed := g.reconcile(assigned, 0.1, cfg)
	assert.False(t, changed)
	assert.Len(t, g.admittedOrder(), 3)
}

func TestAdmissionGate_SeedWithNoPriorState_StartsEmpty(t *testing.T) {
	g := newAdmissionGate()
	g.seed(nil, map[string]bool{"a/0": true})
	assert.Empty(t, g.admittedOrder())
}

func TestGaugeValueForComponent_ParsesRealPrometheusText(t *testing.T) {
	const text = `
# HELP otelcol_processor_metricsbatcher_flushes_in_flight in flight
# TYPE otelcol_processor_metricsbatcher_flushes_in_flight gauge
otelcol_processor_metricsbatcher_flushes_in_flight{component_id="otelcol.processor.metricsbatcher.default",otel_scope_name="x"} 3
otelcol_processor_metricsbatcher_flushes_in_flight{component_id="otelcol.processor.metricsbatcher.other",otel_scope_name="x"} 99
# HELP otelcol_processor_metricsbatcher_flushes_capacity capacity
# TYPE otelcol_processor_metricsbatcher_flushes_capacity gauge
otelcol_processor_metricsbatcher_flushes_capacity{component_id="otelcol.processor.metricsbatcher.default",otel_scope_name="x"} 4
`
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(text))
	require.NoError(t, err)

	v, ok := gaugeValueForComponent(families, admissionFlushMetricInFlight, "otelcol.processor.metricsbatcher.default")
	require.True(t, ok)
	assert.Equal(t, 3.0, v)

	v, ok = gaugeValueForComponent(families, admissionFlushMetricCapacity, "otelcol.processor.metricsbatcher.default")
	require.True(t, ok)
	assert.Equal(t, 4.0, v)

	_, ok = gaugeValueForComponent(families, admissionFlushMetricInFlight, "no.such.component")
	assert.False(t, ok)

	_, ok = gaugeValueForComponent(families, "no_such_metric", "otelcol.processor.metricsbatcher.default")
	assert.False(t, ok)
}

func TestFlushHealthReader_Ratio(t *testing.T) {
	const text = `
# HELP otelcol_processor_metricsbatcher_flushes_in_flight in flight
# TYPE otelcol_processor_metricsbatcher_flushes_in_flight gauge
otelcol_processor_metricsbatcher_flushes_in_flight{component_id="otelcol.processor.metricsbatcher.default"} 3
# HELP otelcol_processor_metricsbatcher_flushes_capacity capacity
# TYPE otelcol_processor_metricsbatcher_flushes_capacity gauge
otelcol_processor_metricsbatcher_flushes_capacity{component_id="otelcol.processor.metricsbatcher.default"} 4
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(text))
	}))
	defer srv.Close()

	r := &flushHealthReader{
		client:      srv.Client(),
		metricsURL:  srv.URL,
		componentID: "otelcol.processor.metricsbatcher.default",
	}

	ratio, err := r.ratio(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0.75, ratio)
}

// TestFlushHealthReader_Ratio_ColdStartTreatsMissingInFlightAsIdle covers
// the real deadlock confirmed live on a fresh deployment: capacity is
// recorded eagerly the moment metricsbatcher starts, but in_flight only
// gets its first data point once a flush actually happens — which itself
// depends on discovery.redpanda having admitted something to scrape. If
// missing in_flight (with capacity present) were treated as an error like
// any other missing metric, admission control would never be able to
// admit anything on a fresh deployment: nothing ever flows through
// metricsbatcher to produce that first data point because nothing was
// ever admitted, and nothing is ever admitted because the health check
// always fails.
func TestFlushHealthReader_Ratio_ColdStartTreatsMissingInFlightAsIdle(t *testing.T) {
	const text = `
# HELP otelcol_processor_metricsbatcher_flushes_capacity capacity
# TYPE otelcol_processor_metricsbatcher_flushes_capacity gauge
otelcol_processor_metricsbatcher_flushes_capacity{component_id="otelcol.processor.metricsbatcher.default"} 4
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(text))
	}))
	defer srv.Close()

	r := &flushHealthReader{
		client:      srv.Client(),
		metricsURL:  srv.URL,
		componentID: "otelcol.processor.metricsbatcher.default",
	}

	ratio, err := r.ratio(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0.0, ratio)
}

func TestFlushHealthReader_Ratio_MissingCapacityIsStillAnError(t *testing.T) {
	const text = `
# HELP otelcol_processor_metricsbatcher_flushes_in_flight in flight
# TYPE otelcol_processor_metricsbatcher_flushes_in_flight gauge
otelcol_processor_metricsbatcher_flushes_in_flight{component_id="otelcol.processor.metricsbatcher.default"} 3
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(text))
	}))
	defer srv.Close()

	r := &flushHealthReader{
		client:      srv.Client(),
		metricsURL:  srv.URL,
		componentID: "otelcol.processor.metricsbatcher.default",
	}

	// Unlike in_flight, capacity missing is NOT treated as cold start —
	// it's recorded eagerly at construction, so its absence is a real
	// signal something's wrong (e.g. a flush_metrics_component_id typo).
	_, err := r.ratio(context.Background())
	assert.Error(t, err)
}

func TestFlushHealthReader_Ratio_NonOKStatusIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	r := &flushHealthReader{
		client:      srv.Client(),
		metricsURL:  srv.URL,
		componentID: "x",
	}

	_, err := r.ratio(context.Background())
	assert.Error(t, err)
}
