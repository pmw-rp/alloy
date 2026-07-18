package metricsbatcher

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// metricsConsumer is the minimal interface needed by Processor to forward data.
type metricsConsumer interface {
	ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error
}

// Config holds configuration for the group-batching processor.
type Config struct {
	// SendBatchMaxSize is the maximum number of data points per output batch.
	// A single group that exceeds this size is emitted as one oversized batch
	// rather than being split.
	SendBatchMaxSize int `mapstructure:"send_batch_max_size"`

	// Timeout is the maximum time to wait before flushing a non-empty pending
	// set, even if SendBatchMaxSize has not been reached.
	Timeout time.Duration `mapstructure:"timeout"`

	// MaxConcurrentFlushes bounds how many flushes (a reconstruct-and-forward
	// call to the next consumer) may be in flight at once, across both the
	// size-triggered inline path and the timeout-triggered async path. Once
	// the cap is reached, a new flush blocks until a slot frees instead of
	// running unbounded — without this, a next consumer with no internal
	// queue of its own (e.g. otelcol.exporter.kafka_router, which sends via a
	// synchronous, blocking produce call) lets a slow send pile up an
	// unbounded number of goroutines, each holding a full batch in memory.
	MaxConcurrentFlushes int `mapstructure:"max_concurrent_flushes"`
}

func defaultConfig() Config {
	return Config{
		SendBatchMaxSize:     8192,
		Timeout:              10 * time.Second,
		MaxConcurrentFlushes: 4,
	}
}

// groupKey uniquely identifies a histogram series across shards/le/suffixes.
type groupKey struct {
	resourceKey  string // stable string from resource attributes
	metricFamily string // metric name with _bucket/_sum/_count stripped
	stableLabels string // data-point labels excluding le, shard, partition
}

// dpEntry holds one data point together with the metric name it came from
// (so we can reconstruct proper Metric objects on emit).
type dpEntry struct {
	metricName string
	metricType pmetric.MetricType
	dp         pmetric.NumberDataPoint
}

// dpGroup accumulates data points for one groupKey.
type dpGroup struct {
	resource pcommon.Resource
	scope    pcommon.InstrumentationScope
	entries  []dpEntry
}

// skippedLabels is the set of per-datapoint label keys that vary within a
// histogram series and must be excluded from the stable group key.
var skippedLabels = map[string]bool{
	"le":        true,
	"shard":     true,
	"partition": true,
}

// metricFamilyName strips the _bucket, _sum, _count suffixes that the OTel
// Prometheus exporter adds when converting classic histograms.
func metricFamilyName(name string) string {
	for _, suffix := range []string{"_bucket", "_sum", "_count"} {
		if strings.HasSuffix(name, suffix) {
			return name[:len(name)-len(suffix)]
		}
	}
	return name
}

// stableKey returns a sorted, deterministic string of data-point attributes
// with le/shard/partition omitted.
func stableKey(attrs pcommon.Map) string {
	keys := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, _ pcommon.Value) bool {
		if !skippedLabels[k] {
			keys = append(keys, k)
		}
		return true
	})
	// insertion sort for stability (n is typically small)
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	var b []byte
	for _, k := range keys {
		v, _ := attrs.Get(k)
		b = fmt.Appendf(b, "%s=%s\x00", k, v.AsString())
	}
	return string(b)
}

// resourceAttrsKey returns a stable string for resource attributes.
func resourceAttrsKey(r pcommon.Resource) string {
	attrs := r.Attributes()
	keys := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	var b []byte
	for _, k := range keys {
		v, _ := attrs.Get(k)
		b = fmt.Appendf(b, "%s=%s\x00", k, v.AsString())
	}
	return string(b)
}

// Processor batches metrics while keeping all data points for the same
// histogram series together.
//
// Prometheus classic histograms are expanded by the OTel exporter into
// individual Gauge metrics named _bucket, _sum, and _count. For aggregators
// that sum across shards, all shards for a given (method, service, …) tuple
// must appear in the same Kafka message. The standard batch processor splits
// on raw data-point count and can scatter shards across multiple messages.
//
// This processor groups data points by:
//
//	(resourceKey, metricFamily, stableLabels)
//
// where:
//   - resourceKey  = stable string of resource attributes (e.g. pod name)
//   - metricFamily = metric name with _bucket/_sum/_count suffix stripped
//   - stableLabels = data-point attributes excluding le, shard, partition
//
// All data points that share the same group key — regardless of shard, le, or
// which suffix variant they belong to — are kept together. Batches are packed
// by complete groups up to SendBatchMaxSize data points; a group that alone
// exceeds the limit is emitted as a single oversized batch rather than split.
type Processor struct {
	cfg          Config
	next         metricsConsumer
	capabilities consumer.Capabilities

	mu      sync.Mutex
	pending map[groupKey]*dpGroup
	timer   *time.Timer

	// flushSem bounds concurrent in-flight flushes — see Config.MaxConcurrentFlushes.
	flushSem chan struct{}
}

func newProcessor(cfg Config, next metricsConsumer) *Processor {
	maxFlushes := cfg.MaxConcurrentFlushes
	if maxFlushes <= 0 {
		maxFlushes = 1
	}
	return &Processor{
		cfg:          cfg,
		next:         next,
		capabilities: consumer.Capabilities{MutatesData: true},
		pending:      make(map[groupKey]*dpGroup),
		flushSem:     make(chan struct{}, maxFlushes),
	}
}

func (p *Processor) Capabilities() consumer.Capabilities {
	return p.capabilities
}

func (p *Processor) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (p *Processor) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if p.timer != nil {
		p.timer.Stop()
	}
	pending := p.pending
	p.pending = make(map[groupKey]*dpGroup)
	p.mu.Unlock()

	if len(pending) > 0 {
		return p.emitGroups(ctx, pending)
	}
	return nil
}

// ConsumeMetrics accepts incoming metrics, fans data points into per-group
// buckets keyed by (resource, metricFamily, stableLabels), then flushes
// complete batches when the total data-point count exceeds SendBatchMaxSize.
func (p *Processor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	p.mu.Lock()

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		rk := resourceAttrsKey(rm.Resource())

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			for k := 0; k < sm.Metrics().Len(); k++ {
				src := sm.Metrics().At(k)
				family := metricFamilyName(src.Name())
				mtype := src.Type()

				var dps pmetric.NumberDataPointSlice
				switch mtype {
				case pmetric.MetricTypeGauge:
					dps = src.Gauge().DataPoints()
				case pmetric.MetricTypeSum:
					dps = src.Sum().DataPoints()
				default:
					// Non-gauge/sum metrics (native histograms, summaries) pass
					// through ungrouped by emitting immediately as a 1-entry batch.
					single := pmetric.NewMetrics()
					rms := single.ResourceMetrics().AppendEmpty()
					rm.Resource().CopyTo(rms.Resource())
					sms := rms.ScopeMetrics().AppendEmpty()
					sm.Scope().CopyTo(sms.Scope())
					dst := sms.Metrics().AppendEmpty()
					src.CopyTo(dst)
					p.mu.Unlock()
					if err := p.next.ConsumeMetrics(ctx, single); err != nil {
						p.mu.Lock()
						return err
					}
					p.mu.Lock()
					continue
				}

				for d := 0; d < dps.Len(); d++ {
					dp := dps.At(d)
					sk := stableKey(dp.Attributes())
					gk := groupKey{
						resourceKey:  rk,
						metricFamily: family,
						stableLabels: sk,
					}

					grp, exists := p.pending[gk]
					if !exists {
						grp = &dpGroup{
							resource: pcommon.NewResource(),
							scope:    pcommon.NewInstrumentationScope(),
						}
						rm.Resource().CopyTo(grp.resource)
						sm.Scope().CopyTo(grp.scope)
						p.pending[gk] = grp
					}

					// Copy the data point so we own it after the input slice is freed.
					newDp := pmetric.NewNumberDataPoint()
					dp.CopyTo(newDp)
					grp.entries = append(grp.entries, dpEntry{
						metricName: src.Name(),
						metricType: mtype,
						dp:         newDp,
					})
				}
			}
		}
	}

	toFlush := p.collectBatchesIfReady()

	if len(p.pending) > 0 && p.timer == nil {
		p.timer = time.AfterFunc(p.cfg.Timeout, func() {
			p.mu.Lock()
			pending := p.pending
			p.pending = make(map[groupKey]*dpGroup)
			p.timer = nil
			p.mu.Unlock()
			if len(pending) > 0 {
				_ = p.emitGroups(context.Background(), pending)
			}
		})
	}

	p.mu.Unlock()

	for _, batch := range toFlush {
		if err := p.emitGroups(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

// collectBatchesIfReady packs pending groups into batches when total data
// points exceed SendBatchMaxSize. Never splits a group across batches.
// Called with p.mu held.
func (p *Processor) collectBatchesIfReady() []map[groupKey]*dpGroup {
	total := 0
	for _, g := range p.pending {
		total += len(g.entries)
	}
	if total < p.cfg.SendBatchMaxSize {
		return nil
	}

	var batches []map[groupKey]*dpGroup
	current := make(map[groupKey]*dpGroup)
	currentSize := 0

	for key, grp := range p.pending {
		if currentSize > 0 && currentSize+len(grp.entries) > p.cfg.SendBatchMaxSize {
			batches = append(batches, current)
			current = make(map[groupKey]*dpGroup)
			currentSize = 0
		}
		current[key] = grp
		currentSize += len(grp.entries)
		delete(p.pending, key)
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}

	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
	}

	return batches
}

// emitGroups reconstructs pmetric.Metrics from a batch of dpGroups and
// forwards it to the next consumer.
//
// Within each group the data points may come from multiple metric names (e.g.
// _bucket, _sum, _count). We reconstruct one Metric per distinct metric name
// so downstream consumers see the correct metric structure.
func (p *Processor) emitGroups(ctx context.Context, groups map[groupKey]*dpGroup) error {
	if len(groups) == 0 {
		return nil
	}

	// Block here, not just around the send below, until a flush slot is
	// free — this is the one choke point every flush path (size-triggered
	// inline, timeout-triggered async, and Shutdown's drain) already funnels
	// through, so it's the natural place to bound total concurrent flushes.
	select {
	case p.flushSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-p.flushSem }()

	// We need to reconstruct ResourceMetrics. Group everything by resourceKey
	// first, then by metric name within each resource.
	type metricKey struct {
		resourceKey string
		metricName  string
	}
	type metricSlot struct {
		resource   pcommon.Resource
		scope      pcommon.InstrumentationScope
		metricName string
		metricType pmetric.MetricType
		dps        []pmetric.NumberDataPoint
	}

	slots := make(map[metricKey]*metricSlot)
	// Track resource/scope per resourceKey for reconstruction.
	type resMeta struct {
		resource pcommon.Resource
		scope    pcommon.InstrumentationScope
	}
	resMetas := make(map[string]resMeta)

	for gk, grp := range groups {
		if _, ok := resMetas[gk.resourceKey]; !ok {
			res := pcommon.NewResource()
			scp := pcommon.NewInstrumentationScope()
			grp.resource.CopyTo(res)
			grp.scope.CopyTo(scp)
			resMetas[gk.resourceKey] = resMeta{resource: res, scope: scp}
		}
		for _, entry := range grp.entries {
			mk := metricKey{resourceKey: gk.resourceKey, metricName: entry.metricName}
			slot, ok := slots[mk]
			if !ok {
				slot = &metricSlot{
					metricName: entry.metricName,
					metricType: entry.metricType,
				}
				slots[mk] = slot
			}
			slot.dps = append(slot.dps, entry.dp)
		}
	}

	// Build the output pmetric.Metrics, grouping by resourceKey.
	// We need one ResourceMetrics per resource, one ScopeMetrics, then one
	// Metric per distinct metric name.
	type rmEntry struct {
		rm    pmetric.ResourceMetrics
		scope pmetric.ScopeMetrics
	}
	rmMap := make(map[string]*rmEntry)

	md := pmetric.NewMetrics()
	for mk, slot := range slots {
		rme, ok := rmMap[mk.resourceKey]
		if !ok {
			rm := md.ResourceMetrics().AppendEmpty()
			meta := resMetas[mk.resourceKey]
			meta.resource.CopyTo(rm.Resource())
			sm := rm.ScopeMetrics().AppendEmpty()
			meta.scope.CopyTo(sm.Scope())
			rme = &rmEntry{rm: rm, scope: sm}
			rmMap[mk.resourceKey] = rme
		}

		metric := rme.scope.Metrics().AppendEmpty()
		metric.SetName(slot.metricName)

		switch slot.metricType {
		case pmetric.MetricTypeGauge:
			gauge := metric.SetEmptyGauge()
			for _, dp := range slot.dps {
				newDp := gauge.DataPoints().AppendEmpty()
				dp.CopyTo(newDp)
			}
		case pmetric.MetricTypeSum:
			sum := metric.SetEmptySum()
			for _, dp := range slot.dps {
				newDp := sum.DataPoints().AppendEmpty()
				dp.CopyTo(newDp)
			}
		}
	}

	return p.next.ConsumeMetrics(ctx, md)
}
