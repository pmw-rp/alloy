// Package kafka_router implements the otelcol.exporter.kafka_router component.
//
// It routes incoming OTLP metrics and logs to Kafka topics via a declarative,
// ordered route list.  Each route carries a topic template, an optional
// fallback_topic template, and an optional list of resource attributes that
// must be present for the route to match.  Routes are evaluated in order; the
// first match wins.  If no route matches a resource, the data is dropped with
// a warning log.
//
// When a primary produce fails and fallback_topic is set on the matched route,
// the payload is retried on the fallback topic.  This handles the case where a
// per-cluster topic does not yet exist: data lands on the customer default topic
// instead of being lost.
//
// Topic templates use ${name} placeholders.  Variables are resolved from
// (highest to lowest precedence):
//
//  1. Resource attributes of the current ResourceMetrics / ResourceLogs slice.
//  2. The vars map declared in the component config.
//  3. Built-in variables: ${signal} ("metrics" or "logs").
//
// Example config:
//
//	otelcol.exporter.kafka_router "collector" {
//	  brokers = ["broker:9092"]
//	  vars    = { "customer" = "pmw" }
//
//	  route {
//	    topic          = "${customer}-${signal}-${cluster_id}"
//	    fallback_topic = "${customer}-${signal}-default"
//	    required_attrs = ["cluster_id"]
//	  }
//	  // no catchall route — missing cluster_id → data dropped
//
//	  sasl {
//	    mechanism = "SCRAM-SHA-256"
//	    username  = "user"
//	    password  = "secret"
//	  }
//	}
//
// All Kafka I/O uses franz-go (github.com/twmb/franz-go).
package kafka_router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"

	"github.com/grafana/alloy/internal/component"
	"github.com/grafana/alloy/internal/component/otelcol"
	"github.com/grafana/alloy/internal/featuregate"
	"github.com/grafana/alloy/syntax/alloytypes"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	otelconsumer "go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func init() {
	component.Register(component.Registration{
		Name:      "otelcol.exporter.kafka_router",
		Stability: featuregate.StabilityExperimental,
		Args:      Arguments{},
		Exports:   otelcol.ConsumerExports{},

		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			return New(opts, args.(Arguments))
		},
	})
}

// RouteArguments defines one entry in the ordered routing table.
type RouteArguments struct {
	// Topic is a template string; ${name} placeholders are substituted at
	// routing time from resource attributes, vars, and built-ins.
	Topic string `alloy:"topic,attr"`
	// FallbackTopic is produced to when the primary Topic produce fails.
	// Supports the same ${name} template syntax as Topic.
	FallbackTopic string `alloy:"fallback_topic,attr,optional"`
	// RequiredAttrs lists resource attribute keys that must be present (and
	// non-empty) for this route to match.
	RequiredAttrs []string `alloy:"required_attrs,attr,optional"`
}

// resolvedRoute holds the concrete topic strings after template substitution.
type resolvedRoute struct {
	topic    string
	fallback string // empty means no fallback configured
}

// Arguments configures otelcol.exporter.kafka_router.
type Arguments struct {
	Brokers       []string          `alloy:"brokers,attr"`
	Compression   string            `alloy:"compression,attr,optional"`
	MaxBatchBytes int32             `alloy:"max_batch_bytes,attr,optional"`
	Vars          map[string]string `alloy:"vars,attr,optional"`
	Routes        []RouteArguments  `alloy:"route,block"`

	SASL *SASLArguments              `alloy:"sasl,block,optional"`
	TLS  *otelcol.TLSClientArguments `alloy:"tls,block,optional"`
}

// SASLArguments configures SASL authentication for the Kafka producer.
// Supported mechanisms: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.
type SASLArguments struct {
	Mechanism string            `alloy:"mechanism,attr"`
	Username  string            `alloy:"username,attr"`
	Password  alloytypes.Secret `alloy:"password,attr"`
}

// Component implements otelcol.exporter.kafka_router.
// It directly implements otelcol.Consumer so it can be exported as Input.
type Component struct {
	opts component.Options

	// stateMu guards client, adm, routes, and vars.
	// ConsumeMetrics/ConsumeLogs hold a read lock; Update holds a write lock.
	stateMu sync.RWMutex
	client  *kgo.Client
	adm     *kadm.Client
	routes  []RouteArguments
	vars    map[string]string

	checker *topicChecker
}

var (
	_ component.Component  = (*Component)(nil)
	_ otelconsumer.Traces  = (*Component)(nil)
	_ otelconsumer.Metrics = (*Component)(nil)
	_ otelconsumer.Logs    = (*Component)(nil)
)

// New creates a new kafka_router component.
func New(opts component.Options, args Arguments) (*Component, error) {
	c := &Component{opts: opts, checker: newTopicChecker(opts.Logger)}
	if err := c.Update(args); err != nil {
		return nil, err
	}
	return c, nil
}

// Run implements component.Component.
func (c *Component) Run(ctx context.Context) error {
	go c.checker.run(ctx, func() topicLister {
		// A nil *kadm.Client boxed into a non-nil topicLister would make
		// topicChecker.check's adm == nil guard useless, so return a true
		// nil interface until Update has built a client.
		adm := c.getAdm()
		if adm == nil {
			return nil
		}
		return adm
	})
	<-ctx.Done()
	c.stateMu.Lock()
	if c.client != nil {
		c.client.Close()
		c.client = nil
		c.adm = nil
	}
	c.stateMu.Unlock()
	return nil
}

// getAdm returns the current admin client, or nil if none is set yet.
// Passed to topicChecker.run so it always sees the client Update last built.
func (c *Component) getAdm() *kadm.Client {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.adm
}

// Update implements component.Component.  Replaces the Kafka client and
// routing configuration atomically, then re-publishes consumer exports.
func (c *Component) Update(args component.Arguments) error {
	newArgs := args.(Arguments)

	if len(newArgs.Routes) == 0 {
		return fmt.Errorf("kafka_router requires at least one route block")
	}

	client, err := buildFranzClient(newArgs)
	if err != nil {
		return fmt.Errorf("building kafka client: %w", err)
	}

	c.stateMu.Lock()
	oldClient := c.client
	c.client = client
	c.adm = kadm.NewClient(client)
	c.routes = newArgs.Routes
	c.vars = newArgs.Vars
	c.stateMu.Unlock()

	if oldClient != nil {
		oldClient.Close()
	}

	c.opts.OnStateChange(otelcol.ConsumerExports{Input: c})
	return nil
}

// Capabilities implements otelconsumer.baseConsumer.
func (c *Component) Capabilities() otelconsumer.Capabilities {
	return otelconsumer.Capabilities{MutatesData: false}
}

// ConsumeTraces drops trace data; kafka_router only handles metrics and logs.
func (c *Component) ConsumeTraces(_ context.Context, _ ptrace.Traces) error {
	c.opts.Logger.Warn("kafka_router received traces, dropping (unsupported signal)")
	return nil
}

// ConsumeMetrics implements otelconsumer.Metrics.
func (c *Component) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	all := md.ResourceMetrics()
	return c.routeAndProduce(ctx, "metrics", all.Len(),
		func(i int) pcommon.Map { return all.At(i).Resource().Attributes() },
		func(indices []int) ([]byte, error) {
			out := pmetric.NewMetrics()
			for _, i := range indices {
				all.At(i).CopyTo(out.ResourceMetrics().AppendEmpty())
			}
			m := pmetric.ProtoMarshaler{}
			return m.MarshalMetrics(out)
		},
	)
}

// ConsumeLogs implements otelconsumer.Logs.
func (c *Component) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	all := ld.ResourceLogs()
	return c.routeAndProduce(ctx, "logs", all.Len(),
		func(i int) pcommon.Map { return all.At(i).Resource().Attributes() },
		func(indices []int) ([]byte, error) {
			out := plog.NewLogs()
			for _, i := range indices {
				all.At(i).CopyTo(out.ResourceLogs().AppendEmpty())
			}
			m := plog.ProtoMarshaler{}
			return m.MarshalLogs(out)
		},
	)
}

// routeAndProduce groups n resources by resolved topic and produces one Kafka
// record per unique topic.  It knows nothing about the signal type — callers
// supply attrsAt to read resource attributes and marshal to serialise a
// topic-filtered subset.
func (c *Component) routeAndProduce(
	ctx context.Context,
	signal string,
	n int,
	attrsAt func(int) pcommon.Map,
	marshal func([]int) ([]byte, error),
) error {
	c.stateMu.RLock()
	client := c.client
	routes := c.routes
	vars := c.vars
	c.stateMu.RUnlock()

	if client == nil {
		return fmt.Errorf("kafka client not initialized")
	}

	type group struct {
		fallback string
		indices  []int
	}
	groups := make(map[string]*group)
	for i := 0; i < n; i++ {
		r, ok := resolveRoute(routes, vars, signal, attrsAt(i))
		if !ok {
			c.opts.Logger.Warn("no matching route, dropping resource", "signal", signal)
			continue
		}
		if groups[r.topic] == nil {
			groups[r.topic] = &group{fallback: r.fallback}
		}
		groups[r.topic].indices = append(groups[r.topic].indices, i)
	}

	var errs []error
	for topic, g := range groups {
		b, err := marshal(g.indices)
		if err != nil {
			errs = append(errs, fmt.Errorf("marshal %s for %s: %w", signal, topic, err))
			continue
		}

		target, fallback := chooseTarget(c.checker, topic, g.fallback)

		c.opts.Logger.Info("producing record", "signal", signal, "topic", target, "bytes", len(b), "resources", len(g.indices))
		if err := produceWithFallback(ctx, client, target, fallback, b, c.opts.Logger); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// resolveRoute evaluates the route list in order and returns the resolved
// primary and fallback topics for the first route whose required_attrs are all
// present as non-empty resource attributes.
// signal is the built-in variable "metrics" or "logs".
// Returns ({}, false) if no route matches.
func resolveRoute(routes []RouteArguments, vars map[string]string, signal string, attrs pcommon.Map) (resolvedRoute, bool) {
	// Build the variable resolution map: built-ins < vars < resource attrs.
	res := make(map[string]string, len(vars)+1+attrs.Len())
	res["signal"] = signal
	for k, v := range vars {
		res[k] = v
	}
	attrs.Range(func(k string, v pcommon.Value) bool {
		res[k] = v.AsString()
		return true
	})

	for _, route := range routes {
		matched := true
		for _, req := range route.RequiredAttrs {
			v, ok := attrs.Get(req)
			if !ok || v.AsString() == "" {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		r := resolvedRoute{
			topic:    applyTemplate(route.Topic, res),
			fallback: applyTemplate(route.FallbackTopic, res),
		}
		return r, true
	}
	return resolvedRoute{}, false
}

// chooseTarget decides which topic routeAndProduce should actually produce
// to for a resolved route. If primary has a fallback configured and hasn't
// been confirmed to exist, it goes straight to fallback (with no further
// fallback of its own) instead of discovering primary's absence via a failed
// produce — franz-go's unknown-topic wait costs real time on every call
// otherwise. register schedules an off-data-path check; once it confirms
// primary exists, subsequent calls target it directly.
func chooseTarget(checker *topicChecker, primary, fallback string) (target, fallbackOut string) {
	if fallback != "" && !checker.knownToExist(primary) {
		checker.register(primary)
		return fallback, ""
	}
	return primary, fallback
}

// produceWithFallback produces payload to topic.  If the produce fails and
// fallbackTopic is non-empty, it logs a warning and retries on the fallback.
func produceWithFallback(ctx context.Context, client *kgo.Client, topic, fallbackTopic string, payload []byte, logger *slog.Logger) error {
	err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: payload}).FirstErr()
	if err == nil {
		return nil
	}
	if fallbackTopic == "" {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}
	if ferr := client.ProduceSync(ctx, &kgo.Record{Topic: fallbackTopic, Value: payload}).FirstErr(); ferr != nil {
		return fmt.Errorf("produce to %s and fallback %s both failed: primary: %w; fallback: %v", topic, fallbackTopic, err, ferr)
	}
	logger.Info("produced to fallback topic", "topic", topic, "fallback_topic", fallbackTopic)
	return nil
}

var templatePlaceholder = regexp.MustCompile(`\$\{([^}]+)\}`)

// applyTemplate substitutes ${name} placeholders in tmpl from vars.
// Unrecognised placeholders are left as-is.
func applyTemplate(tmpl string, vars map[string]string) string {
	return templatePlaceholder.ReplaceAllStringFunc(tmpl, func(match string) string {
		key := match[2 : len(match)-1] // strip ${ and }
		if val, ok := vars[key]; ok {
			return val
		}
		return match
	})
}

// buildFranzClient constructs a franz-go Kafka client from Arguments.
func buildFranzClient(args Arguments) (*kgo.Client, error) {
	maxBatchBytes := args.MaxBatchBytes
	if maxBatchBytes == 0 {
		maxBatchBytes = 10 * 1024 * 1024 // 10 MiB
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(args.Brokers...),
		kgo.ProducerBatchMaxBytes(maxBatchBytes),
		// A missing primary (route) topic is an expected, designed-for case here —
		// produceWithFallback immediately retries on fallback_topic. franz-go's
		// default UnknownTopicRetries (4, gated by a 5s minimum metadata-refresh
		// interval) assumes a newly-created topic that just needs time to appear,
		// costing up to ~20s per produce call before giving up. Fail after the
		// first check instead so the fallback path stays fast.
		kgo.UnknownTopicRetries(0),
	}

	codec, err := compressionCodec(args.Compression)
	if err != nil {
		return nil, err
	}
	opts = append(opts, kgo.ProducerBatchCompression(codec))

	if args.SASL != nil {
		mech, err := saslMechanism(args.SASL)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mech))
	}

	if args.TLS != nil {
		tlsCfg, err := args.TLS.Convert().LoadTLSConfig(context.Background())
		if err != nil {
			return nil, fmt.Errorf("loading TLS config: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	return kgo.NewClient(opts...)
}

// compressionCodec maps a compression name to a franz-go codec.
// Empty string defaults to zstd.
func compressionCodec(name string) (kgo.CompressionCodec, error) {
	switch strings.ToLower(name) {
	case "", "zstd":
		return kgo.ZstdCompression(), nil
	case "snappy":
		return kgo.SnappyCompression(), nil
	case "lz4":
		return kgo.Lz4Compression(), nil
	case "gzip":
		return kgo.GzipCompression(), nil
	case "none":
		return kgo.NoCompression(), nil
	default:
		return kgo.NoCompression(), fmt.Errorf("unsupported compression %q (supported: zstd, snappy, lz4, gzip, none)", name)
	}
}

// saslMechanism converts SASLArguments into a franz-go SASL mechanism.
func saslMechanism(args *SASLArguments) (sasl.Mechanism, error) {
	user := args.Username
	pass := string(args.Password)
	switch strings.ToUpper(args.Mechanism) {
	case "SCRAM-SHA-256":
		return scram.Auth{User: user, Pass: pass}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: user, Pass: pass}.AsSha512Mechanism(), nil
	case "PLAIN":
		return plain.Auth{User: user, Pass: pass}.AsMechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q (supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", args.Mechanism)
	}
}
