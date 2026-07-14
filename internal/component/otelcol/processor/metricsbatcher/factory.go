package metricsbatcher

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
)

const typeStr = "metricsbatcher"

// NewFactory returns a processor.Factory for the metricsbatcher processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType(typeStr),
		func() component.Config { c := defaultConfig(); return &c },
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelDevelopment),
	)
}

func createMetricsProcessor(
	_ context.Context,
	_ processor.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (processor.Metrics, error) {
	c := cfg.(*Config)
	p := newProcessor(*c, &consumerAdapter{next})
	return &processorAdapter{p}, nil
}

// consumerAdapter wraps consumer.Metrics (which requires Capabilities()) into
// our minimal metricsConsumer interface.
type consumerAdapter struct {
	consumer.Metrics
}

// processorAdapter wraps *Processor to satisfy processor.Metrics, which
// embeds component.Component and consumer.Metrics.
type processorAdapter struct {
	*Processor
}

func (a *processorAdapter) Capabilities() consumer.Capabilities {
	return a.Processor.Capabilities()
}

func (a *processorAdapter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	return a.Processor.ConsumeMetrics(ctx, md)
}

func (a *processorAdapter) Start(ctx context.Context, host component.Host) error {
	return a.Processor.Start(ctx, host)
}

func (a *processorAdapter) Shutdown(ctx context.Context) error {
	return a.Processor.Shutdown(ctx)
}
