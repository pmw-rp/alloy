// Package metricsbatcher provides an otelcol.processor.metricsbatcher component.
// It batches metrics while keeping all data points for the same
// (resource attributes, metric name) group together, preventing the standard
// batch processor from splitting a single metric across multiple batches.
package metricsbatcher

import (
	"fmt"
	"time"

	"github.com/grafana/alloy/internal/component"
	"github.com/grafana/alloy/internal/component/otelcol"
	otelcolCfg "github.com/grafana/alloy/internal/component/otelcol/config"
	"github.com/grafana/alloy/internal/component/otelcol/processor"
	"github.com/grafana/alloy/internal/featuregate"
	otelcomponent "go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pipeline"
)

func init() {
	component.Register(component.Registration{
		Name:      "otelcol.processor.metricsbatcher",
		Stability: featuregate.StabilityExperimental,
		Args:      Arguments{},
		Exports:   otelcol.ConsumerExports{},

		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			fact := NewFactory()
			return processor.New(opts, fact, args.(Arguments))
		},
	})
}

// Arguments configures the otelcol.processor.metricsbatcher component.
type Arguments struct {
	// SendBatchMaxSize is the maximum number of data points per output batch.
	// Metric groups that would exceed this size are emitted as oversized batches
	// rather than being split.
	SendBatchMaxSize int `alloy:"send_batch_max_size,attr,optional"`

	// Timeout is the maximum time to hold pending metrics before flushing,
	// even if SendBatchMaxSize has not been reached.
	Timeout time.Duration `alloy:"timeout,attr,optional"`

	// Output configures where to send processed data. Required.
	Output *otelcol.ConsumerArguments `alloy:"output,block"`

	// DebugMetrics configures component internal metrics. Optional.
	DebugMetrics otelcolCfg.DebugMetricsArguments `alloy:"debug_metrics,block,optional"`
}

var _ processor.Arguments = Arguments{}

// DefaultArguments holds default settings for Arguments.
var DefaultArguments = Arguments{
	SendBatchMaxSize: 8192,
	Timeout:          10 * time.Second,
}

// SetToDefault implements syntax.Defaulter.
func (args *Arguments) SetToDefault() {
	*args = DefaultArguments
	args.DebugMetrics.SetToDefault()
}

// Validate implements syntax.Validator.
func (args *Arguments) Validate() error {
	if args.SendBatchMaxSize <= 0 {
		return fmt.Errorf("send_batch_max_size must be positive, got %d", args.SendBatchMaxSize)
	}
	if args.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive, got %v", args.Timeout)
	}
	return nil
}

// Convert implements processor.Arguments. Returns the OTel Config for our processor.
func (args Arguments) Convert() (otelcomponent.Config, error) {
	return &Config{
		SendBatchMaxSize: args.SendBatchMaxSize,
		Timeout:          args.Timeout,
	}, nil
}

// Extensions implements processor.Arguments.
func (args Arguments) Extensions() map[otelcomponent.ID]otelcomponent.Component {
	return nil
}

// Exporters implements processor.Arguments.
func (args Arguments) Exporters() map[pipeline.Signal]map[otelcomponent.ID]otelcomponent.Component {
	return nil
}

// NextConsumers implements processor.Arguments.
func (args Arguments) NextConsumers() *otelcol.ConsumerArguments {
	return args.Output
}

// DebugMetricsConfig implements processor.Arguments.
func (args Arguments) DebugMetricsConfig() otelcolCfg.DebugMetricsArguments {
	return args.DebugMetrics
}
