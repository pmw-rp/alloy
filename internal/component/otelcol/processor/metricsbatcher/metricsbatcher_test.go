package metricsbatcher

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArguments_ValidateRejectsNonPositiveMaxConcurrentFlushes(t *testing.T) {
	args := DefaultArguments
	args.MaxConcurrentFlushes = 0
	assert.Error(t, args.Validate())

	args.MaxConcurrentFlushes = -1
	assert.Error(t, args.Validate())

	args.MaxConcurrentFlushes = 1
	assert.NoError(t, args.Validate())
}

func TestArguments_ConvertCarriesMaxConcurrentFlushes(t *testing.T) {
	args := DefaultArguments
	args.MaxConcurrentFlushes = 7

	cfg, err := args.Convert()
	require.NoError(t, err)

	c, ok := cfg.(*Config)
	require.True(t, ok)
	assert.Equal(t, 7, c.MaxConcurrentFlushes)
}
