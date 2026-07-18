package metricsbatcher

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// blockingConsumer counts how many ConsumeMetrics calls are concurrently
// in flight, tracks the high-water mark, and blocks each call until release
// is closed.
type blockingConsumer struct {
	mu      sync.Mutex
	cur     int
	maxSeen int
	calls   int32

	release chan struct{}
}

func newBlockingConsumer() *blockingConsumer {
	return &blockingConsumer{release: make(chan struct{})}
}

func (c *blockingConsumer) ConsumeMetrics(ctx context.Context, _ pmetric.Metrics) error {
	atomic.AddInt32(&c.calls, 1)
	c.mu.Lock()
	c.cur++
	if c.cur > c.maxSeen {
		c.maxSeen = c.cur
	}
	c.mu.Unlock()

	<-c.release

	c.mu.Lock()
	c.cur--
	c.mu.Unlock()
	return nil
}

func (c *blockingConsumer) waitForInFlight(t *testing.T, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.cur == n
	}, 2*time.Second, time.Millisecond)
}

func dummyGroups(n int) map[groupKey]*dpGroup {
	groups := make(map[groupKey]*dpGroup, n)
	for i := 0; i < n; i++ {
		groups[groupKey{resourceKey: string(rune('a' + i))}] = &dpGroup{
			resource: pcommon.NewResource(),
			scope:    pcommon.NewInstrumentationScope(),
		}
	}
	return groups
}

func TestEmitGroups_BoundsConcurrentFlushes(t *testing.T) {
	const cap = 2
	const attempts = 5

	next := newBlockingConsumer()
	p := newProcessor(Config{SendBatchMaxSize: 1 << 20, Timeout: time.Hour, MaxConcurrentFlushes: cap}, next)

	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.emitGroups(context.Background(), dummyGroups(1))
		}()
	}

	// Exactly `cap` should be in flight, never more, even with more waiting.
	next.waitForInFlight(t, cap)
	time.Sleep(20 * time.Millisecond) // give any over-cap goroutine a chance to (wrongly) proceed
	next.mu.Lock()
	assert.Equal(t, cap, next.cur, "no more than the cap should be in flight at once")
	next.mu.Unlock()

	close(next.release)
	wg.Wait()

	assert.Equal(t, int32(attempts), atomic.LoadInt32(&next.calls), "every flush should eventually run")
	assert.Equal(t, cap, next.maxSeen, "high-water mark should equal the configured cap")
}

func TestEmitGroups_UnderCapDoesNotBlock(t *testing.T) {
	next := newBlockingConsumer()
	close(next.release) // never actually blocks
	p := newProcessor(Config{SendBatchMaxSize: 1 << 20, Timeout: time.Hour, MaxConcurrentFlushes: 4}, next)

	done := make(chan struct{})
	go func() {
		_ = p.emitGroups(context.Background(), dummyGroups(1))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("emitGroups blocked despite being under the concurrency cap")
	}
}

func TestEmitGroups_ContextCancelUnblocksWaiter(t *testing.T) {
	next := newBlockingConsumer()
	p := newProcessor(Config{SendBatchMaxSize: 1 << 20, Timeout: time.Hour, MaxConcurrentFlushes: 1}, next)

	// Occupy the only slot.
	go func() { _ = p.emitGroups(context.Background(), dummyGroups(1)) }()
	next.waitForInFlight(t, 1)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- p.emitGroups(ctx, dummyGroups(1)) }()

	cancel()
	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("emitGroups did not respect context cancellation while waiting for a slot")
	}

	close(next.release)
}

func TestNewProcessor_ClampsNonPositiveMaxConcurrentFlushes(t *testing.T) {
	p := newProcessor(Config{SendBatchMaxSize: 8192, Timeout: time.Second, MaxConcurrentFlushes: 0}, newBlockingConsumer())
	assert.Equal(t, 1, cap(p.flushSem))
}

func TestDefaultConfig_HasPositiveMaxConcurrentFlushes(t *testing.T) {
	cfg := defaultConfig()
	assert.Greater(t, cfg.MaxConcurrentFlushes, 0)
}
