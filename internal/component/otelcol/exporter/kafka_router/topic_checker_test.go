package kafka_router

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeLister is a topicLister test double. exist controls which topics
// ListTopics reports as present; anything else comes back with a non-nil
// Err, matching how a real broker reports UNKNOWN_TOPIC_OR_PARTITION.
type fakeLister struct {
	mu    sync.Mutex
	exist map[string]bool
	calls int
}

func (f *fakeLister) ListTopics(_ context.Context, topics ...string) (kadm.TopicDetails, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	details := make(kadm.TopicDetails, len(topics))
	for _, topic := range topics {
		d := kadm.TopicDetail{Topic: topic}
		if !f.exist[topic] {
			d.Err = errUnknownTopicForTest
		}
		details[topic] = d
	}
	return details, nil
}

func (f *fakeLister) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

var errUnknownTopicForTest = errors.New("UNKNOWN_TOPIC_OR_PARTITION")

func TestTopicChecker_KnownToExist_DefaultsFalse(t *testing.T) {
	tc := newTopicChecker(discardLogger())
	if tc.knownToExist("never-seen") {
		t.Fatalf("expected an unregistered topic to report knownToExist=false")
	}
}

func TestTopicChecker_Register_TracksAsMissingAndRequestsCheck(t *testing.T) {
	tc := newTopicChecker(discardLogger())
	tc.register("t1")

	if tc.knownToExist("t1") {
		t.Fatalf("newly registered topic should not be knownToExist yet")
	}
	missing := tc.missingTopics()
	if len(missing) != 1 || missing[0] != "t1" {
		t.Fatalf("expected missingTopics()=[t1], got %v", missing)
	}

	select {
	case topic := <-tc.newTopics:
		if topic != "t1" {
			t.Fatalf("expected immediate-check request for t1, got %q", topic)
		}
	default:
		t.Fatalf("expected register to enqueue an immediate check")
	}
}

func TestTopicChecker_Register_SecondCallIsNoop(t *testing.T) {
	tc := newTopicChecker(discardLogger())
	tc.register("t1")
	<-tc.newTopics // drain the first request

	tc.register("t1") // already tracked; must not enqueue again or panic

	select {
	case topic := <-tc.newTopics:
		t.Fatalf("expected no second check request, got one for %q", topic)
	default:
	}
	if missing := tc.missingTopics(); len(missing) != 1 {
		t.Fatalf("expected exactly one tracked topic, got %v", missing)
	}
}

func TestTopicChecker_Check_MarksExistingTopic(t *testing.T) {
	tc := newTopicChecker(discardLogger())
	tc.register("exists-topic")

	lister := &fakeLister{exist: map[string]bool{"exists-topic": true}}
	tc.check(context.Background(), lister, []string{"exists-topic"})

	if !tc.knownToExist("exists-topic") {
		t.Fatalf("expected exists-topic to be knownToExist after a successful check")
	}
	if missing := tc.missingTopics(); len(missing) != 0 {
		t.Fatalf("expected no missing topics after confirming existence, got %v", missing)
	}
}

func TestTopicChecker_Check_LeavesMissingTopicMissing(t *testing.T) {
	tc := newTopicChecker(discardLogger())
	tc.register("still-missing")

	lister := &fakeLister{exist: map[string]bool{}}
	tc.check(context.Background(), lister, []string{"still-missing"})

	if tc.knownToExist("still-missing") {
		t.Fatalf("expected still-missing to remain unknown after a check reporting it absent")
	}
	missing := tc.missingTopics()
	if len(missing) != 1 || missing[0] != "still-missing" {
		t.Fatalf("expected missingTopics()=[still-missing], got %v", missing)
	}
}

func TestTopicChecker_Check_NilListerIsNoop(t *testing.T) {
	tc := newTopicChecker(discardLogger())
	tc.register("t1")

	// Must not panic when no admin client has been built yet (component
	// just started, Update hasn't run).
	tc.check(context.Background(), nil, []string{"t1"})

	if tc.knownToExist("t1") {
		t.Fatalf("a nil lister must not be able to mark a topic as existing")
	}
}

func TestTopicChecker_MissingTopics_ExcludesConfirmedExisting(t *testing.T) {
	tc := newTopicChecker(discardLogger())
	tc.register("a")
	tc.register("b")

	lister := &fakeLister{exist: map[string]bool{"a": true}}
	tc.check(context.Background(), lister, []string{"a", "b"})

	missing := tc.missingTopics()
	if len(missing) != 1 || missing[0] != "b" {
		t.Fatalf("expected missingTopics()=[b], got %v", missing)
	}
}

// TestTopicChecker_Run_ChecksNewlyRegisteredTopicImmediately exercises run's
// dispatch loop end-to-end: registering a topic while run is active should
// get it checked (and, since the fake lister reports it as existing, cached)
// without waiting for the periodic sweep.
func TestTopicChecker_Run_ChecksNewlyRegisteredTopicImmediately(t *testing.T) {
	tc := newTopicChecker(discardLogger())
	lister := &fakeLister{exist: map[string]bool{"fast-topic": true}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tc.run(ctx, func() topicLister { return lister })

	tc.register("fast-topic")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tc.knownToExist("fast-topic") {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected run to check and cache fast-topic as existing within 2s")
}

func TestTopicChecker_Run_StopsOnContextCancel(t *testing.T) {
	tc := newTopicChecker(discardLogger())
	lister := &fakeLister{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		tc.run(ctx, func() topicLister { return lister })
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected run to return promptly after context cancellation")
	}
}
