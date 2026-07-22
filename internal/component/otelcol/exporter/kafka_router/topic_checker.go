package kafka_router

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// topicCheckInterval is how often previously-missing topics are re-checked.
const topicCheckInterval = time.Minute

// topicLister is the subset of *kadm.Client's API topicChecker needs. Letting
// tests substitute a fake avoids spinning up a real (or fake) broker just to
// exercise the cache's state transitions. *kadm.Client already satisfies this.
type topicLister interface {
	ListTopics(ctx context.Context, topics ...string) (kadm.TopicDetails, error)
}

// topicChecker tracks whether topics used as a route's primary target
// actually exist, so routeAndProduce can go straight to the fallback topic
// for a topic known to be missing instead of discovering that via a failed
// produce (which pays franz-go's unknown-topic wait on every single call).
//
// A topic defaults to "missing" the moment it's first seen and stays that
// way until an explicit existence check (issued off the data path) proves
// otherwise. Once a topic is confirmed to exist it is not re-checked —
// per-cluster topics in this component are provisioned once and not deleted.
type topicChecker struct {
	logger *slog.Logger

	mu     sync.RWMutex
	exists map[string]bool

	newTopics chan string
}

func newTopicChecker(logger *slog.Logger) *topicChecker {
	return &topicChecker{
		logger:    logger,
		exists:    make(map[string]bool),
		newTopics: make(chan string, 64),
	}
}

// knownToExist reports whether topic has been confirmed to exist.
func (t *topicChecker) knownToExist(topic string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.exists[topic]
}

// register starts tracking topic if this is the first time it's been seen,
// and requests an immediate off-data-path existence check. Cheap to call on
// every produce; only the first call for a given topic does any extra work.
func (t *topicChecker) register(topic string) {
	t.mu.Lock()
	_, tracked := t.exists[topic]
	if !tracked {
		t.exists[topic] = false
	}
	t.mu.Unlock()

	if !tracked {
		select {
		case t.newTopics <- topic:
		default:
			// Checker is busy; the next periodic sweep will pick this up.
		}
	}
}

// missingTopics returns all topics currently tracked as not existing.
func (t *topicChecker) missingTopics() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	missing := make([]string, 0, len(t.exists))
	for topic, ok := range t.exists {
		if !ok {
			missing = append(missing, topic)
		}
	}
	return missing
}

// run periodically re-checks missing topics and immediately checks newly
// registered ones, until ctx is done. getAdm returns the current admin
// client; called fresh per check so a client rebuilt by Update is picked up.
func (t *topicChecker) run(ctx context.Context, getAdm func() topicLister) {
	ticker := time.NewTicker(topicCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case topic := <-t.newTopics:
			t.check(ctx, getAdm(), []string{topic})
		case <-ticker.C:
			if missing := t.missingTopics(); len(missing) > 0 {
				t.check(ctx, getAdm(), missing)
			}
		}
	}
}

// check queries the broker for topics and updates the cache. Topics found to
// now exist are logged once, since that flips routeAndProduce back onto the
// primary topic.
func (t *topicChecker) check(ctx context.Context, adm topicLister, topics []string) {
	if adm == nil {
		return
	}
	details, err := adm.ListTopics(ctx, topics...)
	if err != nil {
		t.logger.Warn("checking topic existence failed", "err", err)
		return
	}
	for _, topic := range topics {
		d, ok := details[topic]
		existsNow := ok && d.Err == nil

		t.mu.Lock()
		wasExists := t.exists[topic]
		t.exists[topic] = existsNow
		t.mu.Unlock()

		if existsNow && !wasExists {
			t.logger.Info("primary topic now exists, switching back to it", "topic", topic)
		}
	}
}
