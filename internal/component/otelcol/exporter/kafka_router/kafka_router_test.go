package kafka_router

import (
	"context"
	"testing"
)

func TestChooseTarget_NoFallbackConfigured(t *testing.T) {
	tc := newTopicChecker(discardLogger())

	target, fallback := chooseTarget(tc, "primary-topic", "")

	if target != "primary-topic" || fallback != "" {
		t.Fatalf("expected (primary-topic, \"\"), got (%q, %q)", target, fallback)
	}
	if missing := tc.missingTopics(); len(missing) != 0 {
		t.Fatalf("a route with no fallback must never register a topic for checking, got %v", missing)
	}
}

func TestChooseTarget_UnconfirmedPrimary_RoutesToFallback(t *testing.T) {
	tc := newTopicChecker(discardLogger())

	target, fallback := chooseTarget(tc, "primary-topic", "fallback-topic")

	if target != "fallback-topic" {
		t.Fatalf("expected target=fallback-topic for an unconfirmed primary, got %q", target)
	}
	if fallback != "" {
		t.Fatalf("expected no further fallback once already routed to fallback, got %q", fallback)
	}
	missing := tc.missingTopics()
	if len(missing) != 1 || missing[0] != "primary-topic" {
		t.Fatalf("expected primary-topic to be registered for checking, got %v", missing)
	}
}

func TestChooseTarget_ConfirmedPrimary_RoutesToPrimary(t *testing.T) {
	tc := newTopicChecker(discardLogger())
	lister := &fakeLister{exist: map[string]bool{"primary-topic": true}}
	tc.register("primary-topic")
	tc.check(context.Background(), lister, []string{"primary-topic"})

	target, fallback := chooseTarget(tc, "primary-topic", "fallback-topic")

	if target != "primary-topic" || fallback != "fallback-topic" {
		t.Fatalf("expected (primary-topic, fallback-topic) once confirmed, got (%q, %q)", target, fallback)
	}
}

func TestChooseTarget_ConfirmedPrimary_DoesNotReRegister(t *testing.T) {
	tc := newTopicChecker(discardLogger())
	lister := &fakeLister{exist: map[string]bool{"primary-topic": true}}
	tc.register("primary-topic")
	<-tc.newTopics // drain the check request from register
	tc.check(context.Background(), lister, []string{"primary-topic"})

	chooseTarget(tc, "primary-topic", "fallback-topic")

	select {
	case topic := <-tc.newTopics:
		t.Fatalf("expected no re-check request for an already-confirmed topic, got one for %q", topic)
	default:
	}
}

// TestChooseTarget_SwitchesBackAfterTopicIsCreated exercises the full
// lifecycle chooseTarget exists for: a primary topic that doesn't exist yet
// routes to fallback and gets scheduled for checking; once an off-data-path
// check confirms it now exists, the very next call routes straight to it.
func TestChooseTarget_SwitchesBackAfterTopicIsCreated(t *testing.T) {
	tc := newTopicChecker(discardLogger())

	target, fallback := chooseTarget(tc, "primary-topic", "fallback-topic")
	if target != "fallback-topic" || fallback != "" {
		t.Fatalf("expected first call to route to fallback, got (%q, %q)", target, fallback)
	}

	// Simulate the background checker confirming the topic now exists.
	lister := &fakeLister{exist: map[string]bool{"primary-topic": true}}
	tc.check(context.Background(), lister, tc.missingTopics())

	target, fallback = chooseTarget(tc, "primary-topic", "fallback-topic")
	if target != "primary-topic" || fallback != "fallback-topic" {
		t.Fatalf("expected second call to route back to primary-topic, got (%q, %q)", target, fallback)
	}
}
