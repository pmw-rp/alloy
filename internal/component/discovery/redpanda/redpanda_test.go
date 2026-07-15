package redpanda

import (
	"io"
	"log/slog"
	"testing"

	"github.com/grafana/ckit/peer"
	"github.com/grafana/ckit/shard"

	"github.com/grafana/alloy/internal/component"
)

type fakeCluster struct {
	ready bool
	peers []peer.Peer
}

func (f fakeCluster) Lookup(_ shard.Key, _ int, _ shard.Op) ([]peer.Peer, error) { return f.peers, nil }
func (f fakeCluster) Peers() []peer.Peer                                         { return f.peers }
func (f fakeCluster) Ready() bool                                                { return f.ready }

func TestPublishTargets_NotReadyPublishesNothing(t *testing.T) {
	var published *Exports
	c := &Component{
		opts: component.Options{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			OnStateChange: func(e component.Exports) {
				exp := e.(Exports)
				published = &exp
			},
		},
		cluster:   fakeCluster{ready: false, peers: []peer.Peer{{Name: "self", Self: true}}},
		uuidCache: map[string]string{},
		tracked:   map[string]trackedPod{},
	}

	c.publishTargets()

	if published == nil {
		t.Fatalf("expected OnStateChange to be called even when not ready")
	}
	if len(published.Targets) != 0 {
		t.Fatalf("expected no targets while cluster is not ready, got %+v", published.Targets)
	}
}

// TestNew_SucceedsBeforeSelfAppearsInPeers is a regression test for a real
// crash: New() used to look itself up in cluster.Peers() synchronously and
// fail the whole component build if it wasn't there yet. But the cluster
// service only registers this node as a peer once its own Run() has
// started the underlying gossip node — a separate lifecycle phase with no
// ordering guarantee relative to component construction. On perfectly
// ordinary startup timing (not just misconfiguration), New() could see an
// empty peer list and crash-loop the component. New() must succeed
// regardless; only Run() is allowed to wait.
func TestNew_SucceedsBeforeSelfAppearsInPeers(t *testing.T) {
	var published *Exports
	opts := component.Options{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnStateChange: func(e component.Exports) {
			exp := e.(Exports)
			published = &exp
		},
		GetServiceData: func(name string) (any, error) {
			return fakeCluster{ready: true, peers: nil}, nil // no peers yet, not even self
		},
	}

	c, err := New(opts, Arguments{})
	if err != nil {
		t.Fatalf("New() must not fail just because self isn't in cluster.Peers() yet, got: %v", err)
	}
	if c == nil {
		t.Fatalf("expected a non-nil component")
	}
	if published == nil {
		t.Fatalf("expected Update (called from New) to publish something, even if empty")
	}
	if len(published.Targets) != 0 {
		t.Fatalf("expected no targets before the raft node exists, got %+v", published.Targets)
	}
}
