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
