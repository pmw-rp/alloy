package redpanda

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/grafana/ckit/peer"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

const (
	raftTimeout    = 10 * time.Second
	raftMaxPool    = 3
	snapshotRetain = 2
)

// raftNode wraps the single Raft peer embedded inside this component
// instance. Every Alloy collector replica runs one discovery.redpanda
// instance, so one raftNode per replica, all part of the same Raft group.
type raftNode struct {
	raft *raft.Raft
	fsm  *fsm
	self string // this node's own peer name, from cluster.Peers()
}

// newRaftNode constructs the embedded Raft peer, bootstrapping a fresh
// single-voter cluster if (and only if) this node has no prior Raft state
// and is the deterministic bootstrapper among currently-visible peers.
//
// selfName/selfHost identify this node; raftBindPort is a TCP port dedicated
// to Raft RPC, separate from Alloy's own gossip port (peer.Peer.Addr is the
// gossip address, not usable directly here).
func newRaftNode(dataDir, selfName, selfHost string, raftBindPort int, peers []peer.Peer, logOutput io.Writer) (*raftNode, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating raft data dir: %w", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("opening raft log store: %w", err)
	}

	snapshotDir := filepath.Join(dataDir, "snapshots")
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating raft snapshot dir: %w", err)
	}
	snapshots, err := raft.NewFileSnapshotStore(snapshotDir, snapshotRetain, logOutput)
	if err != nil {
		return nil, fmt.Errorf("opening raft snapshot store: %w", err)
	}

	bindAddr := fmt.Sprintf("0.0.0.0:%d", raftBindPort)
	advertiseAddr := fmt.Sprintf("%s:%d", selfHost, raftBindPort)
	advertise, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("resolving raft advertise address %q: %w", advertiseAddr, err)
	}
	transport, err := raft.NewTCPTransport(bindAddr, advertise, raftMaxPool, raftTimeout, logOutput)
	if err != nil {
		return nil, fmt.Errorf("creating raft transport: %w", err)
	}

	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(selfName)

	// Bootstrap must happen before raft.NewRaft, and only from exactly one
	// node with an empty log — see the package doc comment for why every
	// node calling BootstrapCluster independently would split-brain rather
	// than safely no-op.
	hasState, err := raft.HasExistingState(logStore, logStore, snapshots)
	if err != nil {
		return nil, fmt.Errorf("checking for existing raft state: %w", err)
	}
	if !hasState && isDeterministicBootstrapper(selfName, peers) {
		bootstrapConf := raft.Configuration{
			Servers: []raft.Server{{
				ID:      raft.ServerID(selfName),
				Address: transport.LocalAddr(),
			}},
		}
		if err := raft.BootstrapCluster(conf, logStore, logStore, snapshots, transport, bootstrapConf); err != nil {
			return nil, fmt.Errorf("bootstrapping raft cluster: %w", err)
		}
	}

	f := newFSM()
	r, err := raft.NewRaft(conf, f, logStore, logStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("starting raft: %w", err)
	}

	return &raftNode{raft: r, fsm: f, self: selfName}, nil
}

// isDeterministicBootstrapper reports whether selfName sorts first among the
// names of currently-visible peers — the one node allowed to bootstrap a
// fresh cluster. Every other node waits to be added as a voter instead.
func isDeterministicBootstrapper(selfName string, peers []peer.Peer) bool {
	if len(peers) == 0 {
		return true
	}
	names := make([]string, 0, len(peers))
	for _, p := range peers {
		names = append(names, p.Name)
	}
	sort.Strings(names)
	return names[0] == selfName
}

func (n *raftNode) isLeader() bool {
	return n.raft.State() == raft.Leader
}

func (n *raftNode) propose(cmd command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("encoding command: %w", err)
	}
	return n.raft.Apply(data, raftTimeout).Error()
}

// currentVoters returns the names of the current Raft voters.
func (n *raftNode) currentVoters() ([]string, error) {
	future := n.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("getting raft configuration: %w", err)
	}
	config := future.Configuration()
	voters := make([]string, 0, len(config.Servers))
	for _, srv := range config.Servers {
		voters = append(voters, string(srv.ID))
	}
	return voters, nil
}

// reconcileMembership adds any gossip peer that isn't yet a Raft voter, and
// removes any Raft voter that's no longer a gossip peer. AddVoter/RemoveServer
// are only accepted when called on the current leader — Raft forwards
// configuration changes through the leader's log — so this is a no-op on
// followers by construction, not something this function needs to check
// itself beyond the isLeader() guard.
func (n *raftNode) reconcileMembership(peers []peer.Peer, raftAddr func(peer.Peer) raft.ServerAddress) error {
	if !n.isLeader() {
		return nil
	}

	future := n.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return fmt.Errorf("getting raft configuration: %w", err)
	}
	config := future.Configuration()

	currentVoters := make(map[raft.ServerID]bool, len(config.Servers))
	for _, srv := range config.Servers {
		currentVoters[srv.ID] = true
	}

	gossipPeers := make(map[raft.ServerID]bool, len(peers))
	for _, p := range peers {
		gossipPeers[raft.ServerID(p.Name)] = true
	}

	for _, p := range peers {
		id := raft.ServerID(p.Name)
		if currentVoters[id] {
			continue
		}
		if err := n.raft.AddVoter(id, raftAddr(p), 0, raftTimeout).Error(); err != nil {
			return fmt.Errorf("adding voter %s: %w", p.Name, err)
		}
	}
	for _, srv := range config.Servers {
		if gossipPeers[srv.ID] {
			continue
		}
		if err := n.raft.RemoveServer(srv.ID, 0, raftTimeout).Error(); err != nil {
			return fmt.Errorf("removing server %s: %w", srv.ID, err)
		}
	}
	return nil
}

// raftAddrFor derives a peer's Raft RPC address from its gossip address's
// host plus this fleet's configured raft bind port. Every replica in a
// rollout shares the same .alloy config, so the port is uniform across the
// fleet even though each peer's host differs.
func raftAddrFor(p peer.Peer, raftBindPort int) raft.ServerAddress {
	host, _, err := net.SplitHostPort(p.Addr)
	if err != nil {
		host = p.Addr
	}
	return raft.ServerAddress(fmt.Sprintf("%s:%d", host, raftBindPort))
}

// diffAdded returns the names present in after but not before.
func diffAdded(before, after []string) []string {
	beforeSet := make(map[string]bool, len(before))
	for _, b := range before {
		beforeSet[b] = true
	}
	var added []string
	for _, a := range after {
		if !beforeSet[a] {
			added = append(added, a)
		}
	}
	return added
}
