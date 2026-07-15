package redpanda

import (
	"sort"
	"testing"
)

func applyCommands(current map[string]assignment, cmds []command) map[string]assignment {
	out := make(map[string]assignment, len(current))
	for k, v := range current {
		out[k] = v
	}
	for _, cmd := range cmds {
		switch cmd.Op {
		case opAssign:
			out[cmd.BrokerID] = assignment{Collector: cmd.Collector, ClusterID: cmd.ClusterID}
		case opUnassign:
			delete(out, cmd.BrokerID)
		}
	}
	return out
}

func loadOf(assignments map[string]assignment) map[string]int {
	load := map[string]int{}
	for _, a := range assignments {
		load[a.Collector]++
	}
	return load
}

func TestReconcileAssignments_Stability(t *testing.T) {
	tracked := map[string]brokerInfo{
		"c0-b0": {id: "c0-b0", clusterID: "c0"},
		"c0-b1": {id: "c0-b1", clusterID: "c0"},
		"c1-b0": {id: "c1-b0", clusterID: "c1"},
	}
	voters := []string{"collector-0", "collector-1", "collector-2"}

	// First pass settles an assignment.
	cmds := reconcileAssignments(map[string]assignment{}, tracked, voters, nil)
	if len(cmds) != 3 {
		t.Fatalf("expected 3 assign commands on first pass, got %d: %+v", len(cmds), cmds)
	}
	settled := applyCommands(map[string]assignment{}, cmds)

	// Re-running with nothing changed must propose nothing at all.
	again := reconcileAssignments(settled, tracked, voters, nil)
	if len(again) != 0 {
		t.Fatalf("expected no commands when nothing changed, got %+v", again)
	}
}

func TestReconcileAssignments_NewBrokerDoesNotDisturbOthers(t *testing.T) {
	tracked := map[string]brokerInfo{
		"c0-b0": {id: "c0-b0", clusterID: "c0"},
		"c1-b0": {id: "c1-b0", clusterID: "c1"},
		"c2-b0": {id: "c2-b0", clusterID: "c2"},
	}
	voters := []string{"collector-0", "collector-1", "collector-2"}

	settled := applyCommands(map[string]assignment{}, reconcileAssignments(map[string]assignment{}, tracked, voters, nil))

	// Add one new broker to an unrelated cluster.
	trackedPlus := map[string]brokerInfo{}
	for k, v := range tracked {
		trackedPlus[k] = v
	}
	trackedPlus["c3-b0"] = brokerInfo{id: "c3-b0", clusterID: "c3"}

	cmds := reconcileAssignments(settled, trackedPlus, voters, nil)

	for _, cmd := range cmds {
		if cmd.BrokerID != "c3-b0" {
			t.Fatalf("expected only the new broker to get a proposal, but got a command for %q: %+v", cmd.BrokerID, cmd)
		}
	}
	if len(cmds) != 1 {
		t.Fatalf("expected exactly 1 command (the new broker), got %d: %+v", len(cmds), cmds)
	}
}

func TestReconcileAssignments_CollectorRemovalOnlyMovesOrphans(t *testing.T) {
	tracked := map[string]brokerInfo{}
	for i := 0; i < 9; i++ {
		id := sprintfID(i)
		tracked[id] = brokerInfo{id: id, clusterID: id} // distinct cluster per broker to avoid same-cluster interference
	}
	voters := []string{"collector-0", "collector-1", "collector-2"}
	settled := applyCommands(map[string]assignment{}, reconcileAssignments(map[string]assignment{}, tracked, voters, nil))

	// Find which brokers were on collector-1, which we'll remove.
	var onCollector1 []string
	for id, a := range settled {
		if a.Collector == "collector-1" {
			onCollector1 = append(onCollector1, id)
		}
	}
	if len(onCollector1) == 0 {
		t.Fatalf("expected at least one broker on collector-1 in this fixture")
	}

	remainingVoters := []string{"collector-0", "collector-2"}
	cmds := reconcileAssignments(settled, tracked, remainingVoters, nil)

	movedIDs := map[string]bool{}
	for _, cmd := range cmds {
		if cmd.Op != opAssign {
			t.Fatalf("unexpected non-assign command after collector removal: %+v", cmd)
		}
		movedIDs[cmd.BrokerID] = true
		if cmd.Collector == "collector-1" {
			t.Fatalf("broker %q was reassigned onto the removed collector", cmd.BrokerID)
		}
	}

	for _, id := range onCollector1 {
		if !movedIDs[id] {
			t.Fatalf("expected orphaned broker %q to be reassigned", id)
		}
	}
	for id, a := range settled {
		if a.Collector != "collector-1" && movedIDs[id] {
			t.Fatalf("broker %q was not an orphan but got reassigned anyway", id)
		}
	}
}

func TestReconcileAssignments_NewCollectorGetsDeliberateRebalance(t *testing.T) {
	tracked := map[string]brokerInfo{}
	for i := 0; i < 9; i++ {
		id := sprintfID(i)
		tracked[id] = brokerInfo{id: id, clusterID: id}
	}
	voters := []string{"collector-0", "collector-1", "collector-2"}
	settled := applyCommands(map[string]assignment{}, reconcileAssignments(map[string]assignment{}, tracked, voters, nil))

	loadBefore := loadOf(settled)
	for _, v := range voters {
		if loadBefore[v] != 3 {
			t.Fatalf("expected perfectly even 3/3/3 split in fixture, got %+v", loadBefore)
		}
	}

	newVoters := append(append([]string{}, voters...), "collector-3")
	cmds := reconcileAssignments(settled, tracked, newVoters, []string{"collector-3"})

	movedOntoNew := 0
	for _, cmd := range cmds {
		if cmd.Collector == "collector-3" {
			movedOntoNew++
		}
	}
	if movedOntoNew == 0 {
		t.Fatalf("expected the new collector to receive at least one broker via deliberate rebalance, got %+v", cmds)
	}

	after := applyCommands(settled, cmds)
	loadAfter := loadOf(after)
	if loadAfter["collector-3"] == 0 {
		t.Fatalf("collector-3 still has zero load after rebalance: %+v", loadAfter)
	}
}

func TestReconcileAssignments_BrokerEvictionOnlyUnassignsThatBroker(t *testing.T) {
	tracked := map[string]brokerInfo{
		"c0-b0": {id: "c0-b0", clusterID: "c0"},
		"c1-b0": {id: "c1-b0", clusterID: "c1"},
	}
	voters := []string{"collector-0", "collector-1"}
	settled := applyCommands(map[string]assignment{}, reconcileAssignments(map[string]assignment{}, tracked, voters, nil))

	trackedMinus := map[string]brokerInfo{"c1-b0": tracked["c1-b0"]}
	cmds := reconcileAssignments(settled, trackedMinus, voters, nil)

	if len(cmds) != 1 || cmds[0].Op != opUnassign || cmds[0].BrokerID != "c0-b0" {
		t.Fatalf("expected exactly one Unassign for c0-b0, got %+v", cmds)
	}
}

func TestReconcileAssignments_PrefersDistinctCollectorForSameCluster(t *testing.T) {
	tracked := map[string]brokerInfo{
		"b0": {id: "b0", clusterID: "same-cluster"},
		"b1": {id: "b1", clusterID: "same-cluster"},
		"b2": {id: "b2", clusterID: "same-cluster"},
	}
	voters := []string{"collector-0", "collector-1", "collector-2"}

	cmds := reconcileAssignments(map[string]assignment{}, tracked, voters, nil)
	settled := applyCommands(map[string]assignment{}, cmds)

	seen := map[string]bool{}
	for _, a := range settled {
		if seen[a.Collector] {
			t.Fatalf("two brokers from the same cluster landed on collector %q, when %d distinct collectors were available: %+v", a.Collector, len(voters), settled)
		}
		seen[a.Collector] = true
	}
}

func sprintfID(i int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	return "broker-" + string(letters[i%len(letters)])
}

func TestReconcileAssignments_DeterministicOrdering(t *testing.T) {
	tracked := map[string]brokerInfo{
		"b0": {id: "b0", clusterID: "c0"},
		"b1": {id: "b1", clusterID: "c1"},
		"b2": {id: "b2", clusterID: "c2"},
	}
	voters := []string{"collector-0", "collector-1", "collector-2"}

	first := reconcileAssignments(map[string]assignment{}, tracked, voters, nil)
	second := reconcileAssignments(map[string]assignment{}, tracked, voters, nil)

	sortCmds := func(cmds []command) {
		sort.Slice(cmds, func(i, j int) bool { return cmds[i].BrokerID < cmds[j].BrokerID })
	}
	sortCmds(first)
	sortCmds(second)

	if len(first) != len(second) {
		t.Fatalf("non-deterministic result: %+v vs %+v", first, second)
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("non-deterministic result at index %d: %+v vs %+v", i, first[i], second[i])
		}
	}
}
