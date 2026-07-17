package redpanda

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

// reconcileImmediate calls reconcileAssignments with a zero grace period, so
// any collector no longer in voters has its brokers reassigned right away —
// the behavior every test below other than the grace-period-specific ones
// wants, and the behavior reconcileAssignments had before danglingSince and
// gracePeriod existed at all.
func reconcileImmediate(current map[string]assignment, tracked map[string]brokerInfo, voters []string) []command {
	return reconcileAssignments(time.Now(), current, tracked, voters, map[string]time.Time{}, 0)
}

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
	cmds := reconcileImmediate(map[string]assignment{}, tracked, voters)
	if len(cmds) != 3 {
		t.Fatalf("expected 3 assign commands on first pass, got %d: %+v", len(cmds), cmds)
	}
	settled := applyCommands(map[string]assignment{}, cmds)

	// Re-running with nothing changed must propose nothing at all.
	again := reconcileImmediate(settled, tracked, voters)
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

	settled := applyCommands(map[string]assignment{}, reconcileImmediate(map[string]assignment{}, tracked, voters))

	// Add one new broker to an unrelated cluster.
	trackedPlus := map[string]brokerInfo{}
	for k, v := range tracked {
		trackedPlus[k] = v
	}
	trackedPlus["c3-b0"] = brokerInfo{id: "c3-b0", clusterID: "c3"}

	cmds := reconcileImmediate(settled, trackedPlus, voters)

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
	settled := applyCommands(map[string]assignment{}, reconcileImmediate(map[string]assignment{}, tracked, voters))

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
	cmds := reconcileImmediate(settled, tracked, remainingVoters)

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
	settled := applyCommands(map[string]assignment{}, reconcileImmediate(map[string]assignment{}, tracked, voters))

	loadBefore := loadOf(settled)
	for _, v := range voters {
		if loadBefore[v] != 3 {
			t.Fatalf("expected perfectly even 3/3/3 split in fixture, got %+v", loadBefore)
		}
	}

	newVoters := append(append([]string{}, voters...), "collector-3")
	cmds := reconcileImmediate(settled, tracked, newVoters)

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
	settled := applyCommands(map[string]assignment{}, reconcileImmediate(map[string]assignment{}, tracked, voters))

	trackedMinus := map[string]brokerInfo{"c1-b0": tracked["c1-b0"]}
	cmds := reconcileImmediate(settled, trackedMinus, voters)

	if len(cmds) != 1 || cmds[0].Op != opUnassign || cmds[0].BrokerID != "c0-b0" {
		t.Fatalf("expected exactly one Unassign for c0-b0, got %+v", cmds)
	}
}

// TestReconcileAssignments_EmptyTrackedNeverEvictsEverything is a regression
// test for a real observation from live testing: a newly-elected leader's
// own c.tracked reflects only its own Update() calls, which run on Alloy's
// normal component-graph schedule — entirely independent of when it becomes
// Raft leader. A leader can call reconcile() before its first Update() has
// ever seen real upstream discovery data, so tracked can be transiently
// empty despite every broker still genuinely existing. Treating that as
// "every broker is gone" wiped every replica's exports fleet-wide for one
// reconcile pass, not just whatever actually changed.
func TestReconcileAssignments_EmptyTrackedNeverEvictsEverything(t *testing.T) {
	tracked := map[string]brokerInfo{
		"c0-b0": {id: "c0-b0", clusterID: "c0"},
		"c1-b0": {id: "c1-b0", clusterID: "c1"},
	}
	voters := []string{"collector-0", "collector-1"}
	settled := applyCommands(map[string]assignment{}, reconcileImmediate(map[string]assignment{}, tracked, voters))

	cmds := reconcileImmediate(settled, map[string]brokerInfo{}, voters)
	if len(cmds) != 0 {
		t.Fatalf("expected no commands when tracked is empty, got %+v", cmds)
	}

	// Once tracked is genuinely repopulated (even with the exact same
	// brokers), nothing should have changed at all — no churn from the
	// empty pass in between.
	cmds = reconcileImmediate(settled, tracked, voters)
	if len(cmds) != 0 {
		t.Fatalf("expected no commands once tracked is repopulated with the same brokers, got %+v", cmds)
	}
}

func TestReconcileAssignments_PrefersDistinctCollectorForSameCluster(t *testing.T) {
	tracked := map[string]brokerInfo{
		"b0": {id: "b0", clusterID: "same-cluster"},
		"b1": {id: "b1", clusterID: "same-cluster"},
		"b2": {id: "b2", clusterID: "same-cluster"},
	}
	voters := []string{"collector-0", "collector-1", "collector-2"}

	cmds := reconcileImmediate(map[string]assignment{}, tracked, voters)
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

// assertNoCollisions fails the test if any collector holds two or more
// brokers from the same cluster.
func assertNoCollisions(t *testing.T, assignments map[string]assignment, tracked map[string]brokerInfo) {
	t.Helper()
	counts := map[string]map[string]int{}
	for id, a := range assignments {
		info := tracked[id]
		if counts[a.Collector] == nil {
			counts[a.Collector] = map[string]int{}
		}
		counts[a.Collector][info.clusterID]++
	}
	for collector, byCluster := range counts {
		for cluster, n := range byCluster {
			if n > 1 {
				t.Fatalf("collector %q holds %d brokers from cluster %q: %+v", collector, n, cluster, assignments)
			}
		}
	}
}

// TestReconcileAssignments_RejoinFixesCollisionsCausedByTemporaryRemoval is a
// regression test for a real observation from live testing: with 3 voters
// and 3 clusters of 3 brokers each cleanly spread one-per-collector, briefly
// losing one voter forces its orphans onto the remaining two — with only two
// candidates for three different clusters, at least one collision (a
// collector holding two brokers from the same cluster) is unavoidable at
// that moment. When the voter rejoins, rebalance() must not just restore
// 3/3/3 load balance — it should also un-do the collisions that arose only
// because of the temporary removal, since fixing them costs nothing extra:
// the brokers moving to bring the rejoined voter back up to a fair share are
// already in flight regardless of which specific ones they are.
func TestReconcileAssignments_RejoinFixesCollisionsCausedByTemporaryRemoval(t *testing.T) {
	tracked := map[string]brokerInfo{}
	clusters := []string{"cluster-a", "cluster-b", "cluster-c"}
	for _, c := range clusters {
		for i := 0; i < 3; i++ {
			id := fmt.Sprintf("%s-%d", c, i)
			tracked[id] = brokerInfo{id: id, clusterID: c}
		}
	}
	voters := []string{"collector-0", "collector-1", "collector-2"}
	settled := applyCommands(map[string]assignment{}, reconcileImmediate(map[string]assignment{}, tracked, voters))
	assertNoCollisions(t, settled, tracked)

	// collector-1 drops out: its orphans are forced onto the remaining two.
	remainingVoters := []string{"collector-0", "collector-2"}
	afterRemoval := applyCommands(settled, reconcileImmediate(settled, tracked, remainingVoters))

	// collector-1 rejoins.
	cmds := reconcileImmediate(afterRemoval, tracked, voters)
	final := applyCommands(afterRemoval, cmds)

	assertNoCollisions(t, final, tracked)

	load := loadOf(final)
	for _, v := range voters {
		if load[v] != 3 {
			t.Fatalf("expected 3/3/3 after rejoin, got %+v", load)
		}
	}
}

// TestReconcileAssignments_GracePeriodDelaysReassignment verifies a
// collector that drops out of the voter set doesn't have its brokers
// reassigned until its grace period actually elapses.
func TestReconcileAssignments_GracePeriodDelaysReassignment(t *testing.T) {
	tracked := map[string]brokerInfo{
		"b0": {id: "b0", clusterID: "c0"},
	}
	voters := []string{"collector-0", "collector-1"}
	danglingSince := map[string]time.Time{}
	const grace = 60 * time.Second
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	settled := applyCommands(map[string]assignment{}, reconcileAssignments(base, map[string]assignment{}, tracked, voters, danglingSince, grace))
	originalCollector := settled["b0"].Collector
	if originalCollector == "" {
		t.Fatalf("expected b0 to be assigned in the initial pass")
	}

	var remainingVoters []string
	for _, v := range voters {
		if v != originalCollector {
			remainingVoters = append(remainingVoters, v)
		}
	}

	// Immediately after removal: still within grace, must not be reassigned.
	cmds := reconcileAssignments(base.Add(1*time.Second), settled, tracked, remainingVoters, danglingSince, grace)
	if len(cmds) != 0 {
		t.Fatalf("expected no reassignment within the grace period, got %+v", cmds)
	}

	// Later, but still short of the grace period.
	cmds = reconcileAssignments(base.Add(grace-time.Second), settled, tracked, remainingVoters, danglingSince, grace)
	if len(cmds) != 0 {
		t.Fatalf("expected no reassignment just before the grace period elapses, got %+v", cmds)
	}

	// Past the grace period: now it should move to the sole remaining voter.
	cmds = reconcileAssignments(base.Add(grace+time.Second), settled, tracked, remainingVoters, danglingSince, grace)
	if len(cmds) != 1 || cmds[0].Op != opAssign || cmds[0].BrokerID != "b0" || cmds[0].Collector != remainingVoters[0] {
		t.Fatalf("expected b0 reassigned to %q after the grace period, got %+v", remainingVoters[0], cmds)
	}
}

// TestReconcileAssignments_CollectorReturnsWithinGraceKeepsOriginalAssignment
// is a regression test for the goal behind the grace period at all: if the
// same-named collector rejoins before its brokers' grace period elapses,
// nothing should ever be proposed for them — they were never touched, so
// they're simply valid again the moment voterSet contains that name once
// more.
func TestReconcileAssignments_CollectorReturnsWithinGraceKeepsOriginalAssignment(t *testing.T) {
	tracked := map[string]brokerInfo{
		"b0": {id: "b0", clusterID: "c0"},
	}
	voters := []string{"collector-0", "collector-1"}
	danglingSince := map[string]time.Time{}
	const grace = 60 * time.Second
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	settled := applyCommands(map[string]assignment{}, reconcileAssignments(base, map[string]assignment{}, tracked, voters, danglingSince, grace))
	originalCollector := settled["b0"].Collector

	var remainingVoters []string
	for _, v := range voters {
		if v != originalCollector {
			remainingVoters = append(remainingVoters, v)
		}
	}

	// Drops out, then rejoins well within the grace period.
	afterRemoval := applyCommands(settled, reconcileAssignments(base.Add(5*time.Second), settled, tracked, remainingVoters, danglingSince, grace))
	if afterRemoval["b0"].Collector != originalCollector {
		t.Fatalf("expected b0 to remain on %q untouched while within grace, got %+v", originalCollector, afterRemoval)
	}

	cmds := reconcileAssignments(base.Add(10*time.Second), afterRemoval, tracked, voters, danglingSince, grace)
	if len(cmds) != 0 {
		t.Fatalf("expected zero commands when the same collector rejoins within its grace period, got %+v", cmds)
	}

	final := applyCommands(afterRemoval, cmds)
	if final["b0"].Collector != originalCollector {
		t.Fatalf("expected b0 still on its original collector %q, got %+v", originalCollector, final)
	}
}

// TestReconcileAssignments_ImbalanceSelfHealsWithoutANewAddEvent is a
// regression test for the "8 brokers on one collector, 1 on the other"
// observation from live testing: a deliberate rebalance is only proposed
// inside the same reconcile pass a collector is freshly added
// (addedCollectors). In production, a 2-voter Raft group has zero fault
// tolerance (every write needs both voters' acks), so if only some of a
// rebalance's Assign commands actually commit — the rest timing out because
// the newly-rejoined voter's connection wasn't fully up yet — nothing ever
// retries the remainder: every later pass sees the same two voters it
// already knew about, so addedCollectors is empty forever after and the
// partial rebalance freezes in place. Load balancing must be an ongoing
// invariant checked on every pass, not a one-shot action tied to a single
// event.
func TestReconcileAssignments_ImbalanceSelfHealsWithoutANewAddEvent(t *testing.T) {
	tracked := map[string]brokerInfo{}
	for i := 0; i < 9; i++ {
		id := sprintfID(i)
		tracked[id] = brokerInfo{id: id, clusterID: id}
	}
	voters := []string{"collector-0", "collector-1"}

	// Simulate a rebalance onto collector-1 that only partially committed:
	// 8 brokers still on collector-0, only 1 made it to collector-1.
	lopsided := map[string]assignment{}
	i := 0
	for id := range tracked {
		collector := "collector-0"
		if i == 0 {
			collector = "collector-1"
		}
		lopsided[id] = assignment{Collector: collector, ClusterID: tracked[id].clusterID}
		i++
	}

	// No new collector in this pass — voters are exactly what they were
	// last pass. Under the old one-shot-on-add design this proposed
	// nothing at all; imbalance must still be corrected.
	cmds := reconcileImmediate(lopsided, tracked, voters)
	if len(cmds) == 0 {
		t.Fatalf("expected a self-correcting rebalance even with no newly-added collector, got no commands")
	}

	after := applyCommands(lopsided, cmds)
	load := loadOf(after)
	diff := load["collector-0"] - load["collector-1"]
	if diff < 0 {
		diff = -diff
	}
	if diff > 1 {
		t.Fatalf("expected load to converge to within 1 of each other, got %+v", load)
	}
}

func TestReconcileAssignments_DeterministicOrdering(t *testing.T) {
	tracked := map[string]brokerInfo{
		"b0": {id: "b0", clusterID: "c0"},
		"b1": {id: "b1", clusterID: "c1"},
		"b2": {id: "b2", clusterID: "c2"},
	}
	voters := []string{"collector-0", "collector-1", "collector-2"}

	first := reconcileImmediate(map[string]assignment{}, tracked, voters)
	second := reconcileImmediate(map[string]assignment{}, tracked, voters)

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
