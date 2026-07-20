package redpanda

import (
	"sort"
	"time"
)

// brokerInfo is the minimal input the allocator needs for one currently
// tracked broker.
type brokerInfo struct {
	id        string
	clusterID string
}

// reconcileAssignments computes the Assign/Unassign commands needed to bring
// the FSM's current assignments in line with the currently-tracked broker
// set and the currently-known voter list. It never revisits an existing,
// still-valid assignment for reasons unrelated to load — only brokers that
// are new, gone, or whose collector is no longer a voter get a proposal
// purely from that. This is what keeps a change to one Redpanda cluster (or
// one collector) from rippling into unrelated assignments: nothing here
// re-sorts or re-processes the full broker set from scratch.
//
// A collector that drops out of the voter set doesn't have its brokers
// reassigned immediately — see the danglingSince/gracePeriod handling below.
// After that, load balance across voters is treated as a standing
// invariant, re-checked on every call rather than only in response to a
// collector being freshly added — see rebalance.
//
// The grace period is skipped entirely for a collector in leaving (see
// votersRequestingRemoval): that signal means the departure is deliberate
// (a graceful scale-down via the preStop hook, not a crash that might come
// back), so waiting to see if the same name rejoins is pure downside —
// confirmed live, every scale-down left those brokers unscraped by anyone
// for the full grace period, since nothing here previously distinguished
// "gone for good" from "might come right back." leaving's brokers are
// reassigned on this same pass instead, landing on their new collector
// before the departing one has even finished shutting down (it's still
// blocked in its own preStop handler at this point — see redpanda.go's
// handleLeave), so the gap shrinks to at most a brief window of
// double-scraping rather than a guaranteed hole in collection.
//
// danglingSince tracks, per collector name, the first time it was observed
// referenced by an assignment but absent from voters; the caller owns and
// persists this map across calls (it's mutated in place) the same way it
// owns tracked/uuidCache. now is passed in rather than read internally so
// the grace period is deterministically testable.
func reconcileAssignments(
	now time.Time,
	current map[string]assignment,
	tracked map[string]brokerInfo,
	voters []string,
	danglingSince map[string]time.Time,
	gracePeriod time.Duration,
	leaving map[string]bool,
) []command {
	voterSet := make(map[string]bool, len(voters))
	for _, v := range voters {
		voterSet[v] = true
	}

	load := make(map[string]int, len(voters))
	for _, v := range voters {
		load[v] = 0
	}

	var cmds []command

	// Brokers that are no longer tracked (evicted after StaleTimeout) lose
	// their assignment outright. Nothing else about them matters anymore.
	//
	// Skipped entirely when tracked is completely empty: a real leader's own
	// c.tracked only reflects data from its own Update() calls, which run on
	// Alloy's normal component-graph schedule, independent of Raft
	// leadership. A replica that just became leader can call reconcile()
	// before its own first Update() has seen real upstream discovery data —
	// found live: a newly-elected leader with zero tracked pods proposed
	// Unassign for every single already-committed broker, not just the ones
	// that actually left, wiping every replica's exports fleet-wide for one
	// reconcile pass before the next Update() repopulated tracked and
	// everything was reassigned from scratch. If tracked is genuinely and
	// correctly empty (no brokers exist at all), current is already empty
	// too in steady state, so skipping changes nothing; the guard only ever
	// changes behavior in the transient-empty case this is guarding against.
	if len(tracked) > 0 {
		unassignedIDs := make([]string, 0)
		for brokerID := range current {
			if _, ok := tracked[brokerID]; !ok {
				unassignedIDs = append(unassignedIDs, brokerID)
			}
		}
		sort.Strings(unassignedIDs)
		for _, brokerID := range unassignedIDs {
			cmds = append(cmds, command{Op: opUnassign, BrokerID: brokerID})
		}
	}

	// Walk currently-tracked brokers in a fixed, deterministic order so that
	// which candidate ends up on which collector doesn't depend on map
	// iteration order.
	trackedIDs := make([]string, 0, len(tracked))
	for id := range tracked {
		trackedIDs = append(trackedIDs, id)
	}
	sort.Strings(trackedIDs)

	// A collector name is "dangling" if some still-tracked broker's
	// assignment points at it but it's no longer a voter — e.g. its replica
	// just restarted and got briefly removed as a voter (see
	// raftnode.go's reconcileMembership, which removes voters immediately
	// rather than delaying that: with only 2 voters, delaying removal would
	// block every write, not just this collector's own brokers, for the
	// whole grace period). Newly-dangling names start their grace period
	// now; names that stopped being dangling (voter again, or no broker
	// references them anymore) get forgotten.
	danglingNow := map[string]bool{}
	for _, brokerID := range trackedIDs {
		if a, has := current[brokerID]; has && !voterSet[a.Collector] {
			danglingNow[a.Collector] = true
		}
	}
	for name := range danglingSince {
		if !danglingNow[name] {
			delete(danglingSince, name)
		}
	}
	for name := range danglingNow {
		if _, ok := danglingSince[name]; !ok {
			danglingSince[name] = now
		}
	}
	dueForReassignment := func(name string) bool {
		if leaving[name] {
			return true
		}
		since, ok := danglingSince[name]
		return ok && !now.Before(since.Add(gracePeriod))
	}

	var orphans []brokerInfo // assigned to a collector that's no longer a voter, and past its grace period
	var newBrokers []brokerInfo
	assignedClusterCollector := make(map[string]map[string]bool) // clusterID -> set of collectors already holding a broker from it

	// effective tracks, broker by broker, which collector this pass
	// considers each tracked broker to currently be on — seeded from the
	// FSM's committed state, then kept in sync as this pass proposes its
	// own moves below, so the rebalance step at the end sees an accurate
	// picture instead of re-deriving decisions already made earlier in the
	// same pass. Brokers still within their collector's grace period are
	// left out entirely: not reassigned, not counted as load anywhere — if
	// the same-named collector returns before the grace period elapses,
	// they're silently valid again next pass with no command needed at all.
	effective := make(map[string]string, len(trackedIDs))

	markAssigned := func(clusterID, collector string) {
		if assignedClusterCollector[clusterID] == nil {
			assignedClusterCollector[clusterID] = make(map[string]bool)
		}
		assignedClusterCollector[clusterID][collector] = true
	}

	for _, brokerID := range trackedIDs {
		info := tracked[brokerID]
		a, has := current[brokerID]
		switch {
		case !has:
			newBrokers = append(newBrokers, info)
		case !voterSet[a.Collector]:
			if dueForReassignment(a.Collector) {
				orphans = append(orphans, info)
			}
		default:
			load[a.Collector]++
			effective[brokerID] = a.Collector
			markAssigned(info.clusterID, a.Collector)
		}
	}

	// pickCollector prefers the lowest-loaded voter that doesn't already
	// hold a broker from this cluster; falls back to plain lowest-loaded if
	// every voter already has one (more brokers in this cluster than
	// voters exist).
	pickCollector := func(clusterID string) string {
		used := assignedClusterCollector[clusterID]
		best, bestLoad := "", -1
		bestAny, bestAnyLoad := "", -1
		for _, v := range voters {
			if bestAny == "" || load[v] < bestAnyLoad {
				bestAny, bestAnyLoad = v, load[v]
			}
			if used != nil && used[v] {
				continue
			}
			if best == "" || load[v] < bestLoad {
				best, bestLoad = v, load[v]
			}
		}
		if best != "" {
			return best
		}
		return bestAny
	}

	assign := func(info brokerInfo) {
		collector := pickCollector(info.clusterID)
		if collector == "" {
			return // no voters at all yet; nothing to assign onto
		}
		cmds = append(cmds, command{Op: opAssign, BrokerID: info.id, ClusterID: info.clusterID, Collector: collector})
		load[collector]++
		effective[info.id] = collector
		markAssigned(info.clusterID, collector)
	}

	// Orphans first (forced by a collector leaving), then genuinely new
	// brokers — both use the same least-loaded selection.
	for _, info := range orphans {
		assign(info)
	}
	for _, info := range newBrokers {
		assign(info)
	}

	cmds = append(cmds, rebalance(voters, load, effective, tracked)...)

	return cmds
}

// rebalance evens out load across voters, moving one broker at a time from
// the currently most-loaded voter to the currently least-loaded until
// they're within 1 of each other (an even split when the total doesn't
// divide evenly) or no voter is more than 1 above another. It runs on every
// reconcile pass unconditionally, not just when a collector was just added:
// a real-world Raft group can only partially apply a batch of Assign
// commands (a 2-voter group in particular has zero fault tolerance — every
// write needs both voters' acks, so a single transient timeout mid-batch
// leaves the rest unapplied), and treating rebalancing as a one-shot action
// tied to a single event meant a partially-applied rebalance had no way to
// ever finish — every later pass saw the same voter set it already knew
// about and proposed nothing further. Checking the invariant on every pass
// instead makes it self-correcting regardless of why the imbalance arose.
//
// Each move is going to happen anyway to fix load, so which specific broker
// moves is chosen to also improve cluster affinity where possible, rather
// than fixing affinity as a separate follow-up pass: that would need its
// own extra moves to undo damage this pass could have just avoided causing
// in the first place, or to fix a collision this pass could have resolved
// for free while moving something off that collector regardless.
func rebalance(voters []string, load map[string]int, effective map[string]string, tracked map[string]brokerInfo) []command {
	if len(voters) < 2 {
		return nil
	}

	brokersOn := make(map[string][]string, len(voters))
	for brokerID, collector := range effective {
		brokersOn[collector] = append(brokersOn[collector], brokerID)
	}
	for _, ids := range brokersOn {
		sort.Strings(ids)
	}

	hasClusterOn := func(clusterID, collector string) bool {
		for _, id := range brokersOn[collector] {
			if tracked[id].clusterID == clusterID {
				return true
			}
		}
		return false
	}

	var cmds []command
	// Bounded by total tracked brokers: each iteration moves exactly one,
	// and no more moves than that could ever be needed to converge.
	for i := 0; i < len(tracked); i++ {
		most, least := mostAndLeastLoaded(voters, load)
		if most == "" || least == "" || load[most]-load[least] <= 1 {
			break
		}

		candidates := brokersOn[most]
		if len(candidates) == 0 {
			break
		}

		clusterCountOnMost := make(map[string]int, len(candidates))
		for _, id := range candidates {
			clusterCountOnMost[tracked[id].clusterID]++
		}
		pick := func(pred func(id string) bool) string {
			for _, id := range candidates {
				if pred(id) {
					return id
				}
			}
			return ""
		}

		// Prefer a broker whose cluster is duplicated on `most` (fixes an
		// existing collision there) and isn't already on `least` (doesn't
		// create a new one) — this move was happening regardless, so it
		// might as well be the one that also improves affinity. Falls back
		// to "just don't create a new collision," then to any broker if
		// every candidate would collide somewhere no matter what.
		brokerID := pick(func(id string) bool {
			k := tracked[id].clusterID
			return clusterCountOnMost[k] >= 2 && !hasClusterOn(k, least)
		})
		if brokerID == "" {
			brokerID = pick(func(id string) bool { return !hasClusterOn(tracked[id].clusterID, least) })
		}
		if brokerID == "" {
			brokerID = candidates[0]
		}
		info := tracked[brokerID]

		cmds = append(cmds, command{Op: opAssign, BrokerID: brokerID, ClusterID: info.clusterID, Collector: least})

		load[most]--
		load[least]++
		brokersOn[most] = removeString(brokersOn[most], brokerID)
		brokersOn[least] = append(brokersOn[least], brokerID)
		sort.Strings(brokersOn[least])
		effective[brokerID] = least
	}
	return cmds
}

// mostAndLeastLoaded returns the names of the currently most- and
// least-loaded voters, breaking ties by name so the result is deterministic.
func mostAndLeastLoaded(voters []string, load map[string]int) (most, least string) {
	for _, v := range voters {
		if most == "" || load[v] > load[most] || (load[v] == load[most] && v < most) {
			most = v
		}
		if least == "" || load[v] < load[least] || (load[v] == load[least] && v < least) {
			least = v
		}
	}
	return most, least
}

// removeString returns ss with target's first occurrence removed.
func removeString(ss []string, target string) []string {
	out := ss[:0]
	for _, s := range ss {
		if s != target {
			out = append(out, s)
		}
	}
	return out
}
