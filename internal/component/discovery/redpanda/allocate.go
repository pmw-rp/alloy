package redpanda

import "sort"

// brokerInfo is the minimal input the allocator needs for one currently
// tracked broker.
type brokerInfo struct {
	id        string
	clusterID string
}

// reconcileAssignments computes the Assign/Unassign commands needed to bring
// the FSM's current assignments in line with the currently-tracked broker
// set and the currently-known voter list. It never revisits an existing,
// still-valid assignment — only brokers that are new, gone, or whose
// collector is no longer a voter get a proposal. This is what keeps a
// change to one Redpanda cluster (or one collector) from rippling into
// unrelated assignments: nothing here re-sorts or re-processes the full
// broker set from scratch.
//
// addedCollectors lists collectors that just became voters in this same
// reconcile pass (if any) — each gets an explicit, bounded rebalance pass
// (see rebalanceOnto) so it actually receives load rather than sitting idle
// until organic churn happens to land brokers on it.
func reconcileAssignments(
	current map[string]assignment,
	tracked map[string]brokerInfo,
	voters []string,
	addedCollectors []string,
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

	// Walk currently-tracked brokers in a fixed, deterministic order so that
	// which candidate ends up on which collector doesn't depend on map
	// iteration order.
	trackedIDs := make([]string, 0, len(tracked))
	for id := range tracked {
		trackedIDs = append(trackedIDs, id)
	}
	sort.Strings(trackedIDs)

	var orphans []brokerInfo // assigned to a collector that's no longer a voter
	var newBrokers []brokerInfo
	assignedClusterCollector := make(map[string]map[string]bool) // clusterID -> set of collectors already holding a broker from it

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
			orphans = append(orphans, info)
		default:
			load[a.Collector]++
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

	for _, ac := range addedCollectors {
		cmds = append(cmds, rebalanceOnto(ac, load, current, tracked)...)
	}

	return cmds
}

// rebalanceOnto moves just enough brokers from the currently most-loaded
// collectors onto newCollector to bring it up to roughly fair share. This is
// a deliberate, explicit action taken only when a collector is added — never
// an incidental side effect of processing order, which is what made an
// earlier, rejected design (bounded-load consistent hashing) leak
// disruption into unrelated parts of the system.
func rebalanceOnto(newCollector string, load map[string]int, current map[string]assignment, tracked map[string]brokerInfo) []command {
	numVoters := len(load)
	if numVoters == 0 {
		return nil
	}
	total := 0
	for _, n := range load {
		total += n
	}
	fairShare := total / numVoters

	need := fairShare - load[newCollector]
	if need <= 0 {
		return nil
	}

	type movable struct {
		brokerID  string
		clusterID string
		collector string
	}
	var candidates []movable
	for brokerID, a := range current {
		info, ok := tracked[brokerID]
		if !ok || a.Collector == newCollector {
			continue
		}
		candidates = append(candidates, movable{brokerID: brokerID, clusterID: info.clusterID, collector: a.Collector})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if load[candidates[i].collector] != load[candidates[j].collector] {
			return load[candidates[i].collector] > load[candidates[j].collector]
		}
		return candidates[i].brokerID < candidates[j].brokerID
	})

	var cmds []command
	moved := 0
	for _, c := range candidates {
		if moved >= need {
			break
		}
		if load[c.collector] <= fairShare {
			continue // don't pull a donor below fair share either
		}
		cmds = append(cmds, command{Op: opAssign, BrokerID: c.brokerID, ClusterID: c.clusterID, Collector: newCollector})
		load[c.collector]--
		load[newCollector]++
		moved++
	}
	return cmds
}
