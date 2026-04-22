package network

// isPruningError reports whether an upstream response looks like it was pruned:
// a 404 on a request whose path targets a historical identifier (slot, epoch, or root).
//
// We deliberately do NOT inspect response body substrings (which vary across
// client implementations and versions — Lighthouse's "not found", Prysm's
// "could not find", Teku's different shape, etc.). HTTP 404 on a historical
// path is a strong enough signal: if it's actually a bad slot number instead
// of pruning, the archive retry will fail identically and we return that error
// to the client. The cost of a false positive is one extra upstream roundtrip.
//
// Named-head identifiers (head, finalized, justified, genesis) never produce a
// pruning-shaped error — pruned nodes serve these fine. The `target.Named`
// check in RequiresArchive and IsHistorical ensures we don't classify a
// transient "node hasn't caught up to head yet" 404 as pruning.
func isPruningError(statusCode int, target HistoricalTarget) bool {
	if statusCode != 404 {
		return false
	}
	if !target.IsHistorical() {
		return false
	}
	// Named heads are never pruning — a 404 there means the node is behind,
	// not that the data is permanently unavailable.
	if target.Named != "" {
		return false
	}
	return true
}
