package network

import "bytes"

// isPruningError reports whether an upstream response looks like it was pruned
// or otherwise cannot be served by this upstream but could be served by an
// archive-capable one.
//
// Classic pruning: HTTP 404 on a request whose path targets a historical
// identifier (slot, epoch, or root). We deliberately do NOT inspect response
// body substrings for 404s — they vary across client implementations and
// versions. If a 404 is actually a bad identifier instead of pruning, the
// archive retry fails identically and we return that error to the client. The
// cost of a false positive is one extra upstream roundtrip.
//
// PeerDAS custody: post-Fusaka (EIP-7594), non-supernode consensus clients
// cannot reconstruct blob data from their column custody and return HTTP 400
// for blob endpoints with a body like "Insufficient data columns to
// reconstruct blobs". These are functionally pruning-shaped — the upstream
// cannot serve, a supernode upstream can — so they should also trigger
// archive promotion. We gate on blob-kind targets and a body substring to
// avoid conflating with legitimate 400 Bad Request responses.
//
// Named-head identifiers (head, finalized, justified, genesis) never produce
// a pruning-shaped error — pruned nodes serve these fine. The `target.Named`
// check ensures we don't classify a transient "node hasn't caught up to head
// yet" 404 as pruning.
func isPruningError(statusCode int, body []byte, target HistoricalTarget) bool {
	if !target.IsHistorical() {
		return false
	}
	if target.Named != "" {
		return false
	}
	if statusCode == 404 {
		return true
	}
	if statusCode == 400 && target.Kind == HistoricalKindBlobSidecars {
		return isPeerDASCustodyBody(body)
	}
	return false
}

// peerDASCustodyBodySignals are substrings that post-Fusaka consensus clients
// include in the 400 response body when they lack the data column custody to
// reconstruct a blob. Match is case-sensitive on the exact phrases emitted by
// current client code paths; broaden as other clients adopt PeerDAS.
var peerDASCustodyBodySignals = [][]byte{
	[]byte("data columns"),      // Lighthouse: "Insufficient data columns to reconstruct blobs"
	[]byte("reconstruct blobs"), // Lighthouse: same message, different anchor
}

func isPeerDASCustodyBody(body []byte) bool {
	for _, sig := range peerDASCustodyBodySignals {
		if bytes.Contains(body, sig) {
			return true
		}
	}
	return false
}

// peerDASBodyPeekLimit caps how many bytes of a 400 response body we read for
// custody-error classification. The known error messages are well under 512B;
// this bound prevents a misbehaving upstream from wasting memory on huge
// error bodies.
const peerDASBodyPeekLimit = 4096
