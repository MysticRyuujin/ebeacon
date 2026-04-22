package network

import (
	"strconv"
	"strings"
)

// HistoricalKind enumerates the categories of Beacon API endpoints that target
// a historical identifier (slot, epoch, root, or named head). Each kind has a
// different retention window on pruned nodes.
type HistoricalKind int

const (
	HistoricalKindNone           HistoricalKind = iota
	HistoricalKindBlockByID                     // /beacon/blocks/{block_id}, /blinded_blocks/{id}, /headers/{id}, /blobs/{id}
	HistoricalKindBlobSidecars                  // /beacon/blob_sidecars/{block_id}
	HistoricalKindStateByID                     // /beacon/states/{state_id}/...
	HistoricalKindAttesterDuties                // /validator/duties/attester/{epoch}
	HistoricalKindProposerDuties                // /validator/duties/proposer/{epoch}
	HistoricalKindSyncDuties                    // /validator/duties/sync/{epoch}
	HistoricalKindRewardsEpoch                  // /beacon/rewards/attestations/{epoch}
)

// SlotsPerEpoch is the Ethereum consensus-layer constant (SLOTS_PER_EPOCH = 32).
const SlotsPerEpoch uint64 = 32

// Per-endpoint retention thresholds expressed in slots. Requests for targets
// older than (headSlot - threshold) are presumed to require an archive upstream.
//
// These are intentionally spec-minimum or below — they describe what a pruned
// node is GUARANTEED to retain, not what any specific client implementation
// retains. A conservative value means fewer false-positive archive promotions.
const (
	// Blob sidecars: MIN_EPOCHS_FOR_BLOB_SIDECARS_REQUESTS = 4096 epochs (~18 days).
	// See EIP-4844. Blobs are aggressively pruned; this is the cleanest archive win.
	blobSidecarsRetentionSlots = 4096 * SlotsPerEpoch

	// States: clients (Lighthouse, Prysm, Teku) retain ~1 week of historical states
	// by default. We use a conservative ~27h threshold — older than this, promote
	// to archive on first attempt. The retry path catches edge cases within 27h.
	statesRetentionSlots uint64 = 8192

	// Blocks: MIN_EPOCHS_FOR_BLOCK_REQUESTS = 33024 epochs (~5 months) per spec.
	blocksRetentionSlots = 33024 * SlotsPerEpoch
)

// HistoricalTarget describes the historical identifier carried by a Beacon API
// request path, if any. Zero-valued (Kind=None) means the path did not target
// a historical identifier and normal routing applies.
type HistoricalTarget struct {
	Kind  HistoricalKind
	Named string  // "head" | "finalized" | "justified" | "genesis" | ""
	Slot  *uint64 // set when the identifier was a numeric slot
	Root  string  // set when the identifier was an 0x-prefixed block/state root
	Epoch *uint64 // set when the endpoint carries a numeric epoch
}

// IsHistorical returns true if the target carries any historical identifier,
// regardless of whether it is demonstrably old enough to require archive.
// Used by the error-driven retry path to decide whether a 404 is pruning-shaped.
func (t HistoricalTarget) IsHistorical() bool {
	return t.Kind != HistoricalKindNone
}

// RequiresArchive returns true when the target identifier is demonstrably older
// than the per-endpoint retention threshold, given the current canonical head
// slot. Returns false for named IDs (head/finalized/justified/genesis), for
// root-based lookups (slot unknown), and when headSlot is 0 (not yet known).
//
// This is best-effort proactive classification — when it returns false, the
// request is routed normally and the error-driven retry path catches any
// pruning-shaped errors.
func (t HistoricalTarget) RequiresArchive(headSlot uint64) bool {
	if headSlot == 0 || t.Named != "" || t.Root != "" {
		return false
	}
	headEpoch := headSlot / SlotsPerEpoch

	switch t.Kind {
	case HistoricalKindBlockByID:
		if t.Slot == nil {
			return false
		}
		return olderThan(*t.Slot, headSlot, blocksRetentionSlots)

	case HistoricalKindBlobSidecars:
		if t.Slot == nil {
			return false
		}
		return olderThan(*t.Slot, headSlot, blobSidecarsRetentionSlots)

	case HistoricalKindStateByID:
		if t.Slot == nil {
			return false
		}
		return olderThan(*t.Slot, headSlot, statesRetentionSlots)

	case HistoricalKindAttesterDuties, HistoricalKindProposerDuties, HistoricalKindSyncDuties, HistoricalKindRewardsEpoch:
		// Beacon API guarantees duties for current and next epoch only. Anything
		// earlier than (current - 1) is historical.
		if t.Epoch == nil {
			return false
		}
		return *t.Epoch+1 < headEpoch

	default:
		return false
	}
}

func olderThan(targetSlot, headSlot, retentionSlots uint64) bool {
	if headSlot <= retentionSlots {
		return false
	}
	return targetSlot < headSlot-retentionSlots
}

// classifyHistoricalTarget parses a Beacon API request path and extracts the
// historical identifier it carries, if any. Unknown or non-historical paths
// return a zero-valued HistoricalTarget.
//
// Segment indexing follows the same scheme as normalizeAPIPath: segments[0]="eth",
// segments[1]="v1"/"v2", segments[2]=category ("beacon", "validator", ...).
func classifyHistoricalTarget(p string) HistoricalTarget {
	p = strings.TrimSpace(p)
	if p == "" || p == "/" {
		return HistoricalTarget{}
	}
	segments := strings.Split(strings.Trim(p, "/"), "/")
	if len(segments) < 4 || segments[0] != "eth" || len(segments[1]) < 2 || segments[1][0] != 'v' {
		return HistoricalTarget{}
	}

	switch segments[2] {
	case "beacon":
		return classifyBeaconPath(segments)
	case "validator":
		return classifyValidatorPath(segments)
	}
	return HistoricalTarget{}
}

func classifyBeaconPath(segments []string) HistoricalTarget {
	// segments[3] is the sub-category under /eth/vN/beacon/
	switch segments[3] {
	case "blocks", "blinded_blocks", "blobs", "headers":
		if len(segments) < 5 {
			return HistoricalTarget{}
		}
		t := parseBlockIdentifier(segments[4])
		t.Kind = HistoricalKindBlockByID
		return t

	case "blob_sidecars":
		if len(segments) < 5 {
			return HistoricalTarget{}
		}
		t := parseBlockIdentifier(segments[4])
		t.Kind = HistoricalKindBlobSidecars
		return t

	case "states":
		if len(segments) < 5 {
			return HistoricalTarget{}
		}
		t := parseBlockIdentifier(segments[4])
		t.Kind = HistoricalKindStateByID
		return t

	case "rewards":
		// /eth/v1/beacon/rewards/attestations/{epoch}
		if len(segments) >= 6 && segments[4] == "attestations" {
			if epoch, ok := parseUint64(segments[5]); ok {
				return HistoricalTarget{Kind: HistoricalKindRewardsEpoch, Epoch: &epoch}
			}
		}
	}
	return HistoricalTarget{}
}

func classifyValidatorPath(segments []string) HistoricalTarget {
	// /eth/v1/validator/duties/{attester|proposer|sync}/{epoch}
	if len(segments) < 6 || segments[3] != "duties" {
		return HistoricalTarget{}
	}
	epoch, ok := parseUint64(segments[5])
	if !ok {
		return HistoricalTarget{}
	}
	switch segments[4] {
	case "attester":
		return HistoricalTarget{Kind: HistoricalKindAttesterDuties, Epoch: &epoch}
	case "proposer":
		return HistoricalTarget{Kind: HistoricalKindProposerDuties, Epoch: &epoch}
	case "sync":
		return HistoricalTarget{Kind: HistoricalKindSyncDuties, Epoch: &epoch}
	}
	return HistoricalTarget{}
}

// parseBlockIdentifier interprets a {block_id} or {state_id} segment according
// to the Beacon API spec: one of "head", "genesis", "finalized", "justified",
// a slot number, or an 0x-prefixed root.
func parseBlockIdentifier(seg string) HistoricalTarget {
	switch seg {
	case "head", "genesis", "finalized", "justified":
		return HistoricalTarget{Named: seg}
	}
	if strings.HasPrefix(seg, "0x") || strings.HasPrefix(seg, "0X") {
		return HistoricalTarget{Root: seg}
	}
	if slot, ok := parseUint64(seg); ok {
		return HistoricalTarget{Slot: &slot}
	}
	return HistoricalTarget{}
}

func parseUint64(s string) (uint64, bool) {
	if s == "" {
		return 0, false
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
