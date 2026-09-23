package upstream

import (
	"sync"
	"time"

	"github.com/mysticryuujin/ebeacon/config"
)

// Block represents a beacon chain block header tracked for fork detection.
type Block struct {
	Slot       uint64
	Root       string
	ParentRoot string
	SeenBy     map[string]bool // upstream IDs that reported this block
	FirstSeen  time.Time
}

// remoteVoter is the SeenBy ID for heads that other eBeacon instances publish
// through shared state. The "@" keeps it outside the configurable upstream ID
// pattern, so a real upstream can never share it.
const remoteVoter = "@remote"

// BlockCache tracks recent block headers from all upstreams to determine the canonical fork.
//
// followDistance controls how many slots of history are retained before pruning.
// It should be at least as large as the expected chain reorg depth; Ethereum
// mainnet finalizes after ~2 epochs (64 slots), so values around 64-128 are typical.
//
// maxHeadDistance is the maximum number of slots an upstream may lag behind the
// highest observed head slot and still be considered "on the canonical fork".
// Beyond this threshold the upstream is treated as lagging and deprioritized.
type BlockCache struct {
	mu              sync.RWMutex
	slotMap         map[uint64][]*Block // slot -> blocks at that slot (forks possible)
	rootMap         map[string]*Block   // root -> block
	maxSlot         uint64              // highest slot seen
	followDistance  uint64
	maxHeadDistance uint64
	genesisTime     int64 // unix seconds; 0 disables future-slot rejection
	secondsPerSlot  int64 // chain slot duration; 0 means the default

	// canonSlot and canonRoot cache the canonical head; every write
	// recomputes them so the per-request fork checks skip the slot walk.
	canonSlot uint64
	canonRoot string
}

// NewBlockCache creates a BlockCache with the given follow distance and max head distance.
func NewBlockCache(followDistance, maxHeadDistance uint64) *BlockCache {
	return &BlockCache{
		slotMap:         make(map[uint64][]*Block),
		rootMap:         make(map[string]*Block),
		followDistance:  followDistance,
		maxHeadDistance: maxHeadDistance,
	}
}

// SetSlotTiming enables wall-clock plausibility checks in AddBlock.
// Must be called before block reporting starts (i.e. before Pool.Start).
func (bc *BlockCache) SetSlotTiming(genesisTime, secondsPerSlot int64) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.genesisTime = genesisTime
	bc.secondsPerSlot = secondsPerSlot
}

func (bc *BlockCache) currentWallClockSlotLocked() (uint64, bool) {
	const clockSkewTolerance = 10 * time.Minute
	if bc.genesisTime <= 0 {
		return 0, false
	}
	now := time.Now().Unix()
	if now <= bc.genesisTime {
		return 0, false
	}
	slotSeconds := bc.secondsPerSlot
	if slotSeconds <= 0 {
		slotSeconds = config.DefaultSecondsPerSlot
	}
	elapsed := now - bc.genesisTime + int64(clockSkewTolerance.Seconds())
	return uint64(elapsed) / uint64(slotSeconds), true
}

// CurrentWallClockSlot returns ok=false when genesis time is unknown (callers fail open).
func (bc *BlockCache) CurrentWallClockSlot() (uint64, bool) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.currentWallClockSlotLocked()
}

// AddBlock records a block header seen by the given upstream.
func (bc *BlockCache) AddBlock(upstreamID string, slot uint64, root, parentRoot string) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// maxSlot is monotonic and everything (canonical-head walk, cleanup,
	// head-lag scoring) keys off it, so one implausible far-future slot —
	// a cross-chain misconfigured upstream or corrupt shared-state replay —
	// would poison fork detection until restart. Reject whole blocks beyond
	// the wall-clock slot when the network's genesis time is known. The margin
	// must absorb realistic host-clock lag (NTP outage, VM suspend); a slow
	// clock otherwise drops every real head and silently freezes fork
	// detection. Cross-chain/corrupt slots are off by millions, so a generous
	// margin still catches them.
	if currentSlot, ok := bc.currentWallClockSlotLocked(); ok && slot > currentSlot {
		return
	}

	if slot > bc.maxSlot {
		bc.maxSlot = slot
	}

	defer bc.refreshCanonicalHeadLocked()

	if b, ok := bc.rootMap[root]; ok {
		b.SeenBy[upstreamID] = true
		return
	}

	b := &Block{
		Slot:       slot,
		Root:       root,
		ParentRoot: parentRoot,
		SeenBy:     map[string]bool{upstreamID: true},
		FirstSeen:  time.Now(),
	}
	bc.rootMap[root] = b
	bc.slotMap[slot] = append(bc.slotMap[slot], b)
}

// CanonicalHead returns the slot and root of the canonical head determined by majority vote.
// At the highest known slot, the block seen by the most upstreams is considered canonical.
func (bc *BlockCache) CanonicalHead() (slot uint64, root string) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.canonicalHeadLocked()
}

// LocalCanonicalHead is CanonicalHead counting only this instance's upstream
// votes. Shared state publishes this head: a head chosen with remote votes
// would echo another instance's vote back to it as a new vote.
func (bc *BlockCache) LocalCanonicalHead() (slot uint64, root string) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.canonicalHead(false)
}

// IsOnCanonicalFork returns true if the upstream has reported a block at or near the canonical head.
//
// The distinction between "lagging" and "forked" matters for routing:
//   - A lagging upstream (slightly behind, but on the same chain) is acceptable
//     for read traffic that doesn't need the absolute latest slot.
//   - A forked upstream (reporting a different block at the same slot) is on a
//     competing minority chain and must not receive validator duties, as acting
//     on its view could result in slashable offenses or missed attestations.
func (bc *BlockCache) IsOnCanonicalFork(upstreamID string) bool {
	return bc.ForkStatus(upstreamID) == "canonical"
}

// CanonicalHeadSeenBy returns the set of upstream IDs that have reported the
// canonical head block. It is used by the router to hard-prefer upstreams that
// are guaranteed to have the newest head when serving requests for named-head
// paths (/head, /finalized, /justified). Returns nil if no canonical head is
// known yet; callers should fail open in that case.
//
// The remoteVoter pseudo-ID published by other ebeacon instances via shared state
// is excluded from the result because it cannot serve client requests.
func (bc *BlockCache) CanonicalHeadSeenBy() map[string]bool {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	_, canonRoot := bc.canonicalHeadLocked()
	if canonRoot == "" {
		return nil
	}
	b, ok := bc.rootMap[canonRoot]
	if !ok || len(b.SeenBy) == 0 {
		return nil
	}
	out := make(map[string]bool, len(b.SeenBy))
	for id := range b.SeenBy {
		if id == remoteVoter {
			continue
		}
		out[id] = true
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// ForkStatus returns a descriptive status for the upstream's fork position:
//   - "canonical" — the upstream has reported the canonical head block or is
//     slightly behind (within maxHeadDistance) on the same chain.
//   - "forked" — the upstream reported a different block at the canonical slot,
//     indicating it is on a competing minority chain.
//   - "lagging" — the upstream has not reported any block near the canonical
//     head and is too far behind to determine its fork status.
func (bc *BlockCache) ForkStatus(upstreamID string) string {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	canonSlot, canonRoot := bc.canonicalHeadLocked()
	if canonSlot == 0 {
		return "canonical" // no data yet, assume canonical
	}

	// Check if upstream has seen the canonical head block.
	if b, ok := bc.rootMap[canonRoot]; ok {
		if b.SeenBy[upstreamID] {
			return "canonical"
		}
	}

	// If the upstream reported a different block at the canonical slot,
	// it is on a competing fork.
	for _, b := range bc.slotMap[canonSlot] {
		if b.SeenBy[upstreamID] {
			return "forked"
		}
	}

	// Upstream is canonical if it is only slightly behind.
	for s := canonSlot - 1; s > 0 && canonSlot-s <= bc.maxHeadDistance; s-- {
		for _, b := range bc.slotMap[s] {
			if b.SeenBy[upstreamID] {
				return "canonical"
			}
		}
	}

	// Too far behind to determine — lagging, not necessarily forked.
	return "lagging"
}

// IsForked reports whether the upstream is on a competing minority chain
// (reported a different block at the canonical slot). It returns false for
// canonical and merely-lagging upstreams, so transient lag does not exclude
// an otherwise-healthy node from sticky/preferred routing.
func (bc *BlockCache) IsForked(upstreamID string) bool {
	return bc.ForkStatus(upstreamID) == "forked"
}

func (bc *BlockCache) canonicalHeadLocked() (uint64, string) {
	return bc.canonSlot, bc.canonRoot
}

func (bc *BlockCache) refreshCanonicalHeadLocked() {
	bc.canonSlot, bc.canonRoot = bc.canonicalHead(true)
}

func (bc *BlockCache) canonicalHead(countRemote bool) (uint64, string) {
	if bc.maxSlot == 0 {
		return 0, ""
	}
	for s := bc.maxSlot; s > 0 && s > bc.maxSlot-bc.maxHeadDistance-1; s-- {
		var best *Block
		bestCount := 0
		for _, b := range bc.slotMap[s] {
			count := len(b.SeenBy)
			if !countRemote && b.SeenBy[remoteVoter] {
				count--
			}
			if count == 0 {
				continue
			}
			// Tie-break by root string to ensure deterministic selection
			// across calls; the ordering has no semantic meaning.
			if count > bestCount || (count == bestCount && b.Root < best.Root) {
				best = b
				bestCount = count
			}
		}
		if best != nil {
			return best.Slot, best.Root
		}
	}
	return 0, ""
}

// Cleanup removes blocks older than followDistance from the current max slot.
func (bc *BlockCache) Cleanup() {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if bc.maxSlot <= bc.followDistance {
		return
	}
	cutoff := bc.maxSlot - bc.followDistance
	defer bc.refreshCanonicalHeadLocked()

	for slot, blocks := range bc.slotMap {
		if slot < cutoff {
			for _, b := range blocks {
				delete(bc.rootMap, b.Root)
			}
			delete(bc.slotMap, slot)
		}
	}
}

// MaxSlot returns the highest slot seen.
func (bc *BlockCache) MaxSlot() uint64 {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.maxSlot
}
