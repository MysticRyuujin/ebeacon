package network

import "testing"

func uint64Ptr(n uint64) *uint64 { return &n }

func TestClassifyHistoricalTarget(t *testing.T) {
	tests := []struct {
		name string
		path string
		want HistoricalTarget
	}{
		// Named block IDs
		{
			name: "block by named head",
			path: "/eth/v2/beacon/blocks/head",
			want: HistoricalTarget{Kind: HistoricalKindBlockByID, Named: "head"},
		},
		{
			name: "block by named finalized",
			path: "/eth/v2/beacon/blocks/finalized",
			want: HistoricalTarget{Kind: HistoricalKindBlockByID, Named: "finalized"},
		},
		{
			name: "block by named genesis",
			path: "/eth/v1/beacon/blocks/genesis",
			want: HistoricalTarget{Kind: HistoricalKindBlockByID, Named: "genesis"},
		},
		// Numeric slot block ID
		{
			name: "block by numeric slot",
			path: "/eth/v2/beacon/blocks/12345",
			want: HistoricalTarget{Kind: HistoricalKindBlockByID, Slot: uint64Ptr(12345)},
		},
		{
			name: "blinded block by numeric slot",
			path: "/eth/v2/beacon/blinded_blocks/9999",
			want: HistoricalTarget{Kind: HistoricalKindBlockByID, Slot: uint64Ptr(9999)},
		},
		{
			name: "header by numeric slot",
			path: "/eth/v1/beacon/headers/42",
			want: HistoricalTarget{Kind: HistoricalKindBlockByID, Slot: uint64Ptr(42)},
		},
		// Root block ID
		{
			name: "block by root",
			path: "/eth/v2/beacon/blocks/0xdeadbeef",
			want: HistoricalTarget{Kind: HistoricalKindBlockByID, Root: "0xdeadbeef"},
		},
		// Blob sidecars
		{
			name: "blob sidecars by slot",
			path: "/eth/v1/beacon/blob_sidecars/500000",
			want: HistoricalTarget{Kind: HistoricalKindBlobSidecars, Slot: uint64Ptr(500000)},
		},
		{
			name: "blob sidecars by root",
			path: "/eth/v1/beacon/blob_sidecars/0xabc",
			want: HistoricalTarget{Kind: HistoricalKindBlobSidecars, Root: "0xabc"},
		},
		{
			name: "blob sidecars by named head",
			path: "/eth/v1/beacon/blob_sidecars/head",
			want: HistoricalTarget{Kind: HistoricalKindBlobSidecars, Named: "head"},
		},
		// States
		{
			name: "state by slot",
			path: "/eth/v1/beacon/states/77/validators",
			want: HistoricalTarget{Kind: HistoricalKindStateByID, Slot: uint64Ptr(77)},
		},
		{
			name: "state by named",
			path: "/eth/v1/beacon/states/finalized/root",
			want: HistoricalTarget{Kind: HistoricalKindStateByID, Named: "finalized"},
		},
		// Duties
		{
			name: "attester duties",
			path: "/eth/v1/validator/duties/attester/100",
			want: HistoricalTarget{Kind: HistoricalKindAttesterDuties, Epoch: uint64Ptr(100)},
		},
		{
			name: "proposer duties",
			path: "/eth/v1/validator/duties/proposer/200",
			want: HistoricalTarget{Kind: HistoricalKindProposerDuties, Epoch: uint64Ptr(200)},
		},
		{
			name: "sync duties",
			path: "/eth/v1/validator/duties/sync/300",
			want: HistoricalTarget{Kind: HistoricalKindSyncDuties, Epoch: uint64Ptr(300)},
		},
		// Rewards epoch
		{
			name: "rewards attestations by epoch",
			path: "/eth/v1/beacon/rewards/attestations/50",
			want: HistoricalTarget{Kind: HistoricalKindRewardsEpoch, Epoch: uint64Ptr(50)},
		},
		// Non-historical
		{
			name: "node syncing",
			path: "/eth/v1/node/syncing",
			want: HistoricalTarget{},
		},
		{
			name: "node version",
			path: "/eth/v1/node/version",
			want: HistoricalTarget{},
		},
		{
			name: "empty path",
			path: "",
			want: HistoricalTarget{},
		},
		{
			name: "short path",
			path: "/eth/v1",
			want: HistoricalTarget{},
		},
		{
			name: "non-eth path",
			path: "/healthz",
			want: HistoricalTarget{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyHistoricalTarget(tt.path)
			if got.Kind != tt.want.Kind {
				t.Errorf("Kind: got %v want %v", got.Kind, tt.want.Kind)
			}
			if got.Named != tt.want.Named {
				t.Errorf("Named: got %q want %q", got.Named, tt.want.Named)
			}
			if got.Root != tt.want.Root {
				t.Errorf("Root: got %q want %q", got.Root, tt.want.Root)
			}
			if !uintEq(got.Slot, tt.want.Slot) {
				t.Errorf("Slot: got %v want %v", deref(got.Slot), deref(tt.want.Slot))
			}
			if !uintEq(got.Epoch, tt.want.Epoch) {
				t.Errorf("Epoch: got %v want %v", deref(got.Epoch), deref(tt.want.Epoch))
			}
		})
	}
}

func TestHistoricalTargetRequiresArchive(t *testing.T) {
	const headSlot uint64 = 10_000_000 // arbitrary "now"
	headEpoch := headSlot / SlotsPerEpoch

	tests := []struct {
		name   string
		target HistoricalTarget
		head   uint64
		want   bool
	}{
		// Named never requires archive (pruned nodes serve head/finalized/etc.)
		{
			name:   "named head",
			target: HistoricalTarget{Kind: HistoricalKindBlockByID, Named: "head"},
			head:   headSlot,
			want:   false,
		},
		{
			name:   "named finalized",
			target: HistoricalTarget{Kind: HistoricalKindBlockByID, Named: "finalized"},
			head:   headSlot,
			want:   false,
		},
		// Root never requires archive (slot unknown)
		{
			name:   "block by root",
			target: HistoricalTarget{Kind: HistoricalKindBlockByID, Root: "0xabc"},
			head:   headSlot,
			want:   false,
		},
		// Unknown head: can't determine
		{
			name:   "unknown head",
			target: HistoricalTarget{Kind: HistoricalKindBlockByID, Slot: uint64Ptr(1)},
			head:   0,
			want:   false,
		},
		// Blocks: 5-month retention threshold
		{
			name:   "recent block",
			target: HistoricalTarget{Kind: HistoricalKindBlockByID, Slot: uint64Ptr(headSlot - 1000)},
			head:   headSlot,
			want:   false,
		},
		{
			name:   "old block (> 5 months)",
			target: HistoricalTarget{Kind: HistoricalKindBlockByID, Slot: uint64Ptr(headSlot - blocksRetentionSlots - 1)},
			head:   headSlot,
			want:   true,
		},
		{
			name:   "block at exact boundary",
			target: HistoricalTarget{Kind: HistoricalKindBlockByID, Slot: uint64Ptr(headSlot - blocksRetentionSlots)},
			head:   headSlot,
			want:   false,
		},
		// Blob sidecars: 18-day retention threshold
		{
			name:   "recent blob",
			target: HistoricalTarget{Kind: HistoricalKindBlobSidecars, Slot: uint64Ptr(headSlot - 1000)},
			head:   headSlot,
			want:   false,
		},
		{
			name:   "old blob (> 18 days)",
			target: HistoricalTarget{Kind: HistoricalKindBlobSidecars, Slot: uint64Ptr(headSlot - blobSidecarsRetentionSlots - 1)},
			head:   headSlot,
			want:   true,
		},
		// States: ~27h retention threshold
		{
			name:   "recent state",
			target: HistoricalTarget{Kind: HistoricalKindStateByID, Slot: uint64Ptr(headSlot - 100)},
			head:   headSlot,
			want:   false,
		},
		{
			name:   "old state (> 27h)",
			target: HistoricalTarget{Kind: HistoricalKindStateByID, Slot: uint64Ptr(headSlot - statesRetentionSlots - 1)},
			head:   headSlot,
			want:   true,
		},
		// Duties: archive if epoch < headEpoch - 1
		{
			name:   "current epoch attester duties",
			target: HistoricalTarget{Kind: HistoricalKindAttesterDuties, Epoch: uint64Ptr(headEpoch)},
			head:   headSlot,
			want:   false,
		},
		{
			name:   "one epoch back duties",
			target: HistoricalTarget{Kind: HistoricalKindAttesterDuties, Epoch: uint64Ptr(headEpoch - 1)},
			head:   headSlot,
			want:   false,
		},
		{
			name:   "two epochs back duties",
			target: HistoricalTarget{Kind: HistoricalKindAttesterDuties, Epoch: uint64Ptr(headEpoch - 2)},
			head:   headSlot,
			want:   true,
		},
		{
			name:   "very old proposer duties",
			target: HistoricalTarget{Kind: HistoricalKindProposerDuties, Epoch: uint64Ptr(1)},
			head:   headSlot,
			want:   true,
		},
		// Rewards epoch uses same duty semantics
		{
			name:   "old rewards epoch",
			target: HistoricalTarget{Kind: HistoricalKindRewardsEpoch, Epoch: uint64Ptr(1)},
			head:   headSlot,
			want:   true,
		},
		// None kind never requires archive
		{
			name:   "none kind",
			target: HistoricalTarget{},
			head:   headSlot,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.target.RequiresArchive(tt.head); got != tt.want {
				t.Errorf("RequiresArchive(%d): got %v want %v", tt.head, got, tt.want)
			}
		})
	}
}

func TestHistoricalTargetIsHistorical(t *testing.T) {
	cases := map[string]struct {
		target HistoricalTarget
		want   bool
	}{
		"empty":         {HistoricalTarget{}, false},
		"block by slot": {HistoricalTarget{Kind: HistoricalKindBlockByID, Slot: uint64Ptr(1)}, true},
		"block by root": {HistoricalTarget{Kind: HistoricalKindBlockByID, Root: "0xa"}, true},
		"named head":    {HistoricalTarget{Kind: HistoricalKindBlockByID, Named: "head"}, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := tc.target.IsHistorical(); got != tc.want {
				t.Errorf("IsHistorical: got %v want %v", got, tc.want)
			}
		})
	}
}

func uintEq(a, b *uint64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func deref(p *uint64) any {
	if p == nil {
		return nil
	}
	return *p
}
