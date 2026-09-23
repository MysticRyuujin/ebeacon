package network

import "testing"

func TestIsFinalizedPath(t *testing.T) {
	t.Parallel()
	const fe, spe = 10, 32 // finalized through slot 320
	tests := []struct {
		path           string
		finalizedEpoch uint64
		want           bool
	}{
		{"/eth/v2/beacon/blocks/320", fe, true},
		{"/eth/v2/beacon/blocks/321", fe, false},
		{"/eth/v2/beacon/blocks/1", 0, false},
		{"/eth/v1/beacon/rewards/attestations/8", fe, true},
		{"/eth/v1/beacon/rewards/attestations/9", fe, false},
		{"/eth/v1/validator/duties/proposer/0", 1, false},
		{"/eth/v2/beacon/blocks/head", fe, false},
	}
	for _, tt := range tests {
		if got := isFinalizedPath(tt.path, tt.finalizedEpoch, spe); got != tt.want {
			t.Errorf("isFinalizedPath(%q, %d) = %v, want %v", tt.path, tt.finalizedEpoch, got, tt.want)
		}
	}
}
