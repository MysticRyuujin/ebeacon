package network

import "testing"

func TestNormalizeAPIPath(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{path: "/eth/v1/beacon/headers/head", want: "/eth/v1/beacon/headers/{block_id}"},
		{path: "/eth/v2/beacon/blocks/12345", want: "/eth/v2/beacon/blocks/{block_id}"},
		{path: "/eth/v1/beacon/blocks/finalized/root", want: "/eth/v1/beacon/blocks/{block_id}/root"},
		{path: "/eth/v1/beacon/states/head/validators", want: "/eth/v1/beacon/states/{state_id}/validators"},
		{path: "/eth/v1/beacon/states/999/validators/1", want: "/eth/v1/beacon/states/{state_id}/validators/{validator_id}"},
		{path: "/eth/v1/beacon/states/head/pending_partial_withdrawals", want: "/eth/v1/beacon/states/{state_id}/pending_partial_withdrawals"},
		{path: "/eth/v1/beacon/rewards/attestations/123", want: "/eth/v1/beacon/rewards/attestations/{epoch}"},
		{path: "/eth/v1/validator/duties/proposer/123", want: "/eth/v1/validator/duties/proposer/{epoch}"},
		{path: "/eth/v1/node/syncing", want: "/eth/v1/node/syncing"},
		{path: "/healthz", want: "/healthz"},
		{path: "/weird/custom/path", want: "/other"},
	}

	for _, tt := range tests {
		if got := normalizeAPIPath(tt.path); got != tt.want {
			t.Fatalf("normalizeAPIPath(%q): got %q want %q", tt.path, got, tt.want)
		}
	}
}
