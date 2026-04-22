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
		{path: "/eth/v1/beacon/blobs/13337749", want: "/eth/v1/beacon/blobs/{block_id}"},
		{path: "/eth/v1/beacon/rewards/blocks/12345", want: "/eth/v1/beacon/rewards/blocks/{block_id}"},
		{path: "/eth/v1/beacon/rewards/sync_committee/12345", want: "/eth/v1/beacon/rewards/sync_committee/{block_id}"},
		{path: "/eth/v1/beacon/light_client/bootstrap/0xabcd", want: "/eth/v1/beacon/light_client/bootstrap/{block_root}"},
		{path: "/eth/v1/beacon/light_client/finality_update", want: "/eth/v1/beacon/light_client/finality_update"},
		{path: "/eth/v1/beacon/light_client/updates", want: "/eth/v1/beacon/light_client/updates"},
		{path: "/eth/v1/validator/liveness/42", want: "/eth/v1/validator/liveness/{epoch}"},
		{path: "/eth/v1/validator/register_validator", want: "/eth/v1/validator/register_validator"},
		{path: "/eth/v1/validator/prepare_beacon_proposer", want: "/eth/v1/validator/prepare_beacon_proposer"},
		{path: "/eth/v1/validator/aggregate_and_proofs", want: "/eth/v1/validator/aggregate_and_proofs"},
		{path: "/eth/v1/validator/contribution_and_proofs", want: "/eth/v1/validator/contribution_and_proofs"},
		{path: "/eth/v1/validator/beacon_committee_subscriptions", want: "/eth/v1/validator/beacon_committee_subscriptions"},
		{path: "/eth/v1/validator/sync_committee_subscriptions", want: "/eth/v1/validator/sync_committee_subscriptions"},
		{path: "/eth/v1/validator/attestation_data", want: "/eth/v1/validator/attestation_data"},
		{path: "/eth/v2/validator/aggregate_attestation", want: "/eth/v2/validator/aggregate_attestation"},
		{path: "/eth/v3/validator/blocks/54321", want: "/eth/v3/validator/blocks/{slot}"},
		{path: "/eth/v1/validator/unknown_write", want: "/eth/v1/validator/other"},
		{path: "/eth/v1/beacon/pool/attestations", want: "/eth/v1/beacon/pool/attestations"},
		{path: "/eth/v1/beacon/pool/voluntary_exits", want: "/eth/v1/beacon/pool/voluntary_exits"},
		{path: "/eth/v1/beacon/pool/unknown", want: "/eth/v1/beacon/pool/other"},
		{path: "/eth/v1/beacon/deposit_snapshot", want: "/eth/v1/beacon/deposit_snapshot"},
		{path: "/eth/v1/debug/beacon/data_column_sidecars/12345", want: "/eth/v1/debug/beacon/data_column_sidecars/{block_id}"},
		{path: "/eth/v2/debug/beacon/states/finalized", want: "/eth/v2/debug/beacon/states/{state_id}"},
		{path: "/healthz", want: "/healthz"},
		{path: "/weird/custom/path", want: "/other"},
	}

	for _, tt := range tests {
		if got := normalizeAPIPath(tt.path); got != tt.want {
			t.Fatalf("normalizeAPIPath(%q): got %q want %q", tt.path, got, tt.want)
		}
	}
}
