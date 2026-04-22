package network

import "strings"

// normalizeAPIPath reduces Beacon API paths to stable route templates so
// metrics and score tracking can be filtered without exploding cardinality.
func normalizeAPIPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "/"
	}
	if path != "/" {
		path = strings.TrimRight(path, "/")
		if path == "" {
			path = "/"
		}
	}
	if path == "/healthz" {
		return path
	}

	segments := strings.Split(strings.Trim(path, "/"), "/")
	if len(segments) < 2 || segments[0] != "eth" || len(segments[1]) < 2 || segments[1][0] != 'v' {
		return "/other"
	}

	prefix := "/" + segments[0] + "/" + segments[1]
	if len(segments) == 2 {
		return prefix
	}

	switch segments[2] {
	case "events":
		return prefix + "/events"

	case "node":
		if len(segments) >= 4 {
			switch segments[3] {
			case "health", "identity", "peer_count", "peers", "syncing", "version":
				return prefix + "/node/" + segments[3]
			}
		}
		return prefix + "/node/other"

	case "config":
		if len(segments) >= 4 {
			switch segments[3] {
			case "deposit_contract", "fork_schedule", "genesis", "spec":
				return prefix + "/config/" + segments[3]
			}
		}
		return prefix + "/config/other"

	case "beacon":
		if len(segments) < 4 {
			return prefix + "/beacon/other"
		}
		switch segments[3] {
		case "genesis":
			return prefix + "/beacon/genesis"
		case "headers":
			if len(segments) == 4 {
				return prefix + "/beacon/headers"
			}
			return prefix + "/beacon/headers/{block_id}"
		case "blocks", "blinded_blocks", "blobs", "blob_sidecars":
			if len(segments) == 5 {
				return prefix + "/beacon/" + segments[3] + "/{block_id}"
			}
			if len(segments) == 6 {
				switch segments[5] {
				case "attestations", "root":
					return prefix + "/beacon/" + segments[3] + "/{block_id}/" + segments[5]
				}
			}
			return prefix + "/beacon/" + segments[3] + "/other"
		case "states":
			if len(segments) < 6 {
				return prefix + "/beacon/states/other"
			}
			base := prefix + "/beacon/states/{state_id}/" + segments[5]
			if len(segments) == 6 {
				return base
			}
			if len(segments) == 7 && segments[5] == "validators" {
				return base + "/{validator_id}"
			}
			return prefix + "/beacon/states/other"
		case "rewards":
			if len(segments) == 6 {
				switch segments[4] {
				case "attestations":
					return prefix + "/beacon/rewards/attestations/{epoch}"
				case "blocks", "sync_committee":
					return prefix + "/beacon/rewards/" + segments[4] + "/{block_id}"
				}
			}
			return prefix + "/beacon/rewards/other"
		case "pool":
			if len(segments) == 5 {
				switch segments[4] {
				case "attestations",
					"attester_slashings",
					"proposer_slashings",
					"sync_committees",
					"voluntary_exits",
					"bls_to_execution_changes":
					return prefix + "/beacon/pool/" + segments[4]
				}
			}
			return prefix + "/beacon/pool/other"
		case "deposit_snapshot":
			if len(segments) == 4 {
				return prefix + "/beacon/deposit_snapshot"
			}
		case "light_client":
			if len(segments) == 4 {
				return prefix + "/beacon/light_client"
			}
			switch segments[4] {
			case "finality_update", "optimistic_update", "updates":
				return prefix + "/beacon/light_client/" + segments[4]
			case "bootstrap":
				if len(segments) == 6 {
					return prefix + "/beacon/light_client/bootstrap/{block_root}"
				}
			}
			return prefix + "/beacon/light_client/other"
		default:
			return prefix + "/beacon/other"
		}

	case "validator":
		if len(segments) == 6 && segments[3] == "duties" {
			switch segments[4] {
			case "attester", "proposer", "sync":
				return prefix + "/validator/duties/" + segments[4] + "/{epoch}"
			}
		}
		if len(segments) == 5 && segments[3] == "liveness" {
			return prefix + "/validator/liveness/{epoch}"
		}
		if len(segments) == 5 && segments[3] == "blocks" {
			// /eth/v3/validator/blocks/{slot}
			return prefix + "/validator/blocks/{slot}"
		}
		if len(segments) == 4 {
			switch segments[3] {
			case "attestation_data",
				"aggregate_and_proofs",
				"aggregate_attestation",
				"beacon_committee_selections",
				"beacon_committee_subscriptions",
				"contribution_and_proofs",
				"prepare_beacon_proposer",
				"register_validator",
				"sync_committee_contribution",
				"sync_committee_selections",
				"sync_committee_subscriptions":
				return prefix + "/validator/" + segments[3]
			}
		}
		return prefix + "/validator/other"

	case "debug":
		if len(segments) == 6 && segments[3] == "beacon" {
			switch segments[4] {
			case "states":
				return prefix + "/debug/beacon/states/{state_id}"
			case "data_column_sidecars":
				return prefix + "/debug/beacon/data_column_sidecars/{block_id}"
			}
		}
		return prefix + "/debug/other"
	}

	return prefix + "/other"
}
