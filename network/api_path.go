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
		return prefix + "/validator/other"

	case "debug":
		if len(segments) == 6 && segments[3] == "beacon" && segments[4] == "states" {
			return prefix + "/debug/beacon/states/{state_id}"
		}
		return prefix + "/debug/other"
	}

	return prefix + "/other"
}
