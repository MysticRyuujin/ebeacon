package main

import (
	"strings"
	"testing"
)

func TestBuildEndpoints_RealWorldHTTPWeights(t *testing.T) {
	cs := chainState{headSlot: 12_345, finalizedEpoch: 384, finalizedSlot: 12_319, prevEpoch: 383}
	eps := buildEndpoints(cs)

	got := make(map[string]int)
	for _, ep := range eps {
		got[ep.name] += ep.weight
	}

	want := map[string]int{
		endpointHeadersByBlockID:     32,
		endpointBlocksV2ByBlockID:    22,
		endpointBlockRootByBlockID:   12,
		endpointStateValidator:       6,
		endpointBeaconBlobs:          4,
		endpointNodeSyncing:          3,
		endpointHeadersList:          2,
		endpointConfigSpec:           1,
		endpointFinalityCheckpoints:  1,
		endpointRewardsSyncCommittee: 1,
		endpointNodeVersion:          1,
		endpointRewardsBlocks:        1,
		endpointRewardsAttestations:  1,
		endpointBeaconBlobSidecars:   1,
		endpointNodeHealth:           1,
	}

	if len(got) != len(want) {
		t.Fatalf("endpoint count: got %d want %d", len(got), len(want))
	}
	for name, wantWeight := range want {
		if gotWeight := got[name]; gotWeight != wantWeight {
			t.Fatalf("weight for %s: got %d want %d", name, gotWeight, wantWeight)
		}
	}
}

func TestBuildEndpoints_UsesFinalizedSlotVariants(t *testing.T) {
	cs := chainState{headSlot: 20_000, finalizedEpoch: 620, finalizedSlot: 19_871, prevEpoch: 619}
	eps := buildEndpoints(cs)

	var foundNumericBlock bool
	for _, ep := range eps {
		switch {
		case ep.name == endpointBlocksV2ByBlockID && ep.path == "/eth/v2/beacon/blocks/19871":
			foundNumericBlock = true
		case strings.Contains(ep.path, "/19999"):
			t.Fatalf("unexpected head-1 slot variant in %s", ep.path)
		}
	}

	if !foundNumericBlock {
		t.Fatal("expected finalized numeric block variant")
	}
}
