package network

import "testing"

func TestIsPruningError(t *testing.T) {
	historicalBySlot := HistoricalTarget{Kind: HistoricalKindBlockByID, Slot: uint64Ptr(1)}
	historicalByRoot := HistoricalTarget{Kind: HistoricalKindBlockByID, Root: "0xabc"}
	namedHead := HistoricalTarget{Kind: HistoricalKindBlockByID, Named: "head"}
	namedFinalized := HistoricalTarget{Kind: HistoricalKindBlockByID, Named: "finalized"}
	nonHistorical := HistoricalTarget{}
	blobHistorical := HistoricalTarget{Kind: HistoricalKindBlobSidecars, Slot: uint64Ptr(1)}
	blobByRoot := HistoricalTarget{Kind: HistoricalKindBlobSidecars, Root: "0xabc"}
	blobNamed := HistoricalTarget{Kind: HistoricalKindBlobSidecars, Named: "head"}
	dutiesHistorical := HistoricalTarget{Kind: HistoricalKindAttesterDuties, Epoch: uint64Ptr(1)}

	peerDASBody := []byte(`{"code":400,"message":"BAD_REQUEST: Insufficient data columns to reconstruct blobs: required 64, but only 0 were found. You may need to run the beacon node with --supernode or --semi-supernode.","stacktraces":[]}`)
	plainBadRequest := []byte(`{"code":400,"message":"Invalid block id"}`)

	tests := []struct {
		name   string
		status int
		body   []byte
		target HistoricalTarget
		want   bool
	}{
		{"404 + historical slot", 404, nil, historicalBySlot, true},
		{"404 + historical root", 404, nil, historicalByRoot, true},
		{"404 + historical blob", 404, nil, blobHistorical, true},
		{"404 + historical duties", 404, nil, dutiesHistorical, true},
		{"404 + named head", 404, nil, namedHead, false},
		{"404 + named finalized", 404, nil, namedFinalized, false},
		{"404 + non-historical", 404, nil, nonHistorical, false},
		{"500 + historical", 500, nil, historicalBySlot, false},
		{"503 + historical", 503, nil, historicalBySlot, false},
		{"200 + historical", 200, nil, historicalBySlot, false},
		{"400 + non-blob historical (no body promotion)", 400, peerDASBody, historicalBySlot, false},
		{"400 + blob + peerdas body", 400, peerDASBody, blobHistorical, true},
		{"400 + blob + plain 400 body", 400, plainBadRequest, blobHistorical, false},
		{"400 + blob + empty body", 400, nil, blobHistorical, false},
		{"400 + blob by root + peerdas body", 400, peerDASBody, blobByRoot, true},
		{"400 + blob named head + peerdas body", 400, peerDASBody, blobNamed, false}, // named never promotes
		{"400 + non-historical + peerdas body", 400, peerDASBody, nonHistorical, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPruningError(tt.status, tt.body, tt.target); got != tt.want {
				t.Errorf("isPruningError(%d): got %v want %v", tt.status, got, tt.want)
			}
		})
	}
}
