package network

import "testing"

func TestIsPruningError(t *testing.T) {
	historicalBySlot := HistoricalTarget{Kind: HistoricalKindBlockByID, Slot: uint64Ptr(1)}
	historicalByRoot := HistoricalTarget{Kind: HistoricalKindBlockByID, Root: "0xabc"}
	namedHead := HistoricalTarget{Kind: HistoricalKindBlockByID, Named: "head"}
	namedFinalized := HistoricalTarget{Kind: HistoricalKindBlockByID, Named: "finalized"}
	nonHistorical := HistoricalTarget{}
	blobHistorical := HistoricalTarget{Kind: HistoricalKindBlobSidecars, Slot: uint64Ptr(1)}
	dutiesHistorical := HistoricalTarget{Kind: HistoricalKindAttesterDuties, Epoch: uint64Ptr(1)}

	tests := []struct {
		name   string
		status int
		target HistoricalTarget
		want   bool
	}{
		{"404 + historical slot", 404, historicalBySlot, true},
		{"404 + historical root", 404, historicalByRoot, true},
		{"404 + historical blob", 404, blobHistorical, true},
		{"404 + historical duties", 404, dutiesHistorical, true},
		{"404 + named head", 404, namedHead, false},
		{"404 + named finalized", 404, namedFinalized, false},
		{"404 + non-historical", 404, nonHistorical, false},
		{"500 + historical", 500, historicalBySlot, false},
		{"503 + historical", 503, historicalBySlot, false},
		{"200 + historical", 200, historicalBySlot, false},
		{"400 + historical", 400, historicalBySlot, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPruningError(tt.status, tt.target); got != tt.want {
				t.Errorf("isPruningError(%d): got %v want %v", tt.status, got, tt.want)
			}
		})
	}
}
