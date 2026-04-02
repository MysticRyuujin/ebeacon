package state

import (
	"testing"
	"time"
)

func TestLocalState_PublishAndSubscribeHead(t *testing.T) {
	t.Parallel()
	st := NewLocalState()
	defer st.Close() //nolint:errcheck

	st.PublishHead("sepolia", 123, "0xabc")

	select {
	case up := <-st.SubscribeHead():
		if up.Network != "sepolia" || up.Slot != 123 || up.Root != "0xabc" {
			t.Fatalf("unexpected head update: %+v", up)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for head update")
	}
}

func TestLocalState_PublishFinalizedMonotonic(t *testing.T) {
	t.Parallel()
	st := NewLocalState()
	defer st.Close() //nolint:errcheck

	st.PublishFinalized(10)
	st.PublishFinalized(8)
	st.PublishFinalized(12)

	if got := st.GetFinalized(); got != 12 {
		t.Fatalf("finalized epoch should be monotonic max, got %d", got)
	}
}
