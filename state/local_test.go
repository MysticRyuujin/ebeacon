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

	st.PublishFinalized("mainnet", 10)
	st.PublishFinalized("mainnet", 8)
	st.PublishFinalized("mainnet", 12)

	if got := st.GetFinalized("mainnet"); got != 12 {
		t.Fatalf("finalized epoch should be monotonic max, got %d", got)
	}
}

func TestLocalState_FinalizedIsPerNetwork(t *testing.T) {
	t.Parallel()
	st := NewLocalState()
	defer st.Close() //nolint:errcheck

	st.PublishFinalized("mainnet", 300000)
	st.PublishFinalized("sepolia", 5000)

	if got := st.GetFinalized("sepolia"); got != 5000 {
		t.Fatalf("sepolia finalized epoch contaminated by mainnet: got %d", got)
	}
	if got := st.GetFinalized("hoodi"); got != 0 {
		t.Fatalf("unknown network must report 0, got %d", got)
	}
}

func TestLocalState_PublishHeadAfterCloseDoesNotPanic(t *testing.T) {
	t.Parallel()
	st := NewLocalState()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 100 {
			st.PublishHead("mainnet", uint64(i+1), "0xabc")
		}
	}()
	st.Close() //nolint:errcheck
	<-done
}
