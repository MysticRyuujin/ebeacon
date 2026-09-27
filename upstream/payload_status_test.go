package upstream

import (
	"testing"

	"github.com/mysticryuujin/ebeacon/config"
)

func newPayloadTestUpstream(t *testing.T) *Upstream {
	t.Helper()
	return New("payload_"+t.Name(), config.UpstreamConfig{ID: "u1", URL: "http://127.0.0.1:1"}, nil, 0)
}

// payloadGaugePresent probes by deleting and then restores the series, so the
// probe itself never creates one.
func payloadGaugePresent(u *Upstream) bool {
	if !metricHeadPayloadFull.DeleteLabelValues(u.NetworkID, u.ID) {
		return false
	}
	u.mu.Lock()
	u.syncPayloadGaugeLocked()
	u.mu.Unlock()
	return true
}

func TestHeadPayloadStatus_MatchesCurrentHead(t *testing.T) {
	t.Parallel()
	u := newPayloadTestUpstream(t)
	u.UpdateHeadBlock(100, "0xaa")

	u.SetHeadPayloadStatus("0xaa", false)
	if got := u.HeadPayloadStatus(); got != "empty" {
		t.Fatalf("status = %q, want empty", got)
	}
	u.SetHeadPayloadStatus("0xaa", true)
	if got := u.HeadPayloadStatus(); got != "full" {
		t.Fatalf("status = %q, want full", got)
	}
	if !payloadGaugePresent(u) {
		t.Fatal("gauge series missing for a known status")
	}

	u.UpdateHeadBlock(101, "0xbb")
	if got := u.HeadPayloadStatus(); got != "unknown" {
		t.Fatalf("status after new head = %q, want unknown", got)
	}
	if payloadGaugePresent(u) {
		t.Fatal("gauge series must be removed when the head root changes")
	}
}

func TestHeadPayloadStatus_EventBeforeHead(t *testing.T) {
	t.Parallel()
	u := newPayloadTestUpstream(t)
	u.UpdateHeadBlock(100, "0xaa")

	u.SetHeadPayloadStatus("0xbb", false)
	if got := u.HeadPayloadStatus(); got != "unknown" {
		t.Fatalf("status before head = %q, want unknown", got)
	}
	u.UpdateHeadBlock(101, "0xbb")
	if got := u.HeadPayloadStatus(); got != "empty" {
		t.Fatalf("status = %q, want empty", got)
	}
	if !payloadGaugePresent(u) {
		t.Fatal("gauge series missing after the head caught up")
	}
}

func TestHeadPayloadStatus_Clear(t *testing.T) {
	t.Parallel()
	u := newPayloadTestUpstream(t)
	u.UpdateHeadBlock(100, "0xaa")
	u.SetHeadPayloadStatus("0xaa", true)

	u.ClearHeadPayloadStatus()
	if got := u.HeadPayloadStatus(); got != "unknown" {
		t.Fatalf("status = %q, want unknown", got)
	}
	if payloadGaugePresent(u) {
		t.Fatal("gauge series must be removed on clear")
	}
}
