package proxy

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLiveProxy_MainnetTekuVersion(t *testing.T) {
	baseURL := strings.TrimRight(os.Getenv("EBEACON_LIVE_BASE_URL"), "/")
	if baseURL == "" {
		t.Skip("set EBEACON_LIVE_BASE_URL to run live proxy tests, e.g. http://127.0.0.1:15555")
	}

	networkID := envOrDefault("EBEACON_LIVE_NETWORK", "mainnet")
	pinnedUpstream := envOrDefault("EBEACON_LIVE_PINNED_UPSTREAM", "teku")
	client := &http.Client{Timeout: 10 * time.Second}

	t.Run("pinned client route returns expected client version", func(t *testing.T) {
		url := fmt.Sprintf("%s/%s/%s/eth/v1/node/version", baseURL, networkID, pinnedUpstream)
		resp, body := mustLiveGET(t, client, url)
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status: got %d body %q", resp.StatusCode, string(body))
		}
		if !strings.Contains(strings.ToLower(string(body)), strings.ToLower(pinnedUpstream)) {
			t.Fatalf("body %q should mention pinned upstream %q", string(body), pinnedUpstream)
		}
	})

	t.Run("default network route returns JSON", func(t *testing.T) {
		url := fmt.Sprintf("%s/%s/eth/v1/node/version", baseURL, networkID)
		resp, body := mustLiveGET(t, client, url)
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status: got %d body %q", resp.StatusCode, string(body))
		}
		if ct := resp.Header.Get("Content-Type"); !strings.Contains(strings.ToLower(ct), "application/json") {
			t.Fatalf("content-type: got %q", ct)
		}
	})
}

func mustLiveGET(t *testing.T, client *http.Client, url string) (*http.Response, []byte) {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		resp.Body.Close() //nolint:errcheck
		t.Fatalf("read %s: %v", url, err)
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	return resp, body
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
