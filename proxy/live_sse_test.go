package proxy

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLiveProxy_PinnedSSEStreamsStayIsolated(t *testing.T) {
	baseURL := strings.TrimRight(os.Getenv("EBEACON_LIVE_BASE_URL"), "/")
	if baseURL == "" {
		t.Skip("set EBEACON_LIVE_BASE_URL to run live proxy SSE tests, e.g. http://127.0.0.1:15555")
	}

	networkID := envOrDefault("EBEACON_LIVE_NETWORK", "mainnet")
	upstreams := strings.Split(envOrDefault("EBEACON_LIVE_SSE_UPSTREAMS", "nimbus,caplin"), ",")
	if len(upstreams) < 2 {
		t.Skip("set EBEACON_LIVE_SSE_UPSTREAMS to two comma-separated upstream ids")
	}
	first := strings.TrimSpace(upstreams[0])
	second := strings.TrimSpace(upstreams[1])
	if first == "" || second == "" {
		t.Skip("EBEACON_LIVE_SSE_UPSTREAMS must contain two non-empty upstream ids")
	}

	type result struct {
		name         string
		upstream     string
		contentType  string
		gotEventData string
		err          error
	}

	client := &http.Client{}
	results := make(chan result, 2)
	var wg sync.WaitGroup

	run := func(name, upstreamID string) {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		url := fmt.Sprintf("%s/%s/%s/eth/v1/events?topics=head", baseURL, networkID, upstreamID)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			results <- result{name: name, err: err}
			return
		}
		req.Header.Set("Accept", "text/event-stream")

		resp, err := client.Do(req)
		if err != nil {
			results <- result{name: name, err: err}
			return
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusOK {
			results <- result{name: name, err: fmt.Errorf("status %d", resp.StatusCode)}
			return
		}

		scanner := bufio.NewScanner(resp.Body)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 512*1024)

		var dataLine string
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				dataLine = strings.TrimPrefix(line, "data: ")
				break
			}
		}
		if err := scanner.Err(); err != nil {
			results <- result{name: name, err: err}
			return
		}
		if dataLine == "" {
			results <- result{name: name, err: fmt.Errorf("no SSE data line received")}
			return
		}

		results <- result{
			name:         name,
			upstream:     resp.Header.Get("X-Ebeacon-Upstream"),
			contentType:  resp.Header.Get("Content-Type"),
			gotEventData: dataLine,
		}
	}

	wg.Add(2)
	go run("first", first)
	go run("second", second)
	wg.Wait()
	close(results)

	got := map[string]result{}
	for res := range results {
		if res.err != nil {
			t.Fatalf("%s stream failed: %v", res.name, res.err)
		}
		got[res.name] = res
	}

	if got["first"].upstream != first {
		t.Fatalf("first stream upstream: got %q want %q", got["first"].upstream, first)
	}
	if got["second"].upstream != second {
		t.Fatalf("second stream upstream: got %q want %q", got["second"].upstream, second)
	}
	if !strings.Contains(strings.ToLower(got["first"].contentType), "text/event-stream") {
		t.Fatalf("first stream content-type: %q", got["first"].contentType)
	}
	if !strings.Contains(strings.ToLower(got["second"].contentType), "text/event-stream") {
		t.Fatalf("second stream content-type: %q", got["second"].contentType)
	}
	if got["first"].gotEventData == "" || got["second"].gotEventData == "" {
		t.Fatal("expected both SSE streams to receive at least one data event")
	}
}
