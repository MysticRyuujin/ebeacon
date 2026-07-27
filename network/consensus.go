package network

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"sync"

	"github.com/mysticryuujin/ebeacon/config"
	"github.com/mysticryuujin/ebeacon/upstream"
)

// ConsensusPolicy sends a request to multiple upstreams and returns the majority result.
type ConsensusPolicy struct {
	MaxParticipants    int
	AgreementThreshold int
	MaxBodyBytes       int64 // 0 = unlimited
}

// NewConsensusPolicy creates a ConsensusPolicy from config.
func NewConsensusPolicy(cfg *config.ConsensusConfig) *ConsensusPolicy {
	if cfg == nil || !cfg.Enabled {
		return nil
	}
	maxP := cfg.MaxParticipants
	if maxP <= 0 {
		maxP = 3
	}
	thresh := cfg.AgreementThreshold
	if thresh <= 0 {
		thresh = 2
	}
	return &ConsensusPolicy{
		MaxParticipants:    maxP,
		AgreementThreshold: thresh,
	}
}

type consensusResult struct {
	body     []byte
	status   int
	headers  http.Header
	upstream *upstream.Upstream
	err      error
}

// Execute sends the request to up to MaxParticipants upstreams and returns the majority response.
func (cp *ConsensusPolicy) Execute(
	ctx context.Context,
	upstreams []*upstream.Upstream,
	buildReq func(u *upstream.Upstream) (*http.Request, error),
) (*http.Response, *upstream.Upstream, error) {
	n := cp.MaxParticipants
	if n > len(upstreams) {
		n = len(upstreams)
	}
	if n == 0 {
		return nil, nil, fmt.Errorf("no upstreams for consensus")
	}

	var wg sync.WaitGroup
	results := make([]consensusResult, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			u := upstreams[idx]
			req, err := buildReq(u)
			if err != nil {
				results[idx] = consensusResult{err: err, upstream: u}
				return
			}
			req = req.WithContext(ctx)
			u.ConsumeRateToken()
			u.IncrActive()
			resp, err := u.Client.Do(req)
			if err != nil {
				u.DecrActive()
				// A client disconnect cancels every participant's request;
				// that is not an upstream fault, so don't open breakers.
				if !isClientCancel(ctx, err) {
					u.CBFailure()
				}
				results[idx] = consensusResult{err: err, upstream: u}
				return
			}
			body, readErr := readBodyCapped(resp.Body, cp.MaxBodyBytes)
			resp.Body.Close() //nolint:errcheck
			u.DecrActive()
			u.RecordResponseStatus(resp.StatusCode)
			if readErr != nil {
				u.CBFailure()
				results[idx] = consensusResult{err: readErr, upstream: u}
				return
			}
			if resp.StatusCode < 500 {
				u.CBSuccess()
			} else {
				u.CBFailure()
				u.RecordError()
			}
			results[idx] = consensusResult{
				body:     body,
				status:   resp.StatusCode,
				headers:  resp.Header,
				upstream: u,
			}
		}(i)
	}
	wg.Wait()

	// Count matching responses by body content.
	// Use a FNV-64a hash as the map key to avoid copying potentially large
	// beacon state response bodies (e.g. /eth/v2/beacon/states) into strings.
	// Cryptographic strength is not needed here — we only need equality comparison
	// across a handful of trusted upstream responses.
	type bodyKey struct {
		status int
		body   uint64
	}
	hashBody := func(b []byte) uint64 {
		h := fnv.New64a()
		h.Write(b) //nolint:errcheck
		return h.Sum64()
	}
	counts := make(map[bodyKey]int)
	for _, r := range results {
		if r.err != nil {
			continue
		}
		counts[bodyKey{status: r.status, body: hashBody(r.body)}]++
	}

	// Pick the largest agreeing group, walking results in participant order so
	// ties break deterministically instead of by map iteration.
	var best consensusResult
	bestCount := 0
	for _, r := range results {
		if r.err != nil {
			continue
		}
		if n := counts[bodyKey{status: r.status, body: hashBody(r.body)}]; n > bestCount {
			best = r
			bestCount = n
		}
	}
	if bestCount >= cp.AgreementThreshold {
		resp := &http.Response{
			StatusCode: best.status,
			Header:     best.headers,
			Body:       io.NopCloser(bytes.NewReader(best.body)),
		}
		return resp, best.upstream, nil
	}

	return nil, nil, fmt.Errorf("consensus not reached: %d participants, need %d agreement", n, cp.AgreementThreshold)
}
