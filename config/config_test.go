package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoad_MinimalValid(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	content := strings.TrimSpace(`
logLevel: warn
server:
  host: "127.0.0.1"
  port: 9000
  maxTimeout: 30s
failsafe:
  timeout:
    duration: 10s
health:
  checkInterval: 30s
  finalityInterval: 2m
  maxSyncDistance: 5
rateLimiting: {}
metrics:
  enabled: false
networks:
  - id: testnet
    upstreams:
      - id: a
        url: "http://127.0.0.1:5052"
    routing:
      loadBalancing: round-robin
    cache:
      enabled: false
`)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.LogLevel != "warn" {
		t.Fatalf("logLevel: got %q", cfg.LogLevel)
	}
	if cfg.Server.Port != 9000 {
		t.Fatalf("server.port: got %d", cfg.Server.Port)
	}
	if cfg.Networks[0].ID != "testnet" {
		t.Fatalf("network id: got %q", cfg.Networks[0].ID)
	}
	if cfg.Health.CheckInterval != 30*time.Second {
		t.Fatalf("health.checkInterval: got %v", cfg.Health.CheckInterval)
	}
}

func TestLoad_CORSDefaults(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	content := strings.TrimSpace(`
logLevel: warn
server:
  host: "127.0.0.1"
  port: 9000
  maxTimeout: 30s
cors: {}
failsafe:
  timeout:
    duration: 10s
health:
  checkInterval: 30s
  finalityInterval: 2m
  maxSyncDistance: 5
rateLimiting: {}
metrics:
  enabled: false
networks:
  - id: testnet
    upstreams:
      - id: a
        url: "http://127.0.0.1:5052"
    routing:
      loadBalancing: round-robin
    cache:
      enabled: false
`)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.CORS == nil {
		t.Fatal("expected cors config")
	}
	if got := cfg.CORS.AllowedOrigins; len(got) != 1 || got[0] != "*" {
		t.Fatalf("cors.allowedOrigins: got %v", got)
	}
	if got := cfg.CORS.AllowedMethods; !strings.EqualFold(strings.Join(got, ","), "GET,HEAD,POST,OPTIONS") {
		t.Fatalf("cors.allowedMethods: got %v", got)
	}
	if cfg.CORS.AllowCredentials == nil || *cfg.CORS.AllowCredentials {
		t.Fatalf("cors.allowCredentials: got %v", cfg.CORS.AllowCredentials)
	}
	if cfg.CORS.MaxAge != 3600 {
		t.Fatalf("cors.maxAge: got %d", cfg.CORS.MaxAge)
	}
}

func TestValidate_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		yaml    string
		wantSub string
	}{
		{
			name: "no networks",
			yaml: `
server: { host: "0.0.0.0", port: 5555, maxTimeout: 60s }
failsafe: { timeout: { duration: 5s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks: []
`,
			wantSub: "at least one network",
		},
		{
			name: "duplicate network id",
			yaml: `
server: { host: "0.0.0.0", port: 5555, maxTimeout: 60s }
failsafe: { timeout: { duration: 5s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: dup
    upstreams: [{ id: a, url: "http://a" }]
    routing: { loadBalancing: round-robin }
    cache: { enabled: false }
  - id: dup
    upstreams: [{ id: b, url: "http://b" }]
    routing: { loadBalancing: round-robin }
    cache: { enabled: false }
`,
			wantSub: "duplicate network id",
		},
		{
			name: "invalid loadBalancing",
			yaml: `
server: { host: "0.0.0.0", port: 5555, maxTimeout: 60s }
failsafe: { timeout: { duration: 5s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: n1
    upstreams: [{ id: a, url: "http://a" }]
    routing: { loadBalancing: invalid-mode }
    cache: { enabled: false }
`,
			wantSub: "invalid loadBalancing",
		},
		{
			name: "invalid cors origins",
			yaml: `
server: { host: "0.0.0.0", port: 5555, maxTimeout: 60s }
cors:
  allowedOrigins: []
failsafe: { timeout: { duration: 5s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: n1
    upstreams: [{ id: a, url: "http://a" }]
    routing: { loadBalancing: round-robin }
    cache: { enabled: false }
`,
			wantSub: "cors.allowedOrigins",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "ebeacon.yaml")
			if err := os.WriteFile(path, []byte(strings.TrimSpace(tt.yaml)), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Fatalf("error %q should contain %q", err.Error(), tt.wantSub)
			}
		})
	}
}

func TestEffectiveFailsafe_NetworkOverride(t *testing.T) {
	t.Parallel()
	global := &Config{
		Failsafe: FailsafeConfig{
			Timeout: &TimeoutConfig{Duration: 30 * time.Second},
			Retry:   &RetryConfig{MaxAttempts: 3},
		},
	}
	net := &NetworkConfig{
		Failsafe: &FailsafeConfig{
			Retry: &RetryConfig{MaxAttempts: 7},
		},
	}
	got := global.EffectiveFailsafe(net)
	if got.Retry == nil || got.Retry.MaxAttempts != 7 {
		t.Fatalf("expected network retry override 7, got %+v", got.Retry)
	}
	if got.Timeout == nil || got.Timeout.Duration != 30*time.Second {
		t.Fatalf("expected global timeout 30s, got %+v", got.Timeout)
	}
}

func TestEffectiveFailsafe_NetworkOverrideMergesCircuitBreaker(t *testing.T) {
	t.Parallel()
	global := &Config{
		Failsafe: FailsafeConfig{
			CircuitBreaker: &CircuitBreakerConfig{
				FailureThreshold: 5,
				SuccessThreshold: 2,
				HalfOpenAfter:    30 * time.Second,
			},
		},
	}
	net := &NetworkConfig{
		Failsafe: &FailsafeConfig{
			CircuitBreaker: &CircuitBreakerConfig{
				FailureThreshold: 9,
			},
		},
	}
	got := global.EffectiveFailsafe(net)
	if got.CircuitBreaker == nil {
		t.Fatal("expected circuit breaker config")
	}
	if got.CircuitBreaker.FailureThreshold != 9 {
		t.Fatalf("failureThreshold: got %d", got.CircuitBreaker.FailureThreshold)
	}
	if got.CircuitBreaker.SuccessThreshold != 2 {
		t.Fatalf("successThreshold should inherit global: got %d", got.CircuitBreaker.SuccessThreshold)
	}
	if got.CircuitBreaker.HalfOpenAfter != 30*time.Second {
		t.Fatalf("halfOpenAfter should inherit global: got %v", got.CircuitBreaker.HalfOpenAfter)
	}
}

func TestValidate_RoutingRuleConflict(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	bad := `
server: { host: "0.0.0.0", port: 5555, maxTimeout: 60s }
failsafe: { timeout: { duration: 5s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: n1
    upstreams: [{ id: a, url: "http://a" }]
    routing:
      loadBalancing: round-robin
      routeRules:
        - pathPattern: "^/x"
          deny: true
          upstreamId: a
    cache: { enabled: false }
`
	if err := os.WriteFile(path, []byte(strings.TrimSpace(bad)), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected mutually exclusive error, got: %v", err)
	}
}

func TestValidate_ProjectsUnsupported(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	legacy := `
server: { host: "0.0.0.0", port: 5555, maxTimeout: 60s }
failsafe: { timeout: { duration: 5s } }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
projects:
  - id: main
    networks:
      - id: mainnet
        upstreams: [{ id: a, url: "http://a" }]
        routing: { loadBalancing: round-robin }
        cache: { enabled: false }
`
	if err := os.WriteFile(path, []byte(strings.TrimSpace(legacy)), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "no longer supported") {
		t.Fatalf("expected unsupported legacy key error, got: %v", err)
	}
}

func TestEffectiveHealth_NetworkOverride(t *testing.T) {
	t.Parallel()
	global := &Config{
		Health: HealthConfig{
			CheckInterval:    15 * time.Second,
			FinalityInterval: 60 * time.Second,
			MaxSyncDistance:  10,
		},
	}
	net := &NetworkConfig{
		Health: &HealthConfig{
			MaxSyncDistance: 99,
		},
	}
	got := global.EffectiveHealth(net)
	if got.MaxSyncDistance != 99 {
		t.Fatalf("maxSyncDistance: got %d", got.MaxSyncDistance)
	}
	if got.CheckInterval != 15*time.Second {
		t.Fatalf("checkInterval should inherit global: got %v", got.CheckInterval)
	}
}

func TestValidate_UpstreamURLAndBlockedRegex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		yaml    string
		wantSub string
	}{
		{
			name: "invalid upstream url",
			yaml: `
server: { host: "0.0.0.0", port: 5555, maxTimeout: 60s }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: n1
    upstreams: [{ id: a, url: "ftp://bad" }]
    routing: { loadBalancing: round-robin }
    cache: { enabled: false }
`,
			wantSub: "must use http or https",
		},
		{
			name: "invalid blocked regex",
			yaml: `
server: { host: "0.0.0.0", port: 5555, maxTimeout: 60s }
health: { checkInterval: 1h, finalityInterval: 1h, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: n1
    upstreams: [{ id: a, url: "http://a" }]
    routing:
      loadBalancing: round-robin
      blockedPaths: ["("]
    cache: { enabled: false }
`,
			wantSub: "invalid regex",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "ebeacon.yaml")
			if err := os.WriteFile(path, []byte(strings.TrimSpace(tt.yaml)), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Fatalf("error %q should contain %q", err.Error(), tt.wantSub)
			}
		})
	}
}

func TestApplyDefaults_SetsGlobalFailsafeTimeout(t *testing.T) {
	t.Parallel()
	cfg := &Config{}
	cfg.applyDefaults()
	if cfg.Failsafe.Timeout == nil {
		t.Fatal("expected default global failsafe timeout")
	}
	if cfg.Failsafe.Timeout.Duration != 30*time.Second {
		t.Fatalf("expected default timeout 30s, got %v", cfg.Failsafe.Timeout.Duration)
	}
}
