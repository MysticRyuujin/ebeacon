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

func TestLoad_DebugLoggingDefaultsAndEnvExpansion(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("EBEACON_DEBUG_LOG", filepath.Join(dir, "debug.log"))
	path := filepath.Join(dir, "ebeacon.yaml")
	content := strings.TrimSpace(`
logLevel: warn
debugLogging:
  enabled: true
  path: "${EBEACON_DEBUG_LOG}"
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
	if !cfg.DebugLogging.Enabled {
		t.Fatal("expected debug logging to be enabled")
	}
	if cfg.DebugLogging.Path != filepath.Join(dir, "debug.log") {
		t.Fatalf("debugLogging.path: got %q", cfg.DebugLogging.Path)
	}
	if cfg.DebugLogging.MaxSizeMB != 100 {
		t.Fatalf("debugLogging.maxSizeMB: got %d", cfg.DebugLogging.MaxSizeMB)
	}
	if cfg.DebugLogging.MaxBackups != 10 {
		t.Fatalf("debugLogging.maxBackups: got %d", cfg.DebugLogging.MaxBackups)
	}
	if cfg.DebugLogging.MaxBodyBytes != 64<<10 {
		t.Fatalf("debugLogging.maxBodyBytes: got %d", cfg.DebugLogging.MaxBodyBytes)
	}
}

func TestValidate_DebugLoggingRequiresPathWhenEnabled(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	content := strings.TrimSpace(`
logLevel: warn
debugLogging:
  enabled: true
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

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "debugLogging.path") {
		t.Fatalf("error %q should mention debugLogging.path", err.Error())
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

func TestLoad_UpstreamHeaderAndURLEnvExpansion(t *testing.T) {
	t.Setenv("EBEACON_TEST_UPSTREAM_TOKEN", "real-token-9001")
	t.Setenv("EBEACON_TEST_UPSTREAM_HOST", "node.example.com")
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	content := strings.TrimSpace(`
logLevel: warn
server: { host: "127.0.0.1", port: 9000, maxTimeout: 30s }
failsafe: { timeout: { duration: 10s } }
health: { checkInterval: 30s, finalityInterval: 2m, maxSyncDistance: 5 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: testnet
    upstreams:
      - id: a
        url: "https://${EBEACON_TEST_UPSTREAM_HOST}:5052"
        headers:
          Authorization: "Bearer ${EBEACON_TEST_UPSTREAM_TOKEN}"
          X-Static: "no-expansion-here"
    routing: { loadBalancing: round-robin }
    cache: { enabled: false }
`)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	u := cfg.Networks[0].Upstreams[0]
	if u.URL != "https://node.example.com:5052" {
		t.Fatalf("upstream URL: got %q", u.URL)
	}
	if got := u.Headers["Authorization"]; got != "Bearer real-token-9001" {
		t.Fatalf("Authorization header: got %q", got)
	}
	if got := u.Headers["X-Static"]; got != "no-expansion-here" {
		t.Fatalf("X-Static header should be untouched: got %q", got)
	}
}

func TestLoad_RedisUsernameEnvExpansion(t *testing.T) {
	t.Setenv("EBEACON_TEST_REDIS_USER", "ebeacon-user")
	t.Setenv("EBEACON_TEST_REDIS_PASS", "ebeacon-pass")
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	content := strings.TrimSpace(`
logLevel: warn
server: { host: "127.0.0.1", port: 9000, maxTimeout: 30s }
failsafe: { timeout: { duration: 10s } }
health: { checkInterval: 30s, finalityInterval: 2m, maxSyncDistance: 5 }
rateLimiting: {}
metrics: { enabled: false }
state:
  driver: redis
  redis:
    url: "redis://127.0.0.1:6379/0"
    username: "${EBEACON_TEST_REDIS_USER}"
    password: "${EBEACON_TEST_REDIS_PASS}"
networks:
  - id: testnet
    upstreams:
      - id: a
        url: "http://127.0.0.1:5052"
    routing: { loadBalancing: round-robin }
    cache:
      enabled: true
      maxSize: 10
      driver: redis
      redis:
        url: "redis://127.0.0.1:6379/1"
        username: "${EBEACON_TEST_REDIS_USER}"
        password: "${EBEACON_TEST_REDIS_PASS}"
`)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.State.Redis.Username; got != "ebeacon-user" {
		t.Fatalf("state.redis.username: got %q", got)
	}
	if got := cfg.State.Redis.Password; got != "ebeacon-pass" {
		t.Fatalf("state.redis.password: got %q", got)
	}
	if got := cfg.Networks[0].Cache.Redis.Username; got != "ebeacon-user" {
		t.Fatalf("cache.redis.username: got %q", got)
	}
	if got := cfg.Networks[0].Cache.Redis.Password; got != "ebeacon-pass" {
		t.Fatalf("cache.redis.password: got %q", got)
	}
}

func TestValidate_ConsensusBounds(t *testing.T) {
	tests := []struct {
		name    string
		fs      FailsafeConfig
		wantSub string
	}{
		{
			name: "maxParticipants zero",
			fs: FailsafeConfig{
				Consensus: &ConsensusConfig{Enabled: true, MaxParticipants: 0, AgreementThreshold: 1},
			},
			wantSub: "maxParticipants must be >= 1",
		},
		{
			name: "agreementThreshold zero",
			fs: FailsafeConfig{
				Consensus: &ConsensusConfig{Enabled: true, MaxParticipants: 3, AgreementThreshold: 0},
			},
			wantSub: "agreementThreshold must be >= 1",
		},
		{
			name: "threshold exceeds participants",
			fs: FailsafeConfig{
				Consensus: &ConsensusConfig{Enabled: true, MaxParticipants: 3, AgreementThreshold: 5},
			},
			wantSub: "cannot exceed consensus.maxParticipants",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateFailsafe(&tt.fs, "test")
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Fatalf("error %q should contain %q", err.Error(), tt.wantSub)
			}
		})
	}
}

func TestValidate_ConsensusOK(t *testing.T) {
	t.Parallel()
	fs := FailsafeConfig{
		Consensus: &ConsensusConfig{Enabled: true, MaxParticipants: 3, AgreementThreshold: 2},
	}
	if err := validateFailsafe(&fs, "test"); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestValidate_ConsensusDisabledSkipsBounds(t *testing.T) {
	t.Parallel()
	fs := FailsafeConfig{
		Consensus: &ConsensusConfig{Enabled: false, MaxParticipants: 0, AgreementThreshold: 0},
	}
	if err := validateFailsafe(&fs, "test"); err != nil {
		t.Fatalf("disabled consensus should skip bounds, got %v", err)
	}
}

func TestEffectiveFailsafe_NetworkConsensusOverride(t *testing.T) {
	t.Parallel()
	global := &Config{
		Failsafe: FailsafeConfig{
			Consensus: &ConsensusConfig{Enabled: false},
		},
	}
	net := &NetworkConfig{
		Failsafe: &FailsafeConfig{
			Consensus: &ConsensusConfig{Enabled: true, MaxParticipants: 3, AgreementThreshold: 2},
		},
	}
	got := global.EffectiveFailsafe(net)
	if got.Consensus == nil || !got.Consensus.Enabled {
		t.Fatalf("expected network consensus override, got %+v", got.Consensus)
	}
	if got.Consensus.MaxParticipants != 3 || got.Consensus.AgreementThreshold != 2 {
		t.Fatalf("consensus fields lost in merge: %+v", got.Consensus)
	}
}

func TestEffectiveFailsafe_NetworkBlockInheritsGlobalTimeout(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	content := strings.TrimSpace(`
server: { host: "0.0.0.0", port: 5555, maxTimeout: 60s }
failsafe:
  timeout: { duration: 120s }
  retry: { maxAttempts: 5 }
health: { checkInterval: 15s, finalityInterval: 60s, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: n1
    upstreams: [{ id: a, url: "http://a" }]
    failsafe:
      retry: { delay: 50ms }
    routing: { loadBalancing: round-robin }
    cache: { enabled: false }
`)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := cfg.EffectiveFailsafe(&cfg.Networks[0])
	if got.Timeout == nil || got.Timeout.Duration != 120*time.Second {
		t.Fatalf("network failsafe block must inherit global timeout 120s, got %+v", got.Timeout)
	}
	if got.Retry == nil || got.Retry.MaxAttempts != 5 {
		t.Fatalf("partial retry override must inherit global maxAttempts 5, got %+v", got.Retry)
	}
	if got.Retry.Delay != 50*time.Millisecond {
		t.Fatalf("network retry delay override lost, got %v", got.Retry.Delay)
	}
}

func TestEffectiveHealth_MergesFollowAndHeadDistance(t *testing.T) {
	t.Parallel()
	global := &Config{
		Health: HealthConfig{
			CheckInterval:    15 * time.Second,
			FinalityInterval: 60 * time.Second,
			MaxSyncDistance:  10,
			FollowDistance:   32,
			MaxHeadDistance:  2,
		},
	}
	net := &NetworkConfig{
		Health: &HealthConfig{
			FollowDistance:  64,
			MaxHeadDistance: 6,
		},
	}
	got := global.EffectiveHealth(net)
	if got.FollowDistance != 64 {
		t.Fatalf("followDistance: got %d", got.FollowDistance)
	}
	if got.MaxHeadDistance != 6 {
		t.Fatalf("maxHeadDistance: got %d", got.MaxHeadDistance)
	}
	if got.CheckInterval != 15*time.Second {
		t.Fatalf("checkInterval should inherit global: got %v", got.CheckInterval)
	}
}

func TestLoad_PartialNetworkHealthAccepted(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	content := strings.TrimSpace(`
server: { host: "0.0.0.0", port: 5555, maxTimeout: 60s }
health: { checkInterval: 15s, finalityInterval: 60s, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: n1
    upstreams: [{ id: a, url: "http://a" }]
    health: { maxHeadDistance: 6 }
    routing: { loadBalancing: round-robin }
    cache: { enabled: false }
`)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("partial network health block must be accepted: %v", err)
	}
	if got := cfg.EffectiveHealth(&cfg.Networks[0]); got.MaxHeadDistance != 6 || got.CheckInterval != 15*time.Second {
		t.Fatalf("effective health merge wrong: %+v", got)
	}
}

func TestValidate_DriverStrings(t *testing.T) {
	t.Parallel()
	base := `
server: { host: "0.0.0.0", port: 5555, maxTimeout: 60s }
health: { checkInterval: 15s, finalityInterval: 60s, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
%s
networks:
  - id: n1
    upstreams: [{ id: a, url: "http://a" }]
    routing: { loadBalancing: round-robin }
    cache: { enabled: %s }
`
	tests := []struct {
		name    string
		state   string
		cache   string
		wantSub string
	}{
		{"state typo", `state: { driver: Redis }`, "false", `state.driver`},
		{"cache typo", ``, `true, driver: rediss `, `cache.driver`},
		{"cache redis missing url", ``, `true, driver: redis `, `cache.redis.url`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "ebeacon.yaml")
			content := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(base, "%s\nnetworks", tt.state+"\nnetworks"), "enabled: %s", "enabled: "+tt.cache))
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), tt.wantSub) {
				t.Fatalf("expected error containing %q, got: %v", tt.wantSub, err)
			}
		})
	}
}

func TestValidate_BareIntegerDurationRejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "ebeacon.yaml")
	content := strings.TrimSpace(`
server: { host: "0.0.0.0", port: 5555, maxTimeout: 60s }
health: { checkInterval: 15, finalityInterval: 60s, maxSyncDistance: 10 }
rateLimiting: {}
metrics: { enabled: false }
networks:
  - id: n1
    upstreams: [{ id: a, url: "http://a" }]
    routing: { loadBalancing: round-robin }
    cache: { enabled: false }
`)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "cannot unmarshal") {
		t.Fatalf("bare integer duration must fail yaml parsing, got: %v", err)
	}
}
