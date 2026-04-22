package config

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	LogLevel       string             `yaml:"logLevel"`
	DebugLogging   DebugLoggingConfig `yaml:"debugLogging"`
	Server         ServerConfig       `yaml:"server"`
	Pprof          PprofConfig        `yaml:"pprof"`
	CORS           *CORSConfig        `yaml:"cors"`
	Auth           *AuthConfig        `yaml:"auth"`         // optional global auth for network routes
	Networks       []NetworkConfig    `yaml:"networks"`     // top-level network routing
	LegacyProjects yaml.Node          `yaml:"projects"`     // unsupported legacy config key
	Failsafe       FailsafeConfig     `yaml:"failsafe"`     // global defaults
	RateLimiting   RateLimitingConfig `yaml:"rateLimiting"` // global defaults
	Health         HealthConfig       `yaml:"health"`       // global defaults
	Metrics        MetricsConfig      `yaml:"metrics"`
	State          StateConfig        `yaml:"state"`
	UI             UIConfig           `yaml:"ui"`
}

// PprofConfig controls the optional pprof debug HTTP server.
type PprofConfig struct {
	Enabled bool   `yaml:"enabled"`
	Host    string `yaml:"host"` // default "127.0.0.1"
	Port    int    `yaml:"port"` // default 6060
}

// AuthConfig supports secret-based authentication with optional named API keys.
// Use header X-EBEACON-Secret-Token, X-API-Key, Authorization: Bearer, ?secret=,
// or embed the key in the URL path.
type AuthConfig struct {
	Secret string         `yaml:"secret"` // backward compat: single unnamed key (id defaults to "default")
	Keys   []APIKeyConfig `yaml:"keys"`   // named API keys with optional tier assignment
	Tiers  []TierConfig   `yaml:"tiers"`  // tier definitions with per-tier rate limits
}

// TierConfig defines a named tier (e.g. "free", "premium") and its rate-limit budget.
// API keys reference a tier by name; the tier's rate limits override global/network defaults.
type TierConfig struct {
	Name         string           `yaml:"name"`
	Description  string           `yaml:"description"`
	RateLimiting *RateLimitConfig `yaml:"rateLimiting"` // overrides global/network rateLimiting for this tier
}

// APIKeyConfig defines a named API key for authentication, per-key metrics, and rate limiting.
type APIKeyConfig struct {
	ID           string           `yaml:"id"`
	Description  string           `yaml:"description"`
	Secret       string           `yaml:"secret"`
	Tier         string           `yaml:"tier"`         // optional: assign this key to a tier (see AuthConfig.Tiers)
	RateLimiting *RateLimitConfig `yaml:"rateLimiting"` // per-key override; takes precedence over tier rate limits
}

type ServerConfig struct {
	Host       string        `yaml:"host"`
	Port       int           `yaml:"port"`
	MaxTimeout time.Duration `yaml:"maxTimeout"`
	EnableGzip *bool         `yaml:"enableGzip"` // nil = true
}

type DebugLoggingConfig struct {
	Enabled      bool   `yaml:"enabled"`
	Path         string `yaml:"path"`
	MaxSizeMB    int    `yaml:"maxSizeMB"`
	MaxBackups   int    `yaml:"maxBackups"`
	MaxBodyBytes int    `yaml:"maxBodyBytes"`
}

type CORSConfig struct {
	AllowedOrigins   []string `yaml:"allowedOrigins"`
	AllowedMethods   []string `yaml:"allowedMethods"`
	AllowedHeaders   []string `yaml:"allowedHeaders"`
	ExposedHeaders   []string `yaml:"exposedHeaders"`
	AllowCredentials *bool    `yaml:"allowCredentials"`
	MaxAge           int      `yaml:"maxAge"`
}

// NetworkConfig represents a single beacon chain network (e.g. mainnet, hoodi, sepolia).
type NetworkConfig struct {
	ID                string              `yaml:"id"`
	GenesisTime       int64               `yaml:"genesisTime"` // unix timestamp of network genesis; enables slot-boundary TTL alignment
	Upstreams         []UpstreamConfig    `yaml:"upstreams"`
	Failsafe          *FailsafeConfig     `yaml:"failsafe"` // overrides global
	FailsafeOverrides []FailsafeOverride  `yaml:"failsafeOverrides"`
	Routing           RoutingConfig       `yaml:"routing"`
	Cache             CacheConfig         `yaml:"cache"`
	Health            *HealthConfig       `yaml:"health"`       // overrides global
	RateLimiting      *RateLimitingConfig `yaml:"rateLimiting"` // overrides global
}

// FailsafeOverride allows per-path failsafe configuration.
type FailsafeOverride struct {
	PathPattern string         `yaml:"pathPattern"`
	Methods     []string       `yaml:"methods"`
	Failsafe    FailsafeConfig `yaml:"failsafe"`
}

type UpstreamConfig struct {
	ID           string                   `yaml:"id"`
	URL          string                   `yaml:"url"`
	Headers      map[string]string        `yaml:"headers"`
	Failsafe     *FailsafeConfig          `yaml:"failsafe"` // overrides network/global
	Priority     int                      `yaml:"priority"` // lower = preferred (default 0)
	Weight       int                      `yaml:"weight"`   // relative weight within same priority (default 1)
	Archive      bool                     `yaml:"archive"`  // true = serves historical data pruned nodes may 404 for
	RateLimiting *UpstreamRateLimitConfig `yaml:"rateLimiting"`
}

// UpstreamRateLimitConfig enables per-upstream rate limit auto-tuning.
type UpstreamRateLimitConfig struct {
	AutoTune           bool          `yaml:"autoTune"`
	InitialRate        float64       `yaml:"initialRate"`
	MinRate            float64       `yaml:"minRate"`
	MaxRate            float64       `yaml:"maxRate"`
	AdjustmentPeriod   time.Duration `yaml:"adjustmentPeriod"`
	ErrorRateThreshold float64       `yaml:"errorRateThreshold"`
}

type FailsafeConfig struct {
	Timeout        *TimeoutConfig        `yaml:"timeout"`
	Retry          *RetryConfig          `yaml:"retry"`
	Hedge          *HedgeConfig          `yaml:"hedge"`
	CircuitBreaker *CircuitBreakerConfig `yaml:"circuitBreaker"`
	Consensus      *ConsensusConfig      `yaml:"consensus"`
}

// ConsensusConfig sends the same request to N upstreams and requires M agreement.
type ConsensusConfig struct {
	Enabled            bool `yaml:"enabled"`
	MaxParticipants    int  `yaml:"maxParticipants"`
	AgreementThreshold int  `yaml:"agreementThreshold"`
}

type TimeoutConfig struct {
	Duration time.Duration `yaml:"duration"`
}

type RetryConfig struct {
	MaxAttempts int           `yaml:"maxAttempts"`
	Delay       time.Duration `yaml:"delay"`
	Backoff     float64       `yaml:"backoff"`
	Jitter      time.Duration `yaml:"jitter"`
	MaxDelay    time.Duration `yaml:"maxDelay"`
}

type HedgeConfig struct {
	Delay    time.Duration `yaml:"delay"`
	MaxCount int           `yaml:"maxCount"`
}

type CircuitBreakerConfig struct {
	FailureThreshold int           `yaml:"failureThreshold"`
	SuccessThreshold int           `yaml:"successThreshold"`
	HalfOpenAfter    time.Duration `yaml:"halfOpenAfter"`
}

type RoutingConfig struct {
	LoadBalancing      string        `yaml:"loadBalancing"` // round-robin, random, least-conn, score
	StickySession      bool          `yaml:"stickySession"`
	StickyTimeout      time.Duration `yaml:"stickyTimeout"`
	BlockedPaths       []string      `yaml:"blockedPaths"`
	RebalanceInterval  time.Duration `yaml:"rebalanceInterval"`
	RebalanceThreshold float64       `yaml:"rebalanceThreshold"` // multiplier over mean (default 1.5)
	RebalanceMaxSweep  int           `yaml:"rebalanceMaxSweep"`  // max sessions moved per tick (default 10)

	ScoreWeights    *ScoreWeightsConfig `yaml:"scoreWeights"`
	ScoreWindowSize int                 `yaml:"scoreWindowSize"` // rolling window size (default 100)

	// ClientRoutes: dugtrio-style path prefixes (e.g. /lighthouse/) that hard-pin traffic to one upstream.
	// When the remainder starts with eth/v, the prefix is stripped for the upstream call (standard Beacon API).
	// If the selected client is unavailable, eBeacon returns 503 instead of failing over to a different client.
	ClientRoutes []ClientRouteConfig `yaml:"clientRoutes"`

	// RouteRules: ordered rules; first match wins. Path is matched after client prefix stripping (regex on URL path).
	// Use for per-endpoint upstream selection or blocking (deny).
	RouteRules []RouteRuleConfig `yaml:"routeRules"`
}

// ScoreWeightsConfig configures the weights for score-based upstream routing.
type ScoreWeightsConfig struct {
	ErrorRate    float64 `yaml:"errorRate"`    // default 4.0
	Latency      float64 `yaml:"latency"`      // default 8.0
	HeadLag      float64 `yaml:"headLag"`      // default 2.0
	SyncDistance float64 `yaml:"syncDistance"` // default 1.0
}

// ClientRouteConfig maps a URL prefix to a single upstream id or client type selector (e.g. /lighthouse/ → lighthouse).
type ClientRouteConfig struct {
	PathPrefix string `yaml:"pathPrefix"`
	UpstreamID string `yaml:"upstreamId"`
}

// RouteRuleConfig selects an upstream or denies the request for matching HTTP method + path.
type RouteRuleConfig struct {
	PathPattern string   `yaml:"pathPattern"` // regex, matched against path (e.g. ^/eth/v1/node/version$)
	Methods     []string `yaml:"methods"`     // empty = any method (GET, POST, ...)
	UpstreamID  string   `yaml:"upstreamId"`  // if set, route to this upstream
	Deny        bool     `yaml:"deny"`        // if true, return 403 (no upstream)
}

type RateLimitingConfig struct {
	PerIP  *RateLimitConfig `yaml:"perIP"`
	Global *RateLimitConfig `yaml:"global"`
}

type RateLimitConfig struct {
	Limit float64 `yaml:"limit"`
	Burst int     `yaml:"burst"`
}

type HealthConfig struct {
	CheckInterval    time.Duration `yaml:"checkInterval"`
	FinalityInterval time.Duration `yaml:"finalityInterval"`
	MaxSyncDistance  uint64        `yaml:"maxSyncDistance"`
	FollowDistance   uint64        `yaml:"followDistance"`  // block cache depth (default 32)
	MaxHeadDistance  uint64        `yaml:"maxHeadDistance"` // max slots behind canonical head (default 2)
}

// CacheConfig controls per-network response caching.
type CacheConfig struct {
	Enabled  bool              `yaml:"enabled"`
	MaxSize  int               `yaml:"maxSize"`
	Driver   string            `yaml:"driver"` // memory (default), redis
	Redis    *RedisCacheConfig `yaml:"redis"`
	Policies []CachePolicy     `yaml:"policies"`
}

// RedisCacheConfig configures the Redis cache backend.
type RedisCacheConfig struct {
	URL        string `yaml:"url"`
	Username   string `yaml:"username"` // overrides URL username; supports ${ENV_VAR}
	Password   string `yaml:"password"` // overrides URL password; supports ${ENV_VAR}
	DB         int    `yaml:"db"`       // overrides URL database index (default 0)
	KeyPrefix  string `yaml:"keyPrefix"`
	MaxRetries int    `yaml:"maxRetries"`
}

// CachePolicy maps a path pattern to a TTL. TTL=0 means cache forever.
type CachePolicy struct {
	Pattern string        `yaml:"pattern"`
	TTL     time.Duration `yaml:"ttl"`
	Methods []string      `yaml:"methods"` // empty defaults to [GET]
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
}

// StateConfig controls shared state for horizontal scaling.
type StateConfig struct {
	Driver string            `yaml:"driver"` // local (default), redis
	Redis  *RedisStateConfig `yaml:"redis"`
}

// RedisStateConfig for shared state via Redis pub/sub.
type RedisStateConfig struct {
	URL        string `yaml:"url"`
	Username   string `yaml:"username"` // overrides URL username; supports ${ENV_VAR}
	Password   string `yaml:"password"` // overrides URL password; supports ${ENV_VAR}
	DB         int    `yaml:"db"`       // overrides URL database index (default 0)
	MaxRetries int    `yaml:"maxRetries"`
}

// UIConfig controls the embedded web dashboard.
type UIConfig struct {
	Enabled  bool        `yaml:"enabled"`
	BasePath string      `yaml:"basePath"` // default /webui
	Auth     *AuthConfig `yaml:"auth"`     // required when Enabled is true
}

// GzipEnabled returns whether gzip compression is enabled (default true).
func (s ServerConfig) GzipEnabled() bool {
	if s.EnableGzip == nil {
		return true
	}
	return *s.EnableGzip
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.applyDefaults()
	cfg.expandSecrets()
	return cfg, cfg.validate()
}

// EffectiveFailsafe merges global defaults with per-network overrides.
func (c *Config) EffectiveFailsafe(net *NetworkConfig) FailsafeConfig {
	result := c.Failsafe
	if net.Failsafe == nil {
		return result
	}
	n := net.Failsafe
	if n.Timeout != nil {
		result.Timeout = mergeTimeoutConfig(result.Timeout, n.Timeout)
	}
	if n.Retry != nil {
		result.Retry = mergeRetryConfig(result.Retry, n.Retry)
	}
	if n.Hedge != nil {
		result.Hedge = mergeHedgeConfig(result.Hedge, n.Hedge)
	}
	if n.CircuitBreaker != nil {
		result.CircuitBreaker = mergeCircuitBreakerConfig(result.CircuitBreaker, n.CircuitBreaker)
	}
	return result
}

// EffectiveHealth merges global health config with per-network overrides.
func (c *Config) EffectiveHealth(net *NetworkConfig) HealthConfig {
	if net.Health == nil {
		return c.Health
	}
	result := c.Health
	if net.Health.CheckInterval != 0 {
		result.CheckInterval = net.Health.CheckInterval
	}
	if net.Health.FinalityInterval != 0 {
		result.FinalityInterval = net.Health.FinalityInterval
	}
	if net.Health.MaxSyncDistance != 0 {
		result.MaxSyncDistance = net.Health.MaxSyncDistance
	}
	return result
}

// EffectiveRateLimiting merges global defaults with per-network overrides.
func (c *Config) EffectiveRateLimiting(net *NetworkConfig) RateLimitingConfig {
	if net.RateLimiting == nil {
		return c.RateLimiting
	}
	return *net.RateLimiting
}

func (c *Config) applyDefaults() {
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.Server.Host == "" {
		c.Server.Host = "0.0.0.0"
	}
	if c.Server.Port == 0 {
		c.Server.Port = 5555
	}
	if c.Server.MaxTimeout == 0 {
		c.Server.MaxTimeout = 60 * time.Second
	}
	applyDebugLoggingDefaults(&c.DebugLogging)
	if c.Health.CheckInterval == 0 {
		c.Health.CheckInterval = 15 * time.Second
	}
	if c.Health.FinalityInterval == 0 {
		c.Health.FinalityInterval = 60 * time.Second
	}
	if c.Health.MaxSyncDistance == 0 {
		c.Health.MaxSyncDistance = 10
	}
	if c.Health.FollowDistance == 0 {
		c.Health.FollowDistance = 32
	}
	if c.Health.MaxHeadDistance == 0 {
		c.Health.MaxHeadDistance = 2
	}
	if c.Metrics.Path == "" {
		c.Metrics.Path = "/metrics"
	}
	if c.State.Driver == "" {
		c.State.Driver = "local"
	}
	if c.UI.BasePath == "" {
		c.UI.BasePath = "/webui"
	}
	if c.CORS != nil {
		applyCORSDefaults(c.CORS)
	}
	applyFailsafeDefaults(&c.Failsafe)
	for i := range c.Networks {
		applyNetworkDefaults(&c.Networks[i])
	}
}

func applyCORSDefaults(cors *CORSConfig) {
	if cors.AllowedOrigins == nil {
		cors.AllowedOrigins = []string{"*"}
	}
	if cors.AllowedMethods == nil {
		cors.AllowedMethods = []string{"GET", "HEAD", "POST", "OPTIONS"}
	}
	if cors.AllowedHeaders == nil {
		cors.AllowedHeaders = []string{
			"content-type",
			"authorization",
			"x-ebeacon-secret-token",
			"x-api-key",
		}
	}
	if cors.AllowCredentials == nil {
		allowCredentials := false
		cors.AllowCredentials = &allowCredentials
	}
	if cors.MaxAge == 0 {
		cors.MaxAge = 3600
	}
}

func applyDebugLoggingDefaults(dl *DebugLoggingConfig) {
	if dl.MaxSizeMB == 0 {
		dl.MaxSizeMB = 100
	}
	if dl.MaxBackups == 0 {
		dl.MaxBackups = 10
	}
	if dl.MaxBodyBytes == 0 {
		dl.MaxBodyBytes = 64 << 10
	}
}

// knownGenesisTimes maps well-known network IDs to their genesis unix timestamps.
// These are permanent chain constants that never change.
var knownGenesisTimes = map[string]int64{
	"mainnet": 1606824023,
	"sepolia": 1655733600,
	"hoodi":   1742213400,
}

func applyNetworkDefaults(n *NetworkConfig) {
	if n.GenesisTime == 0 {
		if t, ok := knownGenesisTimes[strings.ToLower(n.ID)]; ok {
			n.GenesisTime = t
		}
	}
	if n.Routing.LoadBalancing == "" {
		n.Routing.LoadBalancing = "round-robin"
	}
	if n.Routing.StickyTimeout == 0 {
		n.Routing.StickyTimeout = 10 * time.Minute
	}
	if n.Routing.RebalanceInterval == 0 {
		n.Routing.RebalanceInterval = 30 * time.Second
	}
	if n.Routing.RebalanceThreshold == 0 {
		n.Routing.RebalanceThreshold = 1.5
	}
	if n.Routing.RebalanceMaxSweep == 0 {
		n.Routing.RebalanceMaxSweep = 10
	}
	if n.Routing.ScoreWindowSize == 0 {
		n.Routing.ScoreWindowSize = 100
	}
	if n.Cache.MaxSize == 0 {
		n.Cache.MaxSize = 2048
	}
	if n.Cache.Driver == "" {
		n.Cache.Driver = "memory"
	}
	for i := range n.Upstreams {
		if n.Upstreams[i].Weight == 0 {
			n.Upstreams[i].Weight = 1
		}
	}
	if n.Failsafe != nil {
		applyFailsafeDefaults(n.Failsafe)
	}
}

func (c *Config) expandSecrets() {
	expandAuthConfig := func(auth *AuthConfig) {
		if auth == nil {
			return
		}
		if auth.Secret != "" {
			auth.Secret = os.ExpandEnv(auth.Secret)
		}
		for i := range auth.Keys {
			if auth.Keys[i].Secret != "" {
				auth.Keys[i].Secret = os.ExpandEnv(auth.Keys[i].Secret)
			}
		}
	}

	expandAuthConfig(c.Auth)
	expandAuthConfig(c.UI.Auth)
	if c.DebugLogging.Path != "" {
		c.DebugLogging.Path = os.ExpandEnv(c.DebugLogging.Path)
	}

	if c.State.Redis != nil && c.State.Redis.Password != "" {
		c.State.Redis.Password = os.ExpandEnv(c.State.Redis.Password)
	}
	for i := range c.Networks {
		if r := c.Networks[i].Cache.Redis; r != nil && r.Password != "" {
			r.Password = os.ExpandEnv(r.Password)
		}
	}
}

func mergeTimeoutConfig(base, override *TimeoutConfig) *TimeoutConfig {
	if override == nil {
		return base
	}
	out := &TimeoutConfig{}
	if base != nil {
		*out = *base
	}
	if override.Duration != 0 {
		out.Duration = override.Duration
	}
	return out
}

func mergeRetryConfig(base, override *RetryConfig) *RetryConfig {
	if override == nil {
		return base
	}
	out := &RetryConfig{}
	if base != nil {
		*out = *base
	}
	if override.MaxAttempts != 0 {
		out.MaxAttempts = override.MaxAttempts
	}
	if override.Delay != 0 {
		out.Delay = override.Delay
	}
	if override.Backoff != 0 {
		out.Backoff = override.Backoff
	}
	if override.Jitter != 0 {
		out.Jitter = override.Jitter
	}
	if override.MaxDelay != 0 {
		out.MaxDelay = override.MaxDelay
	}
	return out
}

func mergeHedgeConfig(base, override *HedgeConfig) *HedgeConfig {
	if override == nil {
		return base
	}
	out := &HedgeConfig{}
	if base != nil {
		*out = *base
	}
	if override.Delay != 0 {
		out.Delay = override.Delay
	}
	if override.MaxCount != 0 {
		out.MaxCount = override.MaxCount
	}
	return out
}

func mergeCircuitBreakerConfig(base, override *CircuitBreakerConfig) *CircuitBreakerConfig {
	if override == nil {
		return base
	}
	out := &CircuitBreakerConfig{}
	if base != nil {
		*out = *base
	}
	if override.FailureThreshold != 0 {
		out.FailureThreshold = override.FailureThreshold
	}
	if override.SuccessThreshold != 0 {
		out.SuccessThreshold = override.SuccessThreshold
	}
	if override.HalfOpenAfter != 0 {
		out.HalfOpenAfter = override.HalfOpenAfter
	}
	return out
}

func applyFailsafeDefaults(fs *FailsafeConfig) {
	if fs.Timeout == nil {
		fs.Timeout = &TimeoutConfig{Duration: 30 * time.Second}
	} else if fs.Timeout.Duration == 0 {
		fs.Timeout.Duration = 30 * time.Second
	}
	if r := fs.Retry; r != nil {
		if r.MaxAttempts == 0 {
			r.MaxAttempts = 3
		}
		if r.Delay == 0 {
			r.Delay = 100 * time.Millisecond
		}
		if r.Backoff == 0 {
			r.Backoff = 2.0
		}
		if r.MaxDelay == 0 {
			r.MaxDelay = 5 * time.Second
		}
	}
	if cb := fs.CircuitBreaker; cb != nil {
		if cb.FailureThreshold == 0 {
			cb.FailureThreshold = 5
		}
		if cb.SuccessThreshold == 0 {
			cb.SuccessThreshold = 2
		}
		if cb.HalfOpenAfter == 0 {
			cb.HalfOpenAfter = 30 * time.Second
		}
	}
}

func (c *Config) validate() error {
	if err := validateLogLevel(c.LogLevel); err != nil {
		return err
	}
	if err := validateServer(c.Server); err != nil {
		return err
	}
	if err := validateDebugLogging(c.DebugLogging); err != nil {
		return err
	}
	if err := validateCORS(c.CORS); err != nil {
		return err
	}
	if err := validateFailsafe(&c.Failsafe, "failsafe"); err != nil {
		return err
	}
	if err := validateHealth(&c.Health, "health"); err != nil {
		return err
	}
	if err := validateRateLimiting(&c.RateLimiting, "rateLimiting"); err != nil {
		return err
	}
	if c.Metrics.Enabled {
		if !strings.HasPrefix(c.Metrics.Path, "/") {
			return fmt.Errorf("metrics.path must start with \"/\"")
		}
	}
	if c.State.Driver == "redis" && (c.State.Redis == nil || c.State.Redis.URL == "") {
		return fmt.Errorf("state.redis.url is required when state.driver is \"redis\"")
	}
	if c.UI.Enabled && (c.UI.Auth == nil || len(c.UI.Auth.Keys) == 0) {
		return fmt.Errorf("ui.auth with at least one key is required when ui.enabled is true")
	}
	if c.LegacyProjects.Kind != 0 {
		return fmt.Errorf("top-level \"projects\" is no longer supported; use top-level \"networks\" and optional \"auth\"")
	}
	if len(c.Networks) == 0 {
		return fmt.Errorf("at least one network is required (configure \"networks\")")
	}
	seen := make(map[string]bool)
	for i, n := range c.Networks {
		if n.ID == "" {
			return fmt.Errorf("networks[%d]: id is required", i)
		}
		if seen[n.ID] {
			return fmt.Errorf("networks[%d]: duplicate network id %q", i, n.ID)
		}
		seen[n.ID] = true
		if err := validateNetwork(&n, fmt.Sprintf("networks[%d] (%s)", i, n.ID)); err != nil {
			return err
		}
	}
	return nil
}

func validateLogLevel(level string) error {
	switch level {
	case "debug", "info", "warn", "error":
		return nil
	default:
		return fmt.Errorf("invalid logLevel %q", level)
	}
}

func validateServer(s ServerConfig) error {
	if s.Port < 1 || s.Port > 65535 {
		return fmt.Errorf("server.port must be between 1 and 65535")
	}
	if s.MaxTimeout <= 0 {
		return fmt.Errorf("server.maxTimeout must be > 0")
	}
	return nil
}

func validateDebugLogging(dl DebugLoggingConfig) error {
	if !dl.Enabled {
		return nil
	}
	if strings.TrimSpace(dl.Path) == "" {
		return fmt.Errorf("debugLogging.path is required when debugLogging.enabled is true")
	}
	if dl.MaxSizeMB <= 0 {
		return fmt.Errorf("debugLogging.maxSizeMB must be > 0")
	}
	if dl.MaxBackups <= 0 {
		return fmt.Errorf("debugLogging.maxBackups must be > 0")
	}
	if dl.MaxBodyBytes <= 0 {
		return fmt.Errorf("debugLogging.maxBodyBytes must be > 0")
	}
	return nil
}

func validateCORS(cors *CORSConfig) error {
	if cors == nil {
		return nil
	}
	if len(cors.AllowedOrigins) == 0 {
		return fmt.Errorf("cors.allowedOrigins must contain at least one origin")
	}
	for i, origin := range cors.AllowedOrigins {
		if strings.TrimSpace(origin) == "" {
			return fmt.Errorf("cors.allowedOrigins[%d] must not be empty", i)
		}
	}
	for i, method := range cors.AllowedMethods {
		if strings.TrimSpace(method) == "" {
			return fmt.Errorf("cors.allowedMethods[%d] must not be empty", i)
		}
	}
	for i, header := range cors.AllowedHeaders {
		if strings.TrimSpace(header) == "" {
			return fmt.Errorf("cors.allowedHeaders[%d] must not be empty", i)
		}
	}
	for i, header := range cors.ExposedHeaders {
		if strings.TrimSpace(header) == "" {
			return fmt.Errorf("cors.exposedHeaders[%d] must not be empty", i)
		}
	}
	if cors.MaxAge < 0 {
		return fmt.Errorf("cors.maxAge must be >= 0")
	}
	return nil
}

func validateNetwork(n *NetworkConfig, ctx string) error {
	if len(n.Upstreams) == 0 {
		return fmt.Errorf("%s: at least one upstream is required", ctx)
	}
	if err := validateFailsafe(n.Failsafe, ctx+" failsafe"); err != nil {
		return err
	}
	if err := validateHealth(n.Health, ctx+" health"); err != nil {
		return err
	}
	if err := validateRateLimiting(n.RateLimiting, ctx+" rateLimiting"); err != nil {
		return err
	}
	switch n.Routing.LoadBalancing {
	case "round-robin", "random", "least-conn", "score", "":
	default:
		return fmt.Errorf("%s: invalid loadBalancing %q", ctx, n.Routing.LoadBalancing)
	}
	if n.Routing.StickyTimeout <= 0 {
		return fmt.Errorf("%s: stickyTimeout must be > 0", ctx)
	}
	if n.Routing.RebalanceInterval <= 0 {
		return fmt.Errorf("%s: rebalanceInterval must be > 0", ctx)
	}
	for i, pattern := range n.Routing.BlockedPaths {
		if _, err := regexp.Compile(pattern); err != nil {
			return fmt.Errorf("%s blockedPaths[%d]: invalid regex: %w", ctx, i, err)
		}
	}
	if n.Cache.MaxSize <= 0 {
		return fmt.Errorf("%s cache.maxSize must be > 0", ctx)
	}
	for i, p := range n.Cache.Policies {
		if p.Pattern == "" {
			return fmt.Errorf("%s cache.policies[%d]: pattern is required", ctx, i)
		}
		if _, err := regexp.Compile(p.Pattern); err != nil {
			return fmt.Errorf("%s cache.policies[%d]: invalid pattern: %w", ctx, i, err)
		}
		if p.TTL < 0 {
			return fmt.Errorf("%s cache.policies[%d]: ttl must be >= 0", ctx, i)
		}
		for _, method := range p.Methods {
			if strings.TrimSpace(method) == "" {
				return fmt.Errorf("%s cache.policies[%d]: methods must not contain empty values", ctx, i)
			}
		}
	}

	seenUpstream := make(map[string]struct{}, len(n.Upstreams))
	for i, u := range n.Upstreams {
		if u.ID == "" {
			return fmt.Errorf("%s upstreams[%d]: id is required", ctx, i)
		}
		if _, exists := seenUpstream[u.ID]; exists {
			return fmt.Errorf("%s: duplicate upstream id %q", ctx, u.ID)
		}
		seenUpstream[u.ID] = struct{}{}
		if u.URL == "" {
			return fmt.Errorf("%s upstreams[%d]: url is required", ctx, i)
		}
		if err := validateUpstreamURL(u.URL); err != nil {
			return fmt.Errorf("%s upstreams[%d]: %w", ctx, i, err)
		}
		if err := validateFailsafe(u.Failsafe, fmt.Sprintf("%s upstreams[%d] failsafe", ctx, i)); err != nil {
			return err
		}
	}

	return validateNetworkRouting(n, ctx)
}

func validateUpstreamURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid url %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("url %q must use http or https", raw)
	}
	if u.Host == "" {
		return fmt.Errorf("url %q must include a host", raw)
	}
	return nil
}

func validateFailsafe(fs *FailsafeConfig, ctx string) error {
	if fs == nil {
		return nil
	}
	if fs.Timeout != nil && fs.Timeout.Duration < 0 {
		return fmt.Errorf("%s timeout.duration must be >= 0", ctx)
	}
	if fs.Retry != nil {
		if fs.Retry.MaxAttempts < 0 {
			return fmt.Errorf("%s retry.maxAttempts must be >= 0", ctx)
		}
		if fs.Retry.Delay < 0 || fs.Retry.Jitter < 0 || fs.Retry.MaxDelay < 0 {
			return fmt.Errorf("%s retry delays must be >= 0", ctx)
		}
		if fs.Retry.Backoff < 0 || (fs.Retry.Backoff > 0 && fs.Retry.Backoff < 1) {
			return fmt.Errorf("%s retry.backoff must be >= 1", ctx)
		}
	}
	if fs.Hedge != nil {
		if fs.Hedge.Delay < 0 {
			return fmt.Errorf("%s hedge.delay must be >= 0", ctx)
		}
		if fs.Hedge.MaxCount < 0 {
			return fmt.Errorf("%s hedge.maxCount must be >= 0", ctx)
		}
	}
	if fs.CircuitBreaker != nil {
		if fs.CircuitBreaker.FailureThreshold < 0 {
			return fmt.Errorf("%s circuitBreaker.failureThreshold must be >= 0", ctx)
		}
		if fs.CircuitBreaker.SuccessThreshold < 0 {
			return fmt.Errorf("%s circuitBreaker.successThreshold must be >= 0", ctx)
		}
		if fs.CircuitBreaker.HalfOpenAfter < 0 {
			return fmt.Errorf("%s circuitBreaker.halfOpenAfter must be >= 0", ctx)
		}
	}
	return nil
}

func validateHealth(h *HealthConfig, ctx string) error {
	if h == nil {
		return nil
	}
	if h.CheckInterval <= 0 {
		return fmt.Errorf("%s checkInterval must be > 0", ctx)
	}
	if h.FinalityInterval <= 0 {
		return fmt.Errorf("%s finalityInterval must be > 0", ctx)
	}
	return nil
}

func validateRateLimiting(rl *RateLimitingConfig, ctx string) error {
	if rl == nil {
		return nil
	}
	if err := validateRateLimit(rl.PerIP, ctx+" perIP"); err != nil {
		return err
	}
	if err := validateRateLimit(rl.Global, ctx+" global"); err != nil {
		return err
	}
	return nil
}

func validateRateLimit(rl *RateLimitConfig, ctx string) error {
	if rl == nil {
		return nil
	}
	if rl.Limit <= 0 {
		return fmt.Errorf("%s limit must be > 0", ctx)
	}
	if rl.Burst < 1 {
		return fmt.Errorf("%s burst must be >= 1", ctx)
	}
	return nil
}

func validateNetworkRouting(n *NetworkConfig, ctx string) error {
	upstreamIDs := make(map[string]struct{}, len(n.Upstreams))
	for _, u := range n.Upstreams {
		upstreamIDs[u.ID] = struct{}{}
	}
	for i, cr := range n.Routing.ClientRoutes {
		if cr.PathPrefix == "" {
			return fmt.Errorf("%s clientRoutes[%d]: pathPrefix is required", ctx, i)
		}
		if cr.UpstreamID == "" {
			return fmt.Errorf("%s clientRoutes[%d]: upstreamId is required", ctx, i)
		}
		if !strings.HasPrefix(cr.UpstreamID, "client:") {
			if _, ok := upstreamIDs[cr.UpstreamID]; !ok {
				return fmt.Errorf("%s clientRoutes[%d]: unknown upstream id %q", ctx, i, cr.UpstreamID)
			}
		}
	}
	for i, rr := range n.Routing.RouteRules {
		if rr.PathPattern == "" {
			return fmt.Errorf("%s routeRules[%d]: pathPattern is required", ctx, i)
		}
		if _, err := regexp.Compile(rr.PathPattern); err != nil {
			return fmt.Errorf("%s routeRules[%d]: invalid pathPattern: %w", ctx, i, err)
		}
		if rr.Deny && rr.UpstreamID != "" {
			return fmt.Errorf("%s routeRules[%d]: deny and upstreamId are mutually exclusive", ctx, i)
		}
		if !rr.Deny && rr.UpstreamID == "" {
			return fmt.Errorf("%s routeRules[%d]: set upstreamId or deny: true", ctx, i)
		}
		if rr.UpstreamID != "" {
			if _, ok := upstreamIDs[rr.UpstreamID]; !ok {
				return fmt.Errorf("%s routeRules[%d]: unknown upstream id %q", ctx, i, rr.UpstreamID)
			}
		}
		for _, m := range rr.Methods {
			if m == "" {
				return fmt.Errorf("%s routeRules[%d]: empty method in methods list", ctx, i)
			}
		}
	}
	return nil
}
