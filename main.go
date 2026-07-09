package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // register pprof handlers on DefaultServeMux (used by debug server)
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mysticryuujin/ebeacon/api"
	"github.com/mysticryuujin/ebeacon/config"
	"github.com/mysticryuujin/ebeacon/debuglog"
	networkpkg "github.com/mysticryuujin/ebeacon/network"
	"github.com/mysticryuujin/ebeacon/proxy"
	"github.com/mysticryuujin/ebeacon/state"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	configPath := flag.String("config", "ebeacon.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	setupLogging(cfg.LogLevel)
	if err := networkpkg.SetTrustedProxies(cfg.Server.TrustedProxies); err != nil {
		fmt.Fprintf(os.Stderr, "invalid server.trustedProxies: %v\n", err)
		os.Exit(1)
	}
	debugLogCloser, err := debuglog.ConfigureDefault(cfg.DebugLogging)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to set up debug logging: %v\n", err)
		os.Exit(1)
	}
	if debugLogCloser != nil {
		defer debugLogCloser.Close() //nolint:errcheck
		slog.Info("debug logging enabled",
			"path", cfg.DebugLogging.Path,
			"max_size_mb", cfg.DebugLogging.MaxSizeMB,
			"max_backups", cfg.DebugLogging.MaxBackups,
			"max_body_bytes", cfg.DebugLogging.MaxBodyBytes)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sharedState state.SharedState
	switch cfg.State.Driver {
	case "redis":
		rs, err := state.NewRedisState(cfg.State.Redis)
		if err != nil {
			slog.Error("failed to connect to Redis state", "err", err)
			os.Exit(1)
		}
		defer rs.Close() //nolint:errcheck
		sharedState = rs
		redactedURL := cfg.State.Redis.URL
		if u, err := url.Parse(cfg.State.Redis.URL); err == nil {
			redactedURL = u.Redacted()
		}
		slog.Info("shared state: redis", "url", redactedURL)
	default:
		sharedState = state.NewLocalState()
		slog.Info("shared state: local")
	}

	if cfg.Pprof.Enabled {
		host := cfg.Pprof.Host
		if host == "" {
			host = "127.0.0.1"
		}
		port := cfg.Pprof.Port
		if port == 0 {
			port = 6060
		}
		pprofAddr := fmt.Sprintf("%s:%d", host, port)
		go func() {
			slog.Info("pprof debug server starting", "addr", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				slog.Warn("pprof server stopped", "err", err)
			}
		}()
	}

	mux := http.NewServeMux()

	// Collect all networks for the status API
	allNetworks := make(map[string]*networkpkg.Network)
	networks := make(map[string]*networkpkg.Network, len(cfg.Networks))
	for i := range cfg.Networks {
		n, err := networkpkg.New(&cfg.Networks[i], cfg)
		if err != nil {
			slog.Error("failed to create network", "network", cfg.Networks[i].ID, "err", err)
			os.Exit(1)
		}
		n.Pool().SetSharedState(sharedState)
		n.Pool().StartStateSync() // seed finalized epoch before health monitoring starts
		n.Start(ctx)
		networks[cfg.Networks[i].ID] = n
		allNetworks[cfg.Networks[i].ID] = n
		slog.Info("network registered",
			"network", cfg.Networks[i].ID,
			"upstreams", len(cfg.Networks[i].Upstreams),
			"cache", cfg.Networks[i].Cache.Enabled)
	}

	// Fan-out head updates from shared state to all network pools.
	go func() {
		ch := sharedState.SubscribeHead()
		for {
			select {
			case <-ctx.Done():
				return
			case update, ok := <-ch:
				if !ok {
					return
				}
				n, ok := allNetworks[update.Network]
				if !ok {
					continue
				}
				n.Pool().RecordRemoteHead(update.Slot, update.Root)
			}
		}
	}()
	p := proxy.New(networks)
	if cfg.Auth != nil {
		p.SetAuth(cfg.Auth)
	}
	mux.Handle("/", p)
	slog.Info("routing mode", "networks", len(networks), "path", "/{network}/eth/v1/... or prefix-free when single network")

	if cfg.UI.Enabled {
		statusAPI := api.NewStatusAPI(allNetworks)
		if cfg.UI.Auth != nil {
			statusAPI.Auth = cfg.UI.Auth
			slog.Info("web ui auth enabled")
		}
		statusAPI.RegisterRoutes(mux, cfg.UI.BasePath)
		slog.Info("web ui enabled", "path", cfg.UI.BasePath)
	}

	if cfg.Metrics.Enabled {
		mux.Handle(cfg.Metrics.Path, metricsHandler())
		slog.Info("metrics enabled", "path", cfg.Metrics.Path)
	}

	srv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler:      proxy.WrapWithCORS(mux, cfg.CORS),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: cfg.Server.MaxTimeout + 5*time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		slog.Info("ebeacon starting", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down")
	cancel()

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutCancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		slog.Error("shutdown error", "err", err)
	}
}

func setupLogging(level string) {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l})))
}

func metricsHandler() http.Handler {
	return metricsHandlerFor(prometheus.DefaultGatherer, prometheus.DefaultRegisterer)
}

func metricsHandlerFor(gatherer prometheus.Gatherer, registerer prometheus.Registerer) http.Handler {
	return promhttp.InstrumentMetricHandler(registerer, promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
		DisableCompression: true,
	}))
}
