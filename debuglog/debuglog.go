package debuglog

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/mysticryuujin/ebeacon/config"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

type Logger struct {
	enabled      bool
	logger       *slog.Logger
	maxBodyBytes int
}

type Event struct {
	Kind            string
	Network         string
	APIPath         string
	Method          string
	Path            string
	RawQuery        string
	ClientIP        string
	Upstream        string
	Selector        string
	Status          int
	Attempt         int
	Duration        time.Duration
	Err             error
	RequestHeaders  http.Header
	RequestBody     []byte
	ResponseHeaders http.Header
	ResponseBody    []byte
	Note            string
}

var (
	defaultMu     sync.RWMutex
	defaultLogger = &Logger{}
)

var sensitiveHeaderKeys = map[string]struct{}{
	"authorization":          {},
	"cookie":                 {},
	"proxy-authorization":    {},
	"set-cookie":             {},
	"x-api-key":              {},
	"x-ebeacon-secret-token": {},
}

// IsSensitiveHeader reports whether name is a header that should be redacted
// before being logged, displayed in the dashboard, or otherwise exposed.
// The check is case-insensitive.
func IsSensitiveHeader(name string) bool {
	_, ok := sensitiveHeaderKeys[strings.ToLower(name)]
	return ok
}

var sensitiveQueryKeys = map[string]struct{}{
	"access_token":  {},
	"api_key":       {},
	"authorization": {},
	"key":           {},
	"secret":        {},
	"token":         {},
	"use-upstream":  {},
}

func ConfigureDefault(cfg config.DebugLoggingConfig) (io.Closer, error) {
	logger, closer, err := New(cfg)
	if err != nil {
		return nil, err
	}
	SetDefault(logger)
	return closer, nil
}

func New(cfg config.DebugLoggingConfig) (*Logger, io.Closer, error) {
	if !cfg.Enabled {
		return &Logger{}, nil, nil
	}

	dir := filepath.Dir(cfg.Path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, nil, err
		}
	}

	writer := &lumberjack.Logger{
		Filename:   cfg.Path,
		MaxSize:    cfg.MaxSizeMB,
		MaxBackups: cfg.MaxBackups,
		Compress:   true,
	}

	handler := slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: slog.LevelInfo})
	return &Logger{
		enabled:      true,
		logger:       slog.New(handler),
		maxBodyBytes: cfg.MaxBodyBytes,
	}, writer, nil
}

func SetDefault(logger *Logger) *Logger {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	prev := defaultLogger
	if logger == nil {
		defaultLogger = &Logger{}
	} else {
		defaultLogger = logger
	}
	return prev
}

func Default() *Logger {
	defaultMu.RLock()
	defer defaultMu.RUnlock()
	if defaultLogger == nil {
		return &Logger{}
	}
	return defaultLogger
}

func (l *Logger) Enabled() bool {
	return l != nil && l.enabled && l.logger != nil
}

func (l *Logger) LogEvent(event Event) {
	if !l.Enabled() {
		return
	}

	request := map[string]any{
		"path":    event.Path,
		"headers": sanitizeHeaders(event.RequestHeaders),
	}
	if query := sanitizeQuery(event.RawQuery); query != "" {
		request["query"] = query
	}
	if body := previewBody(event.RequestHeaders, event.RequestBody, l.maxBodyBytes); body != nil {
		request["body"] = body
	}

	response := map[string]any{}
	if headers := sanitizeHeaders(event.ResponseHeaders); len(headers) > 0 {
		response["headers"] = headers
	}
	if body := previewBody(event.ResponseHeaders, event.ResponseBody, l.maxBodyBytes); body != nil {
		response["body"] = body
	}

	attrs := []slog.Attr{
		slog.String("kind", event.Kind),
		slog.String("network", event.Network),
		slog.String("api_path", event.APIPath),
		slog.String("method", event.Method),
		slog.Any("request", request),
	}
	if event.ClientIP != "" {
		attrs = append(attrs, slog.String("client_ip", event.ClientIP))
	}
	if event.Upstream != "" {
		attrs = append(attrs, slog.String("upstream", event.Upstream))
	}
	if event.Selector != "" {
		attrs = append(attrs, slog.String("selector", event.Selector))
	}
	if event.Status > 0 {
		attrs = append(attrs, slog.Int("status", event.Status))
	}
	if event.Attempt > 0 {
		attrs = append(attrs, slog.Int("attempt", event.Attempt))
	}
	if event.Duration > 0 {
		attrs = append(attrs, slog.Int64("duration_ms", event.Duration.Milliseconds()))
	}
	if event.Err != nil {
		attrs = append(attrs, slog.String("error", event.Err.Error()))
	}
	if event.Note != "" {
		attrs = append(attrs, slog.String("note", event.Note))
	}
	if len(response) > 0 {
		attrs = append(attrs, slog.Any("response", response))
	}

	l.logger.LogAttrs(context.Background(), slog.LevelInfo, "debug exchange", attrs...)
}

func sanitizeHeaders(headers http.Header) map[string][]string {
	if len(headers) == 0 {
		return nil
	}
	out := make(map[string][]string, len(headers))
	for key, values := range headers {
		if len(values) == 0 {
			continue
		}
		lower := strings.ToLower(key)
		if _, ok := sensitiveHeaderKeys[lower]; ok {
			out[key] = []string{"[REDACTED]"}
			continue
		}
		copied := make([]string, len(values))
		copy(copied, values)
		out[key] = copied
	}
	return out
}

func sanitizeQuery(raw string) string {
	if raw == "" {
		return ""
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		// The raw query may contain a ?secret= credential; never emit it.
		return "[UNPARSEABLE_QUERY_REDACTED]"
	}
	for key := range values {
		if _, ok := sensitiveQueryKeys[strings.ToLower(key)]; ok {
			values.Set(key, "[REDACTED]")
		}
	}
	return values.Encode()
}

func previewBody(headers http.Header, body []byte, maxBodyBytes int) map[string]any {
	if len(body) == 0 || maxBodyBytes <= 0 {
		return nil
	}

	contentType := headers.Get("Content-Type")
	contentEncoding := headers.Get("Content-Encoding")
	var preview []byte
	truncated := len(body) > maxBodyBytes
	decodeError := ""

	if strings.Contains(strings.ToLower(contentEncoding), "gzip") {
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			decodeError = err.Error()
			preview = body[:min(len(body), maxBodyBytes)]
		} else {
			defer zr.Close() //nolint:errcheck
			var readErr error
			preview, truncated, readErr = readPreview(zr, maxBodyBytes)
			if readErr != nil {
				decodeError = readErr.Error()
			}
		}
	} else {
		preview = body[:min(len(body), maxBodyBytes)]
	}

	result := map[string]any{
		"original_bytes": len(body),
		"logged_bytes":   len(preview),
		"truncated":      truncated,
	}
	if contentType != "" {
		result["content_type"] = contentType
	}
	if contentEncoding != "" {
		result["content_encoding"] = contentEncoding
	}
	if decodeError != "" {
		result["decode_error"] = decodeError
	}

	if isTextBody(contentType, preview) {
		result["format"] = "text"
		result["value"] = string(preview)
		return result
	}

	result["format"] = "base64"
	result["value"] = base64.StdEncoding.EncodeToString(preview)
	return result
}

func readPreview(r io.Reader, maxBodyBytes int) ([]byte, bool, error) {
	limited, err := io.ReadAll(io.LimitReader(r, int64(maxBodyBytes+1)))
	if err != nil {
		return nil, false, err
	}
	if len(limited) > maxBodyBytes {
		return limited[:maxBodyBytes], true, nil
	}
	return limited, false, nil
}

func isTextBody(contentType string, body []byte) bool {
	ct := strings.ToLower(contentType)
	if strings.Contains(ct, "application/json") ||
		strings.Contains(ct, "application/problem+json") ||
		strings.Contains(ct, "text/") ||
		strings.Contains(ct, "application/xml") ||
		strings.Contains(ct, "application/x-yaml") {
		return true
	}
	return utf8.Valid(body)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
