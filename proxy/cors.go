package proxy

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/ebeacon/ebeacon/config"
)

func WrapWithCORS(next http.Handler, cors *config.CORSConfig) http.Handler {
	if cors == nil {
		return next
	}

	allowCredentials := cors.AllowCredentials != nil && *cors.AllowCredentials
	allowedMethods := strings.Join(cors.AllowedMethods, ", ")
	allowedHeaders := strings.Join(cors.AllowedHeaders, ", ")
	exposedHeaders := strings.Join(cors.ExposedHeaders, ", ")
	maxAge := ""
	if cors.MaxAge > 0 {
		maxAge = strconv.Itoa(cors.MaxAge)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}

		allowedOrigin, allowed := allowedCORSOrigin(cors, origin, allowCredentials)
		if allowed {
			setCORSHeaders(w.Header(), r, allowedOrigin, allowedMethods, allowedHeaders, exposedHeaders, allowCredentials, maxAge)
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		if r.Method == http.MethodOptions {
			addCORSVary(w.Header(), r)
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func allowedCORSOrigin(cors *config.CORSConfig, origin string, allowCredentials bool) (string, bool) {
	for _, allowedOrigin := range cors.AllowedOrigins {
		allowedOrigin = strings.TrimSpace(allowedOrigin)
		switch {
		case allowedOrigin == "":
			continue
		case allowedOrigin == "*":
			if allowCredentials {
				return origin, true
			}
			return "*", true
		case strings.EqualFold(allowedOrigin, origin):
			return origin, true
		}
	}
	return "", false
}

func setCORSHeaders(headers http.Header, r *http.Request, allowedOrigin, allowedMethods, allowedHeaders, exposedHeaders string, allowCredentials bool, maxAge string) {
	headers.Set("Access-Control-Allow-Origin", allowedOrigin)
	addCORSVary(headers, r)
	if allowedMethods != "" {
		headers.Set("Access-Control-Allow-Methods", allowedMethods)
	}
	if allowedHeaders != "" {
		headers.Set("Access-Control-Allow-Headers", allowedHeaders)
	}
	if exposedHeaders != "" {
		headers.Set("Access-Control-Expose-Headers", exposedHeaders)
	}
	if allowCredentials {
		headers.Set("Access-Control-Allow-Credentials", "true")
	}
	if maxAge != "" {
		headers.Set("Access-Control-Max-Age", maxAge)
	}
}

func addCORSVary(headers http.Header, r *http.Request) {
	mergeVaryHeader(headers, "Origin")
	if r.Method == http.MethodOptions {
		mergeVaryHeader(headers, "Access-Control-Request-Method")
		mergeVaryHeader(headers, "Access-Control-Request-Headers")
	}
}

func mergeVaryHeader(headers http.Header, value string) {
	for _, existing := range headers.Values("Vary") {
		for _, part := range strings.Split(existing, ",") {
			if strings.EqualFold(strings.TrimSpace(part), value) {
				return
			}
		}
	}
	headers.Add("Vary", value)
}
