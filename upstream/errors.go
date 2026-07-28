package upstream

import (
	"errors"
	"net/url"
)

// SanitizeError removes credential-bearing URL components from transport
// errors before they are logged or returned to debug output. net/http commonly
// wraps failures in *url.Error with the full request URL, including provider
// API keys embedded in userinfo, paths, or query strings.
func SanitizeError(err error) error {
	if err == nil {
		return nil
	}
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		return err
	}
	clean := *urlErr
	clean.URL = sanitizeErrorURL(urlErr.URL)
	clean.Err = SanitizeError(urlErr.Err)
	return &clean
}

func sanitizeErrorURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return "[REDACTED_URL]"
	}
	if parsed.Scheme == "" {
		return "//" + parsed.Host
	}
	return parsed.Scheme + "://" + parsed.Host
}
