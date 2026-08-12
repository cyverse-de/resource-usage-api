package clients

import (
	"fmt"
	"net/url"
	"strings"
)

// parseBaseURL parses a client's raw base URL and normalizes its path. Values that could only produce broken
// request URLs later (missing host, non-HTTP scheme) are rejected here so misconfiguration fails at startup.
func parseBaseURL(rawURL string) (*url.URL, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("base URL %q must use the http or https scheme", rawURL)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("base URL %q has no host", rawURL)
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	return parsed, nil
}
