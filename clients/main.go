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

// joinPath returns base with elements appended to its path, escaping each one so that a username
// containing a character with meaning in a URL still names the user it was meant to. url.URL.JoinPath
// does not do this: it unescapes what it joins, so an element carrying a percent sign mangles the
// path, or drops it altogether when the sign is followed by something that cannot be a hex escape.
func joinPath(base *url.URL, elements ...string) *url.URL {
	joined := *base

	// EscapedPath is the encoded form of the base's own path, which RawPath only holds when the two
	// differ. Starting from anything else leaves RawPath inconsistent with Path, and url quietly falls
	// back to re-escaping Path, discarding the escaping done here.
	joined.RawPath = base.EscapedPath()

	for _, element := range elements {
		joined.Path += "/" + element
		joined.RawPath += "/" + url.PathEscape(element)
	}

	return &joined
}
