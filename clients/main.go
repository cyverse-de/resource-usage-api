package clients

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// A regular expression used to remove suffixes from usernames.
var usernameSuffixRegexp = regexp.MustCompile("@.*$")

// An HTTP client to be used by all of the client libraries. The timeout matches the one the NATS request/reply
// calls it replaced used, so a wedged downstream service can't pin a goroutine indefinitely.
var client = http.Client{Transport: http.DefaultTransport, Timeout: 30 * time.Second}

// HTTPError represents an error returned by an HTTP service
type HTTPError struct {
	statusCode int
	message    string
}

// NewHTTPError returns a new HTTPError.
func NewHTTPError(statusCode int, message string) *HTTPError {
	return &HTTPError{
		statusCode: statusCode,
		message:    message,
	}
}

// Error returns the error message associated with an HTTPError.
func (e *HTTPError) Error() string {
	return e.message
}

// StatusCode returns the status code associated with an HTTPError.
func (e *HTTPError) StatusCode() int {
	return e.statusCode
}

// GetStatusCode returns the appropriate status code to use for an error returned by one of the client libraries.
// If the error happens to be an HTTP error, then the original status code is returned. Otherwise, the code defaults
// to http.StatusInternalServerError.
func GetStatusCode(e error) int {
	herror, ok := e.(*HTTPError)
	if ok {
		return herror.StatusCode()
	}
	return http.StatusInternalServerError
}

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

// BuildURL builds a URL from a base URL and zero or URL path components.
func BuildURL(baseURL *url.URL, components ...string) *url.URL {
	newURL := *baseURL

	// Escape all of the path components.
	escapedComponents := make([]string, len(components))
	for i, component := range components {
		escapedComponents[i] = url.PathEscape(component)
	}

	// Add the components to the path.
	newURL.Path = fmt.Sprintf("%s/%s", newURL.Path, strings.Join(escapedComponents, "/"))

	// Return the new URL.
	return &newURL
}

// StripUsernameSuffix removes the username suffix from a username.
func StripUsernameSuffix(username string) string {
	return usernameSuffixRegexp.ReplaceAllString(username, "")
}
