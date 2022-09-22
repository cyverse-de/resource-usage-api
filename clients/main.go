package clients

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// An HTTP client to be used by all of the client libraries.
var client = http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}

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
	} else {
		return http.StatusInternalServerError
	}
}
