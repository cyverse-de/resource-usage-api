package clients

import (
	"errors"
	"fmt"
	"net/http"
)

// StatusCoder is implemented by errors that carry an HTTP status. It lets a handler map a failure
// from a downstream service onto a response status without inspecting concrete error types.
type StatusCoder interface {
	StatusCode() int
}

// HTTPError represents an error returned by an HTTP service.
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

// NoUsageRecordedError reports that the subscriptions service has no usage recorded for a user and
// resource type. Callers treat it as a signal to enqueue an asynchronous refresh rather than as a
// failure.
type NoUsageRecordedError struct {
	Username     string
	ResourceType string
}

func (e *NoUsageRecordedError) Error() string {
	return fmt.Sprintf("subscriptions has no %s usage recorded for %s", e.ResourceType, e.Username)
}

// StatusCode reports the status a caller should surface for a missing usage record.
func (e *NoUsageRecordedError) StatusCode() int {
	return http.StatusNotFound
}

// GetStatusCode returns the status code to use for an error returned by one of the client libraries,
// defaulting to 500 for errors that carry no status of their own. It unwraps, so a status survives
// being wrapped with additional context.
func GetStatusCode(e error) int {
	var coder StatusCoder
	if errors.As(e, &coder) {
		return coder.StatusCode()
	}
	return http.StatusInternalServerError
}
