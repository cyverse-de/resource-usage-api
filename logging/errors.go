package logging

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
)

// HTTPErrorHandler is an echo.HTTPErrorHandler for this application.
func HTTPErrorHandler(err error, c echo.Context) {
	code := http.StatusInternalServerError
	var body any

	switch t := err.(type) {
	case ErrorResponse:
		code = statusCodeFor(t)
		body = t
	case *ErrorResponse:
		code = statusCodeFor(*t)
		body = t
	case *echo.HTTPError:
		code = t.Code
		body = ErrorResponse{Message: echoErrorMessage(t), ErrorCode: strconv.Itoa(t.Code)}
	default:
		body = NewErrorResponse(err)
	}

	c.JSON(code, body) //nolint - lack of return value is required by Echo.
}

// statusCodeFor returns the status an ErrorResponse should be sent with, defaulting to 400 for
// responses that predate the HTTPStatusCode field.
func statusCodeFor(e ErrorResponse) int {
	if e.HTTPStatusCode != 0 {
		return e.HTTPStatusCode
	}
	return http.StatusBadRequest
}

// echoErrorMessage extracts the human-readable message from an echo error, whose Message field is
// typed as any and may hold either a string or an error.
func echoErrorMessage(e *echo.HTTPError) string {
	switch m := e.Message.(type) {
	case string:
		return m
	case error:
		return m.Error()
	default:
		return http.StatusText(e.Code)
	}
}

// ErrorResponse represents an HTTP response body containing error information. This type implements
// the error interface so that it can be returned as an error from from existing functions.
//
// swagger:response errorResponse
type ErrorResponse struct {
	Message string `json:"message"`
	// HTTPStatusCode is the status the response is sent with. It is not part of the response body.
	HTTPStatusCode int             `json:"-"`
	ErrorCode      string          `json:"error_code,omitempty"`
	Details        *map[string]any `json:"details,omitempty"`
}

// ErrorBytes returns a byte-array representation of an ErrorResponse.
func (e ErrorResponse) ErrorBytes() []byte {
	bytes, err := json.Marshal(e)
	if err != nil {
		Log.Errorf("unable to marshal %+v as JSON", e)
		return make([]byte, 0)
	}
	return bytes
}

// Error returns a string representation of an ErrorResponse.
func (e ErrorResponse) Error() string {
	return string(e.ErrorBytes())
}

// NewErrorResponse constructs an ErrorResponse based on the message passed in, but does not send
// it over the wire. This is to aid in converting to labstack/echo.
func NewErrorResponse(err error) ErrorResponse {
	var errorResponse ErrorResponse
	switch val := err.(type) {
	case ErrorResponse:
		errorResponse = val
	case error:
		errorResponse = ErrorResponse{Message: val.Error()}
	}
	return errorResponse
}
