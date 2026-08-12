package logging

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
)

// handle runs the error handler against a throwaway request and returns what the client would see.
func handle(t *testing.T, err error) (int, map[string]any) {
	t.Helper()

	rec := httptest.NewRecorder()
	c := echo.New().NewContext(httptest.NewRequest(http.MethodGet, "/", nil), rec)
	HTTPErrorHandler(err, c)

	var body map[string]any
	if decodeErr := json.NewDecoder(rec.Body).Decode(&body); decodeErr != nil {
		t.Fatalf("unable to decode the response body: %v", decodeErr)
	}
	return rec.Code, body
}

// The data-usage routes signal "no usage recorded yet, a refresh has been enqueued" with a 404, and
// report upstream failures as 500s. Collapsing those onto 400 would strand callers that branch on the
// status, so HTTPStatusCode has to win.
func TestHTTPErrorHandlerHonorsStatusCode(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantStatus  int
		wantMessage string
		wantCode    int
	}{
		{
			name:        "not found is preserved",
			err:         ErrorResponse{Message: "no data usage found", ErrorCode: http.StatusNotFound, HTTPStatusCode: http.StatusNotFound},
			wantStatus:  http.StatusNotFound,
			wantMessage: "no data usage found",
			wantCode:    http.StatusNotFound,
		},
		{
			name:        "internal error is preserved",
			err:         ErrorResponse{Message: "upstream exploded", ErrorCode: http.StatusInternalServerError, HTTPStatusCode: http.StatusInternalServerError},
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "upstream exploded",
			wantCode:    http.StatusInternalServerError,
		},
		{
			name:        "pointer receiver is handled the same way",
			err:         &ErrorResponse{Message: "no username provided", ErrorCode: http.StatusBadRequest, HTTPStatusCode: http.StatusBadRequest},
			wantStatus:  http.StatusBadRequest,
			wantMessage: "no username provided",
			wantCode:    http.StatusBadRequest,
		},
		{
			// Responses built before HTTPStatusCode existed leave it zero.
			name:        "missing status falls back to bad request",
			err:         ErrorResponse{Message: "something was wrong with the request"},
			wantStatus:  http.StatusBadRequest,
			wantMessage: "something was wrong with the request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, body := handle(t, tt.err)

			if status != tt.wantStatus {
				t.Errorf("status = %d, want %d", status, tt.wantStatus)
			}
			if body["message"] != tt.wantMessage {
				t.Errorf("message = %v, want %q", body["message"], tt.wantMessage)
			}
			// error_code is a JSON number: terrain's schema for this service types it as an integer.
			if tt.wantCode != 0 && body["error_code"] != float64(tt.wantCode) {
				t.Errorf("error_code = %v (%T), want %d as a number", body["error_code"], body["error_code"], tt.wantCode)
			}
			if _, ok := body["HTTPStatusCode"]; ok {
				t.Error("HTTPStatusCode should not appear in the response body")
			}
		})
	}
}

// Echo reports unmatched routes and the like as *echo.HTTPError, whose Message is typed as any.
func TestHTTPErrorHandlerUnwrapsEchoErrors(t *testing.T) {
	tests := []struct {
		name        string
		err         *echo.HTTPError
		wantStatus  int
		wantMessage string
	}{
		{
			name:        "string message",
			err:         &echo.HTTPError{Code: http.StatusNotFound, Message: "Not Found"},
			wantStatus:  http.StatusNotFound,
			wantMessage: "Not Found",
		},
		{
			name:        "error message",
			err:         &echo.HTTPError{Code: http.StatusBadRequest, Message: errors.New("bad request")},
			wantStatus:  http.StatusBadRequest,
			wantMessage: "bad request",
		},
		{
			name:        "unexpected message type falls back to the status text",
			err:         &echo.HTTPError{Code: http.StatusTeapot, Message: struct{}{}},
			wantStatus:  http.StatusTeapot,
			wantMessage: http.StatusText(http.StatusTeapot),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, body := handle(t, tt.err)

			if status != tt.wantStatus {
				t.Errorf("status = %d, want %d", status, tt.wantStatus)
			}
			if body["message"] != tt.wantMessage {
				t.Errorf("message = %v, want %q", body["message"], tt.wantMessage)
			}
		})
	}
}

func TestHTTPErrorHandlerDefaultsToInternalServerError(t *testing.T) {
	status, body := handle(t, errors.New("something unexpected"))

	if status != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", status, http.StatusInternalServerError)
	}
	if body["message"] != "something unexpected" {
		t.Errorf("message = %v, want %q", body["message"], "something unexpected")
	}
}
