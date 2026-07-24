package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/p/go/svcerror"
)

func TestSubscriptionsClientValidation(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		wantErr bool
	}{
		{name: "plain http", baseURL: "http://subscriptions", wantErr: false},
		{name: "https with a path", baseURL: "https://example.org/subscriptions/", wantErr: false},
		{name: "missing scheme", baseURL: "subscriptions", wantErr: true},
		{name: "unsupported scheme", baseURL: "nats://subscriptions", wantErr: true},
		{name: "missing host", baseURL: "http://", wantErr: true},
		{name: "empty", baseURL: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := SubscriptionsClient(tt.baseURL)
			if tt.wantErr != (err != nil) {
				t.Fatalf("SubscriptionsClient(%q) error = %v, wantErr %v", tt.baseURL, err, tt.wantErr)
			}
		})
	}
}

func testUpdate() *qms.Update {
	return &qms.Update{
		ValueType:    "usages",
		Value:        1.5,
		Operation:    &qms.UpdateOperation{Name: "ADD"},
		ResourceType: &qms.ResourceType{Name: "cpu.hours", Unit: "cpu hours"},
		User:         &qms.QMSUser{Username: "someuser"},
	}
}

func TestAddUserUpdateRequest(t *testing.T) {
	var (
		gotMethod string
		gotPath   string
		gotBody   qms.AddUpdateRequest
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decoding the request body: %s", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"update":{"uuid":"some-uuid"}}`))
	}))
	defer srv.Close()

	c, err := SubscriptionsClient(srv.URL)
	if err != nil {
		t.Fatalf("building the client: %s", err)
	}

	// The suffixed username should be stripped before it reaches the path.
	if err := c.AddUserUpdate(context.Background(), "someuser@example.org", testUpdate()); err != nil {
		t.Fatalf("AddUserUpdate returned an error: %s", err)
	}

	if gotMethod != http.MethodPut {
		t.Errorf("method = %s, want PUT", gotMethod)
	}
	if want := "/user/someuser/updates"; gotPath != want {
		t.Errorf("path = %s, want %s", gotPath, want)
	}
	if gotBody.Update == nil {
		t.Fatal("the request body carried no update")
	}
	if gotBody.Update.ValueType != "usages" {
		t.Errorf("value_type = %q, want %q", gotBody.Update.ValueType, "usages")
	}
	if gotBody.Update.ResourceType == nil || gotBody.Update.ResourceType.Name != "cpu.hours" {
		t.Errorf("resource_type did not round-trip: %+v", gotBody.Update.ResourceType)
	}
	if gotBody.Update.Operation == nil || gotBody.Update.Operation.Name != "ADD" {
		t.Errorf("operation did not round-trip: %+v", gotBody.Update.Operation)
	}
}

func TestAddUserUpdateErrors(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		wantErr    bool
		wantStatus int
		wantMsg    string
	}{
		{
			name:    "success",
			status:  http.StatusOK,
			body:    `{"update":{"uuid":"some-uuid"}}`,
			wantErr: false,
		},
		{
			name:    "unset error code is not an error",
			status:  http.StatusOK,
			body:    `{"error":{"error_code":"UNSET","status_code":0,"message":""},"update":{}}`,
			wantErr: false,
		},
		{
			// A populated error envelope on a 200 still means the request failed.
			name:       "error envelope on a 2xx response",
			status:     http.StatusOK,
			body:       `{"error":{"error_code":"NOT_FOUND","status_code":404,"message":"user name not found"}}`,
			wantErr:    true,
			wantStatus: http.StatusNotFound,
			wantMsg:    "user name not found",
		},
		{
			// The envelope message must survive into the error so log-based triage sees the reason.
			name:       "non-2xx status",
			status:     http.StatusBadRequest,
			body:       `{"error":{"error_code":"BAD_REQUEST","status_code":400,"message":"nope"}}`,
			wantErr:    true,
			wantStatus: http.StatusBadRequest,
			wantMsg:    "nope",
		},
		{
			name:       "non-2xx status with an unparseable body",
			status:     http.StatusBadGateway,
			body:       `bad gateway`,
			wantErr:    true,
			wantStatus: http.StatusBadGateway,
			wantMsg:    "returned 502",
		},
		{
			name:    "unparseable body",
			status:  http.StatusOK,
			body:    `not json`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			c, err := SubscriptionsClient(srv.URL)
			if err != nil {
				t.Fatalf("building the client: %s", err)
			}

			err = c.AddUserUpdate(context.Background(), "someuser", testUpdate())
			if tt.wantErr && err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected no error, got %s", err)
			}
			if tt.wantStatus != 0 {
				if got := GetStatusCode(err); got != tt.wantStatus {
					t.Errorf("status code = %d, want %d", got, tt.wantStatus)
				}
			}
			if tt.wantMsg != "" && !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("error %q does not contain %q", err, tt.wantMsg)
			}
		})
	}
}

func TestServiceError(t *testing.T) {
	tests := []struct {
		name    string
		serr    *svcerror.ServiceError
		wantErr bool
	}{
		{name: "nil", serr: nil, wantErr: false},
		{name: "unset", serr: &svcerror.ServiceError{ErrorCode: svcerror.ErrorCode_UNSET}, wantErr: false},
		{
			name:    "populated without a status code",
			serr:    &svcerror.ServiceError{ErrorCode: svcerror.ErrorCode_INTERNAL, Message: "boom"},
			wantErr: true,
		},
		{
			name:    "populated with a status code",
			serr:    &svcerror.ServiceError{ErrorCode: svcerror.ErrorCode_NOT_FOUND, StatusCode: 404, Message: "gone"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := serviceError(tt.serr)
			if tt.wantErr != (err != nil) {
				t.Fatalf("serviceError() = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
