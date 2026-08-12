package clients

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/p/go/svcerror"
	"github.com/cyverse-de/resource-usage-api/config"
)

func testConfig() *config.Config {
	return &config.Config{UserSuffix: "example.org"}
}

// newTestClient returns a client pointed at a server that records the request and replies with body.
func newTestClient(t *testing.T, status int, body string, record func(*http.Request)) *Subscriptions {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if record != nil {
			record(r)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	c, err := SubscriptionsClient(srv.URL, testConfig())
	if err != nil {
		t.Fatalf("building the client: %s", err)
	}
	return c
}

func TestSubscriptionsClientValidation(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		wantErr bool
	}{
		{name: "plain http", baseURL: "http://subscriptions", wantErr: false},
		{name: "https with a path", baseURL: "https://example.org/subscriptions/", wantErr: false},
		{name: "https with a path prefix", baseURL: "https://example.org/prefix/", wantErr: false},
		{name: "missing scheme", baseURL: "subscriptions", wantErr: true},
		{name: "unsupported scheme", baseURL: "nats://subscriptions", wantErr: true},
		{name: "missing host", baseURL: "http://", wantErr: true},
		{name: "empty", baseURL: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := SubscriptionsClient(tt.baseURL, testConfig())
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

	c := newTestClient(t, http.StatusOK, `{"update":{"uuid":"some-uuid"}}`, func(r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decoding the request body: %s", err)
		}
	})

	if err := c.AddUserUpdate(context.Background(), "someuser@example.org", testUpdate()); err != nil {
		t.Fatalf("AddUserUpdate returned an error: %s", err)
	}

	if gotMethod != http.MethodPut {
		t.Errorf("method = %s, want PUT", gotMethod)
	}
	// The username reaches subscriptions fully qualified. subscriptions strips the domain itself, so
	// either form works, and the qualified one matches what the body already carried.
	if want := "/user/someuser@example.org/updates"; gotPath != want {
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
			c := newTestClient(t, tt.status, tt.body, nil)

			err := c.AddUserUpdate(context.Background(), "someuser", testUpdate())
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

func TestUserCurrentDataUsage(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantNoUsage bool
		wantTotal   int64
	}{
		{
			name:      "data.size usage is picked out of the list",
			body:      `{"usages":[{"uuid":"cpu","usage":3,"resource_type":{"name":"cpu.hours"}},{"uuid":"data","usage":42,"resource_type":{"name":"data.size"}}]}`,
			wantTotal: 42,
		},
		{
			// The data-usage lookup branches on this to enqueue an async refresh and report a 404.
			name:        "no data.size usage is reported as such",
			body:        `{"usages":[{"uuid":"cpu","usage":3,"resource_type":{"name":"cpu.hours"}}]}`,
			wantNoUsage: true,
		},
		{
			name:        "empty usage list",
			body:        `{"usages":[]}`,
			wantNoUsage: true,
		},
		{
			name: "a usage without a resource type is skipped",
			body: `{"usages":[{"uuid":"orphan","usage":9}]}`,
			// Nothing matches, so the typed error still comes back rather than a nil dereference.
			wantNoUsage: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClient(t, http.StatusOK, tt.body, nil)

			usage, err := c.UserCurrentDataUsage(context.Background(), "someuser")
			if tt.wantNoUsage {
				var noUsage *NoUsageRecordedError
				if !errors.As(err, &noUsage) {
					t.Fatalf("error = %v, want a *NoUsageRecordedError", err)
				}
				if got := GetStatusCode(err); got != http.StatusNotFound {
					t.Errorf("status code = %d, want 404", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if usage.Total != tt.wantTotal {
				t.Errorf("total = %d, want %d", usage.Total, tt.wantTotal)
			}
			// The summary response declares these as strings, so they must never marshal to null.
			if usage.Time == nil || usage.LastModified == nil {
				t.Errorf("timestamps should always be populated: %+v", usage)
			}
		})
	}
}

func TestUserCurrentDataUsageRequest(t *testing.T) {
	var gotPath, gotMethod string

	c := newTestClient(t, http.StatusOK, `{"usages":[{"usage":1,"resource_type":{"name":"data.size"}}]}`,
		func(r *http.Request) {
			gotPath = r.URL.Path
			gotMethod = r.Method
		})

	if _, err := c.UserCurrentDataUsage(context.Background(), "someuser@elsewhere.net"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %s, want GET", gotMethod)
	}
	// FixUsername replaces whatever suffix came in with the configured one.
	if want := "/users/someuser@example.org/usages"; gotPath != want {
		t.Errorf("path = %s, want %s", gotPath, want)
	}
}

func TestAllResourceOveragesForUser(t *testing.T) {
	var gotPath string

	c := newTestClient(t, http.StatusOK, `{"overages":[{"resource_name":"data.size","quota":1,"usage":2}]}`,
		func(r *http.Request) { gotPath = r.URL.Path })

	overages, err := c.AllResourceOveragesForUser(context.Background(), "someuser")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if want := "/users/someuser@example.org/overages"; gotPath != want {
		t.Errorf("path = %s, want %s", gotPath, want)
	}
	if len(overages.Overages) != 1 || overages.Overages[0].ResourceName != "data.size" {
		t.Errorf("overages did not round-trip: %+v", overages.Overages)
	}
}

func TestUpdateUsageForUser(t *testing.T) {
	var (
		gotPath   string
		gotMethod string
		gotBody   qms.AddUpdateRequest
	)

	c := newTestClient(t, http.StatusOK, `{"update":{"uuid":"some-uuid","value":42}}`, func(r *http.Request) {
		gotPath = r.URL.Path
		gotMethod = r.Method
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
	})

	usage, err := c.UpdateUsageForUser(context.Background(), "someuser", 42)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if gotMethod != http.MethodPut {
		t.Errorf("method = %s, want PUT", gotMethod)
	}
	// Note the singular "user" segment here; the GET routes use "users".
	if want := "/user/someuser@example.org/updates"; gotPath != want {
		t.Errorf("path = %s, want %s", gotPath, want)
	}
	if gotBody.Update == nil {
		t.Fatal("the request body carried no update")
	}
	if gotBody.Update.ValueType != "usages" {
		t.Errorf("value_type = %q, want %q", gotBody.Update.ValueType, "usages")
	}
	if gotBody.Update.Operation == nil || gotBody.Update.Operation.Name != "SET" {
		t.Errorf("operation did not round-trip: %+v", gotBody.Update.Operation)
	}
	if gotBody.Update.ResourceType == nil || gotBody.Update.ResourceType.Name != "data.size" {
		t.Errorf("resource_type did not round-trip: %+v", gotBody.Update.ResourceType)
	}
	if usage.Total != 42 {
		t.Errorf("total = %d, want 42", usage.Total)
	}
}

func TestUpdateUsageForUserMissingUpdate(t *testing.T) {
	c := newTestClient(t, http.StatusOK, `{}`, nil)

	if _, err := c.UpdateUsageForUser(context.Background(), "someuser", 42); err == nil {
		t.Fatal("expected an error when the response carries no update")
	}
}

func TestGetSubscriptionSummary(t *testing.T) {
	var gotPath string

	c := newTestClient(t, http.StatusOK, `{"subscription":{"uuid":"sub-uuid"}}`,
		func(r *http.Request) { gotPath = r.URL.Path })

	summary, err := c.GetSubscriptionSummary(context.Background(), "someuser")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if want := "/summary/someuser@example.org"; gotPath != want {
		t.Errorf("path = %s, want %s", gotPath, want)
	}
	if summary.Subscription == nil || summary.Subscription.Uuid != "sub-uuid" {
		t.Errorf("subscription did not round-trip: %+v", summary.Subscription)
	}
}

// Every read path has to surface the server's failure rather than a zero value.
func TestErrorHandling(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		body        string
		wantContain string
	}{
		{
			name:        "error envelope on a 2xx response",
			status:      http.StatusOK,
			body:        `{"error":{"error_code":"NOT_FOUND","status_code":404,"message":"user name not found"}}`,
			wantContain: "user name not found",
		},
		{
			name:        "non-2xx status carries the server message",
			status:      http.StatusInternalServerError,
			body:        `{"error":{"error_code":"INTERNAL","status_code":500,"message":"boom"}}`,
			wantContain: "boom",
		},
		{
			name:        "non-2xx with an unparseable body still reports the status",
			status:      http.StatusBadGateway,
			body:        `<html>bad gateway</html>`,
			wantContain: "502",
		},
		{
			name:   "unparseable body",
			status: http.StatusOK,
			body:   `not json`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClient(t, tt.status, tt.body, nil)

			assertErr := func(call string, err error) {
				t.Helper()
				if err == nil {
					t.Errorf("%s: expected an error, got nil", call)
					return
				}
				if tt.wantContain != "" && !strings.Contains(err.Error(), tt.wantContain) {
					t.Errorf("%s: error %q does not contain %q", call, err, tt.wantContain)
				}
			}

			_, err := c.UserCurrentDataUsage(context.Background(), "someuser")
			assertErr("UserCurrentDataUsage", err)
			_, err = c.AllResourceOveragesForUser(context.Background(), "someuser")
			assertErr("AllResourceOveragesForUser", err)
			_, err = c.UpdateUsageForUser(context.Background(), "someuser", 1)
			assertErr("UpdateUsageForUser", err)
			_, err = c.GetSubscriptionSummary(context.Background(), "someuser")
			assertErr("GetSubscriptionSummary", err)
		})
	}
}

func TestBaseURLPathPrefix(t *testing.T) {
	var gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"usages":[{"usage":1,"resource_type":{"name":"data.size"}}]}`))
	}))
	t.Cleanup(srv.Close)

	c, err := SubscriptionsClient(srv.URL+"/prefix/", testConfig())
	if err != nil {
		t.Fatalf("building the client: %s", err)
	}

	if _, err := c.UserCurrentDataUsage(context.Background(), "someuser"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if want := "/prefix/users/someuser@example.org/usages"; gotPath != want {
		t.Errorf("path = %s, want %s", gotPath, want)
	}
}

func TestAddUserUpdatesBatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/bad@") {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":{"error_code":"INTERNAL","status_code":500,"message":"boom"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"update":{"uuid":"good-uuid","value":5}}`))
	}))
	t.Cleanup(srv.Close)

	c, err := SubscriptionsClient(srv.URL, testConfig())
	if err != nil {
		t.Fatalf("building the client: %s", err)
	}

	res, err := c.AddUserUpdatesBatch(context.Background(), map[string]float64{"good": 5, "bad": 6})
	if err == nil {
		t.Error("expected the failed user's error to be returned")
	}
	// The failed user must not leave a nil placeholder among the successful results.
	if len(res) != 1 {
		t.Fatalf("results = %d entries, want 1: %+v", len(res), res)
	}
	if res[0] == nil || res[0].ID != "good-uuid" {
		t.Errorf("the successful update did not round-trip: %+v", res[0])
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

// GetStatusCode has to see through wrapping, since client errors pick up context on the way up.
func TestGetStatusCodeUnwraps(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"nil", nil, http.StatusInternalServerError},
		{"plain error", errors.New("boom"), http.StatusInternalServerError},
		{"http error", NewHTTPError(http.StatusNotFound, "gone"), http.StatusNotFound},
		{"wrapped http error", errors.Join(errors.New("context"), NewHTTPError(http.StatusBadGateway, "gone")), http.StatusBadGateway},
		{"no usage recorded", &NoUsageRecordedError{Username: "u", ResourceType: "data.size"}, http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetStatusCode(tt.err); got != tt.want {
				t.Errorf("GetStatusCode(%v) = %d, want %d", tt.err, got, tt.want)
			}
		})
	}
}
