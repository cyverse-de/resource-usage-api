package internal

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cyverse-de/resource-usage-api/config"
	"github.com/labstack/echo/v4"
)

// The data usage routes take the username as the first path segment, so they share the root with the
// static "summary" branch. Echo resolves static segments before parameters and backtracks when the
// static branch dead-ends, which is what lets the two coexist; this pins that down, since a change to
// the route table could silently start routing summary requests to the data usage handlers.
func TestRouteTable(t *testing.T) {
	app := New(&Dependencies{Config: &config.Config{UserSuffix: "example.org"}})
	router := app.Router()

	tests := []struct {
		name       string
		method     string
		path       string
		wantRoute  string
		wantStatus int
	}{
		{"greeting", http.MethodGet, "/", "/", 0},
		{"summary", http.MethodGet, "/summary/someuser", "/summary/:username", 0},
		{"summary with a trailing slash", http.MethodGet, "/summary/someuser/", "/summary/:username/", 0},
		{"current data usage", http.MethodGet, "/someuser/data/current", "/:username/data/current", 0},
		{"data usage update", http.MethodPost, "/someuser/data/update", "/:username/data/update", 0},
		{"data overage", http.MethodGet, "/someuser/data/overage", "/:username/data/overage", 0},

		// A user named "summary" still reaches the data usage routes by way of echo's backtracking.
		{"user named summary", http.MethodGet, "/summary/data/current", "/:username/data/current", 0},

		{"unknown path", http.MethodGet, "/someuser/nope", "", http.StatusNotFound},
		{"summary is not a collection", http.MethodGet, "/summary", "", http.StatusNotFound},
		{"wrong method on update", http.MethodGet, "/someuser/data/update", "", http.StatusMethodNotAllowed},
		{"wrong method on summary", http.MethodPost, "/summary/someuser", "", http.StatusMethodNotAllowed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := router.NewContext(httptest.NewRequest(tt.method, tt.path, nil), httptest.NewRecorder())
			router.Router().Find(tt.method, tt.path, c)

			if tt.wantStatus != 0 {
				err := c.Handler()(c)
				if err == nil {
					t.Fatalf("%s %s: expected status %d, got a matched route", tt.method, tt.path, tt.wantStatus)
				}
				he, ok := err.(*echo.HTTPError)
				if !ok {
					t.Fatalf("%s %s: unexpected error type %T: %v", tt.method, tt.path, err, err)
				}
				if he.Code != tt.wantStatus {
					t.Errorf("%s %s: status = %d, want %d", tt.method, tt.path, he.Code, tt.wantStatus)
				}
				return
			}

			if got := c.Path(); got != tt.wantRoute {
				t.Errorf("%s %s: matched route = %q, want %q", tt.method, tt.path, got, tt.wantRoute)
			}
		})
	}
}
