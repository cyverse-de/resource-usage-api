package summarizer

import (
	"testing"

	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/pkg/errors"
)

// A summary is a user-facing response, so the errors it carries must describe the request rather than
// the databases and services behind it.
func TestSafeMessage(t *testing.T) {
	const fallback = "unable to load the user's data usage"

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "an unknown user is the caller's own answer",
			err:  &db.UserNotFoundError{Username: "someuser@example.org"},
			want: (&db.UserNotFoundError{Username: "someuser@example.org"}).Error(),
		},
		{
			name: "a missing usage record is reported as such",
			err:  &clients.NoUsageRecordedError{Username: "someuser@example.org", ResourceType: "data.size"},
			want: (&clients.NoUsageRecordedError{Username: "someuser@example.org", ResourceType: "data.size"}).Error(),
		},
		{
			name: "a wrapped one is still recognized",
			err:  errors.Wrap(&db.UserNotFoundError{Username: "someuser@example.org"}, "loading the user"),
			want: "loading the user: " + (&db.UserNotFoundError{Username: "someuser@example.org"}).Error(),
		},
		{
			name: "a database error is not repeated to the caller",
			err:  errors.New(`pq: relation "public.user_data_usage" does not exist`),
			want: fallback,
		},
		{
			name: "neither is an error from the subscriptions service",
			err:  clients.NewHTTPError(502, "http://subscriptions/users/someuser/usages returned 502"),
			want: fallback,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := safeMessage(tt.err, fallback); got != tt.want {
				t.Errorf("message = %q, want %q", got, tt.want)
			}
		})
	}
}
