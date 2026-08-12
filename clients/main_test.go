package clients

import (
	"net/url"
	"testing"
)

// Request paths are built from usernames, so an element carrying a character with meaning in a URL has
// to survive being joined onto the base. url.URL.JoinPath unescapes what it joins, which is what these
// cases guard against.
func TestJoinPath(t *testing.T) {
	tests := []struct {
		name     string
		base     string
		elements []string
		want     string
	}{
		{
			name:     "plain elements",
			base:     "http://subscriptions",
			elements: []string{"users", "someuser@example.org", "usages"},
			want:     "http://subscriptions/users/someuser@example.org/usages",
		},
		{
			name:     "base carrying a path prefix",
			base:     "https://example.org/prefix",
			elements: []string{"users", "someuser@example.org", "usages"},
			want:     "https://example.org/prefix/users/someuser@example.org/usages",
		},
		{
			name:     "percent sign in a username",
			base:     "http://subscriptions",
			elements: []string{"users", "100%er@example.org", "usages"},
			want:     "http://subscriptions/users/100%25er@example.org/usages",
		},
		{
			name:     "separator in a username stays inside its segment",
			base:     "http://subscriptions",
			elements: []string{"users", "../admin@example.org", "usages"},
			want:     "http://subscriptions/users/..%2Fadmin@example.org/usages",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base, err := parseBaseURL(tt.base)
			if err != nil {
				t.Fatalf("parsing the base URL: %s", err)
			}

			if got := joinPath(base, tt.elements...).String(); got != tt.want {
				t.Errorf("URL = %s, want %s", got, tt.want)
			}
		})
	}
}

// The escaping has to be the kind a server decodes back to the username that went in.
func TestJoinPathRoundTrips(t *testing.T) {
	base, err := parseBaseURL("http://subscriptions")
	if err != nil {
		t.Fatalf("parsing the base URL: %s", err)
	}

	joined := joinPath(base, "users", "100%er@example.org", "usages")

	parsed, err := url.Parse(joined.String())
	if err != nil {
		t.Fatalf("the joined URL could not be parsed back: %s", err)
	}
	if want := "/users/100%er@example.org/usages"; parsed.Path != want {
		t.Errorf("decoded path = %s, want %s", parsed.Path, want)
	}
}
