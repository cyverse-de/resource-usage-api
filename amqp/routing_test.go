package amqp

import (
	"strings"
	"testing"

	"github.com/cyverse-de/resource-usage-api/config"
	"github.com/cyverse-de/resource-usage-api/db"
)

// The publisher interface is declared in db so that package does not depend on this one; the two have
// to stay compatible.
var _ db.Publisher = (*AMQP)(nil)

// The routing key prefix the HTTP side publishes on has to be the one the consumer binds to.
func TestSingleUserPrefixMatchesPublisher(t *testing.T) {
	if SingleUserPrefix != db.SingleUserRoutingKeyPrefix {
		t.Errorf("consumer binds %q but the publisher uses %q", SingleUserPrefix, db.SingleUserRoutingKeyPrefix)
	}
}

// The batch bounds are parsed straight out of the routing key. A key that does not carry both halves
// must be reported rather than indexed blindly, which would panic in an unrecovered goroutine.
func TestBatchBoundsParsing(t *testing.T) {
	tests := []struct {
		name       string
		routingKey string
		wantErr    bool
		wantStart  string
		wantEnd    string
	}{
		{
			name:       "well formed key",
			routingKey: BatchUserPrefix + ".alice.bob",
			wantStart:  "alice",
			wantEnd:    "bob",
		},
		{
			// A username containing a dot lands in the end bound rather than truncating the range.
			name:       "end username containing a dot",
			routingKey: BatchUserPrefix + ".alice.bob.smith",
			wantStart:  "alice",
			wantEnd:    "bob.smith",
		},
		{"no second bound", BatchUserPrefix + ".alice", true, "", ""},
		{"trailing separator only", BatchUserPrefix + ".alice.", true, "", ""},
		{"empty start bound", BatchUserPrefix + "..bob", true, "", ""},
		{"no bounds at all", BatchUserPrefix + ".", true, "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, err := parseBatchBounds(tt.routingKey)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error for %q, got %q..%q", tt.routingKey, start, end)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tt.routingKey, err)
			}
			if start != tt.wantStart || end != tt.wantEnd {
				t.Errorf("bounds = %q..%q, want %q..%q", start, end, tt.wantStart, tt.wantEnd)
			}
		})
	}
}

func TestSingleUserNameParsing(t *testing.T) {
	tests := []struct {
		name       string
		routingKey string
		wantErr    bool
		wantUser   string
	}{
		{"well formed key", SingleUserPrefix + ".alice", false, "alice"},
		{"no username", SingleUserPrefix + ".", true, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			user, err := parseSingleUsername(tt.routingKey)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error for %q, got %q", tt.routingKey, user)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tt.routingKey, err)
			}
			if user != tt.wantUser {
				t.Errorf("username = %q, want %q", user, tt.wantUser)
			}
		})
	}
}

// The batch fan-out publishes keys that its own consumer must be able to parse back.
func TestBatchRoutingKeysRoundTrip(t *testing.T) {
	cfg := &config.Config{UserSuffix: "example.org"}

	key := strings.Join([]string{BatchUserPrefix, cfg.TrimUserSuffix("alice@example.org"), cfg.TrimUserSuffix("bob@example.org")}, ".")

	start, end, err := parseBatchBounds(key)
	if err != nil {
		t.Fatalf("published key %q could not be parsed back: %v", key, err)
	}
	if start != "alice" || end != "bob" {
		t.Errorf("bounds = %q..%q, want alice..bob", start, end)
	}
}
