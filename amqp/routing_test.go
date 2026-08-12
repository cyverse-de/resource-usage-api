package amqp

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/cyverse-de/resource-usage-api/config"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/streadway/amqp"
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
		// The consumer's binding ends in "#", which matches zero words, so the bare prefix is delivered
		// here too and must not be sliced past.
		{"bare prefix", BatchUserPrefix, true, "", ""},
		{"prefix of the prefix", "index.usage.data", true, "", ""},
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
		{"username containing a dot", SingleUserPrefix + ".alice.jones", false, "alice.jones"},
		{"no username", SingleUserPrefix + ".", true, ""},
		// The binding matches the bare prefix as well, and slicing past it would panic the consumer.
		{"bare prefix", SingleUserPrefix, true, ""},
		{"prefix of the prefix", "index.usage.data", true, ""},
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

// Bounds published in the message body have to survive usernames containing dots, which is the case
// the routing key cannot represent: alice.jones..bob.smith and alice..jones.bob.smith produce the
// same key.
func TestBatchMessageBoundsRoundTrip(t *testing.T) {
	tests := []struct{ start, end string }{
		{"alice", "bob"},
		{"alice.jones", "bob.smith"},
		{"alice.jones", "bob"},
	}

	for _, tt := range tests {
		t.Run(tt.start+".."+tt.end, func(t *testing.T) {
			body, err := json.Marshal(&batchBounds{Start: tt.start, End: tt.end})
			if err != nil {
				t.Fatalf("building the message body: %v", err)
			}

			delivery := amqp.Delivery{
				RoutingKey: strings.Join([]string{BatchUserPrefix, tt.start, tt.end}, "."),
				Body:       body,
			}

			start, end, err := parseBatchMessage(delivery)
			if err != nil {
				t.Fatalf("published message could not be parsed back: %v", err)
			}
			if start != tt.start || end != tt.end {
				t.Errorf("bounds = %q..%q, want %q..%q", start, end, tt.start, tt.end)
			}
		})
	}
}

// Messages enqueued before the bounds moved into the body carry an empty one and still have to be
// handled, and an unreadable body has to be reported as something redelivery cannot fix.
func TestBatchMessageBodyHandling(t *testing.T) {
	tests := []struct {
		name              string
		body              string
		wantErr           bool
		wantUnprocessable bool
		wantStart         string
		wantEnd           string
	}{
		{name: "empty body falls back to the routing key", body: "", wantStart: "alice", wantEnd: "bob"},
		{name: "unparseable body", body: "{", wantErr: true, wantUnprocessable: true},
		{name: "body missing a bound", body: `{"start":"alice"}`, wantErr: true, wantUnprocessable: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delivery := amqp.Delivery{RoutingKey: BatchUserPrefix + ".alice.bob", Body: []byte(tt.body)}

			start, end, err := parseBatchMessage(delivery)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got %q..%q", start, end)
				}
				var unreadable *unprocessableError
				if errors.As(err, &unreadable) != tt.wantUnprocessable {
					t.Errorf("error %q is reported as unprocessable = %v, want %v", err, !tt.wantUnprocessable, tt.wantUnprocessable)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if start != tt.wantStart || end != tt.wantEnd {
				t.Errorf("bounds = %q..%q, want %q..%q", start, end, tt.wantStart, tt.wantEnd)
			}
		})
	}
}
