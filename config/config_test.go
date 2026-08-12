package config

import (
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/providers/confmap"
)

// baseSettings is a minimal set of settings that passes validation. Tests override or drop individual
// keys rather than rebuilding the whole thing.
func baseSettings() map[string]any {
	return map[string]any{
		"db.uri":             "postgres://de@dedb:5432/de",
		"icat.uri":           "postgres://icat@icat-db:5432/ICAT",
		"icat.zone":          "iplant",
		"icat.rootresources": []string{"mainIngestRes", "mainReplRes"},
		"users.domain":       "example.org",
		"amqp.uri":           "amqp://guest:guest@rabbit:5672/",
		"amqp.exchange.name": "de",
		"amqp.exchange.type": "topic",
	}
}

// load builds a Koanf instance from the base settings with the given overrides applied. A nil value
// removes the key, standing in for a setting missing from the configuration file.
func load(t *testing.T, overrides map[string]any) *koanf.Koanf {
	t.Helper()

	settings := baseSettings()
	maps.Copy(settings, overrides)
	for key, value := range overrides {
		if value == nil {
			delete(settings, key)
		}
	}

	k := koanf.New(".")
	if err := k.Load(confmap.Provider(settings, "."), nil); err != nil {
		t.Fatalf("unable to build the test configuration: %v", err)
	}
	return k
}

func TestNewAppliesDefaults(t *testing.T) {
	c, err := New(load(t, nil))
	if err != nil {
		t.Fatalf("expected the configuration to be valid, got %v", err)
	}

	if c.RefreshInterval != DefaultRefreshInterval {
		t.Errorf("refresh interval = %v, want %v", c.RefreshInterval, DefaultRefreshInterval)
	}
	if c.BatchSize != DefaultBatchSize {
		t.Errorf("batch size = %d, want %d", c.BatchSize, DefaultBatchSize)
	}
	if c.DBSchema != DefaultDBSchema {
		t.Errorf("db schema = %q, want %q", c.DBSchema, DefaultDBSchema)
	}
}

// A leading "@" on users.domain is normalized away at load so nothing downstream has to cope with
// both spellings.
func TestNewTrimsUserSuffix(t *testing.T) {
	c, err := New(load(t, map[string]any{"users.domain": "@example.org"}))
	if err != nil {
		t.Fatalf("expected the configuration to be valid, got %v", err)
	}
	if c.UserSuffix != "example.org" {
		t.Errorf("user suffix = %q, want %q", c.UserSuffix, "example.org")
	}
}

func TestNewRejectsBadConfigurations(t *testing.T) {
	tests := []struct {
		name      string
		overrides map[string]any
		wantErr   string
	}{
		{"missing db uri", map[string]any{"db.uri": nil}, "db.uri"},
		{"missing icat uri", map[string]any{"icat.uri": nil}, "icat.uri"},
		{"missing icat zone", map[string]any{"icat.zone": nil}, "icat.zone"},
		{"missing user domain", map[string]any{"users.domain": nil}, "users.domain"},
		{"missing amqp uri", map[string]any{"amqp.uri": nil}, "amqp.uri"},
		{"missing exchange name", map[string]any{"amqp.exchange.name": nil}, "amqp.exchange.name"},
		{"missing exchange type", map[string]any{"amqp.exchange.type": nil}, "amqp.exchange.type"},

		// An absent or empty list yields an empty slice rather than nil, so a nil check would let this
		// through and the service would report zero data usage for every user.
		{"missing root resources", map[string]any{"icat.rootresources": nil}, "icat.rootresources"},
		{"empty root resources", map[string]any{"icat.rootresources": []string{}}, "icat.rootresources"},

		// A zero or negative batch size reaches the ICAT batch query as a modulus and a loop increment.
		{"zero batch size", map[string]any{"amqp.batchsize": 0}, "amqp.batchsize"},
		{"negative batch size", map[string]any{"amqp.batchsize": -5}, "amqp.batchsize"},

		// A bare integer must not be silently reinterpreted as a nanosecond count.
		{"bare integer refresh interval", map[string]any{"datausage.refreshinterval": 3}, "datausage.refreshinterval"},
		{"unparsable refresh interval", map[string]any{"datausage.refreshinterval": "soon"}, "datausage.refreshinterval"},
		{"negative refresh interval", map[string]any{"datausage.refreshinterval": "-1h"}, "datausage.refreshinterval"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(load(t, tt.overrides))
			if err == nil {
				t.Fatalf("expected an error mentioning %q, got none", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

func TestNewReadsExplicitValues(t *testing.T) {
	c, err := New(load(t, map[string]any{
		"datausage.refreshinterval": "30m",
		"amqp.batchsize":            25,
		"amqp.queueprefix":          "qa",
		"qms.enabled":               true,
	}))
	if err != nil {
		t.Fatalf("expected the configuration to be valid, got %v", err)
	}

	if c.RefreshInterval != 30*time.Minute {
		t.Errorf("refresh interval = %v, want 30m", c.RefreshInterval)
	}
	if c.BatchSize != 25 {
		t.Errorf("batch size = %d, want 25", c.BatchSize)
	}
	if !c.QMSEnabled {
		t.Error("expected QMS to be enabled")
	}
	if len(c.RootResourceNames) != 2 || c.RootResourceNames[0] != "mainIngestRes" {
		t.Errorf("root resources = %v, want [mainIngestRes mainReplRes]", c.RootResourceNames)
	}
}

// FixUsername has to be idempotent: the summary routes are reachable with either a bare or an
// already-qualified username, and the DE database only matches the qualified form.
func TestFixUsername(t *testing.T) {
	c := &Config{UserSuffix: "iplantcollaborative.org"}

	tests := []struct {
		name     string
		username string
		want     string
	}{
		{"bare username", "johnw", "johnw@iplantcollaborative.org"},
		{"already qualified", "johnw@iplantcollaborative.org", "johnw@iplantcollaborative.org"},
		{"foreign domain", "johnw@example.net", "johnw@iplantcollaborative.org"},
		{"username that is a tail of the domain", "org", "org@iplantcollaborative.org"},
		{"username that is the domain", "iplantcollaborative.org", "iplantcollaborative.org@iplantcollaborative.org"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := c.FixUsername(tt.username); got != tt.want {
				t.Errorf("FixUsername(%q) = %q, want %q", tt.username, got, tt.want)
			}
		})
	}
}

func TestTrimUserSuffix(t *testing.T) {
	c := &Config{UserSuffix: "example.org"}

	tests := []struct {
		name     string
		username string
		want     string
	}{
		{"configured domain is removed", "johnw@example.org", "johnw"},
		{"bare username is unchanged", "johnw", "johnw"},
		// iRODS federation puts a foreign zone here, and it has to survive.
		{"foreign zone is preserved", "johnw@otherzone", "johnw@otherzone"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := c.TrimUserSuffix(tt.username); got != tt.want {
				t.Errorf("TrimUserSuffix(%q) = %q, want %q", tt.username, got, tt.want)
			}
		})
	}
}

// The queue names are reused from the retired data-usage-api service, so they must not drift.
func TestDataUsageQueueNames(t *testing.T) {
	tests := []struct {
		name           string
		prefix         string
		wantBatch      string
		wantIndividual string
	}{
		{"no prefix", "", "data-usage-api.batch", "data-usage-api.individual"},
		{"with prefix", "qa", "qa.data-usage-api.batch", "qa.data-usage-api.individual"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch, individual := (&Config{AMQPQueuePrefix: tt.prefix}).DataUsageQueueNames()
			if batch != tt.wantBatch {
				t.Errorf("batch queue = %q, want %q", batch, tt.wantBatch)
			}
			if individual != tt.wantIndividual {
				t.Errorf("individual queue = %q, want %q", individual, tt.wantIndividual)
			}
		})
	}
}
