// Package config loads and validates the service's settings.
package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/knadh/koanf"
)

// Defaults for settings that may be omitted from the configuration file. The service formerly used
// configurate, which merged an inline default document; koanf has no equivalent, so the defaults live
// here instead.
const (
	DefaultRefreshInterval = 3 * time.Hour
	DefaultDBSchema        = "public"
	DefaultBatchSize       = 100
)

// Config holds the settings needed to run the service.
type Config struct {
	DBURI    string
	DBSchema string

	ICATURI           string
	Zone              string
	RootResourceNames []string

	// UserSuffix is the user domain, stored without a leading "@".
	UserSuffix string

	RefreshInterval time.Duration
	BatchSize       int

	AMQPURI          string
	AMQPExchangeName string
	AMQPExchangeType string
	AMQPQueuePrefix  string

	QMSEnabled bool
}

// New builds a Config from a Koanf instance, applying defaults and validating the result.
//
// Configuration keys are lowercase and free of underscores because go-mod/cfg maps environment
// variables onto keys by lowercasing them and replacing "_" with the key delimiter. A key such as
// icat.rootResources or amqp.batch_size could never be overridden via DISCOENV_*.
func New(k *koanf.Koanf) (*Config, error) {
	refreshInterval, err := duration(k, "datausage.refreshinterval", DefaultRefreshInterval)
	if err != nil {
		return nil, err
	}

	c := &Config{
		DBURI:             k.String("db.uri"),
		DBSchema:          stringOrDefault(k, "db.schema", DefaultDBSchema),
		ICATURI:           k.String("icat.uri"),
		Zone:              k.String("icat.zone"),
		RootResourceNames: k.Strings("icat.rootresources"),
		UserSuffix:        strings.Trim(k.String("users.domain"), "@"),
		RefreshInterval:   refreshInterval,
		BatchSize:         intOrDefault(k, "amqp.batchsize", DefaultBatchSize),
		AMQPURI:           k.String("amqp.uri"),
		AMQPExchangeName:  k.String("amqp.exchange.name"),
		AMQPExchangeType:  k.String("amqp.exchange.type"),
		AMQPQueuePrefix:   k.String("amqp.queueprefix"),
		QMSEnabled:        k.Bool("qms.enabled"),
	}

	if err := c.Validate(); err != nil {
		return nil, err
	}

	return c, nil
}

// duration reads a duration setting. koanf's own Duration getter interprets bare integers as
// nanoseconds and silently discards parse errors, either of which would turn a misconfigured refresh
// interval into an effectively-zero one, so the value is parsed here instead.
func duration(k *koanf.Koanf, key string, fallback time.Duration) (time.Duration, error) {
	raw := k.String(key)
	if raw == "" {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration such as \"3h\": %w", key, err)
	}

	return parsed, nil
}

func stringOrDefault(k *koanf.Koanf, key, fallback string) string {
	if v := k.String(key); v != "" {
		return v
	}
	return fallback
}

func intOrDefault(k *koanf.Koanf, key string, fallback int) int {
	if !k.Exists(key) {
		return fallback
	}
	return k.Int(key)
}

// Validate reports the first missing or unusable setting.
func (c *Config) Validate() error {
	switch {
	case c.DBURI == "":
		return errors.New("db.uri must be set")
	case c.DBSchema == "":
		return errors.New("db.schema must be set")
	case c.ICATURI == "":
		return errors.New("icat.uri must be set")
	case c.Zone == "":
		return errors.New("icat.zone must be set")
	// koanf returns an empty slice rather than nil for an absent key, so this has to test the length.
	// An empty resource list makes the ICAT usage query match nothing and report zero usage for every
	// user, which is worse than failing to start.
	case len(c.RootResourceNames) == 0:
		return errors.New("icat.rootresources must be set")
	case c.UserSuffix == "":
		return errors.New("users.domain must be set")
	case c.RefreshInterval <= 0:
		return errors.New("datausage.refreshinterval must be greater than zero")
	// A batch size of zero reaches the ICAT batch query as a modulus and as a loop increment.
	case c.BatchSize <= 0:
		return errors.New("amqp.batchsize must be greater than zero")
	case c.AMQPURI == "":
		return errors.New("amqp.uri must be set")
	case c.AMQPExchangeName == "":
		return errors.New("amqp.exchange.name must be set")
	case c.AMQPExchangeType == "":
		return errors.New("amqp.exchange.type must be set")
	}

	return nil
}

// dataUsageQueueBase is the queue name the data-usage-api service used. The merged service keeps it so
// that the existing durable queues and their bindings are reused rather than left with stranded
// messages.
const dataUsageQueueBase = "data-usage-api"

// DataUsageQueueNames returns the batch and individual data-usage queue names.
func (c *Config) DataUsageQueueNames() (batch, individual string) {
	if c.AMQPQueuePrefix != "" {
		return fmt.Sprintf("%s.%s.batch", c.AMQPQueuePrefix, dataUsageQueueBase),
			fmt.Sprintf("%s.%s.individual", c.AMQPQueuePrefix, dataUsageQueueBase)
	}
	return dataUsageQueueBase + ".batch", dataUsageQueueBase + ".individual"
}
