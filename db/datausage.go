package db

import (
	"context"
	"fmt"
	"time"

	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/config"
	"github.com/pkg/errors"
)

// SingleUserRoutingKeyPrefix is the routing key prefix for a single user's data usage refresh.
const SingleUserRoutingKeyPrefix = "index.usage.data.user"

// Publisher publishes a message on the service's AMQP exchange. It is declared here, rather than
// taken as a concrete AMQP client, so that this package does not depend on the messaging layer.
type Publisher interface {
	PublishContext(ctx context.Context, key string, body []byte) error
}

// DataUsage reads a user's recorded data usage, enqueuing a refresh when the reading is missing or
// stale.
type DataUsage struct {
	de        Queryer
	subs      *clients.Subscriptions
	publisher Publisher
	config    *config.Config
}

// NewDataUsage returns a DataUsage backed by the DE database and the subscriptions service.
func NewDataUsage(de Queryer, subs *clients.Subscriptions, publisher Publisher, cfg *config.Config) *DataUsage {
	return &DataUsage{de: de, subs: subs, publisher: publisher, config: cfg}
}

// enqueueRefresh asks the batch workers to recompute the user's usage from the ICAT database. A
// failure here is logged rather than returned: the caller can still answer from what QMS has.
func (d *DataUsage) enqueueRefresh(ctx context.Context, username string) {
	key := fmt.Sprintf("%s.%s", SingleUserRoutingKeyPrefix, d.config.TrimUserSuffix(username))
	log.Tracef("Enqueuing update message for %s", username)
	if err := d.publisher.PublishContext(ctx, key, []byte{}); err != nil {
		log.Error(errors.Wrap(err, "Failed enqueuing update message"))
	}
}

// CurrentForUser returns the user's current data usage as recorded in QMS. The username must already
// be domain-qualified.
//
// A clients.NoUsageRecordedError is returned when QMS has nothing for the user, after enqueuing a
// refresh so that a later request can answer.
func (d *DataUsage) CurrentForUser(ctx context.Context, username string) (*clients.UserDataUsage, error) {
	// The DE database is the authority on user identity; QMS may hold a different record.
	userInfo, err := NewDE(d.de, d.config).GetUserInfo(ctx, username)
	if err != nil {
		return nil, err
	}

	usage, err := d.subs.UserCurrentDataUsage(ctx, username)
	if err != nil {
		var noUsage *clients.NoUsageRecordedError
		if errors.As(err, &noUsage) {
			d.enqueueRefresh(ctx, username)
		}
		return nil, err
	}

	usage.UserID = userInfo.ID
	usage.Username = userInfo.Username

	// LastModified is the field QMS moves when it records a new reading. Time carries the creation of
	// the usage record, which never moves, so measuring staleness from it would enqueue a refresh on
	// every request once the record itself was older than the interval.
	if usage.LastModified != nil && usage.LastModified.Add(d.config.RefreshInterval).Before(time.Now()) {
		d.enqueueRefresh(ctx, username)
	}

	return usage, nil
}
