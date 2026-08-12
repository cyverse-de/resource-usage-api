package amqp

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

// Routing key prefixes for data usage refresh requests.
const (
	SingleUserPrefix = "index.usage.data.user"
	BatchUserPrefix  = "index.usage.data.batch.user"
)

// Timeouts for the data usage handlers. A batch walks a range of users through the ICAT database and
// needs considerably longer than a single user.
const (
	singleUserTimeout = time.Minute
	batchTimeout      = 5 * time.Minute
)

// recvDataUsage dispatches a data usage message by routing key. The message is acknowledged only once
// it has been handled; anything that fails, or that matches no branch, is rejected so that the
// consumer does not sit on an outstanding delivery and stall behind its prefetch limit.
func (a *AMQP) recvDataUsage(ctx context.Context, delivery amqp.Delivery) {
	var err error

	log := log.WithContext(ctx)
	log.Tracef("Got message: %s", delivery.RoutingKey)

	switch key := delivery.RoutingKey; {
	case key == "index.all" || key == "index.usage.data":
		err = a.sendBatchMessages(ctx)
	case strings.HasPrefix(key, BatchUserPrefix):
		err = a.updateUserBatch(ctx, key)
	case strings.HasPrefix(key, SingleUserPrefix):
		err = a.updateUser(ctx, key)
	default:
		log.Errorf("no handler for routing key %s, rejecting message", key)
		reject(delivery)
		return
	}

	if err != nil {
		log.Error(errors.Wrap(err, "Error handling message"))
		reject(delivery)
		return
	}

	if err = delivery.Ack(false); err != nil {
		log.Error(errors.Wrapf(err, "Error acknowledging message: %s", delivery.RoutingKey))
	}
}

// databases returns a coordinator over the DE and ICAT databases. A fresh one is built per message
// because it holds the transactions for the work in hand.
func (a *AMQP) databases() *db.BothDatabases {
	return db.NewBoth(a.deps.DEDB, a.deps.ICAT, a.deps.Config, a.deps.Subscriptions)
}

// parseSingleUsername reads the username a single-user refresh names.
func parseSingleUsername(routingKey string) (string, error) {
	username := routingKey[len(SingleUserPrefix)+1:]
	if username == "" {
		return "", errors.Errorf("no username in routing key %s", routingKey)
	}
	return username, nil
}

// parseBatchBounds reads the "<start>.<end>" username pair a batch refresh names. A key carrying
// anything else cannot be acted on, and indexing it blindly would take the process down from an
// unrecovered goroutine.
func parseBatchBounds(routingKey string) (start, end string, err error) {
	bounds := strings.SplitN(routingKey[len(BatchUserPrefix)+1:], ".", 2)
	if len(bounds) != 2 || bounds[0] == "" || bounds[1] == "" {
		return "", "", errors.Errorf("routing key %s does not name a start and end username", routingKey)
	}
	return bounds[0], bounds[1], nil
}

// updateUser recalculates one user's data usage. The username is the remainder of the routing key.
func (a *AMQP) updateUser(ctx context.Context, routingKey string) error {
	username, err := parseSingleUsername(routingKey)
	if err != nil {
		return err
	}
	user := a.deps.Config.FixUsername(username)

	log.Tracef("Recalculating usage for %s asynchronously", user)

	ctx, cancel := context.WithTimeout(ctx, singleUserTimeout)
	defer cancel()

	if _, err := a.databases().UpdateUserDataUsage(ctx, user); err != nil {
		return errors.Wrap(err, "Failed updating usage information")
	}

	return nil
}

// updateUserBatch recalculates data usage for the range of users named in the routing key.
func (a *AMQP) updateUserBatch(ctx context.Context, routingKey string) error {
	start, end, err := parseBatchBounds(routingKey)
	if err != nil {
		return err
	}

	log.Infof("Updating the user batch from %s to %s", start, end)

	ctx, cancel := context.WithTimeout(ctx, batchTimeout)
	defer cancel()

	if _, err := a.databases().UpdateUserDataUsageBatch(ctx, start, end); err != nil {
		return errors.Wrap(err, "Failed updating usage information")
	}

	return nil
}

// sendBatchMessages splits the user list into batches and enqueues a message for each one.
func (a *AMQP) sendBatchMessages(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, singleUserTimeout)
	defer cancel()

	icattx, err := a.deps.ICAT.BeginTxx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "Error creating ICAT transaction")
	}
	defer icattx.Rollback() // nolint:errcheck

	// Randomly add a number between -2 and 2
	// i.e. in [0,5) minus 2
	// This makes the bounds different each run, which will help us catch 0
	// values that might otherwise repeatedly fall between batch bounds
	boundModifier := rand.Intn(5) - 2

	batches, err := db.NewICAT(icattx, a.deps.Config).GetUserBatchBounds(ctx, a.deps.Config.BatchSize+boundModifier)
	if err != nil {
		return errors.Wrap(err, "Failed getting user batch bounds")
	}
	log.Tracef("batches: %+v", batches)

	var overallError error
	for _, batch := range batches {
		start := a.deps.Config.TrimUserSuffix(batch[0])
		end := a.deps.Config.TrimUserSuffix(batch[1])
		err = a.client.PublishContext(ctx, fmt.Sprintf("%s.%s.%s", BatchUserPrefix, start, end), []byte{})
		if err != nil {
			log.Error(errors.Wrapf(err, "Error publishing message for batch %s - %s", start, end))
			overallError = err
			// continue anyway though in case it's specific to this one batch
		}
	}

	return overallError
}
