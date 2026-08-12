package amqp

import (
	"context"
	"encoding/json"
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
// it has been handled; anything that fails is returned to the broker so that the consumer does not sit
// on an outstanding delivery and stall behind its prefetch limit.
func (a *AMQP) recvDataUsage(ctx context.Context, delivery amqp.Delivery) {
	var err error

	log := log.WithContext(ctx)
	log.Tracef("Got message: %s", delivery.RoutingKey)

	switch key := delivery.RoutingKey; {
	case key == "index.all" || key == "index.usage.data":
		err = a.sendBatchMessages(ctx)
	case strings.HasPrefix(key, BatchUserPrefix):
		err = a.updateUserBatch(ctx, delivery)
	case strings.HasPrefix(key, SingleUserPrefix):
		err = a.updateUser(ctx, key)
	default:
		log.Errorf("no handler for routing key %s, dropping message", key)
		reject(delivery)
		return
	}

	if err != nil {
		log.Error(errors.Wrap(err, "Error handling message"))

		// A message this service cannot read is dropped, since redelivering it changes nothing.
		// Everything else failed on something transient — ICAT or subscriptions unreachable, a query
		// outrunning its deadline — and goes back on the queue: dropping it would leave that user's
		// usage stale until the next sweep, with nothing to say it had been skipped.
		var unreadable *unprocessableError
		if errors.As(err, &unreadable) {
			reject(delivery)
			return
		}
		retry(ctx, delivery)
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

// batchBounds is the body of a batch refresh message. The bounds travel in the body because a routing
// key cannot carry them unambiguously: usernames contain dots, which are also the separator, so the
// key for alice.jones..bob.smith reads back as alice..jones.bob.smith.
type batchBounds struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// parseSingleUsername reads the username a single-user refresh names. The prefix is trimmed rather
// than indexed past because the consumer's binding matches the bare prefix too, and slicing a key
// that short would take the process down from an unrecovered goroutine.
func parseSingleUsername(routingKey string) (string, error) {
	username := strings.TrimPrefix(routingKey, SingleUserPrefix+".")
	if username == "" || username == routingKey {
		return "", unprocessable(errors.Errorf("no username in routing key %s", routingKey))
	}
	return username, nil
}

// parseBatchMessage reads the username pair a batch refresh names. Messages enqueued before the
// bounds moved into the body carry an empty one, and are read from the routing key as before.
func parseBatchMessage(delivery amqp.Delivery) (start, end string, err error) {
	if len(delivery.Body) == 0 {
		return parseBatchBounds(delivery.RoutingKey)
	}

	var bounds batchBounds
	if err := json.Unmarshal(delivery.Body, &bounds); err != nil {
		return "", "", unprocessable(errors.Wrapf(err, "unable to parse the bounds in the body of %s", delivery.RoutingKey))
	}
	if bounds.Start == "" || bounds.End == "" {
		return "", "", unprocessable(errors.Errorf("the body of %s does not name a start and end username", delivery.RoutingKey))
	}
	return bounds.Start, bounds.End, nil
}

// parseBatchBounds reads the "<start>.<end>" username pair from a batch refresh's routing key. It
// splits at the first dot, so a start username containing one lands partly in the end bound; the
// bounds in the message body are authoritative for anything this service publishes.
func parseBatchBounds(routingKey string) (start, end string, err error) {
	remainder := strings.TrimPrefix(routingKey, BatchUserPrefix+".")
	if remainder == routingKey {
		return "", "", unprocessable(errors.Errorf("routing key %s does not name a start and end username", routingKey))
	}

	bounds := strings.SplitN(remainder, ".", 2)
	if len(bounds) != 2 || bounds[0] == "" || bounds[1] == "" {
		return "", "", unprocessable(errors.Errorf("routing key %s does not name a start and end username", routingKey))
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
		err = errors.Wrap(err, "Failed updating usage information")

		// A user the DE does not know is not going to appear because the message came back.
		var unknownUser *db.UserNotFoundError
		if errors.As(err, &unknownUser) {
			return unprocessable(err)
		}
		return err
	}

	return nil
}

// updateUserBatch recalculates data usage for the range of users the message names.
func (a *AMQP) updateUserBatch(ctx context.Context, delivery amqp.Delivery) error {
	start, end, err := parseBatchMessage(delivery)
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

	// The configured size is validated, but the modifier can still take it to zero or below, where the
	// ICAT query divides by it and the bounds loop counts away from its limit and never ends.
	batchSize := max(a.deps.Config.BatchSize+boundModifier, 1)

	batches, err := db.NewICAT(icattx, a.deps.Config).GetUserBatchBounds(ctx, batchSize)
	if err != nil {
		return errors.Wrap(err, "Failed getting user batch bounds")
	}
	log.Tracef("batches: %+v", batches)

	var overallError error
	for _, batch := range batches {
		start := a.deps.Config.TrimUserSuffix(batch[0])
		end := a.deps.Config.TrimUserSuffix(batch[1])

		body, err := json.Marshal(&batchBounds{Start: start, End: end})
		if err != nil {
			return errors.Wrapf(err, "Error building the message for batch %s - %s", start, end)
		}

		// The bounds are repeated in the routing key so that they show up wherever messages are being
		// watched; the body is what the consumer reads.
		err = a.client.PublishContext(ctx, fmt.Sprintf("%s.%s.%s", BatchUserPrefix, start, end), body)
		if err != nil {
			log.Error(errors.Wrapf(err, "Error publishing message for batch %s - %s", start, end))
			overallError = err
			// continue anyway though in case it's specific to this one batch
		}
	}

	return overallError
}
