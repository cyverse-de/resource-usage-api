package amqp

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cyverse-de/messaging/v9"
	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/config"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "amqp"})

// retryDelay is how long a redelivered message is held before it goes back on the queue again.
const retryDelay = 30 * time.Second

// Configuration describes the broker connection and the queues this service consumes from.
type Configuration struct {
	URI          string
	Reconnect    bool
	Exchange     string
	ExchangeType string

	// Queue receives analysis status updates.
	Queue         string
	PrefetchCount int

	// BatchQueue and IndividualQueue receive data usage refresh requests. They are consumed one
	// message at a time because a single batch can occupy the ICAT database for minutes.
	BatchQueue             string
	IndividualQueue        string
	DataUsagePrefetchCount int
}

type analysisUpdateJob struct {
	UUID     string `json:"uuid"`
	CondorID string `json:"condor_id"` // not actually used for anything...yet.
}

type analysisUpdateMsg struct {
	Job     analysisUpdateJob  `json:"Job"`
	State   messaging.JobState `json:"State"`
	Message string             `json:"Message"`
	SentOn  string             `json:"SentOn"`
	Sender  string             `json:"Sender"`
}

// HandlerFn processes an analysis status update. An error means the update was not recorded, and the
// message is redelivered rather than dropped.
type HandlerFn func(context context.Context, externalID string, state messaging.JobState) error

// Dependencies are the collaborators the data usage handlers need.
type Dependencies struct {
	DEDB          *sqlx.DB
	ICAT          *sqlx.DB
	Config        *config.Config
	Subscriptions *clients.Subscriptions
}

// AMQP consumes analysis status updates and data usage refresh requests, and publishes the refresh
// requests that the HTTP handlers enqueue.
type AMQP struct {
	client  *messaging.Client
	handler HandlerFn
	deps    *Dependencies
}

// New connects to the broker and registers this service's consumers.
//
// Listen has to be running before a consumer is added: AddConsumer blocks until the listen loop picks
// the registration up.
func New(cfg *Configuration, handler HandlerFn, deps *Dependencies) (*AMQP, error) {
	log.Debug("creating a new AMQP client")
	client, err := messaging.NewClient(cfg.URI, cfg.Reconnect)
	if err != nil {
		return nil, err
	}
	log.Debug("done creating a new AMQP client")

	a := &AMQP{
		client:  client,
		handler: handler,
		deps:    deps,
	}

	if err = a.client.SetupPublishing(cfg.Exchange); err != nil {
		return nil, err
	}

	go a.client.Listen()

	log.Debug("adding the analysis updates consumer")
	client.AddConsumer(
		cfg.Exchange,
		cfg.ExchangeType,
		cfg.Queue,
		messaging.UpdatesKey,
		a.recv,
		cfg.PrefetchCount,
	)

	// The queue names are inherited from the data-usage-api service so that its durable queues and
	// their bindings carry over and no enqueued refresh is stranded.
	log.Debug("adding the data usage batch consumer")
	client.AddConsumerMulti(
		cfg.Exchange,
		cfg.ExchangeType,
		cfg.BatchQueue,
		[]string{"index.all", "index.usage.data", BatchUserPrefix + ".#"},
		a.recvDataUsage,
		cfg.DataUsagePrefetchCount,
	)

	log.Debug("adding the data usage individual consumer")
	client.AddConsumerMulti(
		cfg.Exchange,
		cfg.ExchangeType,
		cfg.IndividualQueue,
		[]string{SingleUserPrefix + ".#"},
		a.recvDataUsage,
		cfg.DataUsagePrefetchCount,
	)
	log.Debug("done adding consumers")

	return a, err
}

// PublishContext publishes a message on the service's exchange.
func (a *AMQP) PublishContext(ctx context.Context, key string, body []byte) error {
	return a.client.PublishContext(ctx, key, body)
}

func (a *AMQP) recv(context context.Context, delivery amqp.Delivery) {
	var (
		update analysisUpdateMsg
		err    error
	)

	var log = log.WithContext(context)

	redelivered := delivery.Redelivered
	if err = json.Unmarshal(delivery.Body, &update); err != nil {
		log.Error(err)
		if err = delivery.Reject(!redelivered); err != nil {
			log.Error(err)
		}
		return
	}

	log.Debugf("UUID is %s", update.Job.UUID)
	log.Debugf("state is %s", update.State)

	if update.State == "" {
		log.Error("state was unset, dropping message")
		reject(delivery)
		return
	}
	if update.Job.UUID == "" {
		log.Error("external ID was unset, dropping message")
		reject(delivery)
		return
	}

	// Acknowledged only after the handler has run, so that a crash mid-calculation leaves the message
	// to be redelivered rather than silently dropping the analysis's usage. A handler that reports a
	// failure gets the same treatment: the hours it could not record are only recoverable while the
	// message is still outstanding.
	if err = a.handler(context, update.Job.UUID, update.State); err != nil {
		log.Error(errors.Wrapf(err, "Error handling the update for analysis %s", update.Job.UUID))
		retry(context, delivery)
		return
	}

	if err = delivery.Ack(false); err != nil {
		log.Error(err)
	}
}

// reject discards a message that this service can never process.
func reject(delivery amqp.Delivery) {
	if err := delivery.Reject(false); err != nil {
		log.Error(err)
	}
}

// retry returns a message to the broker after a failure that another attempt may get past. A message
// that has already come back once is held first, so that a dependency which stays down is retried at
// a distance rather than spun against.
func retry(ctx context.Context, delivery amqp.Delivery) {
	if delivery.Redelivered {
		select {
		case <-ctx.Done():
		case <-time.After(retryDelay):
		}
	}

	if err := delivery.Reject(true); err != nil {
		log.Error(err)
	}
}

func (a *AMQP) Close() {
	a.client.Close()
}
