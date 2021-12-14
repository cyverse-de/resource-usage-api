package amqp

import (
	"encoding/json"

	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"gopkg.in/cyverse-de/messaging.v6"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "amqp"})

type Configuration struct {
	URI           string
	Reconnect     bool
	Exchange      string
	ExchangeType  string
	Queue         string
	PrefetchCount int
}

type HandlerFn func(externalID string, state messaging.JobState)

type AMQP struct {
	client  *messaging.Client
	handler HandlerFn
}

func New(config *Configuration, handler HandlerFn) (*AMQP, error) {
	log.Debug("creating a new AMQP client")
	client, err := messaging.NewClient(config.URI, config.Reconnect)
	if err != nil {
		return nil, err
	}
	log.Debug("done creating a new AMQP client")

	a := &AMQP{
		client:  client,
		handler: handler,
	}

	go a.client.Listen()

	log.Debug("adding a consumer")
	client.AddConsumer(
		config.Exchange,
		config.ExchangeType,
		config.Queue,
		messaging.UpdatesKey,
		a.recv,
		config.PrefetchCount,
	)
	log.Debug("done adding a consumer")

	return a, err
}

func (a *AMQP) recv(delivery amqp.Delivery) {
	var (
		update messaging.UpdateMessage
		err    error
	)

	redelivered := delivery.Redelivered
	if err = json.Unmarshal(delivery.Body, &update); err != nil {
		log.Error(err)
		if err = delivery.Reject(!redelivered); err != nil {
			log.Error(err)
		}
		return
	}

	log.Infof("%s is the body", string(delivery.Body))

	if update.State == "" {
		log.Error("state was unset, dropping message")
		return
	}
	if update.Job.InvocationID == "" {
		log.Error("invocation/external ID was unset, dropping message")
		return
	}

	a.handler(update.Job.InvocationID, update.State)
}

func (a *AMQP) Listen() {
	a.client.Listen()
}

func (a *AMQP) Close() {
	a.client.Close()
}
