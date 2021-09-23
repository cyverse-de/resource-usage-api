package amqp

import (
	"encoding/json"

	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/streadway/amqp"
	"gopkg.in/cyverse-de/messaging.v6"
)

var log = logging.Log

type Configuration struct {
	URI           string
	Reconnect     bool
	Exchange      string
	ExchangeType  string
	Queue         string
	PrefetchCount int
}

type HandlerFn func(userID, externalID, state string)

type AMQP struct {
	client  *messaging.Client
	handler HandlerFn
}

func New(config *Configuration, handler HandlerFn) (*AMQP, error) {
	client, err := messaging.NewClient(config.URI, config.Reconnect)
	if err != nil {
		return nil, err
	}

	a := &AMQP{
		client:  client,
		handler: handler,
	}

	client.AddConsumer(config.Exchange, config.ExchangeType, config.Queue, messaging.UpdatesKey, a.recv, config.PrefetchCount)

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

	if update.State == "" {
		log.Error("state was unset, dropping message")
	}
	if update.Job.InvocationID == "" {
		log.Error("invocation/external ID was unset, dropping message")
	}

	a.handler(update.Job.UserID, update.Job.InvocationID, string(update.State))
}

func (a *AMQP) Listen() {
	a.client.Listen()
}

func (a *AMQP) Close() {
	a.client.Close()
}
