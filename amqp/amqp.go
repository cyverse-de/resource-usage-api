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

	if err = a.client.SetupPublishing(config.Exchange); err != nil {
		return nil, err
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
		update analysisUpdateMsg
		err    error
	)

	if err = delivery.Ack(false); err != nil {
		log.Error(err)
		return
	}

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
	log.Debugf("%+v", update)

	log.Infof("%s is the body", string(delivery.Body))

	if update.State == "" {
		log.Error("state was unset, dropping message")
		return
	}
	if update.Job.UUID == "" {
		log.Error("external ID was unset, dropping message")
		return
	}

	a.handler(update.Job.UUID, update.State)
}

func (a *AMQP) Send(routingKey string, data []byte) error {
	log = log.WithFields(logrus.Fields{"context": "sending usage to QMS"})
	log.Debugf("routing key: %s, message: %s", routingKey, string(data))
	return a.client.Publish(routingKey, data)
}

func (a *AMQP) Listen() {
	a.client.Listen()
}

func (a *AMQP) Close() {
	a.client.Close()
}
