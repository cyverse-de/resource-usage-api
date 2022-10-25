package main

import (
	"strings"

	"github.com/nats-io/nats.go"
)

func QueueName(queueBase, suffix string) string {
	return strings.Join([]string{queueBase, suffix}, ".")
}

func QueueSubscribe(subject, queue string, natsConn *nats.EncodedConn, handler nats.Handler) error {
	var err error

	_, err = natsConn.QueueSubscribe(subject, queue, handler)
	if err != nil {
		return err
	}

	return nil
}
