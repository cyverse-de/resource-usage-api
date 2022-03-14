package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/worker"
	"github.com/sirupsen/logrus"
)

const CPUHoursAttr = "cpu.hours"
const CPUHoursUnit = "cpu hours"

func (a *App) SendTotal(username string) error {
	var err error

	dedb := db.New(a.database)

	userID, err := dedb.UserID(context.Background(), username)
	if err != nil {
		return err
	}

	log = log.WithFields(logrus.Fields{"context": "send message callback", "user": username})

	log.Debug("getting current CPU hours")
	currentCPUHours, err := dedb.CurrentCPUHoursForUser(context.Background(), username)
	if err != nil {
		return err
	}
	log.Debugf("current CPU hours: %s", currentCPUHours.Total.String())

	update := &worker.UsageUpdate{
		Attribute: CPUHoursAttr,
		Value:     currentCPUHours.Total.String(),
		Unit:      CPUHoursUnit,
		Username:  strings.TrimSuffix(username, fmt.Sprintf("@%s", a.userSuffix)),
		UserID:    userID,
	}

	log.Debug("marshalling update")
	marshalled, err := json.Marshal(update)
	if err != nil {
		return err
	}
	log.Debug("done marshalling update")

	log.Debug("sending update")
	if err = a.amqpClient.Send(a.amqpUsageRoutingKey, marshalled); err != nil {
		return err
	}
	log.Debug("done sending update")

	return nil
}

func (a *App) SendTotalCallback() worker.MessageSender {
	return func(workItem *db.CPUUsageWorkItem) {
		if err := a.SendTotal(workItem.CreatedBy); err != nil {
			log.Error(err)
		}
	}
}
