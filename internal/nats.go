package internal

import (
	"context"
	"encoding/json"

	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/worker"
	"github.com/sirupsen/logrus"
)

const CPUHoursAttr = "cpu.hours"
const CPUHoursUnit = "cpu hours"

func (a *App) SendTotal(ctx context.Context, userID string) error {
	var err error

	dedb := db.New(a.database)

	log = log.WithFields(logrus.Fields{"context": "send message callback", "user-id": userID})

	// Get the user name from the created by UUID.
	username, err := dedb.Username(ctx, userID)
	if err != nil {
		return err
	}

	log = log.WithFields(logrus.Fields{"username": username})
	log.Debug("found username")

	log.Debug("getting current CPU hours")
	currentCPUHours, err := dedb.CurrentCPUHoursForUser(ctx, username)
	if err != nil {
		return err
	}
	log.Debugf("current CPU hours: %s", currentCPUHours.Total.String())

	v, err := currentCPUHours.Total.Float64()
	if err != nil {
		return err
	}
	update := pbinit.NewAddUsage(username, "cpu.hours", "ADD", v)

	jsonUpdate, err := json.Marshal(update)
	if err != nil {
		log.Errorf("unable to JSON encode the usage update for %s: %s", username, err.Error())
		log.Debug("sending update")
	} else {
		log.Debugf("sending update: %s", jsonUpdate)
	}
	log.Debug("sending update")

	if err = gotelnats.Publish(ctx, a.natsClient, "cyverse.qms.user.usages.add", update); err != nil {
		return err
	}
	log.Debug("done sending update")

	return nil
}

func (a *App) SendTotalCallback() worker.MessageSender {
	return func(context context.Context, workItem *db.CPUUsageWorkItem) {
		log = log.WithFields(logrus.Fields{"context": "callback for send total"})

		log.Debugf("work item %+v", workItem)

		if err := a.SendTotal(context, workItem.CreatedBy); err != nil {
			log.WithContext(context).Error(err)
		}
	}
}
