package internal

import (
	"context"

	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/worker"
	"github.com/sirupsen/logrus"
)

const CPUHoursAttr = "cpu.hours"
const CPUHoursUnit = "cpu hours"

func (a *App) SendTotal(context context.Context, username string) error {
	var err error

	dedb := db.New(a.database)

	log = log.WithFields(logrus.Fields{"context": "send message callback", "user": username})

	log.Debug("getting current CPU hours")
	currentCPUHours, err := dedb.CurrentCPUHoursForUser(context, username)
	if err != nil {
		return err
	}
	log.Debugf("current CPU hours: %s", currentCPUHours.Total.String())

	v, err := currentCPUHours.Total.Float64()
	if err != nil {
		return err
	}
	update := pbinit.NewAddUsage(username, "cpu.hours", "ADD", v)

	log.Debug("sending update")
	if err = gotelnats.Publish(context, a.natsClient, "cyverse.qms.user.usages.add", update); err != nil {
		return err
	}
	log.Debug("done sending update")

	return nil
}

func (a *App) SendTotalCallback() worker.MessageSender {
	return func(context context.Context, workItem *db.CPUUsageWorkItem) {
		if err := a.SendTotal(context, workItem.CreatedBy); err != nil {
			log.WithContext(context).Error(err)
		}
	}
}
