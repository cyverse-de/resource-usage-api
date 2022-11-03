package internal

import (
	"context"
	"encoding/json"

	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const CPUHoursAttr = "cpu.hours"
const CPUHoursUnit = "cpu hours"

const QMSSubjectAddUsage = "cyverse.qms.user.usages.add"

const QMSUpdateOperationAdd = "ADD"

// SendUpdate sends a CPU usage update to QMS.
func (a *App) SendUpdate(ctx context.Context, usageEvent *db.CPUUsageWorkItem) error {
	var err error

	// Initialize.
	dedb := db.New(a.database)
	userID := usageEvent.CreatedBy
	log = log.WithFields(logrus.Fields{"context": "send CPU usage update", "user-id": userID})

	// Look up the username.
	username, err := dedb.Username(ctx, userID)
	if err != nil {
		return errors.Wrapf(err, "unable to look up the username for user ID %s", userID)
	}

	// Update the logger to include the username.
	log = log.WithField("username", username)
	log.Debugf("username for user ID %s is %s", userID, username)

	// Build the update message body.
	v, err := usageEvent.Value.Float64()
	if err != nil {
		return errors.Wrap(err, "unable to convert the usage value to float64")
	}

	// Determine the update amount or return if QMS doesn't support the update type.
	var updateAmount float64
	switch usageEvent.EventType {
	case db.CPUHoursAdd:
		updateAmount = v
	case db.CPUHoursSubtract:
		updateAmount = -v
	default:
		log.Infof("ignoring update of event type %s", usageEvent.EventType)
		return nil
	}

	// Format and log the update.
	update := pbinit.NewAddUsage(username, CPUHoursAttr, QMSUpdateOperationAdd, updateAmount)
	jsonUpdate, err := json.Marshal(update)
	if err != nil {
		log.Errorf("unable to JSON encode the usage update for %s: %s", username, err)
		log.Debug("sending update")
	} else {
		log.Debugf("sending update: %s", jsonUpdate)
	}

	// Send the update.
	if err = gotelnats.Publish(ctx, a.natsClient, QMSSubjectAddUsage, update); err != nil {
		return err
	}
	log.Debug("done sending update")

	return nil
}
