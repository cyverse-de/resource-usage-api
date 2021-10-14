package worker

import (
	"context"
	"database/sql"
	"time"

	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

type totalUpdater func(int64, int64) int64

func (w *Worker) updateCPUHoursTotal(context context.Context, log *logrus.Entry, workItem *db.CPUUsageWorkItem, updateFn totalUpdater) error {
	tx, err := w.db.Beginx()
	if err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	log.Debugf("began transaction for updating CPU hours total from work item %s", workItem.ID)

	txdb := db.New(tx)

	// Get the user name from the created by UUID.
	username, err := txdb.Username(context, workItem.CreatedBy)
	if err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	log.Debugf("got username %s for user ID %s", username, workItem.CreatedBy)

	// Get the current value
	cpuhours, err := txdb.CurrentCPUHoursForUser(context, username)
	if err == sql.ErrNoRows {
		log.Infof("adding new total record for user %s", username)

		start := time.Now()
		cpuhours = &db.CPUHours{
			Total:          0,
			UserID:         workItem.CreatedBy,
			EffectiveStart: start,
			EffectiveEnd:   start.AddDate(0, 0, int(w.NewUserTotalInterval)),
		}

		log.Debugf("inserting 0 total hours for user %s since they didn't have a total", username)

		if ierr := txdb.InsertCurrentCPUHoursForUser(context, cpuhours); ierr != nil {
			log.Error(ierr)
			err = multierr.Append(err, ierr)

			log.Info("rolling back transaction")
			if rerr := tx.Rollback(); rerr != nil {
				err = multierr.Append(err, rerr)
			}

			return err
		}

		log.Debugf("done inserting 0 total hours for user %s", username)
	} else if err != nil {
		log.Error(err)
		log.Info("rolling back transaction")
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	// modify it with the value stored in the work item.
	cpuhours.Total = updateFn(cpuhours.Total, workItem.Value)
	log.Infof("new total for user %s is %d based on a work item value of %d", username, cpuhours.Total, workItem.Value)

	// set the new current value.
	if err = txdb.UpdateCPUHoursTotal(context, cpuhours); err != nil {
		log.Error(err)
		log.Info("rolling back transaction")
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		log.Error(err)
		log.Info("rolling back transaction")
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	log.Infof("committing transaction for updating the total to %d for user %s", cpuhours.Total, username)

	return nil
}

func (w *Worker) AddCPUHours(context context.Context, workItem *db.CPUUsageWorkItem) error {
	log := logging.Log.WithFields(logrus.Fields{"context": "adding CPU hours"})
	return w.updateCPUHoursTotal(context, log, workItem, func(current int64, add int64) int64 {
		return current + add
	})
}

func (w *Worker) SubtractCPUHours(context context.Context, workItem *db.CPUUsageWorkItem) error {
	log := logging.Log.WithFields(logrus.Fields{"context": "subtracting CPU hours"})
	return w.updateCPUHoursTotal(context, log, workItem, func(current int64, subtract int64) int64 {
		return current - subtract
	})
}

func (w *Worker) ResetCPUHours(context context.Context, workItem *db.CPUUsageWorkItem) error {
	log := logging.Log.WithFields(logrus.Fields{"context": "resetting CPU hours"})
	return w.updateCPUHoursTotal(context, log, workItem, func(_ int64, newValue int64) int64 {
		return newValue
	})
}
