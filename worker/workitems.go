package worker

import (
	"context"
	"database/sql"
	"time"

	"github.com/cyverse-de/resource-usage-api/db"
	"go.uber.org/multierr"
)

type totalUpdater func(int64, int64) int64

func (w *Worker) updateCPUHoursTotal(context context.Context, workItem *db.CPUUsageWorkItem, updateFn totalUpdater) error {
	tx, err := w.db.Beginx()
	if err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	txdb := db.New(tx)

	// Get the user name from the created by UUID.
	userID, err := txdb.Username(context, workItem.CreatedBy)
	if err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	// Get the current value
	cpuhours, err := txdb.CurrentCPUHoursForUser(context, userID)
	if err == sql.ErrNoRows {
		start := time.Now()
		cpuhours = &db.CPUHours{
			Total:          0,
			UserID:         userID,
			EffectiveStart: start,
			EffectiveEnd:   start.AddDate(0, 0, int(w.NewUserTotalInterval)),
		}
		if ierr := txdb.InsertCurrentCPUHoursForUser(context, cpuhours); ierr != nil {
			log.Error(ierr)
			err = multierr.Append(err, ierr)

			if rerr := tx.Rollback(); rerr != nil {
				err = multierr.Append(err, rerr)
			}

			return err
		}
	} else if err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	// modify it with the value stored in the work item.
	cpuhours.Total = updateFn(cpuhours.Total, workItem.Value)

	// set the new current value.
	if err = txdb.UpdateCPUHoursTotal(context, cpuhours); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	return nil
}

func (w *Worker) AddCPUHours(context context.Context, workItem *db.CPUUsageWorkItem) error {
	return w.updateCPUHoursTotal(context, workItem, func(current int64, add int64) int64 {
		return current + add
	})
}

func (w *Worker) SubtractCPUHours(context context.Context, workItem *db.CPUUsageWorkItem) error {
	return w.updateCPUHoursTotal(context, workItem, func(current int64, subtract int64) int64 {
		return current - subtract
	})
}

func (w *Worker) ResetCPUHours(context context.Context, workItem *db.CPUUsageWorkItem) error {
	return w.updateCPUHoursTotal(context, workItem, func(_ int64, newValue int64) int64 {
		return newValue
	})
}
