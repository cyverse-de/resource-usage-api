package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/cyverse-de/resource-usage-api/logging"
)

var log = logging.Log

// AddCPUUsageEvent adds a new usage event to the database with the default values for
// the work queue fields.
func (d *Database) AddCPUUsageEvent(context context.Context, event *CPUUsageEvent) error {
	_, err := d.db.ExecContext(
		context,
		insertCPUHourEventStmt,
		event.RecordDate,
		event.EffectiveDate,
		event.EventType,
		event.Value,
		event.CreatedBy,
	)
	return err
}

// UnclaimedUnprocessedEvents returns a listing of the CPUUsageWorkItem for records that are not
// claimed, processed, being processed, expired, and have not reached the maximum number of attempts.
func (d *Database) UnclaimedUnprocessedEvents(context context.Context) ([]CPUUsageWorkItem, error) {
	var workItems []CPUUsageWorkItem

	rows, err := d.db.QueryxContext(context, unprocessedEventsQuery)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var h CPUUsageWorkItem
		err = rows.StructScan(&h)
		if err != nil {
			return nil, err
		}
		workItems = append(workItems, h)
	}

	if err = rows.Err(); err != nil {
		return workItems, err
	}

	return workItems, nil
}

// ClaimEvent marks an CPU usage event in the database as claimed for work by the entity
// represented by the claimedBy string.
func (d *Database) ClaimEvent(context context.Context, id, claimedBy string) error {
	_, err := d.db.ExecContext(
		context,
		claimedByStmt,
		id,
		claimedBy,
	)
	return err
}

// ProcessingEvent marks as CPU usage event as being processed. It's not complete yet, but
// it's in progress.
func (d *Database) ProcessingEvent(context context.Context, id string) error {
	_, err := d.db.ExecContext(
		context,
		processingStmt,
		id,
	)
	return err
}

// FinishedProcessingEvent marks an event as processed.
func (d *Database) FinishedProcessingEvent(context context.Context, id string) error {
	_, err := d.db.ExecContext(
		context,
		finishedProcessingStmt,
		id,
	)
	return err
}

// RegisterWorker adds a new worker to the database. Returns the worker's assigned ID.
func (d *Database) RegisterWorker(context context.Context, workerName string, expiration time.Time) (string, error) {
	var (
		newID string
		err   error
	)

	if err = d.db.GetContext(
		context,
		&newID,
		registerWorkerStmt,
		workerName,
		expiration,
	); err != nil {
		return "", err
	}

	return newID, nil
}

// UnregisterWorker removes a worker from the database.
func (d *Database) UnregisterWorker(context context.Context, workerID string) error {
	_, err := d.db.ExecContext(
		context,
		unregisterWorkerStmt,
		workerID,
	)
	return err
}

// RefreshWorkerRegistration updates the workers activation expiration date.
func (d *Database) RefreshWorkerRegistration(context context.Context, workerID string) (*time.Time, error) {
	newTime := time.Now().Add(48 * time.Hour)
	_, err := d.db.ExecContext(
		context,
		refreshWorkerStmt,
		workerID,
		newTime,
	)
	return &newTime, err
}

// PurgeExpiredWorkers clears out all workers whose registration has expired. Returns
// the number of rows affected.
func (d *Database) purgeExpiredWorkers(context context.Context, tx *sql.Tx) (int64, error) {
	result, err := tx.ExecContext(
		context,
		purgeExpiredWorkersStmt,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// PurgeExpiredWorkSeekers clears out all workers that have been looking for work from
// the queue too long. Returns the number of rows affected.
func (d *Database) purgeExpiredWorkSeekers(context context.Context, tx *sql.Tx) (int64, error) {
	result, err := tx.ExecContext(
		context,
		purgeExpiredWorkSeekersStmt,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// PurgeExpiredWorkClaims will mark an event as unclaimed if it's not processed, not
// being processed, and the current time is equal to or past the claim expiration date.
func (d *Database) purgeExpiredWorkClaims(context context.Context, tx *sql.Tx) (int64, error) {
	result, err := tx.ExecContext(
		context,
		purgeExpiredWorkClaimsStmt,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// resetWorkClaimsForInactiveWorkers will mark an event as unclaimed if the worker that
// claimed it is inactive.
func (d *Database) resetWorkClaimsForInactiveWorkers(context context.Context, tx *sql.Tx) (int64, error) {
	result, err := tx.ExecContext(
		context,
		resetWorkClaimForInactiveWorkersStmt,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// EnforceExpirations will clean up the database of expired workers, work claims,
// and work seekers.
func (d *Database) EnforceExpirations(context context.Context) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}

	expiredWS, err := d.purgeExpiredWorkSeekers(context, tx)
	if err != nil {
		return err
	}
	log.Infof("%d expired work seekers were cleaned up", expiredWS)

	expiredW, err := d.purgeExpiredWorkers(context, tx)
	if err != nil {
		return err
	}
	log.Infof("%d expired workers were cleaned up", expiredW)

	inactiveClaims, err := d.resetWorkClaimsForInactiveWorkers(context, tx)
	if err != nil {
		return err
	}
	log.Infof("%d claims assigned to inactive workers were cleaned up", inactiveClaims)

	expiredWC, err := d.purgeExpiredWorkClaims(context, tx)
	if err != nil {
		return err
	}
	log.Infof("%d expired work claims were cleaned up", expiredWC)

	return tx.Commit()
}

// GettingWork records that the worker is looking up work.
func (d *Database) GettingWork(context context.Context, workerID string, expiration time.Time) error {
	_, err := d.db.ExecContext(
		context,
		gettingWorkStmt,
		workerID,
		expiration,
	)
	return err
}

// DoneGettingWork records that the worker is not looking up work.
func (d *Database) DoneGettingWork(context context.Context, workerID string) error {
	_, err := d.db.ExecContext(
		context,
		notGettingWorkStmt,
		workerID,
	)
	return err
}

// SetWorking records whether the worker is working on something.
func (d *Database) SetWorking(context context.Context, workerID string, working bool) error {
	_, err := d.db.ExecContext(
		context,
		setWorkingStmt,
		workerID,
		working,
	)
	return err
}
