package db

import (
	"context"
	"time"

	"github.com/guregu/null"
)

type Worker struct {
	ID                   string    `db:"id" json:"id"`
	Name                 string    `db:"name" json:"name"`
	AddedOn              string    `db:"added_on" json:"added_on"`
	Active               bool      `db:"active" json:"active"`
	ActivationExpiresOn  null.Time `db:"activation_expires_on" json:"activation_expires_on"`
	DeactivatedOn        null.Time `db:"deactivated_on" json:"deactivated_on"`
	ActivatedOn          null.Time `db:"activated_on" json:"activated_on"`
	GettingWork          bool      `db:"getting_work" json:"getting_work"`
	GettingWorkOn        null.Time `db:"getting_work_on" json:"getting_work_on"`
	GettingWorkExpiresOn null.Time `db:"getting_work_expires_on" json:"getting_work_expires_on"`
	Working              bool      `db:"working" json:"working"`
	WorkingOn            null.Time `db:"working_on" json:"working_on"`
	LastModified         time.Time `db:"last_modified" json:"last_modified"`
}

func (d *Database) ListWorkers(context context.Context) ([]Worker, error) {
	var workers []Worker
	const q = `
		SELECT 
			id,
			name,
			added_on,
			active,
			activation_expires_on,
			deactivated_on,
			activated_on,
			getting_work,
			getting_work_on,
			getting_work_expires_on,
			working,
			working_on,
			last_modified
		FROM cpu_usage_workers;
	`

	rows, err := d.db.QueryxContext(context, q)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var worker Worker
		if err = rows.StructScan(&worker); err != nil {
			return nil, err
		}
		workers = append(workers, worker)
	}

	if err = rows.Err(); err != nil {
		return workers, err
	}

	return workers, nil
}

func (d *Database) Worker(context context.Context, id string) (*Worker, error) {
	var worker Worker
	const q = `
			SELECT 
			id,
			name,
			added_on,
			active,
			activation_expires_on,
			deactivated_on,
			activated_on,
			getting_work,
			getting_work_on,
			getting_work_expires_on,
			working,
			working_on,
			last_modified
		FROM cpu_usage_workers
		WHERE id = $1;`
	err := d.db.QueryRowxContext(context, q, id).StructScan(&worker)
	return &worker, err
}

func (d *Database) UpdateWorker(context context.Context, worker *Worker) error {
	const q = `
		UPDATE cpu_usage_workers
		SET name = $2,
			added_on = $3,
			active = $4,
			activation_expires_on = $5,
			deactivated_on = $6,
			activated_on = $7,
			getting_work = $8,
			getting_work_on = $9,
			getting_work_expires_on = $10,
			working = $11,
			working_on =$12
		WHERE id = $1;
	`
	_, err := d.db.ExecContext(
		context,
		q,
		worker.ID,
		worker.Name,
		worker.AddedOn,
		worker.Active,
		worker.ActivationExpiresOn,
		worker.DeactivatedOn,
		worker.ActivatedOn,
		worker.GettingWork,
		worker.GettingWorkOn,
		worker.GettingWorkExpiresOn,
		worker.Working,
		worker.WorkingOn,
	)
	return err
}

func (d *Database) DeleteWorker(context context.Context, id string) error {
	const q = `
		DELETE FROM cpu_usage_workers WHERE id = $1;
	`
	_, err := d.db.ExecContext(context, q, id)
	return err
}

// RegisterWorker adds a new worker to the database. Returns the worker's assigned ID.
func (d *Database) RegisterWorker(context context.Context, workerName string, expiration time.Time) (string, error) {
	var (
		newID string
		err   error
	)

	const q = `
		INSERT INTO cpu_usage_workers
			(name, activation_expires_on)
		VALUES
			($1, $2)
		RETURNING id;
	`
	err = d.db.QueryRowxContext(context, q, workerName, expiration).Scan(&newID)
	return newID, err
}

// UnregisterWorker removes a worker from the database.
func (d *Database) UnregisterWorker(context context.Context, workerID string) error {
	const q = `
		UPDATE cpu_usage_workers
		SET active = false,
			getting_work = false
		WHERE id = $1;
	`
	_, err := d.db.ExecContext(context, q, workerID)
	return err
}

// RefreshWorkerRegistration updates the workers activation expiration date.
func (d *Database) RefreshWorkerRegistration(context context.Context, workerID string, expirationInterval time.Duration) (*time.Time, error) {
	const q = `
		UPDATE cpu_usage_workers
		SET activation_expires_on = $2
		WHERE id = $1;
	`
	newTime := time.Now().Add(expirationInterval)
	_, err := d.db.ExecContext(context, q, workerID, newTime)
	return &newTime, err
}

// PurgeExpiredWorkers clears out all workers whose registration has expired. Returns
// the number of rows affected. Only purge workers (set their activation flag to false)
// if they're not getting work, they're not actively working on something, and the
// activation timestamp has passed.
func (d *Database) PurgeExpiredWorkers(context context.Context) (int64, error) {
	const q = `
		UPDATE cpu_usage_workers
		SET active = false,
			activation_expires_on = NULL
		WHERE active
		AND NOT getting_work
		AND NOT working
		AND CURRENT_TIMESTAMP >= COALESCE(activation_expires_on, to_timestamp(0));
	`
	result, err := d.db.ExecContext(context, q)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// PurgeExpiredWorkSeekers clears out all workers that have been looking for work from
// the queue too long. Returns the number of rows affected.
func (d *Database) PurgeExpiredWorkSeekers(context context.Context) (int64, error) {
	const q = `
		UPDATE cpu_usage_workers
		SET getting_work = false,
			getting_work_on = NULL,
			getting_work_expires_on = NULL
		WHERE active
		AND getting_work
		AND NOT working
		AND CURRENT_TIMESTAMP >= COALESCE(getting_work_expires_on, to_timestamp(0));
	`

	result, err := d.db.ExecContext(context, q)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// PurgeExpiredWorkClaims will mark an event as unclaimed if it's not processed, not
// being processed, and the current time is equal to or past the claim expiration date.
func (d *Database) PurgeExpiredWorkClaims(context context.Context) (int64, error) {
	const q = `
		UPDATE cpu_usage_events
		SET claimed = false,
			claimed_by = NULL,
			claimed_on = NULL
		WHERE claimed = true
		AND processing = false
		AND processed = false
		AND CURRENT_TIMESTAMP >= COALESCE(claim_expires_on, to_timestamp(0));
	`
	result, err := d.db.ExecContext(context, q)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// resetWorkClaimsForInactiveWorkers will mark an event as unclaimed if the worker that
// claimed it is inactive.
func (d *Database) ResetWorkClaimsForInactiveWorkers(context context.Context) (int64, error) {
	const q = `
		UPDATE cpu_usage_events
		SET claimed = false,
			claimed_by = NULL,
			claimed_on = NULL
		FROM ( SELECT id FROM cpu_usage_workers WHERE NOT active ) AS sub
		WHERE claimed = true
		AND claimed_by = sub.id;
	`

	result, err := d.db.ExecContext(context, q)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// GettingWork records that the worker is looking up work.
func (d *Database) GettingWork(context context.Context, workerID string, expiration time.Time) error {
	const q = `
		UPDATE cpu_usage_workers
		SET getting_work = true,
			getting_work_expires_on = $2
		WHERE id = $1;
	`
	_, err := d.db.ExecContext(context, q, workerID, expiration)
	return err
}

// DoneGettingWork records that the worker is not looking up work.
func (d *Database) DoneGettingWork(context context.Context, workerID string) error {
	const q = `
		UPDATE cpu_usage_workers
		SET getting_work = false,
			getting_work_expires_on = NULL
		WHERE id = $1;
	`
	_, err := d.db.ExecContext(context, q, workerID)
	return err
}

// SetWorking records whether the worker is working on something.
func (d *Database) SetWorking(context context.Context, workerID string, working bool) error {
	const q = `
		UPDATE cpu_usage_workers
		SET working = $2
		WHERE id = $1;
	`
	_, err := d.db.ExecContext(context, q, workerID, working)
	return err
}
