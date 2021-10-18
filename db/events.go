//
// This is primarily for the admin endpoints related to the events/work items.
// The terminology gets a little confused because a work item is a superset of
// an event.
//

package db

import (
	"context"
	"database/sql"
	"time"
)

type CPUUsageEvent struct {
	ID            string    `db:"id" json:"id"`
	RecordDate    time.Time `db:"record_date" json:"record_date"`
	EffectiveDate time.Time `db:"effective_date" json:"effective_date"`
	EventType     EventType `db:"event_type" json:"event_type"`
	Value         int64     `db:"value" json:"value"`
	CreatedBy     string    `db:"created_by" json:"created_by"`
	LastModified  string    `db:"last_modified" json:"last_modified"`
}

type CPUUsageWorkItem struct {
	CPUUsageEvent
	Claimed               bool
	ClaimedBy             sql.NullString `db:"claimed_by" json:"claimed_by"`
	ClaimExpiresOn        sql.NullTime   `db:"claim_expires_on" json:"claim_expires_on"`
	ClaimedOn             sql.NullTime   `db:"claimed_on" json:"claimed_on"`
	Processed             bool
	Processing            bool
	ProcessedOn           sql.NullTime `db:"processed_on" json:"processed_on"`
	MaxProcessingAttempts int          `db:"max_processing_attempts" json:"max_processing_attempts"`
	Attempts              int
}

// AddCPUUsageEvent adds a new usage event to the database with the default values for
// the work queue fields.
func (d *Database) AddCPUUsageEvent(context context.Context, event *CPUUsageEvent) error {
	const q = `
		INSERT INTO cpu_usage_events
			(record_date, effective_date, event_type_id, value, created_by) 
		VALUES 
			($1, $2, (SELECT id FROM cpu_usage_event_types WHERE name = $3), $4, $5);
	`

	_, err := d.db.ExecContext(
		context,
		q,
		event.RecordDate,
		event.EffectiveDate,
		event.EventType,
		event.Value,
		event.CreatedBy,
	)
	return err
}

// ClaimEvent marks an CPU usage event in the database as claimed for work by the entity
// represented by the claimedBy string.
func (d *Database) ClaimEvent(context context.Context, id, claimedBy string) error {
	const q = `
		UPDATE cpu_usage_events
		SET claimed = true,
			claimed_by = $2
		WHERE id = $1;
	`
	_, err := d.db.ExecContext(context, q, id, claimedBy)
	return err
}

// ProcessingEvent marks as CPU usage event as being processed. It's not complete yet, but
// it's in progress.
func (d *Database) ProcessingEvent(context context.Context, id string) error {
	const q = `
		UPDATE cpu_usage_events
		SET processing = true,
			attempts = attempts + 1
		WHERE id = $1;
	`
	_, err := d.db.ExecContext(context, q, id)
	return err
}

// FinishedProcessingEvent marks an event as processed.
func (d *Database) FinishedProcessingEvent(context context.Context, id string) error {
	const q = `
		UPDATE cpu_usage_events
		SET processing = false,
			processed = true
		WHERE id = $1;
	`
	_, err := d.db.ExecContext(context, q, id)
	return err
}

// UnclaimedUnprocessedEvents returns a listing of the CPUUsageWorkItem for records that are not
// claimed, processed, being processed, expired, and have not reached the maximum number of attempts.
func (d *Database) UnclaimedUnprocessedEvents(context context.Context) ([]CPUUsageWorkItem, error) {
	var workItems []CPUUsageWorkItem

	const q = `
		SELECT 
			c.id,
			c.record_date,
			c.effective_date,
			e.name event_type,
			c.value,
			c.created_by,
			c.last_modified,
			c.claimed,
			c.claimed_by,
			c.claimed_on,
			c.claim_expires_on,
			c.processed,
			c.processing,
			c.processed_on,
			c.max_processing_attempts,
			c.attempts
		FROM cpu_usage_events c
		JOIN users u ON c.created_by = u.id
		JOIN cpu_usage_event_types e ON c.event_type_id = e.id
		WHERE NOT c.claimed
		AND NOT c.processed
		AND NOT c.processing
		AND c.attempts < c.max_processing_attempts
		AND CURRENT_TIMESTAMP >= COALESCE(c.claim_expires_on, to_timestamp(0));
	`

	rows, err := d.db.QueryxContext(context, q)
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

func (d *Database) ListEvents(context context.Context) ([]CPUUsageWorkItem, error) {
	var workItems []CPUUsageWorkItem

	const q = `
		SELECT 
			c.id,
			c.record_date,
			c.effective_date,
			e.name event_type,
			c.value,
			c.created_by,
			c.last_modified,
			c.claimed,
			c.claimed_by,
			c.claimed_on,
			c.claim_expires_on,
			c.processed,
			c.processing,
			c.processed_on,
			c.max_processing_attempts,
			c.attempts
		FROM cpu_usage_events c
		JOIN users u ON c.created_by = u.id
		JOIN cpu_usage_event_types e ON c.event_type_id = e.id;
	`

	rows, err := d.db.QueryxContext(context, q)
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

func (d *Database) ListAllUserEvents(context context.Context, username string) ([]CPUUsageWorkItem, error) {
	var workItems []CPUUsageWorkItem

	const q = `
		SELECT 
			c.id,
			c.record_date,
			c.effective_date,
			e.name event_type,
			c.value,
			c.created_by,
			c.last_modified,
			c.claimed,
			c.claimed_by,
			c.claimed_on,
			c.claim_expires_on,
			c.processed,
			c.processing,
			c.processed_on,
			c.max_processing_attempts,
			c.attempts
		FROM cpu_usage_events c
		JOIN users u ON c.created_by = u.id
		JOIN cpu_usage_event_types e ON c.event_type_id = e.id
		WHERE u.username = $1;
	`

	rows, err := d.db.QueryxContext(context, q, username)
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

func (d *Database) Event(context context.Context, id string) (*CPUUsageWorkItem, error) {
	var workItem CPUUsageWorkItem

	const q = `
		SELECT 
			c.id,
			c.record_date,
			c.effective_date,
			e.name event_type,
			c.value,
			c.created_by,
			c.last_modified,
			c.claimed,
			c.claimed_by,
			c.claimed_on,
			c.claim_expires_on,
			c.processed,
			c.processing,
			c.processed_on,
			c.max_processing_attempts,
			c.attempts
		FROM cpu_usage_events c
		JOIN cpu_usage_event_types e ON c.event_type_id = e.id
		WHERE c.id = $1;
	`
	err := d.db.QueryRowxContext(context, q, id).StructScan(&workItem)
	if err != nil {
		return nil, err
	}
	return &workItem, nil
}

func (d *Database) UpdateEvent(context context.Context, workItem *CPUUsageWorkItem) error {
	const q = `
		UPDATE cpu_usage_events
		SET record_date = $2,
			effective_date = $3,
			event_type_id = (SELECT id FROM cpu_usage_event_types WHERE name = $4 ORDER BY last_modified DESC LIMIT 1),
			value = $5,
			created_by = $6,
			claimed = $7,
			claimed_by = $8,
			claimed_on = $9,
			claim_expires_on = $10,
			max_processing_attempts = $11,
			attempts = $12
		WHERE id = $1;
	`

	_, err := d.db.ExecContext(
		context,
		q,
		workItem.ID,
		workItem.RecordDate,
		workItem.EffectiveDate,
		workItem.EventType,
		workItem.Value,
		workItem.CreatedBy,
		workItem.Claimed,
		workItem.ClaimedBy,
		workItem.ClaimedOn,
		workItem.ClaimExpiresOn,
		workItem.MaxProcessingAttempts,
		workItem.Attempts,
	)
	return err
}

func (d *Database) DeleteEvent(context context.Context, id string) error {
	const q = `
		DELETE FROM cpu_usage_events WHERE id = $1;
	`
	_, err := d.db.ExecContext(context, q, id)
	return err
}
