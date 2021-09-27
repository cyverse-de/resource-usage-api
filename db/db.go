package db

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

type CPUHours struct {
	ID             string    `db:"id" json:"id"`
	UserID         string    `db:"user_id" json:"user_id"`
	Username       string    `db:"username" json:"username"`
	Total          int64     `db:"total" json:"total"`
	EffectiveStart time.Time `db:"effective_start" json:"effective_start"`
	EffectiveEnd   time.Time `db:"effective_end" json:"effective_end"`
	LastModified   time.Time `db:"last_modified" json:"last_modified"`
}

type CPUUsageEvent struct {
	ID            string    `db:"id" json:"id"`
	RecordDate    time.Time `db:"record_date" json:"record_date"`
	EffectiveDate time.Time `db:"effective_date" json:"effective_date"`
	EventType     string    `db:"event_type" json:"event_type"`
	Value         int64     `db:"value" json:"value"`
	CreatedBy     string    `db:"created_by" json:"created_by"`
	LastModified  string    `db:"last_modified" json:"last_modified"`
}

type CPUUsageWorkItem struct {
	CPUUsageEvent
	Claimed               bool
	ClaimedBy             string
	ClaimExpiresOn        time.Time
	Processed             bool
	Processing            bool
	ProcessedOn           time.Time
	MaxProcessingAttempts int
	Attempts              int
}

type Database struct {
	db *sqlx.DB
}

func New(db *sqlx.DB) *Database {
	return &Database{db: db}
}

func (d *Database) CurrentCPUHoursForUser(context context.Context, username string) (*CPUHours, error) {
	var cpuHours CPUHours
	err := d.db.QueryRowxContext(context, currentCPUHoursForUserQuery, username).Scan(&cpuHours)
	return &cpuHours, err
}

func (d *Database) AllCPUHoursForUser(context context.Context, username string) ([]CPUHours, error) {
	var cpuHours []CPUHours

	rows, err := d.db.QueryxContext(context, allCPUHoursForUserQuery, username)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var h CPUHours
		err = rows.StructScan(&h)
		if err != nil {
			return nil, err
		}
		cpuHours = append(cpuHours, h)
	}

	if err = rows.Err(); err != nil {
		return cpuHours, err
	}

	return cpuHours, nil
}

func (d *Database) AdminAllCurrentCPUHours(context context.Context) ([]CPUHours, error) {
	var cpuHours []CPUHours

	rows, err := d.db.QueryxContext(context, currentCPUHoursQuery)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var h CPUHours
		err = rows.StructScan(&h)
		if err != nil {
			return nil, err
		}
		cpuHours = append(cpuHours, h)
	}

	if err = rows.Err(); err != nil {
		return cpuHours, err
	}

	return cpuHours, nil
}

func (d *Database) AdminAllCPUHours(context context.Context) ([]CPUHours, error) {
	var cpuHours []CPUHours

	rows, err := d.db.QueryxContext(context, allCPUHoursQuery)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var h CPUHours
		err = rows.StructScan(&h)
		if err != nil {
			return nil, err
		}
		cpuHours = append(cpuHours, h)
	}

	if err = rows.Err(); err != nil {
		return cpuHours, err
	}

	return cpuHours, nil
}

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

func (d *Database) FinishedProcessingEvent(context context.Context, id string) error {
	_, err := d.db.ExecContext(
		context,
		finishedProcessingStmt,
		id,
	)
	return err
}
