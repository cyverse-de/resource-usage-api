package db

import (
	"context"
	"database/sql"
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

type DatabaseAccessor interface {
	QueryRowxContext(context.Context, string, ...interface{}) *sqlx.Row
	QueryxContext(context.Context, string, ...interface{}) (*sqlx.Rows, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

type Database struct {
	db DatabaseAccessor
}

func New(db DatabaseAccessor) *Database {
	return &Database{db: db}
}

func (d *Database) Username(context context.Context, userID string) (string, error) {
	var username string

	const q = `
		SELECT username
		FROM users
		WHERE id = $1;
	`

	err := d.db.QueryRowxContext(context, q, userID).Scan(&username)
	if err != nil {
		return "", err
	}

	return username, nil
}

func (d *Database) UserID(context context.Context, username string) (string, error) {
	var userID string

	const q = `
		SELECT id
		FROM users
		WHERE username = $1;
	`

	err := d.db.QueryRowxContext(context, q, username).Scan(&userID)
	if err != nil {
		return "", err
	}

	return userID, nil
}

func (d *Database) CurrentCPUHoursForUser(context context.Context, username string) (*CPUHours, error) {
	var cpuHours CPUHours

	const q = `
		SELECT 
			t.id,
			t.total,
			t.user_id,
			u.username,
			lower(t.effective_range) effective_start,
			upper(t.effective_range) effective_end,
			t.last_modified
		FROM cpu_usage_totals t
		JOIN users u ON t.user_id = u.id
		WHERE u.username = $1
		AND t.effective_range @> CURRENT_TIMESTAMP::timestamp
		LIMIT 1;
	`
	err := d.db.QueryRowxContext(context, q, username).StructScan(&cpuHours)
	if err != nil {
		return nil, err
	}
	return &cpuHours, err
}

func (d *Database) InsertCurrentCPUHoursForUser(context context.Context, cpuHours *CPUHours) error {
	const q = `
		INSERT INTO cpu_usage_totals
			(total, user_id, effective_range)
		VALUES
			($1, $2, tsrange($3, $4, '[)'));
	`
	_, err := d.db.ExecContext(
		context,
		q,
		cpuHours.Total,
		cpuHours.UserID,
		cpuHours.EffectiveStart,
		cpuHours.EffectiveEnd,
	)
	return err
}

func (d *Database) AllCPUHoursForUser(context context.Context, username string) ([]CPUHours, error) {
	var (
		err      error
		cpuHours []CPUHours
		rows     *sqlx.Rows
	)

	const q = `
		SELECT
			t.id,
			t.total,
			lower(t.effective_range) effective_start,
			upper(t.effective_range) effective_end,
			t.last_modified
		FROM cpu_usage_totals t
		JOIN users u ON t.user_id = u.id
		WHERE u.username = $1;
	`

	rows, err = d.db.QueryxContext(context, q, username)
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

	const q = `
		SELECT 
			t.id,
			t.total,
			t.user_id,
			u.username,
			lower(t.effective_range) effective_start,
			upper(t.effective_range) effective_end,
			t.last_modified
		FROM cpu_usage_totals t
		JOIN users u ON t.user_id = u.id
		WHERE t.effective_range @> CURRENT_TIMESTAMP::timestamp;
	`

	rows, err := d.db.QueryxContext(context, q)
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

	const q = `
		SELECT 
			t.id,
			t.total,
			t.user_id,
			u.username,
			lower(t.effective_range) effective_start,
			upper(t.effective_range) effective_end,
			t.last_modified
		FROM cpu_usage_totals t
		JOIN users u ON t.user_id = u.id;
	`

	rows, err := d.db.QueryxContext(context, q)
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

func (d *Database) UpdateCPUHoursTotal(context context.Context, totalObj *CPUHours) error {
	const q = `
		UPDATE cpu_usage_totals
		SET total = $2
		WHERE user_id = $1
		AND effective_range @> CURRENT_TIMESTAMP::timestamp;
	`

	_, err := d.db.ExecContext(
		context,
		q,
		totalObj.UserID,
		totalObj.Total,
	)
	return err
}
