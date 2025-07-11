package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/jmoiron/sqlx"
)

var log = logging.Log // nolint

type CPUHours struct {
	ID             string      `db:"id" json:"id"`
	UserID         string      `db:"user_id" json:"user_id"`
	Username       string      `db:"username" json:"username"`
	Total          apd.Decimal `db:"total" json:"total"`
	EffectiveStart time.Time   `db:"effective_start" json:"effective_start"`
	EffectiveEnd   time.Time   `db:"effective_end" json:"effective_end"`
	LastModified   time.Time   `db:"last_modified" json:"last_modified"`
}

// User has information about a user from the DE's database.
type User struct {
	ID       string `db:"id" json:"id"`
	Username string `db:"username" json:"username"`
}

type DatabaseAccessor interface {
	QueryRowxContext(context.Context, string, ...interface{}) *sqlx.Row
	QueryxContext(context.Context, string, ...interface{}) (*sqlx.Rows, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	BeginTxx(context.Context, *sql.TxOptions) (*sqlx.Tx, error)
}

type TxAccessor interface {
	QueryRowxContext(context.Context, string, ...interface{}) *sqlx.Row
	QueryxContext(context.Context, string, ...interface{}) (*sqlx.Rows, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	Commit() error
	Rollback() error
}

type Queryer interface {
	QueryRowxContext(context.Context, string, ...interface{}) *sqlx.Row
	QueryxContext(context.Context, string, ...interface{}) (*sqlx.Rows, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

type Database struct {
	db DatabaseAccessor
	tx TxAccessor
}

func New(db DatabaseAccessor) *Database {
	return &Database{db: db}
}

func (d *Database) Q() Queryer {
	if d.tx != nil {
		return d.tx
	} else {
		return d.db
	}
}

func (d *Database) Begin(context context.Context) error {
	tx, err := d.db.BeginTxx(context, nil)
	if err != nil {
		return err
	}
	d.tx = tx
	return nil
}

func (d *Database) Commit() error {
	if d.tx != nil {
		err := d.tx.Commit()
		d.tx = nil
		return err
	}
	return nil
}

func (d *Database) Rollback() error {
	if d.tx != nil {
		err := d.tx.Rollback()
		d.tx = nil
		return err
	}
	return nil
}

func (d *Database) Username(context context.Context, userID string) (string, error) {
	var username string

	const q = `
		SELECT username
		FROM users
		WHERE id = $1;
	`

	err := d.Q().QueryRowxContext(context, q, userID).Scan(&username)
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

	err := d.Q().QueryRowxContext(context, q, username).Scan(&userID)
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
	err := d.Q().QueryRowxContext(context, q, username).StructScan(&cpuHours)
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
	_, err := d.Q().ExecContext(
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
			t.user_id,
			u.username,
			lower(t.effective_range) effective_start,
			upper(t.effective_range) effective_end,
			t.last_modified
		FROM cpu_usage_totals t
		JOIN users u ON t.user_id = u.id
		WHERE u.username = $1;
	`

	rows, err = d.Q().QueryxContext(context, q, username)
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

	rows, err := d.Q().QueryxContext(context, q)
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

	rows, err := d.Q().QueryxContext(context, q)
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

	_, err := d.Q().ExecContext(
		context,
		q,
		totalObj.UserID,
		totalObj.Total,
	)
	return err
}

func (d *Database) MillicoresReserved(context context.Context, analysisID string) (int64, error) {
	const q = `
		SELECT millicores_reserved
		FROM jobs
		WHERE id = $1;
	`
	var millicores int64
	err := d.Q().QueryRowxContext(context, q, analysisID).Scan(&millicores)
	return millicores, err
}

func (d *Database) UsersWithCalculableAnalyses(context context.Context) ([]User, error) {
	var users []User

	const q = `
		SELECT
			DISTINCT ON (u.id) u.id,
			u.username
		FROM users u
		JOIN jobs j ON j.user_id = u.id
		WHERE j.millicores_reserved != 0
		AND j.start_date IS NOT NULL
		AND j.end_date IS NOT NULL;
	`

	rows, err := d.Q().QueryxContext(context, q)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var u User
		err = rows.StructScan(&u)
		if err != nil {
			return nil, err
		}
		users = append(users, u)
	}

	if err = rows.Err(); err != nil {
		return users, err
	}

	return users, nil
}
