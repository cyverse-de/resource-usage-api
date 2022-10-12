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

func (d *Database) MillicoresReserved(context context.Context, analysisID string) (int64, error) {
	const q = `
		SELECT millicores_reserved
		FROM jobs
		WhERE id = $1;
	`
	var millicores int64
	err := d.db.QueryRowxContext(context, q, analysisID).Scan(&millicores)
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

	rows, err := d.db.QueryxContext(context, q)
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
