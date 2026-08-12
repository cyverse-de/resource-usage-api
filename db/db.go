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

// Queryer is the query surface shared by a connection pool and a transaction. Code that must run
// inside a transaction takes this rather than DatabaseAccessor, so that a pool cannot be passed by
// mistake.
type Queryer interface {
	QueryRowxContext(context.Context, string, ...any) *sqlx.Row
	QueryxContext(context.Context, string, ...any) (*sqlx.Rows, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	GetContext(context.Context, any, string, ...any) error
	SelectContext(context.Context, any, string, ...any) error
}

// DatabaseAccessor is a Queryer that can also open transactions and report pool statistics. Only a
// connection pool satisfies it.
type DatabaseAccessor interface {
	Queryer
	BeginTxx(context.Context, *sql.TxOptions) (*sqlx.Tx, error)
	Stats() sql.DBStats
}

// TxAccessor is a Queryer that can be committed or rolled back. Only a transaction satisfies it.
type TxAccessor interface {
	Queryer
	Commit() error
	Rollback() error
}

// The interfaces above are meaningful only if the two concrete types keep satisfying exactly the ones
// they are meant to.
var (
	_ DatabaseAccessor = (*sqlx.DB)(nil)
	_ TxAccessor       = (*sqlx.Tx)(nil)
	_ Queryer          = (*sqlx.DB)(nil)
	_ Queryer          = (*sqlx.Tx)(nil)
)

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
