package db

import (
	"context"
	"database/sql"

	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/guregu/null"
	"github.com/jmoiron/sqlx"
)

var log = logging.Log // nolint

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

type Analysis struct {
	ID         string      `db:"id"`
	AppID      string      `db:"app_id"`
	StartDate  null.Time   `db:"start_date"`
	EndDate    null.Time   `db:"end_date"`
	Status     string      `db:"status"`
	Deleted    bool        `db:"deleted"`
	Submission string      `db:"submission"`
	UserID     string      `db:"user_id"`
	JobType    string      `db:"job_type"`
	SystemID   string      `db:"system_id"`
	Subdomain  null.String `db:"subdomain"`
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

// GetAnalysisIDByExternalID returns the analysis ID based on the external ID
// passed in.
func (d *Database) GetAnalysisIDByExternalID(context context.Context, externalID string) (string, error) {
	var analysisID string
	const q = `
		SELECT j.id
		FROM jobs j
		JOIN job_steps s ON s.job_id = j.id
		WHERE s.external_id = $1
	`
	err := d.db.QueryRowxContext(context, q, externalID).Scan(&analysisID)
	if err != nil {
		return "", err
	}
	return analysisID, nil
}

func (d *Database) AnalysisWithoutUser(context context.Context, analysisID string) (*Analysis, error) {
	const q = `
		SELECT
			j.id,
			j.app_id,
			j.start_date,
			j.end_date,
			j.status,
			j.deleted,
			j.submission,
			j.user_id,
			j.subdomain,
			t.name job_type,
			t.system_id
		FROM jobs j
		JOIN job_types t ON j.job_type_id = t.id
		WHERE j.id = $1;
	`
	var analysis Analysis
	err := d.db.QueryRowxContext(context, q, analysisID).StructScan(&analysis)
	return &analysis, err
}
