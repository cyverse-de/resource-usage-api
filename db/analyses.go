package db

import (
	"context"
	"time"

	"github.com/guregu/null"
)

type Analysis struct {
	ID              string      `db:"id"`
	AppID           string      `db:"app_id"`
	StartDate       null.Time   `db:"start_date"`
	EndDate         null.Time   `db:"end_date"`
	Status          string      `db:"status"`
	Deleted         bool        `db:"deleted"`
	Submission      string      `db:"submission"`
	UserID          string      `db:"user_id"`
	JobType         string      `db:"job_type"`
	SystemID        string      `db:"system_id"`
	Subdomain       null.String `db:"subdomain"`
	UsageLastUpdate null.Time   `db:"usage_last_update"`
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
	err := d.Q().QueryRowxContext(context, q, externalID).Scan(&analysisID)
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
			j.usage_last_update,
			t.name job_type,
			t.system_id
		FROM jobs j
		JOIN job_types t ON j.job_type_id = t.id
		WHERE j.id = $1
		FOR NO KEY UPDATE;
	`
	var analysis Analysis
	err := d.Q().QueryRowxContext(context, q, analysisID).StructScan(&analysis)
	return &analysis, err
}

// SetUsageLastUpdate updates the `usage_last_update` column of the jobs table to the provided time
func (d *Database) SetUsageLastUpdate(context context.Context, analysisID string, usagetime time.Time) error {
	const q = `
		UPDATE jobs 
		SET usage_last_update = $2
		WHERE id = $1
	`

	_, err := d.Q().ExecContext(
		context,
		q,
		analysisID,
		usagetime.Local(), // we store things in the DB as non-UTC time
	)
	return err
}
