package db

import (
	"context"
	"database/sql"
)

type Analysis struct {
	ID         string
	AppID      string
	StartDate  sql.NullTime
	EndDate    sql.NullTime
	Status     string
	Deleted    bool
	Submission string
	UserID     string
	JobType    string
	SysteMID   string
}

// GetAnalysisIDByExternalID returns the analysis ID based on the external ID
// passed in.
func (d *Database) GetAnalysisIDByExternalID(context context.Context, externalID string) (string, error) {
	var analysisID string

	const analysisIDByExternalIDQuery = `
		SELECT j.id
		FROM jobs j
		JOIN job_steps s ON s.job_id = j.id
		WHERE s.external_id = $1
	`

	err := d.db.QueryRowxContext(context, analysisIDByExternalIDQuery, externalID).Scan(&analysisID)
	if err != nil {
		return "", err
	}

	return analysisID, nil
}

func (d *Database) Analysis(context context.Context, userID, id string) (*Analysis, error) {
	var analysis Analysis

	const analysisQuery = `
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
		JOIN job_types t ON j.job_type_id = job_types.id
		WHERE j.id = $1
		AND j.user_id = $2;
	`

	err := d.db.QueryRowxContext(context, analysisQuery, id, userID).StructScan(&analysis)

	return &analysis, err
}
