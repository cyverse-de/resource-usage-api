package db

import (
	"context"
	"time"

	"github.com/guregu/null"
)

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

func (d *Database) Analysis(context context.Context, userID, id string) (*Analysis, error) {
	var analysis Analysis
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
		JOIN job_types t ON j.job_type_id = job_types.id
		WHERE j.id = $1
		AND j.user_id = $2;
	`
	err := d.db.QueryRowxContext(context, q, id, userID).StructScan(&analysis)
	return &analysis, err
}

type CalculableAnalysis struct {
	ID                 string    `db:"id"`
	StartDate          time.Time `db:"start_date"`
	EndDate            time.Time `db:"end_date"`
	MillicoresReserved int64     `db:"millicores_reserved"`
}

func (d *Database) AdminAllCalculableAnalyses(context context.Context, userID string) ([]CalculableAnalysis, error) {
	var analyses []CalculableAnalysis
	const q = `
		SELECT
			j.id,
			j.start_date,
			j.end_date,
			j.millicores_reserved
		FROM jobs j
		WHERE j.user_id = $1
		AND j.millicores_reserved != 0
		AND j.start_date IS NOT NULL
		AND j.end_date IS NOT NULL;
	`
	rows, err := d.db.QueryxContext(context, q, userID)
	if err != nil {
		return nil, err
	}

	log.Debugf("user %s", userID)

	for rows.Next() {
		var a CalculableAnalysis
		err = rows.StructScan(&a)
		if err != nil {
			return nil, err
		}
		log.Debugf("id: %s; start_date: %s; end_date: %s; millicores_reserved: %s", a.ID, a.StartDate, a.EndDate, a.MillicoresReserved)

		analyses = append(analyses, a)
	}

	if err = rows.Err(); err != nil {
		return analyses, err
	}

	return analyses, nil
}
