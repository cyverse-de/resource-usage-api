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
	err := d.db.QueryRowxContext(context, analysisIDByExternalIDQuery, externalID).Scan(&analysisID)
	if err != nil {
		return "", err
	}
	return analysisID, nil
}

func (d *Database) Analysis(context context.Context, userID, id string) (*Analysis, error) {
	var analysis Analysis
	err := d.db.QueryRowxContext(context, analysisQuery, id, userID).StructScan(&analysis)
	return &analysis, err
}
