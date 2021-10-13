package db

import (
	"fmt"
	"testing"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"go.uber.org/multierr"
)

var queries = map[string]string{
	"analysisIDByExternalIDQuery":          analysisIDByExternalIDQuery,
	"analysisQuery":                        analysisQuery,
	"usernameQuery":                        usernameQuery,
	"currentCPUHoursForUserQuery":          currentCPUHoursForUserQuery,
	"addCurrentCPUHoursForUserStmt":        addCurrentCPUHoursForUserStmt,
	"updateCurrentCPUHoursForUserQuery":    updateCurrentCPUHoursForUserQuery,
	"allCPUHoursForUserQuery":              allCPUHoursForUserQuery,
	"currentCPUHoursQuery":                 currentCPUHoursQuery,
	"allCPUHoursQuery":                     allCPUHoursQuery,
	"insertCPUHourEventStmt":               insertCPUHourEventStmt,
	"unprocessedEventsQuery":               unprocessedEventsQuery,
	"claimedByStmt":                        claimedByStmt,
	"processingStmt":                       processingStmt,
	"finishedProcessingStmt":               finishedProcessingStmt,
	"registerWorkerStmt":                   registerWorkerStmt,
	"unregisterWorkerStmt":                 unregisterWorkerStmt,
	"refreshWorkerStmt":                    refreshWorkerStmt,
	"purgeExpiredWorkersStmt":              purgeExpiredWorkersStmt,
	"purgeExpiredWorkSeekersStmt":          purgeExpiredWorkSeekersStmt,
	"purgeExpiredWorkClaimsStmt":           purgeExpiredWorkClaimsStmt,
	"resetWorkClaimForInactiveWorkersStmt": resetWorkClaimForInactiveWorkersStmt,
	"gettingWorkStmt":                      gettingWorkStmt,
	"notGettingWorkStmt":                   notGettingWorkStmt,
	"setWorkingStmt":                       setWorkingStmt,
}

func TestSQLSyntax(t *testing.T) {
	var err error

	for name, query := range queries {
		if _, perr := parser.Parse(query); perr != nil {
			err = multierr.Append(err, fmt.Errorf("error parsing %s: %w", name, perr))
		}
	}

	for _, e := range multierr.Errors(err) {
		t.Error(e)
	}
}
