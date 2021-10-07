package db

import (
	"fmt"
	"testing"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"go.uber.org/multierr"
)

func TestSQLSyntax(t *testing.T) {
	var err error

	queries := []map[string]string{
		{"analysisIDByExternalIDQuery": analysisIDByExternalIDQuery},
		{"analysisQuery": analysisQuery},
		{"currentCPUHoursForUserQuery": currentCPUHoursForUserQuery},
		{"updateCurrentCPUHoursForUserQuery": updateCurrentCPUHoursForUserQuery},
		{"allCPUHoursForUserQuery": allCPUHoursForUserQuery},
		{"currentCPUHoursQuery": currentCPUHoursQuery},
		{"allCPUHoursQuery": allCPUHoursQuery},
		{"insertCPUHourEventStmt": insertCPUHourEventStmt},
		{"unprocessedEventsQuery": unprocessedEventsQuery},
		{"claimedByStmt": claimedByStmt},
		{"processingStmt": processingStmt},
		{"finishedProcessingStmt": finishedProcessingStmt},
		{"registerWorkerStmt": registerWorkerStmt},
		{"unregisterWorkerStmt": unregisterWorkerStmt},
		{"refreshWorkerStmt": refreshWorkerStmt},
		{"purgeExpiredWorkersStmt": purgeExpiredWorkersStmt},
		{"purgeExpiredWorkSeekersStmt": purgeExpiredWorkSeekersStmt},
		{"purgeExpiredWorkClaimsStmt": purgeExpiredWorkClaimsStmt},
		{"resetWorkClaimForInactiveWorkersStmt": resetWorkClaimForInactiveWorkersStmt},
		{"gettingWorkStmt": gettingWorkStmt},
		{"notGettingWorkStmt": notGettingWorkStmt},
		{"setWorkingStmt": setWorkingStmt},
	}

	for _, q := range queries {
		for name, query := range q {
			if _, perr := parser.Parse(query); perr != nil {
				err = multierr.Append(err, fmt.Errorf("error parsing %s: %w", name, perr))
			}
		}
	}

	for _, e := range multierr.Errors(err) {
		t.Error(e)
	}
}
