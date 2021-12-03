package cpuhours

import (
	"context"
	"fmt"

	"github.com/cyverse-de/resource-usage-api/db"
)

type CPUHours struct {
	//amqp *messaging.Client
	db *db.Database
}

func (c *CPUHours) CalculateForAnalysis(context context.Context, externalID string) (float64, error) {
	analysisID, err := c.db.GetAnalysisIDByExternalID(context, externalID)
	if err != nil {
		return 0.0, err
	}

	millicoresReserved, err := c.db.MillicoresReserved(context, analysisID)
	if err != nil {
		return 0.0, err
	}

	analysis, err := c.db.AnalysisWithoutUser(context, analysisID)
	if err != nil {
		return 0.0, err
	}

	if !analysis.StartDate.Valid {
		return 0.0, fmt.Errorf("start date is null")
	}

	if !analysis.EndDate.Valid {
		return 0.0, fmt.Errorf("end date is null")
	}

	startTime := analysis.StartDate.Time
	endTime := analysis.EndDate.Time

	timeSpent := endTime.Sub(startTime)
	cpuHours := millicoresReserved * timeSpent.Hours()

	return cpuHours, nil

}
