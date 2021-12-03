package cpuhours

import (
	"context"
	"fmt"
	"time"

	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/sirupsen/logrus"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "cpuhours"})

type CPUHours struct {
	db *db.Database
}

func New(db *db.Database) *CPUHours {
	return &CPUHours{
		db: db,
	}
}

func (c *CPUHours) CalculateForAnalysis(context context.Context, externalID string) error {
	log = log.WithFields(logrus.Fields{"context": "calculating CPU hours", "externalID": externalID})

	log.Debug("getting analysis id")
	analysisID, err := c.db.GetAnalysisIDByExternalID(context, externalID)
	if err != nil {
		return err
	}
	log.Debug("done getting analysis id")

	log = log.WithFields(logrus.Fields{"analysisID": analysisID})

	log.Debug("getting millicores reserved")
	millicoresReserved, err := c.db.MillicoresReserved(context, analysisID)
	if err != nil {
		return err
	}
	log.Debug("done getting millicores reserved")

	log.Debug("getting analysis info")
	analysis, err := c.db.AnalysisWithoutUser(context, analysisID)
	if err != nil {
		return err
	}
	log.Debug("done getting analysis info")

	if !analysis.StartDate.Valid {
		return fmt.Errorf("start date is null")
	}

	if !analysis.EndDate.Valid {
		return fmt.Errorf("end date is null")
	}

	startTime := analysis.StartDate.Time
	endTime := analysis.EndDate.Time

	timeSpent := endTime.Sub(startTime).Hours()
	cpuHours := (millicoresReserved * timeSpent) / 1000.0 // Convert millicores to cores/CPUs
	nowTime := time.Now()

	log.Infof("run time is %f hours; millicores reserved is %f; cpu hours is %f", timeSpent, millicoresReserved, cpuHours)

	event := db.CPUUsageEvent{
		CreatedBy:     analysis.UserID,
		EffectiveDate: nowTime,
		RecordDate:    nowTime,
		Value:         cpuHours,
	}

	log.Debug("adding cpu usage event")
	if err = c.db.AddCPUUsageEvent(context, &event); err != nil {
		log.Error(err)
	}
	log.Debug("done adding cpu usage event")

	return nil
}
