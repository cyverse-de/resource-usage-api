package cpuhours

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/apd"
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

	var endTime time.Time

	// It's possible for this to be reached before the database is updated with the actual
	// end date. Use the current date in that case, which should normally work out to be
	// in the user's favor, slightly.
	if analysis.EndDate.Valid {
		endTime = analysis.EndDate.Time
	} else {
		endTime = time.Now()
	}

	startTime := analysis.StartDate.Time

	log.Infof("start date: %s, end date: %s", startTime.String(), endTime.String())

	timeSpent, err := apd.New(0, 0).SetFloat64(endTime.Sub(startTime).Hours())
	if err != nil {
		return err
	}

	mcReserved := apd.New(0, 0).SetInt64(millicoresReserved)
	cpuHours := apd.New(0, 0)
	mc2cores := apd.New(1000, 0)

	bc := apd.BaseContext.WithPrecision(15)
	_, err = bc.Mul(cpuHours, mcReserved, timeSpent)
	if err != nil {
		return err
	}

	_, err = bc.Quo(cpuHours, cpuHours, mc2cores)
	if err != nil {
		return err
	}

	//cpuHours := (millicoresReserved * timeSpent) / 1000.0 // Convert millicores to cores/CPUs
	nowTime := time.Now()

	log.Infof("run time is %s hours; millicores reserved is %s; cpu hours is %s", timeSpent.String(), mcReserved.String(), cpuHours.String())

	event := db.CPUUsageEvent{
		CreatedBy:     analysis.UserID,
		EffectiveDate: nowTime,
		RecordDate:    nowTime,
		EventType:     db.CPUHoursAdd,
		Value:         *cpuHours,
	}

	log.Debug("adding cpu usage event")
	if err = c.db.AddCPUUsageEvent(context, &event); err != nil {
		log.Error(err)
	}
	log.Debug("done adding cpu usage event")

	return nil
}
