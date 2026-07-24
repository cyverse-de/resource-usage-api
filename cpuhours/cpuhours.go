package cpuhours

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cyverse-de/p/go/ptypes"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/sirupsen/logrus"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "cpuhours"})

type CPUHours struct {
	db            *db.Database
	subscriptions *clients.Subscriptions
}

type CalculationResult struct {
	CPUHours  *apd.Decimal
	Analysis  *db.Analysis
	BasisTime time.Time
	CalcTime  time.Time
}

func New(db *db.Database, subscriptions *clients.Subscriptions) *CPUHours {
	return &CPUHours{
		db:            db,
		subscriptions: subscriptions,
	}
}

// CPUHoursForAnalysis returns the CPU hours total for the analysis as a decimal value.
func (c *CPUHours) CPUHoursForAnalysis(context context.Context, analysisID string) (CalculationResult, error) {
	var (
		basisTime time.Time
		calcTime  time.Time
		analysis  *db.Analysis
		err       error
		res       CalculationResult
	)
	msgLog := log.WithFields(logrus.Fields{"context": "calculating CPU hours", "analysisID": analysisID})

	msgLog.Debug("getting millicores reserved")
	millicoresReserved, err := c.db.MillicoresReserved(context, analysisID)
	if err != nil {
		return res, err
	}
	msgLog.Debug("done getting millicores reserved")

	for i := 0; i < 5; i++ { // Try five times, then use time.Now().UTC() instead
		msgLog.Debug("getting analysis info and locking row")
		analysis, err = c.db.AnalysisWithoutUser(context, analysisID)
		if err != nil {
			return res, err
		}
		msgLog.Debug("done getting analysis info")

		if !analysis.StartDate.Valid {
			return res, fmt.Errorf("start date is null")
		}

		// It's possible for this to be reached before the database is updated with the actual
		// end date. If that's the case, wait a bit and try again.
		//
		// We drop and restart the transaction here to avoid lock
		// issues and allow the end date to get set by other processes
		if !analysis.EndDate.Valid {
			if err := c.db.Rollback(); err != nil {
				msgLog.WithError(err).Error("failed to rollback transaction")
			}
			time.Sleep(5 * time.Second)
			c.db.Begin(context) // nolint: errcheck
			continue

		} else {
			calcTime = analysis.EndDate.Time.UTC()
			break
		}
	}

	res.Analysis = analysis

	if calcTime.IsZero() {
		calcTime = time.Now().UTC()
	}

	// Start calculation at the most recent of StartTime or UsageLastUpdate
	// calculate to EndDate or now, whichever is earlier
	// so start -> now, last update -> now, start -> end time already past, or last update -> end time already past
	// then update last update time to the now value that was used
	basisTime = analysis.StartDate.Time.UTC()
	if analysis.UsageLastUpdate.Valid && analysis.UsageLastUpdate.Time.UTC().After(basisTime) {
		basisTime = analysis.UsageLastUpdate.Time.UTC()
	}

	res.BasisTime = basisTime
	res.CalcTime = calcTime
	msgLog.Infof("basis date: %s, end date: %s", basisTime.String(), calcTime.String())

	timeSpent, err := apd.New(0, 0).SetFloat64(calcTime.Sub(basisTime).Hours())
	if err != nil {
		return res, err
	}

	mcReserved := apd.New(0, 0).SetInt64(millicoresReserved)
	cpuHours := apd.New(0, 0)
	mc2cores := apd.New(1000, 0)

	bc := apd.BaseContext.WithPrecision(15)
	_, err = bc.Mul(cpuHours, mcReserved, timeSpent)
	if err != nil {
		return res, err
	}

	_, err = bc.Quo(cpuHours, cpuHours, mc2cores)
	if err != nil {
		return res, err
	}

	msgLog.Infof("run time is %s hours; millicores reserved is %s; cpu hours is %s", timeSpent.String(), mcReserved.String(), cpuHours.String())

	err = c.db.SetUsageLastUpdate(context, analysisID, calcTime)
	if err != nil {
		return res, err
	}

	res.CPUHours = cpuHours

	return res, nil
}

func (c *CPUHours) addEvent(context context.Context, res CalculationResult) error {
	var err error
	analysis := res.Analysis
	cpuHours := res.CPUHours

	floatValue, err := cpuHours.Float64()
	if err != nil {
		return err
	}

	username, err := c.db.Username(context, analysis.UserID)
	if err != nil {
		return err
	}

	metajson, err := json.Marshal(res)
	if err != nil {
		return err
	}

	update := &qms.Update{
		ValueType:     "usages",
		Value:         floatValue,
		EffectiveDate: ptypes.Now(),
		Operation: &qms.UpdateOperation{
			Name: "ADD",
		},
		ResourceType: &qms.ResourceType{
			Name: "cpu.hours",
			Unit: "cpu hours",
		},
		User: &qms.QMSUser{
			Username: username,
		},
		Metadata: string(metajson),
	}

	msgLog := log.WithFields(logrus.Fields{"context": "adding event", "analysisID": analysis.ID})

	msgLog.Debugf("adding cpu usage event of %f for %s", floatValue, username)
	if err = c.subscriptions.AddUserUpdate(context, username, update); err != nil {
		msgLog.WithError(err).Error("Failed to add CPU usage event")
		return err
	}
	msgLog.Debug("after add cpu usage event")

	return nil
}

func (c *CPUHours) CalculateForAnalysisByID(context context.Context, analysisID string) error {
	var (
		res CalculationResult
		err error
	)

	res, err = c.CPUHoursForAnalysis(context, analysisID)
	if err != nil {
		return err
	}

	return c.addEvent(context, res)
}

func (c *CPUHours) CalculateForAnalysis(context context.Context, externalID string) error {
	log.Debug("getting analysis id")

	// We'll do this lookup outside the transaction to limit the lock time
	analysisID, err := c.db.GetAnalysisIDByExternalID(context, externalID)
	if err != nil {
		return err
	}
	log.Debug("done getting analysis id")

	err = c.db.Begin(context)
	if err != nil {
		return err
	}
	defer c.db.Rollback() // nolint:errcheck

	err = c.CalculateForAnalysisByID(context, analysisID)
	if err != nil {
		rollbackErr := c.db.Rollback()
		if rollbackErr != nil {
			log.WithError(rollbackErr).Error("failed to rollback transaction")
		}
		return err
	} else {
		return c.db.Commit()
	}
}
