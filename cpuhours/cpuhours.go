package cpuhours

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/go-mod/subjects"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "cpuhours"})

type CPUHours struct {
	db *db.Database
	nc *nats.EncodedConn
}

func New(db *db.Database, nc *nats.EncodedConn) *CPUHours {
	return &CPUHours{
		db: db,
		nc: nc,
	}
}

// CPUHoursForAnalysis returns the CPU hours total for the analysis as a decimal value.
func (c *CPUHours) CPUHoursForAnalysis(context context.Context, analysisID string) (*apd.Decimal, *db.Analysis, error) {
	var (
		endTime  time.Time
		analysis *db.Analysis
		err      error
	)
	log = log.WithFields(logrus.Fields{"context": "calculating CPU hours", "analysisID": analysisID})

	log.Debug("getting millicores reserved")
	millicoresReserved, err := c.db.MillicoresReserved(context, analysisID)
	if err != nil {
		return nil, nil, err
	}
	log.Debug("done getting millicores reserved")

	for {
		log.Debug("getting analysis info")
		analysis, err = c.db.AnalysisWithoutUser(context, analysisID)
		if err != nil {
			return nil, nil, err
		}
		log.Debug("done getting analysis info")

		if !analysis.StartDate.Valid {
			return nil, nil, fmt.Errorf("start date is null")
		}

		// It's possible for this to be reached before the database is updated with the actual
		// end date. If that's the case, wait a bit and try again.
		if !analysis.EndDate.Valid {
			time.Sleep(5 * time.Second)
			continue

		} else {
			endTime = analysis.EndDate.Time.UTC()
			break
		}
	}

	startTime := analysis.StartDate.Time.UTC()

	log.Infof("start date: %s, end date: %s", startTime.String(), endTime.String())

	timeSpent, err := apd.New(0, 0).SetFloat64(endTime.Sub(startTime).Hours())
	if err != nil {
		return nil, nil, err
	}

	mcReserved := apd.New(0, 0).SetInt64(millicoresReserved)
	cpuHours := apd.New(0, 0)
	mc2cores := apd.New(1000, 0)

	bc := apd.BaseContext.WithPrecision(15)
	_, err = bc.Mul(cpuHours, mcReserved, timeSpent)
	if err != nil {
		return nil, nil, err
	}

	_, err = bc.Quo(cpuHours, cpuHours, mc2cores)
	if err != nil {
		return nil, nil, err
	}

	log.Infof("run time is %s hours; millicores reserved is %s; cpu hours is %s", timeSpent.String(), mcReserved.String(), cpuHours.String())

	return cpuHours, analysis, nil
}

func (c *CPUHours) addEvent(context context.Context, analysis *db.Analysis, cpuHours *apd.Decimal) error {
	var err error

	floatValue, err := cpuHours.Float64()
	if err != nil {
		return err
	}

	username, err := c.db.Username(context, analysis.UserID)
	if err != nil {
		return err
	}

	update := &qms.Update{
		ValueType:     "usages",
		Value:         floatValue,
		EffectiveDate: timestamppb.Now(),
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
	}

	request := pbinit.NewAddUpdateRequest(update)
	response := pbinit.NewQMSAddUpdateResponse()
	_, span := pbinit.InitQMSAddUpdateRequest(request, subjects.QMSAddUserUpdate)
	defer span.End()

	log = log.WithFields(logrus.Fields{"context": "adding event", "analysisID": analysis.ID})

	log.Debug("adding cpu usage event")
	if err = gotelnats.Request(context, c.nc, subjects.QMSAddUserUpdate, request, response); err != nil {
		return err
	}
	log.Debug("after add cpu usage event")

	return nil
}

func (c *CPUHours) CalculateForAnalysisByID(context context.Context, analysisID string) error {
	var (
		cpuHours *apd.Decimal
		analysis *db.Analysis
		err      error
	)

	cpuHours, analysis, err = c.CPUHoursForAnalysis(context, analysisID)
	if err != nil {
		return err
	}

	return c.addEvent(context, analysis, cpuHours)
}

func (c *CPUHours) CalculateForAnalysis(context context.Context, externalID string) error {
	log.Debug("getting analysis id")
	analysisID, err := c.db.GetAnalysisIDByExternalID(context, externalID)
	if err != nil {
		return err
	}
	log.Debug("done getting analysis id")

	return c.CalculateForAnalysisByID(context, analysisID)
}
