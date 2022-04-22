package internal

import (
	"context"
	"net/http"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/labstack/echo/v4"
)

type totalUpdaterFn func(*apd.Decimal, *db.CPUUsageWorkItem) (*apd.Decimal, error)

func totalAdder(curr *apd.Decimal, workItem *db.CPUUsageWorkItem) (*apd.Decimal, error) {
	result := apd.New(0, 0)
	_, err := apd.BaseContext.WithPrecision(15).Add(result, curr, &workItem.Value)
	return result, err
}

func totalSubtracter(curr *apd.Decimal, workItem *db.CPUUsageWorkItem) (*apd.Decimal, error) {
	result := apd.New(0, 0)
	_, err := apd.BaseContext.WithPrecision(15).Sub(result, curr, &workItem.Value)
	return result, err
}

func totalReplacer(_ *apd.Decimal, workItem *db.CPUUsageWorkItem) (*apd.Decimal, error) {
	return &workItem.Value, nil
}

func (a *App) updateCPUHours(context context.Context, workItem *db.CPUUsageWorkItem, updater totalUpdaterFn) error {
	var log = log.WithContext(context)
	// Open a transaction.
	tx, err := a.database.Beginx()
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error(rbErr)
		}
		return err
	}

	d := db.New(tx)

	// Get the current total.
	total, err := d.CurrentCPUHoursForUser(context, workItem.CreatedBy)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error(rbErr)
		}
		return err
	}

	newTotal, err := updater(&total.Total, workItem)
	if err != nil {
		log.Error(err)
		return err
	}

	total.Total = *newTotal

	// Set the new total.
	if err = d.UpdateCPUHoursTotal(context, total); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error(rbErr)
		}
		return err
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error(rbErr)
		}
		return err
	}

	return nil
}

func (a *App) CPUHoursAdd(context context.Context, workItem *db.CPUUsageWorkItem) error {
	return a.updateCPUHours(context, workItem, totalAdder)
}

func (a *App) CPUHoursSubtract(context context.Context, workItem *db.CPUUsageWorkItem) error {
	return a.updateCPUHours(context, workItem, totalSubtracter)
}

func (a *App) CPUHoursReset(context context.Context, workItem *db.CPUUsageWorkItem) error {
	return a.updateCPUHours(context, workItem, totalReplacer)
}

//
// HTTP Handlers
//

func (a *App) totalHandler(c echo.Context, eventType db.EventType) error {
	var (
		err        error
		user       string
		valueParam string
		value      *apd.Decimal
	)
	context := c.Request().Context()

	user = c.Param("username")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "user was not set")
	}
	user = a.FixUsername(user)

	d := db.New(a.database)

	userID, err := d.UserID(context, user)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "user not found")
	}

	valueParam = c.Param("value")
	if valueParam == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "value was not set")
	}

	value, _, err = apd.NewFromString(valueParam)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "value must be parsable as a 64-bit integer")
	}

	event := db.CPUUsageEvent{
		EventType:     eventType,
		RecordDate:    time.Now(),
		EffectiveDate: time.Now(),
		CreatedBy:     userID,
		Value:         *value,
	}

	return d.AddCPUUsageEvent(context, &event)
}

func (a *App) AddToTotalHandler(c echo.Context) error {
	var log = log.WithContext(c.Request().Context())
	err := a.totalHandler(c, db.CPUHoursAdd)
	if err != nil {
		log.Error(err)
	}
	return err
}

func (a *App) SubtractFromTotalHandler(c echo.Context) error {
	var log = log.WithContext(c.Request().Context())
	err := a.totalHandler(c, db.CPUHoursSubtract)
	if err != nil {
		log.Error(err)
	}
	return err
}

func (a *App) ResetTotalHandler(c echo.Context) error {
	var log = log.WithContext(c.Request().Context())
	err := a.totalHandler(c, db.CPUHoursReset)
	if err != nil {
		log.Error(err)
	}
	return err
}
