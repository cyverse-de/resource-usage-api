package internal

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/labstack/echo/v4"
)

type totalUpdaterFn func(float64, *db.CPUUsageWorkItem) float64

func totalAdder(curr float64, workItem *db.CPUUsageWorkItem) float64 {
	return curr + workItem.Value
}

func totalSubtracter(curr float64, workItem *db.CPUUsageWorkItem) float64 {
	return curr - workItem.Value
}

func totalReplacer(_ float64, workItem *db.CPUUsageWorkItem) float64 {
	return workItem.Value
}

func (a *App) updateCPUHours(context context.Context, workItem *db.CPUUsageWorkItem, updater totalUpdaterFn) error {
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

	total.Total = updater(total.Total, workItem)

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
		value      float64
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
	value, err = strconv.ParseFloat(valueParam, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "value must be parsable as a 64-bit integer")
	}

	event := db.CPUUsageEvent{
		EventType:     eventType,
		RecordDate:    time.Now(),
		EffectiveDate: time.Now(),
		CreatedBy:     userID,
		Value:         value,
	}

	return d.AddCPUUsageEvent(context, &event)
}

func (a *App) AddToTotalHandler(c echo.Context) error {
	err := a.totalHandler(c, db.CPUHoursAdd)
	if err != nil {
		log.Error(err)
	}
	return err
}

func (a *App) SubtractFromTotalHandler(c echo.Context) error {
	err := a.totalHandler(c, db.CPUHoursSubtract)
	if err != nil {
		log.Error(err)
	}
	return err
}

func (a *App) ResetTotalHandler(c echo.Context) error {
	err := a.totalHandler(c, db.CPUHoursReset)
	if err != nil {
		log.Error(err)
	}
	return err
}
