package main

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/labstack/echo/v4"
)

type totalUpdaterFn func(int64, *db.CPUUsageWorkItem) int64

func totalAdder(curr int64, workItem *db.CPUUsageWorkItem) int64 {
	return curr + workItem.Value
}

func totalSubtracter(curr int64, workItem *db.CPUUsageWorkItem) int64 {
	return curr - workItem.Value
}

func totalReplacer(_ int64, workItem *db.CPUUsageWorkItem) int64 {
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
		value      int64
	)
	context := c.Request().Context()

	user = c.Param("username")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "user was not set")
	}
	user = a.FixUsername(user)

	valueParam = c.Param("value")
	if valueParam == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "value was not set")
	}
	value, err = strconv.ParseInt(valueParam, 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "value must be parsable as a 64-bit integer")
	}

	event := db.CPUUsageEvent{
		EventType:     eventType,
		EffectiveDate: time.Now(),
		CreatedBy:     user,
		Value:         value,
	}

	d := db.New(a.database)

	return d.AddCPUUsageEvent(context, &event)
}

func (a *App) AddToTotalHandler(c echo.Context) error {
	return a.totalHandler(c, db.CPUHoursAdd)
}

func (a *App) SubtractFromTotalHandler(c echo.Context) error {
	return a.totalHandler(c, db.CPUHoursSubtract)
}

func (a *App) ResetTotalHandler(c echo.Context) error {
	return a.totalHandler(c, db.CPUHoursReset)
}
