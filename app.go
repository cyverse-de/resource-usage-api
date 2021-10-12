package main

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
)

// App encapsulates the application logic.
type App struct {
	database   *sqlx.DB
	router     *echo.Echo
	userSuffix string
}

/**
	Per-User Endpoints
**/

func (a *App) FixUsername(username string) string {
	if !strings.HasSuffix(username, a.userSuffix) {
		return fmt.Sprintf("%s@%s", username, a.userSuffix)
	}
	return username
}

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

// EnforceExpirations will clean up the database of expired workers, work claims,
// and work seekers.
func (a *App) EnforceExpirations(context context.Context) error {
	tx, err := a.database.Beginx()
	if err != nil {
		return err
	}

	d := db.New(tx)

	expiredWS, err := d.PurgeExpiredWorkSeekers(context)
	if err != nil {
		return err
	}
	log.Infof("%d expired work seekers were cleaned up", expiredWS)

	expiredW, err := d.PurgeExpiredWorkers(context)
	if err != nil {
		return err
	}
	log.Infof("%d expired workers were cleaned up", expiredW)

	inactiveClaims, err := d.ResetWorkClaimsForInactiveWorkers(context)
	if err != nil {
		return err
	}
	log.Infof("%d claims assigned to inactive workers were cleaned up", inactiveClaims)

	expiredWC, err := d.PurgeExpiredWorkClaims(context)
	if err != nil {
		return err
	}
	log.Infof("%d expired work claims were cleaned up", expiredWC)

	return tx.Commit()
}

// GreetingHandler handles requests that simply need to know if the service is running.
func (a *App) GreetingHandler(context echo.Context) error {
	return context.String(http.StatusOK, "Hello from resource-usage-api.")
}

// CurrentCPUHours looks up the total CPU hours for the current recording period.
func (a *App) CurrentCPUHoursHandler(c echo.Context) error {
	context := c.Request().Context()

	user := c.Param("username")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "user was not set")
	}
	user = a.FixUsername(user)

	log.Debugf("username %s", user)

	d := db.New(a.database)
	results, err := d.CurrentCPUHoursForUser(context, user)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, results)
}

// AllCPUHoursHandler returns all of the total CPU hours totals, regardless of recording period.
func (a *App) AllCPUHoursHandler(c echo.Context) error {
	context := c.Request().Context()

	user := c.Param("username")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "user was not set")
	}
	user = a.FixUsername(user)

	d := db.New(a.database)
	results, err := d.AllCPUHoursForUser(context, user)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, results)
}

/**
	Admin Endpoints
**/

// AdminAllCurrentCPUHoursTotalsHandler looks up all of the total CPU hours totals for all users.
func (a *App) AdminAllCurrentCPUHoursHandler(c echo.Context) error {
	context := c.Request().Context()

	d := db.New(a.database)
	results, err := d.AdminAllCurrentCPUHours(context)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, results)
}

// AdminAllCPUHoursTotalsHandler returns all of the total CPU hours totals for all recording periods, regardless of user.
func (a *App) AdminAllCPUHoursTotalsHandler(c echo.Context) error {
	context := c.Request().Context()

	d := db.New(a.database)
	results, err := d.AdminAllCPUHours(context)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, results)
}

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

func NewApp(db *sqlx.DB, userSuffix string) *App {
	app := &App{
		database:   db,
		router:     echo.New(),
		userSuffix: userSuffix,
	}

	app.router.HTTPErrorHandler = logging.HTTPErrorHandler
	app.router.GET("/", app.GreetingHandler).Name = "greeting"

	cpuroute := app.router.Group("/:username")
	cpuroute.GET("/total", app.CurrentCPUHoursHandler)
	cpuroute.GET("/total/all", app.AdminAllCPUHoursTotalsHandler)

	modifyroutes := cpuroute.Group("/update")
	modifyroutes.POST("/add/:value", app.AddToTotalHandler)
	modifyroutes.POST("/subtract/:value", app.SubtractFromTotalHandler)
	modifyroutes.POST("/reset/:value", app.ResetTotalHandler)

	admin := app.router.Group("/admin")
	cpuadmin := admin.Group("/cpu")
	cpuadmin.GET("/totals", app.AdminAllCurrentCPUHoursHandler)
	cpuadmin.GET("/totals/all", app.AdminAllCPUHoursTotalsHandler)

	return app
}
