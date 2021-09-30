package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"

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
		return fmt.Sprintf("%s%s", username, a.userSuffix)
	}
	return username
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

func NewApp(db *sqlx.DB) *App {
	app := &App{
		database: db,
		router:   echo.New(),
	}

	app.router.HTTPErrorHandler = logging.HTTPErrorHandler
	app.router.GET("/", app.GreetingHandler).Name = "greeting"

	cpuroute := app.router.Group("/cpu")
	cpuroute.GET("/total", app.CurrentCPUHoursHandler)
	cpuroute.GET("/total/all", app.AdminAllCPUHoursTotalsHandler)

	admin := app.router.Group("/admin")
	cpuadmin := admin.Group("/cpu")
	cpuadmin.GET("/totals", app.AdminAllCurrentCPUHoursHandler)
	cpuadmin.GET("/totals/all", app.AdminAllCPUHoursTotalsHandler)

	return app
}

func (a *App) CPUHoursAdd(context context.Context, workItem *db.CPUUsageWorkItem) error {
	// Open a transaction.
	// Get the current total.
	// Add the new value from the event to the total.
	// Set the new total.
	// Mark the work item as processed.
	// Commit the transaction
	return nil
}

func (a *App) CPUHoursSubtract(context context.Context, workItem *db.CPUUsageEvent) error {
	// Open a transaction.
	// Get the current total.
	// Subtract the new value in the work item from the total.
	// Set the new total
	// Mark the work item as processed.
	// Commit the transaction.
	return nil
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
