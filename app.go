package main

import (
	"net/http"

	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
)

// App encapsulates the application logic.
type App struct {
	db     *sqlx.DB
	router *echo.Echo
}

/**
	Per-User Endpoints
**/

// GreetingHandler handles requests that simply need to know if the service is running.
func (a *App) GreetingHandler(context echo.Context) error {
	return context.String(http.StatusOK, "Hello from resource-usage-api.")
}

// CurrentCPUHours looks up the total CPU hours for the current recording period.
func (a *App) CurrentCPUHoursHandler(context echo.Context) error {
	return nil
}

// AllCPUHoursHandler returns all of the total CPU hours totals, regardless of recording period.
func (a *App) AllCPUHoursHandler(context echo.Context) error {
	return nil
}

/**
	Admin Endpoints
**/

// AdminAllCurrentCPUHoursTotalsHandler looks up all of the total CPU hours totals for all users.
func (a *App) AdminAllCurrentCPUHoursHandler(context echo.Context) error {
	return nil
}

// AdminAllCPUHoursTotalsHandler returns all of the total CPU hours totals for all recording periods, regardless of user.
func (a *App) AdminAllCPUHoursTotalsHandler(context echo.Context) error {
	return nil
}

func NewApp(db *sqlx.DB) *App {
	app := &App{
		db:     db,
		router: echo.New(),
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
