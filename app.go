package main

import (
	"fmt"
	"strings"

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

func (a *App) FixUsername(username string) string {
	if !strings.HasSuffix(username, a.userSuffix) {
		return fmt.Sprintf("%s@%s", username, a.userSuffix)
	}
	return username
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
	cpuroute.GET("/total/all", app.AllCPUHoursHandler)

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
