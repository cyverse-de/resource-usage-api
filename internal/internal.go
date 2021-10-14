package internal

import (
	"fmt"
	"strings"

	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "internal"})

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

func New(db *sqlx.DB, userSuffix string) *App {
	app := &App{
		database:   db,
		router:     echo.New(),
		userSuffix: userSuffix,
	}

	return app
}

func (a *App) Router() *echo.Echo {
	a.router.HTTPErrorHandler = logging.HTTPErrorHandler
	a.router.GET("/", a.GreetingHandler).Name = "greeting"

	cpuroute := a.router.Group("/:username/cpu")
	cpuroute.GET("/total", a.CurrentCPUHoursHandler)
	cpuroute.GET("/total/all", a.AllCPUHoursHandler)

	modifyroutes := cpuroute.Group("/update")
	modifyroutes.POST("/add/:value", a.AddToTotalHandler)
	modifyroutes.POST("/subtract/:value", a.SubtractFromTotalHandler)
	modifyroutes.POST("/reset/:value", a.ResetTotalHandler)

	admin := a.router.Group("/admin")
	cpuadmin := admin.Group("/cpu")
	cpuadmin.GET("/totals", a.AdminAllCurrentCPUHoursHandler)
	cpuadmin.GET("/totals/all", a.AdminAllCPUHoursTotalsHandler)

	return a.router
}
