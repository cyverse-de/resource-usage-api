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
	database         *sqlx.DB
	router           *echo.Echo
	userSuffix       string
	dataUsageBase    string
	dataUsageCurrent string
}

// AppConfiguration contains the settings needed to configure the App.
type AppConfiguration struct {
	UserSuffix               string
	DataUsageBase            string
	CurrentDataUsageEndpoint string
}

func (a *App) FixUsername(username string) string {
	if !strings.HasSuffix(username, a.userSuffix) {
		return fmt.Sprintf("%s@%s", username, a.userSuffix)
	}
	return username
}

func New(db *sqlx.DB, config *AppConfiguration) *App {
	app := &App{
		database:         db,
		router:           echo.New(),
		userSuffix:       config.UserSuffix,
		dataUsageBase:    config.DataUsageBase,
		dataUsageCurrent: config.CurrentDataUsageEndpoint,
	}

	return app
}

func (a *App) Router() *echo.Echo {
	a.router.HTTPErrorHandler = logging.HTTPErrorHandler
	a.router.GET("/", a.GreetingHandler).Name = "greeting"

	summaryRoute := a.router.Group("/summary/:username")
	summaryRoute.GET("/", a.GetUserSummary)
	summaryRoute.GET("", a.GetUserSummary)

	cpuroute := a.router.Group("/:username/cpu")
	cpuroute.GET("/total", a.CurrentCPUHoursHandler)
	cpuroute.GET("/total/all", a.AllCPUHoursHandler)

	modifyroutes := cpuroute.Group("/update")
	modifyroutes.POST("/add/:value", a.AddToTotalHandler)
	modifyroutes.POST("/subtract/:value", a.SubtractFromTotalHandler)
	modifyroutes.POST("/reset/:value", a.ResetTotalHandler)

	admin := a.router.Group("/admin")

	workers := admin.Group("/workers")
	workers.GET("", a.AdminListWorkersHandler)
	workers.GET("/", a.AdminListWorkersHandler)
	workers.GET("/:id", a.AdminGetWorkerHandler)
	workers.DELETE("/:id", a.AdminDeleteWorkerHandler)

	cpuadmin := admin.Group("/cpu")
	cpuadmin.GET("/totals", a.AdminAllCurrentCPUHoursHandler)
	cpuadmin.GET("/totals/all", a.AdminAllCPUHoursTotalsHandler)

	events := cpuadmin.Group("/events")
	events.GET("", a.AdminListEvents)
	events.GET("/", a.AdminListEvents)
	events.GET("/user/:username", a.AdminListAllUserEventsHandler)
	events.GET("/:id", a.AdminGetEventHandler)

	return a.router
}
