package internal

import (
	"fmt"
	"strings"

	"github.com/cyverse-de/resource-usage-api/amqp"
	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"go.opentelemetry.io/contrib/instrumentation/github.com/labstack/echo/otelecho"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "internal"})

// App encapsulates the application logic.
type App struct {
	database            *sqlx.DB
	router              *echo.Echo
	userSuffix          string
	dataUsageClient     *clients.DataUsageAPI
	amqpClient          *amqp.AMQP
	natsClient          *nats.EncodedConn
	amqpUsageRoutingKey string
	qmsClient           *clients.QMSAPI
	qmsEnabled          bool
}

// AppConfiguration contains the settings needed to configure the App.
type AppConfiguration struct {
	UserSuffix               string
	DataUsageBaseURL         string
	CurrentDataUsageEndpoint string
	AMQPClient               *amqp.AMQP
	NATSClient               *nats.EncodedConn
	AMQPUsageRoutingKey      string
	QMSEnabled               bool
	QMSBaseURL               string
}

func (a *App) FixUsername(username string) string {
	if !strings.HasSuffix(username, a.userSuffix) {
		return fmt.Sprintf("%s@%s", username, a.userSuffix)
	}
	return username
}

// New creates a new app instance for provided configuration.
func New(db *sqlx.DB, config *AppConfiguration) (*App, error) {

	// Create the client libraries for the downstream services.
	dataUsageClient, err := clients.DataUsageAPIClient(config.DataUsageBaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create the data-usage-api client")
	}
	qmsClient, err := clients.QMSAPIClient(config.QMSBaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create the QMS client")
	}

	// Create the app instance.
	app := &App{
		database:            db,
		router:              echo.New(),
		userSuffix:          config.UserSuffix,
		dataUsageClient:     dataUsageClient,
		amqpClient:          config.AMQPClient,
		natsClient:          config.NATSClient,
		amqpUsageRoutingKey: config.AMQPUsageRoutingKey,
		qmsClient:           qmsClient,
		qmsEnabled:          config.QMSEnabled,
	}

	return app, nil
}

func (a *App) Router() *echo.Echo {
	a.router.Use(otelecho.Middleware("resource-usage-api"))

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
	cpuadmin.POST("/recalculate/for/:username", a.AdminRecalculateCPUHoursTotalHandler)
	cpuadmin.GET("/recalculate/can", a.AdminUsersWithCalculableAnalysesHandler)
	cpuadmin.POST("/resend/total/for/:username", a.AdminResendTotalToQMSHandler)

	events := cpuadmin.Group("/events")
	events.GET("", a.AdminListEvents)
	events.GET("/", a.AdminListEvents)
	events.GET("/user/:username", a.AdminListAllUserEventsHandler)
	events.GET("/:id", a.AdminGetEventHandler)

	return a.router
}
