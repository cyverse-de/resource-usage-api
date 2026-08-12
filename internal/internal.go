package internal

import (
	"net/http"

	"github.com/cyverse-de/resource-usage-api/amqp"
	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/config"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "internal"})

// dataSizeResource is the QMS resource type used for data store usage.
const dataSizeResource = "data.size"

// App encapsulates the application logic.
type App struct {
	database      *sqlx.DB
	icat          *sqlx.DB
	router        *echo.Echo
	config        *config.Config
	amqpClient    *amqp.AMQP
	subscriptions *clients.Subscriptions
	dataUsage     *db.DataUsage
}

// Dependencies are the collaborators the App needs.
type Dependencies struct {
	DEDB          *sqlx.DB
	ICAT          *sqlx.DB
	Config        *config.Config
	AMQPClient    *amqp.AMQP
	Subscriptions *clients.Subscriptions
}

// New creates a new app instance for the provided dependencies.
func New(deps *Dependencies) *App {
	return &App{
		database:      deps.DEDB,
		icat:          deps.ICAT,
		router:        echo.New(),
		config:        deps.Config,
		amqpClient:    deps.AMQPClient,
		subscriptions: deps.Subscriptions,
		dataUsage:     db.NewDataUsage(deps.DEDB, deps.Subscriptions, deps.AMQPClient, deps.Config),
	}
}

func (a *App) HelloHandler(c echo.Context) error {
	return c.String(http.StatusOK, "Hello from resource-usage-api")
}

// Router registers the service's routes.
//
// The data usage routes take the username as the first path segment, which echo resolves after the
// static segments it shares the root with. Adding middleware to that group would register a
// catch-all under it and change how the other routes match, so leave it bare.
func (a *App) Router() *echo.Echo {
	a.router.HTTPErrorHandler = logging.HTTPErrorHandler
	a.router.GET("/", a.HelloHandler)

	summaryRoute := a.router.Group("/summary/:username")
	summaryRoute.GET("/", a.GetUserSummary)
	summaryRoute.GET("", a.GetUserSummary)

	userdata := a.router.Group("/:username/data")
	userdata.GET("/current", a.UserCurrentUsageHandler)
	userdata.POST("/update", a.UpdateUserCurrentUsageHandler)
	userdata.GET("/overage", a.UserDataOverageHandler)

	return a.router
}
