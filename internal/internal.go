package internal

import (
	"net/http"

	"github.com/cyverse-de/resource-usage-api/amqp"
	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/config"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "internal"})

// App encapsulates the application logic.
type App struct {
	database        *sqlx.DB
	router          *echo.Echo
	config          *config.Config
	dataUsageClient *clients.DataUsageAPI
	amqpClient      *amqp.AMQP
	subscriptions   *clients.Subscriptions
}

// AppConfiguration contains the settings needed to configure the App.
type AppConfiguration struct {
	Config           *config.Config
	DataUsageBaseURL string
	AMQPClient       *amqp.AMQP
	Subscriptions    *clients.Subscriptions
}

// New creates a new app instance for provided configuration.
func New(db *sqlx.DB, appConfig *AppConfiguration) (*App, error) {
	// Create the client libraries for the downstream services.
	dataUsageClient, err := clients.DataUsageAPIClient(appConfig.DataUsageBaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create the data-usage-api client")
	}

	// Create the app instance.
	app := &App{
		database:        db,
		router:          echo.New(),
		config:          appConfig.Config,
		dataUsageClient: dataUsageClient,
		amqpClient:      appConfig.AMQPClient,
		subscriptions:   appConfig.Subscriptions,
	}

	return app, nil
}

func (a *App) HelloHandler(c echo.Context) error {
	return c.String(http.StatusOK, "Hello from resource-usage-api")
}

func (a *App) Router() *echo.Echo {
	a.router.HTTPErrorHandler = logging.HTTPErrorHandler
	a.router.GET("/", a.HelloHandler)

	summaryRoute := a.router.Group("/summary/:username")
	summaryRoute.GET("/", a.GetUserSummary)
	summaryRoute.GET("", a.GetUserSummary)

	return a.router
}
