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

	summaryRoute := a.router.Group("/summary/:username")
	summaryRoute.GET("/", a.GetUserSummary)
	summaryRoute.GET("", a.GetUserSummary)

	return a.router
}
