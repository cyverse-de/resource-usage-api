package internal

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/cyverse-de/resource-usage-api/amqp"
	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "internal"})

// App encapsulates the application logic.
type App struct {
	database             *sqlx.DB
	router               *echo.Echo
	userSuffix           string
	dataUsageClient      *clients.DataUsageAPI
	amqpClient           *amqp.AMQP
	natsClient           *nats.EncodedConn
	amqpUsageRoutingKey  string
	qmsEnabled           bool
	subscriptionsBaseURI string
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
	SubscriptionsBaseURI     string
}

func (a *App) FixUsername(username string) string {
	if !strings.HasSuffix(a.userSuffix, username) {
		// Only add a @ if the configured user suffix doesn't already
		// start with one.
		if strings.HasPrefix(a.userSuffix, "@") {
			return fmt.Sprintf("%s%s", username, a.userSuffix)
		}
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

	// Create the app instance.
	app := &App{
		database:             db,
		router:               echo.New(),
		userSuffix:           config.UserSuffix,
		dataUsageClient:      dataUsageClient,
		amqpClient:           config.AMQPClient,
		natsClient:           config.NATSClient,
		amqpUsageRoutingKey:  config.AMQPUsageRoutingKey,
		qmsEnabled:           config.QMSEnabled,
		subscriptionsBaseURI: config.SubscriptionsBaseURI,
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
