package main

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"context"

	"github.com/cyverse-de/messaging/v9"
	"github.com/cyverse-de/resource-usage-api/amqp"
	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/config"
	"github.com/cyverse-de/resource-usage-api/cpuhours"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/internal"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/knadh/koanf"
	"github.com/sirupsen/logrus"

	"github.com/cyverse-de/go-mod/cfg"

	_ "github.com/lib/pq"
)

const serviceName = "resource-usage-api"

var log = logging.Log.WithFields(logrus.Fields{"package": "main"})

func getHandler(dbClient *sqlx.DB, subscriptions *clients.Subscriptions) amqp.HandlerFn {
	dedb := db.New(dbClient)
	cpuhours := cpuhours.New(dedb, subscriptions)

	return func(ctx context.Context, externalID string, state messaging.JobState) {
		var err error

		msgLog := log.WithFields(logrus.Fields{"externalID": externalID}).WithContext(ctx)

		// TODO: should this happen for non-failed/succeeded messages?
		if state == messaging.FailedState || state == messaging.SucceededState {
			msgLog.Debug("calculating CPU hours for analysis")
			if err = cpuhours.CalculateForAnalysis(ctx, externalID); err != nil {
				msgLog.Error(err)
			}
			msgLog.Debug("done calculating CPU hours for analysis")
		} else {
			msgLog.Debugf("received status is %s, ignoring", state)
		}
	}
}

func main() {
	var (
		err    error
		k      *koanf.Koanf
		dbconn *sqlx.DB

		configPath        = flag.String("config", cfg.DefaultConfigPath, "Full path to the configuration file")
		dotEnvPath        = flag.String("dotenv-path", cfg.DefaultDotEnvPath, "Path to the dotenv file")
		envPrefix         = flag.String("env-prefix", cfg.DefaultEnvPrefix, "The prefix for environment variables")
		listenPort        = flag.Int("port", 60000, "The port the service listens on for requests")
		queue             = flag.String("queue", serviceName, "The AMQP queue name for this service")
		reconnect         = flag.Bool("reconnect", false, "Whether the AMQP client should reconnect on failure")
		logLevel          = flag.String("log-level", "info", "One of trace, debug, info, warn, error, fatal, or panic.")
		dataUsageBase     = flag.String("data-usage-base-url", "http://data-usage-api", "The base URL for contacting the data-usage-api service")
		subscriptionsBase = flag.String("subscriptions-base-uri", "http://subscriptions", "The base URL for contacting the subscriptions service")
	)

	flag.Parse()

	logging.SetupLogging(*logLevel)

	log.Infof("config path is %s", *configPath)
	log.Infof("listen port is %d", *listenPort)
	log.Infof("dotenv file is %s", *dotEnvPath)
	log.Infof("subscriptions base URI is %s", *subscriptionsBase)

	k, err = cfg.Init(&cfg.Settings{
		EnvPrefix:   *envPrefix,
		ConfigPath:  *configPath,
		DotEnvPath:  *dotEnvPath,
		StrictMerge: false,
		FileType:    cfg.YAML,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("done reading configuration from %s", *configPath)

	configuration, err := config.New(k)
	if err != nil {
		log.Fatal(err)
	}

	dbconn = sqlx.MustConnect("postgres", configuration.DBURI)
	log.Info("done connecting to the database")
	dbconn.SetMaxOpenConns(10)
	dbconn.SetConnMaxIdleTime(time.Minute)

	subscriptionsClient, err := clients.SubscriptionsClient(*subscriptionsBase, configuration)
	if err != nil {
		log.Fatal(err)
	}

	amqpConfig := amqp.Configuration{
		URI:           configuration.AMQPURI,
		Exchange:      configuration.AMQPExchangeName,
		ExchangeType:  configuration.AMQPExchangeType,
		Reconnect:     *reconnect,
		Queue:         *queue,
		PrefetchCount: 10,
	}

	log.Infof("AMQP exchange name: %s", amqpConfig.Exchange)
	log.Infof("AMQP exchange type: %s", amqpConfig.ExchangeType)
	log.Infof("AMQP reconnect: %v", amqpConfig.Reconnect)
	log.Infof("AMQP queue name: %s", amqpConfig.Queue)
	log.Infof("AMQP prefetch amount %d", amqpConfig.PrefetchCount)

	amqpClient, err := amqp.New(&amqpConfig, getHandler(dbconn, subscriptionsClient))
	if err != nil {
		log.Fatal(err)
	}
	defer amqpClient.Close()
	log.Debug("after close")

	log.Info("done connecting to the AMQP broker")

	appConfig := &internal.AppConfiguration{
		Config:           configuration,
		DataUsageBaseURL: *dataUsageBase,
		AMQPClient:       amqpClient,
		Subscriptions:    subscriptionsClient,
	}

	app, err := internal.New(dbconn, appConfig)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("listening on port %d", *listenPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", strconv.Itoa(*listenPort)), app.Router()))
}
