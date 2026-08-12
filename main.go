package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
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
	return func(ctx context.Context, externalID string, state messaging.JobState) error {
		msgLog := log.WithFields(logrus.Fields{"externalID": externalID}).WithContext(ctx)

		// TODO: should this happen for non-failed/succeeded messages?
		if state != messaging.FailedState && state != messaging.SucceededState {
			msgLog.Debugf("received status is %s, ignoring", state)
			return nil
		}

		msgLog.Debug("calculating CPU hours for analysis")

		// Deliveries are handled concurrently and db.Database carries the in-flight transaction on
		// itself, so each message gets its own rather than sharing one across goroutines.
		calculator := cpuhours.New(db.New(dbClient), subscriptions)

		if err := calculator.CalculateForAnalysis(ctx, externalID); err != nil {
			// Nothing about the analysis is going to change because the update was redelivered, so
			// these are acknowledged rather than retried forever.
			if errors.Is(err, sql.ErrNoRows) || errors.Is(err, cpuhours.ErrNoStartDate) {
				msgLog.WithError(err).Error("the analysis has no CPU hours to record, dropping the message")
				return nil
			}
			return err
		}
		msgLog.Debug("done calculating CPU hours for analysis")

		return nil
	}
}

func main() {
	var (
		err      error
		k        *koanf.Koanf
		dbconn   *sqlx.DB
		icatconn *sqlx.DB

		configPath = flag.String("config", cfg.DefaultConfigPath, "Full path to the configuration file")
		dotEnvPath = flag.String("dotenv-path", cfg.DefaultDotEnvPath, "Path to the dotenv file")
		envPrefix  = flag.String("env-prefix", cfg.DefaultEnvPrefix, "The prefix for environment variables")
		listenPort = flag.Int("port", 60000, "The port the service listens on for requests")
		queue      = flag.String("queue", serviceName, "The AMQP queue name for this service")
		// The data usage consumers are the ones the merged data-usage-api brought over, and that service
		// always reconnected. Without it the messaging client exits the process when the broker
		// restarts, taking the HTTP API down with it.
		reconnect         = flag.Bool("reconnect", true, "Whether the AMQP client should reconnect on failure")
		logLevel          = flag.String("log-level", "info", "One of trace, debug, info, warn, error, fatal, or panic.")
		subscriptionsBase = flag.String("subscriptions-base-uri", "http://subscriptions", "The base URL for contacting the subscriptions service")
		shutdownTimeout   = flag.Duration("shutdown-timeout", time.Minute, "How long to let in-flight work finish after a shutdown signal")
	)

	flag.Parse()

	// Established before anything else so that a signal arriving during startup is not missed.
	signals, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

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

	// The deferred closes below run in reverse order, so the broker connection is closed before the
	// pools its handlers are using.
	dbconn = sqlx.MustConnect("postgres", configuration.DBURI)
	log.Info("done connecting to the DE database")
	dbconn.SetMaxOpenConns(10)
	dbconn.SetConnMaxIdleTime(time.Minute)
	defer dbconn.Close() // nolint:errcheck

	icatconn = sqlx.MustConnect("postgres", configuration.ICATURI)
	log.Info("done connecting to the ICAT database")
	icatconn.SetMaxOpenConns(10)
	icatconn.SetConnMaxIdleTime(time.Minute)
	defer icatconn.Close() // nolint:errcheck

	subscriptionsClient, err := clients.SubscriptionsClient(*subscriptionsBase, configuration)
	if err != nil {
		log.Fatal(err)
	}

	batchQueue, individualQueue := configuration.DataUsageQueueNames()

	amqpConfig := amqp.Configuration{
		URI:          configuration.AMQPURI,
		Exchange:     configuration.AMQPExchangeName,
		ExchangeType: configuration.AMQPExchangeType,
		Reconnect:    *reconnect,

		Queue:         *queue,
		PrefetchCount: 10,

		BatchQueue:             batchQueue,
		IndividualQueue:        individualQueue,
		DataUsagePrefetchCount: 1,
	}

	log.Infof("AMQP exchange name: %s", amqpConfig.Exchange)
	log.Infof("AMQP exchange type: %s", amqpConfig.ExchangeType)
	log.Infof("AMQP reconnect: %v", amqpConfig.Reconnect)
	log.Infof("AMQP queue names: %s, %s, %s", amqpConfig.Queue, amqpConfig.BatchQueue, amqpConfig.IndividualQueue)
	log.Infof("AMQP prefetch amount %d", amqpConfig.PrefetchCount)

	amqpDeps := &amqp.Dependencies{
		DEDB:          dbconn,
		ICAT:          icatconn,
		Config:        configuration,
		Subscriptions: subscriptionsClient,
	}

	amqpClient, err := amqp.New(&amqpConfig, getHandler(dbconn, subscriptionsClient), amqpDeps)
	if err != nil {
		log.Fatal(err)
	}
	defer amqpClient.Close()

	log.Info("done connecting to the AMQP broker")

	app := internal.New(&internal.Dependencies{
		DEDB:          dbconn,
		ICAT:          icatconn,
		Config:        configuration,
		AMQPClient:    amqpClient,
		Subscriptions: subscriptionsClient,
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", strconv.Itoa(*listenPort)),
		Handler: app.Router(),
	}

	go func() {
		log.Infof("listening on port %d", *listenPort)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	<-signals.Done()
	log.Info("shutting down")

	// Stop accepting requests before closing the broker connection, so that a lookup already in flight
	// can still enqueue the refresh it wants.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancelShutdown()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.WithError(err).Error("the HTTP server did not shut down cleanly")
	}

	log.Info("shutdown complete")
}
