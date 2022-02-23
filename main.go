package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/resource-usage-api/amqp"
	"github.com/cyverse-de/resource-usage-api/cpuhours"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/internal"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/cyverse-de/resource-usage-api/worker"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
	"gopkg.in/cyverse-de/messaging.v6"

	_ "github.com/lib/pq"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "main"})

func getHandler(dbClient *sqlx.DB) amqp.HandlerFn {
	dedb := db.New(dbClient)
	cpuhours := cpuhours.New(dedb)

	return func(externalID string, state messaging.JobState) {
		var err error
		context := context.Background()

		log = log.WithFields(logrus.Fields{"externalID": externalID})

		if state == messaging.FailedState || state == messaging.SucceededState {
			log.Debug("calculating CPU hours for analysis")
			if err = cpuhours.CalculateForAnalysis(context, externalID); err != nil {
				log.Error(err)
			}
			log.Debug("done calculating CPU hours for analysis")
		} else {
			log.Debugf("received status is %s, ignoring", state)
		}
	}
}

const CPUHoursAttr = "cpu.hours"
const CPUHoursUnit = "cpu hours"

func sendMsgCB(dbClient *sqlx.DB, a *amqp.AMQP, routingKey string) worker.MessageSender {
	dedb := db.New(dbClient)
	return func(workItem *db.CPUUsageWorkItem) {
		var err error

		userID := workItem.CreatedBy

		username, err := dedb.Username(context.Background(), userID)
		if err != nil {
			log.Error(err)
			return
		}

		log = log.WithFields(logrus.Fields{"context": "send message callback", "user": username})

		log.Debug("getting current CPU hours")
		currentCPUHours, err := dedb.CurrentCPUHoursForUser(context.Background(), username)
		if err != nil {
			log.Error(err)
			return
		}
		log.Debugf("current CPU hours: %s", currentCPUHours.Total.String())

		update := &worker.UsageUpdate{
			Attribute: CPUHoursAttr,
			Value:     currentCPUHours.Total.String(),
			Unit:      CPUHoursUnit,
			Username:  username,
			UserID:    userID,
		}

		log.Debug("marshalling update")
		marshalled, err := json.Marshal(update)
		if err != nil {
			log.Error(err)
			return
		}
		log.Debug("done marshalling update")

		log.Debug("sending update")
		if err = a.Send(routingKey, marshalled); err != nil {
			log.Error(err)
			return
		}
		log.Debug("done sending update")
	}

}

func main() {
	var (
		err    error
		config *viper.Viper
		dbconn *sqlx.DB

		configPath               = flag.String("config", "/etc/iplant/de/jobservices.yml", "Full path to the configuration file")
		listenPort               = flag.Int("port", 60000, "The port the service listens on for requests")
		queue                    = flag.String("queue", "resource-usage-api", "The AMQP queue name for this service")
		reconnect                = flag.Bool("reconnect", false, "Whether the AMQP client should reconnect on failure")
		logLevel                 = flag.String("log-level", "info", "One of trace, debug, info, warn, error, fatal, or panic.")
		workerLifetimeFlag       = flag.String("worker-lifetime", "1h", "The lifetime of a worker. Must parse as a time.Duration.")
		claimLifetimeFlag        = flag.String("claim-lifetime", "2m", "The lifetime of a work claim. Must parse as a time.Duration.")
		seekingLifetimeFlag      = flag.String("seeking-lifetime", "2m", "The amount of time a worker may spend looking for a work item to process. Must parse as a time.Duration.")
		refreshIntervalFlag      = flag.String("refresh-interval", "5m", "The time between worker re-registration/refreshes. Must parse as a time.Duration.")
		purgeWorkersIntervalFlag = flag.String("purge-workers-interval", "6m", "The time between attempts to clean out expired workers. Must parse as a time.Duration.")
		purgeSeekersIntervalFlag = flag.String("purge-seeker-interval", "5m", "The time between attempts to purge workers seeking work items for too long. Must parse as a time.Duration.")
		purgeClaimsIntervalFlag  = flag.String("purge-claims-interval", "6m", "The time between attemtps to purge expired work claims. Must parse as a time.Duration.")
		newUserTotalIntervalFlag = flag.String("new-user-total-interval", "365", "The number of days that user gets for new CPU hours tracking. Must parse as an integer.")
		usageRoutingKey          = flag.String("usage-routing-key", "qms.usages", "The routing key to use when sending usage updates over AMQP")
		dataUsageBase            = flag.String("data-usage-base-url", "http://data-usage-api", "The base URL for contacting the data-usage-api service")
		dataUsageCurrentSuffix   = flag.String("data-usage-current-suffix", "/data/current", "The data-usage-api endpoints start with /:username, so this is the rest of the path after that.")
	)

	flag.Parse()
	logging.SetupLogging(*logLevel)

	log.Infof("config path is %s", *configPath)
	log.Infof("listen port is %d", listenPort)

	config, err = configurate.Init(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("done reading configuration from %s", *configPath)

	dbURI := config.GetString("db.uri")
	if dbURI == "" {
		log.Fatal("db.uri must be set in the configuration file")
	}

	amqpURI := config.GetString("amqp.uri")
	if amqpURI == "" {
		log.Fatal("amqp.uri must be set in the configuration file")
	}

	amqpExchange := config.GetString("amqp.exchange.name")
	if amqpExchange == "" {
		log.Fatal("amqp.exchange.name must be set in the configuration file")
	}

	amqpExchangeType := config.GetString("amqp.exchange.type")
	if amqpExchangeType == "" {
		log.Fatal("amqp.exchange.type must be set in the configuration file")
	}

	userSuffix := config.GetString("users.domain")
	if userSuffix == "" {
		log.Fatal("users.domain must be set in the configuration file")
	}

	workerLifetime, err := time.ParseDuration(*workerLifetimeFlag)
	if err != nil {
		log.Fatal(err)
	}

	refreshInterval, err := time.ParseDuration(*refreshIntervalFlag)
	if err != nil {
		log.Fatal(err)
	}

	purgeWorkersInterval, err := time.ParseDuration(*purgeWorkersIntervalFlag)
	if err != nil {
		log.Fatal(err)
	}

	purgeSeekersInterval, err := time.ParseDuration(*purgeSeekersIntervalFlag)
	if err != nil {
		log.Fatal(err)
	}

	purgeClaimsInterval, err := time.ParseDuration(*purgeClaimsIntervalFlag)
	if err != nil {
		log.Fatal(err)
	}

	claimLifetime, err := time.ParseDuration(*claimLifetimeFlag)
	if err != nil {
		log.Fatal(err)
	}

	seekingLifetime, err := time.ParseDuration(*seekingLifetimeFlag)
	if err != nil {
		log.Fatal(err)
	}

	newUserTotalInterval, err := strconv.ParseInt(*newUserTotalIntervalFlag, 10, 64)
	if err != nil {
		log.Fatal(err)
	}

	dbconn = sqlx.MustConnect("postgres", dbURI)
	log.Info("done connecting to the database")

	amqpConfig := amqp.Configuration{
		URI:           amqpURI,
		Exchange:      amqpExchange,
		ExchangeType:  amqpExchangeType,
		Reconnect:     *reconnect,
		Queue:         *queue,
		PrefetchCount: 0,
	}

	log.Infof("AMQP exchange name: %s", amqpConfig.Exchange)
	log.Infof("AMQP exchange type: %s", amqpConfig.ExchangeType)
	log.Infof("AMQP reconnect: %v", amqpConfig.Reconnect)
	log.Infof("AMQP queue name: %s", amqpConfig.Queue)
	log.Infof("AMQP prefetch amount %d", amqpConfig.PrefetchCount)

	amqpClient, err := amqp.New(&amqpConfig, getHandler(dbconn))
	if err != nil {
		log.Fatal(err)
	}
	defer amqpClient.Close()
	log.Debug("after close")

	log.Info("done connecting to the AMQP broker")

	appConfig := &internal.AppConfiguration{
		UserSuffix:               userSuffix,
		DataUsageBase:            *dataUsageBase,
		CurrentDataUsageEndpoint: *dataUsageCurrentSuffix,
	}

	app := internal.New(dbconn, appConfig)

	workerConfig := worker.Config{
		Name:                    strings.ReplaceAll(uuid.New().String(), "-", ""),
		ExpirationInterval:      workerLifetime,
		RefreshInterval:         refreshInterval,
		WorkerPurgeInterval:     purgeWorkersInterval,
		WorkSeekerPurgeInterval: purgeSeekersInterval,
		WorkClaimPurgeInterval:  purgeClaimsInterval,
		ClaimLifetime:           claimLifetime,
		WorkSeekingLifetime:     seekingLifetime,
		NewUserTotalInterval:    newUserTotalInterval,
		MessageSender:           sendMsgCB(dbconn, amqpClient, *usageRoutingKey),
	}

	log.Infof("worker name is %s", workerConfig.Name)

	w, err := worker.New(context.Background(), &workerConfig, dbconn)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("worker ID is %s", w.ID)

	go w.Start(context.Background())

	log.Infof("listening on port %d", *listenPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", strconv.Itoa(*listenPort)), app.Router()))
}
