package main

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/resource-usage-api/amqp"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

var log = logging.Log

func getHandler(dbClient *sqlx.DB) amqp.HandlerFn {
	dedb := db.New(dbClient)

	return func(userID, externalID, state string) {
		event := db.CPUUsageEvent{
			CreatedBy: userID,
		}

		// Set up a context with a deadline of 2 minutes. This should prevent a backlog of go routines
		// from building up.
		ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(time.Minute*2))
		defer cancelFn()

		// Look up the analysis ID from the externalID.
		analysisID, err := dedb.GetAnalysisIDByExternalID(ctx, externalID)
		if err != nil {
			log.Error(err)
			return
		}

		// Get the start date of the analysis.
		analysis, err := dedb.Analysis(ctx, userID, analysisID)
		if err != nil {
			log.Error(err)
			return
		}

		if !analysis.StartDate.Valid {
			log.Errorf("analysis %s: start date was null", analysis.ID)
		}
		startDate := analysis.StartDate.Time

		// Get the current date. Can't really depend on the sent_on field.
		nowTime := time.Now()

		// Calculate the number of hours betwen the start date and the current date.
		hours := nowTime.Sub(startDate).Hours()

		// Get the number of millicores requested for the analysis.
		// TODO: figure out the right way to handle default values. Default to 1.0 for now.
		millicores := 1000.0

		// Multiply the number of hours by the number of millicores.
		// Divide the result by 1000 to get the number of CPU hours. 1000 millicores = 1 CPU core.
		cpuHours := (millicores * hours) / 1000.0

		// Add the event to the database.
		event.EffectiveDate = nowTime
		event.RecordDate = nowTime
		event.Value = int64(cpuHours)

		if err = dedb.AddCPUUsageEvent(ctx, &event); err != nil {
			log.Error(err)
		}
	}
}

func main() {
	var (
		err    error
		config *viper.Viper
		db     *sqlx.DB

		configPath = flag.String("config", "/etc/iplant/de/jobservices.yml", "Full path to the configuration file")
		listenPort = flag.Int("port", 60000, "The port the service listens on for requests")
		userSuffix = flag.String("user-suffix", "@iplantcollaborative.org", "The user suffix for all users in the DE installation")
		queue      = flag.String("queue", "resource-usage-api", "The AMQP queue name for this service")
		reconnect  = flag.Bool("reconnect", false, "Whether the AMQP client should reconnect on failure")
		logLevel   = flag.String("log-level", "warn", "One of trace, debug, info, warn, error, fatal, or panic.")
	)

	flag.Parse()
	logging.SetupLogging(*logLevel)

	log.Infof("config path is %s", *configPath)
	log.Infof("listen port is %d", listenPort)
	log.Infof("user suffix is %s", *userSuffix)

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

	amqpConfig := amqp.Configuration{
		URI:          amqpURI,
		Exchange:     amqpExchange,
		ExchangeType: amqpExchangeType,
		Reconnect:    *reconnect,
		Queue:        *queue,
	}

	amqpClient, err := amqp.New(&amqpConfig, getHandler(db))
	if err != nil {
		log.Fatal(err)
	}
	go amqpClient.Listen()
	defer amqpClient.Close()

	db = sqlx.MustConnect("postgres", dbURI)

	app := NewApp(db)
	log.Infof("listening on port %d", *listenPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", strconv.Itoa(*listenPort)), app.router))
}
