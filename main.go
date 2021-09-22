package main

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/resource-usage-api/amqp"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
)

var log = logging.Log

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

	amqpClient, err := amqp.New(&amqpConfig, func(externalID, state string) {
		log.Infof("external id: %s, state: %s", externalID, state)
	})
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
