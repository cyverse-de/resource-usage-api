package main

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cyverse-de/messaging/v9"
	"github.com/cyverse-de/resource-usage-api/amqp"
	"github.com/cyverse-de/resource-usage-api/cpuhours"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/internal"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/knadh/koanf"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/cyverse-de/go-mod/cfg"
	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/go-mod/protobufjson"

	_ "expvar"

	_ "github.com/lib/pq"
)

const serviceName = "resource-usage-api"

var log = logging.Log.WithFields(logrus.Fields{"package": "main"})

func getHandler(dbClient *sqlx.DB, nc *nats.EncodedConn) amqp.HandlerFn {
	dedb := db.New(dbClient)
	cpuhours := cpuhours.New(dedb, nc)

	return func(context context.Context, externalID string, state messaging.JobState) {
		var err error

		log = log.WithFields(logrus.Fields{"externalID": externalID}).WithContext(context)

		// TODO: should this happen for non-failed/succeeded messages?
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

func main() {
	var (
		err    error
		config *koanf.Koanf
		dbconn *sqlx.DB

		configPath        = flag.String("config", cfg.DefaultConfigPath, "Full path to the configuration file")
		dotEnvPath        = flag.String("dotenv-path", cfg.DefaultDotEnvPath, "Path to the dotenv file")
		noCreds           = flag.Bool("no-creds", false, "Turn off NATS creds support")
		noTLS             = flag.Bool("no-tls", false, "Turn off TLS support in the NATS connection")
		tlsCert           = flag.String("tlscert", gotelnats.DefaultTLSCertPath, "Path to the NATS TLS cert file")
		tlsKey            = flag.String("tlskey", gotelnats.DefaultTLSKeyPath, "Path to the NATS TLS key file")
		caCert            = flag.String("tlsca", gotelnats.DefaultTLSCAPath, "Path to the NATS TLS CA file")
		credsPath         = flag.String("creds", gotelnats.DefaultCredsPath, "Path to the NATS creds file")
		envPrefix         = flag.String("env-prefix", cfg.DefaultEnvPrefix, "The prefix for environment variables")
		maxReconnects     = flag.Int("max-reconnects", gotelnats.DefaultMaxReconnects, "Maximum number of reconnection attempts to NATS")
		reconnectWait     = flag.Int("reconnect-wait", gotelnats.DefaultReconnectWait, "Seconds to wait between reconnection attempts to NATS")
		listenPort        = flag.Int("port", 60000, "The port the service listens on for requests")
		queue             = flag.String("queue", serviceName, "The AMQP queue name for this service")
		reconnect         = flag.Bool("reconnect", false, "Whether the AMQP client should reconnect on failure")
		logLevel          = flag.String("log-level", "info", "One of trace, debug, info, warn, error, fatal, or panic.")
		usageRoutingKey   = flag.String("usage-routing-key", "qms.usages", "The routing key to use when sending usage updates over AMQP")
		dataUsageBase     = flag.String("data-usage-base-url", "http://data-usage-api", "The base URL for contacting the data-usage-api service")
		subscriptionsBase = flag.String("subscriptions-base-uri", "http://subscriptions", "The base URL for contacting the subscriptions service")
	)

	flag.Parse()

	logging.SetupLogging(*logLevel)

	nats.RegisterEncoder("protojson", protobufjson.NewCodec(protobufjson.WithEmitUnpopulated()))

	log.Infof("config path is %s", *configPath)
	log.Infof("listen port is %d", listenPort)
	log.Infof("NATS TLS cert file is %s", *tlsCert)
	log.Infof("NATS TLS key file is %s", *tlsKey)
	log.Infof("NATS CA cert file is %s", *caCert)
	log.Infof("NATS creds file is %s", *credsPath)
	log.Infof("dotenv file is %s", *dotEnvPath)

	config, err = cfg.Init(&cfg.Settings{
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

	dbURI := config.String("db.uri")
	if dbURI == "" {
		log.Fatal("db.uri must be set in the configuration file")
	}

	amqpURI := config.String("amqp.uri")
	if amqpURI == "" {
		log.Fatal("amqp.uri must be set in the configuration file")
	}

	amqpExchange := config.String("amqp.exchange.name")
	if amqpExchange == "" {
		log.Fatal("amqp.exchange.name must be set in the configuration file")
	}

	amqpExchangeType := config.String("amqp.exchange.type")
	if amqpExchangeType == "" {
		log.Fatal("amqp.exchange.type must be set in the configuration file")
	}

	userSuffix := config.String("users.domain")
	if userSuffix == "" {
		log.Fatal("users.domain must be set in the configuration file")
	}

	qmsEnabled := config.Bool("qms.enabled")

	natsCluster := config.String("nats.cluster")
	if natsCluster == "" {
		log.Fatalf("The %sNATS_CLUSTER environment variable or nats.cluster configuration value must be set", *envPrefix)
	}

	dbconn = sqlx.MustConnect("postgres", dbURI)
	log.Info("done connecting to the database")
	dbconn.SetMaxOpenConns(10)
	dbconn.SetConnMaxIdleTime(time.Minute)

	options := []nats.Option{
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(*maxReconnects),
		nats.ReconnectWait(time.Duration(*reconnectWait) * time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				log.Errorf("disconnected from nats: %s", err.Error())
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Infof("reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Errorf("connection closed: %s", nc.LastError().Error())
		}),
	}

	if !*noTLS {
		options = append(options, nats.RootCAs(*caCert))
		options = append(options, nats.ClientCert(*tlsCert, *tlsKey))
	}

	if !*noCreds {
		options = append(options, nats.UserCredentials(*credsPath))
	}

	nc, err := nats.Connect(
		natsCluster,
		options...,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("configured servers: %s", strings.Join(nc.Servers(), " "))
	log.Infof("connected to NATS host: %s", nc.ConnectedServerName())

	natsClient, err := nats.NewEncodedConn(nc, "protojson")
	if err != nil {
		log.Fatal(err)
	}

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

	amqpClient, err := amqp.New(&amqpConfig, getHandler(dbconn, natsClient))
	if err != nil {
		log.Fatal(err)
	}
	defer amqpClient.Close()
	log.Debug("after close")

	log.Info("done connecting to the AMQP broker")

	appConfig := &internal.AppConfiguration{
		UserSuffix:           userSuffix,
		DataUsageBaseURL:     *dataUsageBase,
		AMQPClient:           amqpClient,
		NATSClient:           natsClient,
		AMQPUsageRoutingKey:  *usageRoutingKey,
		QMSEnabled:           qmsEnabled,
		SubscriptionsBaseURI: *subscriptionsBase,
	}

	app, err := internal.New(dbconn, appConfig)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("listening on port %d", *listenPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", strconv.Itoa(*listenPort)), app.Router()))
}
