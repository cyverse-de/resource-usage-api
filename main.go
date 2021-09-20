package main

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"

	"github.com/cyverse-de/configurate"
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
	db = sqlx.MustConnect("postgres", dbURI)

	app := NewApp(db)
	log.Infof("listening on port %d", *listenPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", strconv.Itoa(*listenPort)), app.router))
}
