package logging

import (
	"log"

	"github.com/sirupsen/logrus"
)

var Log = logrus.WithFields(logrus.Fields{
	"service": "resource-usage-api",
	"art-id":  "resource-usage-api",
	"group":   "org.cyverse",
})

func SetupLogging(configuredLevel string) {
	var level logrus.Level

	switch configuredLevel {
	case "trace":
		level = logrus.TraceLevel
	case "debug":
		level = logrus.DebugLevel
	case "info":
		level = logrus.InfoLevel
	case "warn":
		level = logrus.WarnLevel
	case "error":
		level = logrus.ErrorLevel
	case "fatal":
		level = logrus.FatalLevel
	case "panic":
		level = logrus.PanicLevel
	default:
		log.Fatal("incorrect log level")
	}

	Log.Logger.SetLevel(level)
}
