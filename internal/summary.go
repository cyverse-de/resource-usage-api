package internal

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/cyverse-de/resource-usage-api/internal/summarizer"
	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
)

var client = http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}

const otelName = "github.com/cyverse-de/resource-usage-api/internal"

// GetUserSummary is an echo request handler for requests to get a user's
// resource usage and current plan (if QMS is enabled).
func (a *App) GetUserSummary(c echo.Context) error {
	context := c.Request().Context()
	user := c.Param("username")
	log := log.WithFields(logrus.Fields{"context": "get user summary", "user": user}).WithContext(context)

	// Create the summarizer instance.
	var summarizerInstance summarizer.Summarizer
	if a.qmsEnabled {
		summarizerInstance = &summarizer.QMSSummarizer{
			Context:   c.Request().Context(),
			Log:       log,
			User:      a.FixUsername(user),
			OTelName:  otelName,
			Database:  a.database,
			QMSClient: a.qmsClient,
		}
	} else {
		summarizerInstance = &summarizer.DefaultSummarizer{
			Context:         c.Request().Context(),
			Log:             log,
			User:            a.FixUsername(user),
			OTelName:        otelName,
			Database:        a.database,
			DataUsageClient: a.dataUsageClient,
		}
	}

	// Obtain the summary and send it to the caller.
	summary := summarizerInstance.LoadSummary()
	return c.JSON(http.StatusOK, &summary)
}
