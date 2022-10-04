package summarizer

import (
	"context"
	"database/sql"
	"net/http"

	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
)

type DefaultSummarizer struct {
	Context         context.Context
	Log             *logrus.Entry
	User            string
	OTelName        string
	Database        *sqlx.DB
	DataUsageClient *clients.DataUsageAPI
}

// loadCPUUsage loads the user's CPU usage information from the DE database.
func (d *DefaultSummarizer) loadCPUUsage(summary *UserSummary) {

	// Start an OpenTelemetry span.
	ctx, span := otel.Tracer(d.OTelName).Start(d.Context, "summary: CPU hours")

	// Load the CPU usage information from the database.
	database := db.New(d.Database)
	cpuHours, err := database.CurrentCPUHoursForUser(ctx, d.User)
	if err == sql.ErrNoRows {
		cpuHours = &db.CPUHours{}
		summary.Errors = append(
			summary.Errors,
			APIError{
				Field:     "cpu_usage",
				Message:   "no current CPU hours found for user",
				ErrorCode: http.StatusNotFound,
			},
		)
	} else if err != nil {
		d.Log.WithContext(ctx).Error(err)
		cpuHours = &db.CPUHours{}
		summary.Errors = append(
			summary.Errors,
			APIError{
				Field:     "cpu_usage",
				Message:   err.Error(),
				ErrorCode: http.StatusInternalServerError,
			},
		)
	}

	// Save the CPU usage information in the summary.
	summary.CPUUsage = cpuHours

	// Close the OpenTelemetry span.
	span.End()
}

// loadDataUsage loads the user's data store usage information from data-usage-api.
func (d *DefaultSummarizer) loadDataUsage(summary *UserSummary) {

	// Start an OpenTelemetry span.
	ctx, span := otel.Tracer(d.OTelName).Start(d.Context, "summary: data usage")

	// Obtain the data store usage information.
	usage, err := d.DataUsageClient.GetUsageSummary(ctx, d.User)
	if err != nil {
		d.Log.WithContext(ctx).Error(err)
		summary.Errors = append(
			summary.Errors,
			APIError{
				Field:     "data_usage",
				Message:   err.Error(),
				ErrorCode: clients.GetStatusCode(err),
			},
		)
	}

	// Save the Data usage information in the summary.
	summary.DataUsage = usage

	// Close the OpenTelemetry span.
	span.End()
}

// LoadSummary aggregates and summarizes the user's resource usage information.
func (d *DefaultSummarizer) LoadSummary() *UserSummary {
	var summary UserSummary

	// Load the CPU usage information.
	d.loadCPUUsage(&summary)

	// Load the data usage information.
	d.loadDataUsage(&summary)

	// This resource usage summarizer leaves the subscription information blank.
	summary.UserPlan = nil

	return &summary
}
