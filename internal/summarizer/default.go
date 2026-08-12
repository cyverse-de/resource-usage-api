package summarizer

import (
	"context"
	"database/sql"
	"net/http"

	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

type DefaultSummarizer struct {
	Context   context.Context
	Log       *logrus.Entry
	User      string
	Database  *sqlx.DB
	DataUsage *db.DataUsage
}

// loadCPUUsage loads the user's CPU usage information from the DE database.
func (d *DefaultSummarizer) loadCPUUsage(summary *UserSummary) {
	ctx := d.Context

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
				Message:   "unable to load the user's CPU hours",
				ErrorCode: http.StatusInternalServerError,
			},
		)
	}

	// Save the CPU usage information in the summary.
	summary.CPUUsage = cpuHours
}

// loadDataUsage loads the user's data store usage information.
func (d *DefaultSummarizer) loadDataUsage(summary *UserSummary) {
	ctx := d.Context

	// Obtain the data store usage information.
	usage, err := d.DataUsage.CurrentForUser(ctx, d.User)
	if err != nil {
		d.Log.WithContext(ctx).Error(err)
		summary.Errors = append(
			summary.Errors,
			APIError{
				Field:     "data_usage",
				Message:   safeMessage(err, "unable to load the user's data usage"),
				ErrorCode: clients.GetStatusCode(err),
			},
		)
		// The field is reported as an object even when it could not be loaded, so that callers see an
		// empty record alongside the error rather than a null.
		usage = &clients.UserDataUsage{}
	}

	// Save the Data usage information in the summary.
	summary.DataUsage = usage
}

// LoadSummary aggregates and summarizes the user's resource usage information.
func (d *DefaultSummarizer) LoadSummary() *UserSummary {
	var summary UserSummary

	// Load the CPU usage information.
	d.loadCPUUsage(&summary)

	// Load the data usage information.
	d.loadDataUsage(&summary)

	// This resource usage summarizer leaves the subscription information blank.
	summary.Subscription = nil

	return &summary
}
