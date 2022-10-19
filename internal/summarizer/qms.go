package summarizer

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"

	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
)

type QMSSummarizer struct {
	Context   context.Context
	Log       *logrus.Entry
	User      string
	OTelName  string
	Database  *sqlx.DB
	QMSClient *clients.QMSAPI
}

// loadUserID looks up the user ID in the DE database and adds it to the summary
func (s *QMSSummarizer) loadUserID(summary *UserSummary) string {

	// Start an OpenTelemetry span.
	ctx, span := otel.Tracer(s.OTelName).Start(s.Context, "summary: user ID")

	// Look up the user ID.
	database := db.New(s.Database)
	userID, err := database.UserID(ctx, s.User)
	if err == sql.ErrNoRows {
		err = errors.Wrapf(err, "user %s not found", s.User)
		s.Log.WithContext(ctx).Error(err)
		summary.Errors = append(
			summary.Errors,
			*NewAPIError("cpu_usage", err.Error(), http.StatusNotFound),
			*NewAPIError("data_usage", err.Error(), http.StatusNotFound),
		)
	} else if err != nil {
		err = errors.Wrapf(err, "unable to look up the user ID for %s", s.User)
		s.Log.WithContext(ctx).Error(err)
		summary.Errors = append(
			summary.Errors,
			*NewAPIError("cpu_usage", err.Error(), http.StatusInternalServerError),
			*NewAPIError("data_usage", err.Error(), http.StatusInternalServerError),
		)
	}

	// Store the username and User ID; the user ID will be empty if an error occurred.
	summary.CPUUsage.Username = s.User
	summary.CPUUsage.UserID = userID
	summary.DataUsage.Username = s.User
	summary.DataUsage.UserID = userID

	// Close the OpenTelemetry span.
	span.End()

	return userID
}

// loadUserPlan retrieves the user's subscription plan information from QMS.
func (s *QMSSummarizer) loadUserPlan(summary *UserSummary) {

	// Start an OpenTelemetry span.
	ctx, span := otel.Tracer(s.OTelName).Start(s.Context, "summary: user plan")

	// Obtain the user plan information.
	userPlan, err := s.QMSClient.GetUserPlan(ctx, s.User)
	if err != nil {
		s.Log.WithContext(ctx).Error(err)
		summary.Errors = append(
			summary.Errors,
			*NewAPIError("user_plan", err.Error(), clients.GetStatusCode(err)),
		)
	}

	// Save the user plan information in the summary.
	summary.UserPlan = userPlan

	// Add the CPU usage to the summary.
	cpuUsage := userPlan.ExtractUsage(clients.ResourceTypeCPUHours)
	if cpuUsage != nil {
		summary.CPUUsage.ID = cpuUsage.ID
		summary.CPUUsage.EffectiveEnd = userPlan.EffectiveEndDate
		summary.CPUUsage.EffectiveStart = userPlan.EffectiveStartDate
		if cpuUsage.LastModifiedAt != nil {
			summary.CPUUsage.LastModified = *cpuUsage.LastModifiedAt
		}
		_, err = summary.CPUUsage.Total.SetFloat64(cpuUsage.Usage)
		if err != nil {
			err = errors.Wrap(err, "unable to set the CPU usage total")
			summary.Errors = append(
				summary.Errors,
				*NewAPIError("cpu_usage", err.Error(), http.StatusInternalServerError),
			)
		}
	} else {
		msg := fmt.Sprintf("no current CPU hours found for %s", s.User)
		summary.Errors = append(
			summary.Errors,
			*NewAPIError("cpu_usage", msg, http.StatusNotFound),
		)
	}

	// Add the data usage to the summary.
	dataUsage := userPlan.ExtractUsage(clients.ResourceTypeDataSize)
	if dataUsage != nil {
		summary.DataUsage.ID = dataUsage.ID
		summary.DataUsage.Total = int64(dataUsage.Usage)
		if dataUsage.LastModifiedAt != nil {
			summary.DataUsage.LastModified = dataUsage.LastModifiedAt
			summary.DataUsage.Time = dataUsage.LastModifiedAt
		}
	} else {
		msg := fmt.Sprintf("no data usage found for %s", s.User)
		summary.Errors = append(
			summary.Errors,
			*NewAPIError("data_usage", msg, http.StatusNotFound),
		)
	}

	// Close the OpenTelemetry span.
	span.End()
}

// LoadSummary aggregates and summarizes the user's resource usage information.
func (s *QMSSummarizer) LoadSummary() *UserSummary {
	var summary UserSummary

	// Add empty usage records to the summary information.
	summary.CPUUsage = &db.CPUHours{}
	summary.DataUsage = &clients.UserDataUsage{}

	// Look up the user ID in the database.
	s.loadUserID(&summary)

	// Load the user plan information from QMS.
	s.loadUserPlan(&summary)

	return &summary
}
