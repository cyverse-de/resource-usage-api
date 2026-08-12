package summarizer

import (
	"errors"

	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/sirupsen/logrus"
)

type APIError struct {
	Field     string `json:"field"`
	Message   string `json:"message"`
	ErrorCode int    `json:"error_code"`
}

// safeMessage returns the error's own text when it describes the request rather than this service's
// internals, and the fallback otherwise. Database errors name schemas, tables and constraints, and
// the errors from the services behind this one carry their URLs; a summary is a user-facing response
// and neither belongs in it.
func safeMessage(err error, fallback string) string {
	var unknownUser *db.UserNotFoundError
	var noUsage *clients.NoUsageRecordedError

	if errors.As(err, &unknownUser) || errors.As(err, &noUsage) {
		return err.Error()
	}
	return fallback
}

// UserSummary contains the data summarizing the user's current resource
// usages and their current plan.
type UserSummary struct {
	CPUUsage     *db.CPUHours           `json:"cpu_usage"`
	DataUsage    *clients.UserDataUsage `json:"data_usage"`
	Subscription *clients.Subscription  `json:"subscription"`
	Errors       []APIError             `json:"errors"`
}

// The interface used to load the usage summary information.
type Summarizer interface {
	LoadSummary() *UserSummary
}

var log = logging.Log.WithFields(logrus.Fields{"package": "summarizer"})
