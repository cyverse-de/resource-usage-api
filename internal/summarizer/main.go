package summarizer

import (
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
