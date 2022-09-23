package summarizer

import (
	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/db"
)

type APIError struct {
	Field     string `json:"field"`
	Message   string `json:"message"`
	ErrorCode int    `json:"error_code"`
}

// UserSummary contains the data summarizing the user's current resource
// usages and their current plan.
type UserSummary struct {
	CPUUsage  *db.CPUHours           `json:"cpu_usage"`
	DataUsage *clients.UserDataUsage `json:"data_usage"`
	UserPlan  *clients.UserPlan      `json:"user_plan"`
	Errors    []APIError             `json:"errors"`
}

// The interface used to load the usage summary information.
type Summarizer interface {
	LoadSummary() *UserSummary
}
