package summarizer

import (
	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/db"
)

// User is the QMS representation of a user.
type User struct {
	ID       string `json:"id"`
	Username string `json:"username"`
}

// Plan is the representation of a plan.
type Plan struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type ResourceType struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Unit string `json:"description"`
}

type Quota struct {
	ID           string       `json:"id"`
	Quota        float64      `json:"quota"`
	ResourceType ResourceType `json:"resource_type"`
}

type Usage struct {
	ID           string       `json:"id"`
	Usage        float64      `json:"usage"`
	ResourceType ResourceType `json:"resource_type"`
}

// UserPlan is the representation of a user plan.
type UserPlan struct {
	ID                 string  `json:"id"`
	EffectiveStartDate string  `json:"effective_start_date"`
	EffectiveEndDate   string  `json:"effective_end_date"`
	User               User    `json:"users"`
	Plan               Plan    `json:"plan"`
	Quotas             []Quota `json:"quotas"`
	Usages             []Usage `json:"-"`
}

type UserPlanResult struct {
	Result UserPlan `json:"result"`
}

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
	UserPlan  *UserPlan              `json:"user_plan"`
	Errors    []APIError             `json:"errors"`
}

// The interface used to load the usage summary information.
type Summarizer interface {
	LoadSummary() *UserSummary
}
