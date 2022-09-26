package clients

import "time"

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
	ID                 string    `json:"id"`
	EffectiveStartDate time.Time `json:"effective_start_date"`
	EffectiveEndDate   time.Time `json:"effective_end_date"`
	User               User      `json:"users"`
	Plan               Plan      `json:"plan"`
	Quotas             []Quota   `json:"quotas"`
	Usages             []Usage   `json:"usages"`
}

// Resource type name constants.
const (
	ResourceTypeCPUHours = "cpu.hours"
	ResourceTypeDataSize = "data.size"
)

// ExtractUsage extracts the usage record for a given resource type from the user plan.
func (up *UserPlan) ExtractUsage(resourceType string) *Usage {

	// Search for the usage record matching the givn resource type.
	for _, usageRecord := range up.Usages {
		if usageRecord.ResourceType.Name == resourceType {
			return &usageRecord
		}
	}

	return nil
}
