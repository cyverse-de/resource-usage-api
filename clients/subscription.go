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
	ID             string       `json:"id"`
	Quota          float64      `json:"quota"`
	ResourceType   ResourceType `json:"resource_type"`
	LastModifiedAt *time.Time   `json:"last_modified_at"`
}

type Usage struct {
	ID             string       `json:"id"`
	Usage          float64      `json:"usage"`
	ResourceType   ResourceType `json:"resource_type"`
	LastModifiedAt *time.Time   `json:"last_modified_at"`
}

type Addon struct {
	ID            string       `json:"id"`
	Name          string       `json:"name"`
	Description   string       `json:"description"`
	ResourceType  ResourceType `json:"resource_type"`
	DefaultAmount float64      `json:"default_amount"`
	DefaultPaid   bool         `json:"default_paid"`
}

type AddonRate struct {
	ID            string    `json:"id"`
	EffectiveDate time.Time `json:"effective_date"`
	Rate          float64   `json:"rate"`
}

type SubscriptionAddon struct {
	ID        string    `json:"id"`
	Addon     Addon     `json:"addon"`
	Amount    float64   `json:"amount"`
	Paid      bool      `json:"paid"`
	AddonRate AddonRate `json:"addon_rate"`
}

// Subscription is the representation of a user plan.
type Subscription struct {
	ID                 string              `json:"id"`
	EffectiveStartDate time.Time           `json:"effective_start_date"`
	EffectiveEndDate   time.Time           `json:"effective_end_date"`
	User               User                `json:"users"`
	Plan               Plan                `json:"plan"`
	Quotas             []Quota             `json:"quotas"`
	Usages             []Usage             `json:"usages"`
	Addons             []SubscriptionAddon `json:"addons"`
}

// Resource type name constants.
const (
	ResourceTypeCPUHours = "cpu.hours"
	ResourceTypeDataSize = "data.size"
)

// ExtractUsage extracts the usage record for a given resource type from the user plan.
func (s *Subscription) ExtractUsage(resourceType string) *Usage {

	// Search for the usage record matching the givn resource type.
	for _, usageRecord := range s.Usages {
		if usageRecord.ResourceType.Name == resourceType {
			return &usageRecord
		}
	}

	return nil
}
