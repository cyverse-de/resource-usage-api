package summarizer

import (
	"context"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/db"
)

type HTTPSummarizer struct {
	Context       context.Context
	Subscriptions *clients.Subscriptions
	User          string
}

func (h *HTTPSummarizer) LoadSummary() *UserSummary {
	var summary UserSummary

	response, err := h.Subscriptions.GetSubscriptionSummary(h.Context, h.User)
	if err != nil {
		log.Error(err)
		return &summary
	}

	summary.Subscription = &clients.Subscription{
		ID:                 response.Subscription.Uuid,
		EffectiveStartDate: response.Subscription.EffectiveStartDate.AsTime(),
		EffectiveEndDate:   response.Subscription.EffectiveEndDate.AsTime(),
		User: clients.User{
			ID:       response.Subscription.User.Uuid,
			Username: response.Subscription.User.Username,
		},
		Plan: clients.Plan{
			ID:          response.Subscription.Plan.Uuid,
			Name:        response.Subscription.Plan.Name,
			Description: response.Subscription.Plan.Description,
		},
		Quotas: make([]clients.Quota, 0),
		Usages: make([]clients.Usage, 0),
		Addons: make([]clients.SubscriptionAddon, 0),
	}

	for _, rQuota := range response.Subscription.Quotas {
		quotaLMA := rQuota.LastModifiedAt.AsTime()
		q := clients.Quota{
			ID:    rQuota.Uuid,
			Quota: float64(rQuota.Quota),
			ResourceType: clients.ResourceType{
				ID:   rQuota.ResourceType.Uuid,
				Name: rQuota.ResourceType.Name,
				Unit: rQuota.ResourceType.Unit,
			},
			LastModifiedAt: &quotaLMA,
		}
		summary.Subscription.Quotas = append(summary.Subscription.Quotas, q)

	}

	log.Debug("after settings quotas")

	for _, rUsage := range response.Subscription.Usages {
		lma := rUsage.LastModifiedAt.AsTime()
		u := clients.Usage{
			ID:    rUsage.Uuid,
			Usage: rUsage.Usage,
			ResourceType: clients.ResourceType{
				ID:   rUsage.ResourceType.Uuid,
				Name: rUsage.ResourceType.Name,
				Unit: rUsage.ResourceType.Unit,
			},
			LastModifiedAt: &lma,
		}
		summary.Subscription.Usages = append(summary.Subscription.Usages, u)

		if u.ResourceType.Name == "cpu.hours" {
			ct, err := apd.New(0, 0).SetFloat64(rUsage.Usage)
			if err != nil {
				log.Error(err)
				return nil
			}
			summary.CPUUsage = &db.CPUHours{
				ID:             rUsage.Uuid,
				UserID:         response.Subscription.User.Uuid,
				Username:       response.Subscription.User.Username,
				Total:          *ct,
				EffectiveStart: response.Subscription.EffectiveStartDate.AsTime(),
				EffectiveEnd:   response.Subscription.EffectiveEndDate.AsTime(),
				LastModified:   *u.LastModifiedAt,
			}
		}

		if u.ResourceType.Name == "data.size" {
			dt, err := apd.New(0, 0).SetFloat64(u.Usage)
			if err != nil {
				log.Error(err)
				return nil
			}
			dv, err := dt.Int64()
			if err != nil {
				log.Error(err)
				return nil
			}
			// Time carries the creation time here to match the data-usage lookup, which reports the
			// same field from CreatedAt.
			createdAt := rUsage.CreatedAt.AsTime()
			summary.DataUsage = &clients.UserDataUsage{
				ID:           rUsage.Uuid,
				UserID:       response.Subscription.User.Uuid,
				Username:     response.Subscription.User.Username,
				Total:        dv,
				Time:         &createdAt,
				LastModified: &lma,
			}
		}
	}

	for _, rSubsAddon := range response.Subscription.Addons {
		addonRateEffectiveDate := rSubsAddon.AddonRate.EffectiveDate.AsTime()
		a := clients.SubscriptionAddon{
			ID: rSubsAddon.Uuid,
			Addon: clients.Addon{
				ID:          rSubsAddon.Addon.Uuid,
				Name:        rSubsAddon.Addon.Name,
				Description: rSubsAddon.Addon.Description,
				ResourceType: clients.ResourceType{
					ID:   rSubsAddon.Addon.ResourceType.Uuid,
					Name: rSubsAddon.Addon.ResourceType.Name,
					Unit: rSubsAddon.Addon.ResourceType.Unit,
				},
				DefaultAmount: float64(rSubsAddon.Addon.DefaultAmount),
				DefaultPaid:   rSubsAddon.Addon.DefaultPaid,
			},
			Amount: float64(rSubsAddon.Amount),
			Paid:   rSubsAddon.Paid,
			AddonRate: clients.AddonRate{
				ID:            rSubsAddon.AddonRate.Uuid,
				EffectiveDate: addonRateEffectiveDate,
				Rate:          float64(rSubsAddon.AddonRate.Rate),
			},
		}
		summary.Subscription.Addons = append(summary.Subscription.Addons, a)
	}

	if summary.CPUUsage == nil {
		summary.CPUUsage = &db.CPUHours{
			EffectiveStart: response.Subscription.EffectiveStartDate.AsTime(),
			EffectiveEnd:   response.Subscription.EffectiveEndDate.AsTime(),
			UserID:         response.Subscription.User.Uuid,
			Username:       response.Subscription.User.Username,
		}
	}

	if summary.DataUsage == nil {
		var zeroTimestamp time.Time
		summary.DataUsage = &clients.UserDataUsage{
			UserID:       response.Subscription.User.Uuid,
			Username:     response.Subscription.User.Username,
			Time:         &zeroTimestamp,
			LastModified: &zeroTimestamp,
		}
	}

	return &summary
}
