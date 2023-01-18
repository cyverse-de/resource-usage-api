package summarizer

import (
	"context"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/go-mod/subjects"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "summarizer"})

type SubscriptionSummarizer struct {
	Context context.Context
	User    string
	Client  *nats.EncodedConn
}

func (s *SubscriptionSummarizer) LoadSummary() *UserSummary {
	var (
		err     error
		request *qms.RequestByUsername
		summary UserSummary
	)

	log := log.WithFields(logrus.Fields{"context": "load summary"})
	request = pbinit.NewQMSRequestByUsername()
	ctx, span := pbinit.InitQMSRequestByUsername(request, subjects.QMSUserSummary)
	defer span.End()

	log = log.WithFields(logrus.Fields{"user": s.User})

	request.Username = s.User

	log.Debug("before sending nats request")
	response := pbinit.NewSubscriptionResponse()
	if err = gotelnats.Request(ctx, s.Client, subjects.QMSUserSummary, request, response); err != nil {
		log.Error(err)
		return nil
	}
	log.Debug("after sending nats request")

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
			dTime := rUsage.LastModifiedAt.AsTime()
			summary.DataUsage = &clients.UserDataUsage{
				ID:           rUsage.Uuid,
				UserID:       response.Subscription.User.Uuid,
				Username:     response.Subscription.User.Username,
				Total:        dv,
				Time:         &dTime,
				LastModified: &lma,
			}
		}
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
