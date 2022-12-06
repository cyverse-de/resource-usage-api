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
	response := pbinit.NewUserPlanResponse()
	if err = gotelnats.Request(ctx, s.Client, subjects.QMSUserSummary, request, response); err != nil {
		log.Error(err)
		return nil
	}
	log.Debug("after sending nats request")

	summary.UserPlan = &clients.UserPlan{
		ID:                 response.UserPlan.Uuid,
		EffectiveStartDate: response.UserPlan.EffectiveStartDate.AsTime(),
		EffectiveEndDate:   response.UserPlan.EffectiveEndDate.AsTime(),
		User: clients.User{
			ID:       response.UserPlan.User.Uuid,
			Username: response.UserPlan.User.Username,
		},
		Plan: clients.Plan{
			ID:          response.UserPlan.Plan.Uuid,
			Name:        response.UserPlan.Plan.Name,
			Description: response.UserPlan.Plan.Description,
		},
		Quotas: make([]clients.Quota, 0),
		Usages: make([]clients.Usage, 0),
	}

	for _, rQuota := range response.UserPlan.Quotas {
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
		summary.UserPlan.Quotas = append(summary.UserPlan.Quotas, q)

	}

	log.Debug("after settings quotas")

	for _, rUsage := range response.UserPlan.Usages {
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
		summary.UserPlan.Usages = append(summary.UserPlan.Usages, u)

		if u.ResourceType.Name == "cpu.hours" {
			ct, err := apd.New(0, 0).SetFloat64(rUsage.Usage)
			if err != nil {
				log.Error(err)
				return nil
			}
			summary.CPUUsage = &db.CPUHours{
				ID:             rUsage.Uuid,
				UserID:         response.UserPlan.User.Uuid,
				Username:       response.UserPlan.User.Username,
				Total:          *ct,
				EffectiveStart: response.UserPlan.EffectiveStartDate.AsTime(),
				EffectiveEnd:   response.UserPlan.EffectiveEndDate.AsTime(),
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
				UserID:       response.UserPlan.User.Uuid,
				Username:     response.UserPlan.User.Username,
				Total:        dv,
				Time:         &dTime,
				LastModified: &lma,
			}
		}
	}

	if summary.CPUUsage == nil {
		summary.CPUUsage = &db.CPUHours{
			EffectiveStart: response.UserPlan.EffectiveStartDate.AsTime(),
			EffectiveEnd:   response.UserPlan.EffectiveEndDate.AsTime(),
			UserID:         response.UserPlan.User.Uuid,
			Username:       response.UserPlan.User.Username,
		}
	}

	if summary.DataUsage == nil {
		var zeroTimestamp time.Time
		summary.DataUsage = &clients.UserDataUsage{
			UserID:       response.UserPlan.User.Uuid,
			Username:     response.UserPlan.User.Username,
			Time:         &zeroTimestamp,
			LastModified: &zeroTimestamp,
		}
	}

	return &summary
}
