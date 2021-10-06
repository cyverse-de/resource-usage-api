package worker

import (
	"context"
	"time"

	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/go-co-op/gocron"
)

var log = logging.Log

type Worker struct {
	ID        string
	Scheduler *gocron.Scheduler
}

type Config struct {
	Name                    string
	Expiration              time.Time
	ExpirationInterval      time.Duration
	RefreshInterval         time.Duration
	WorkerPurgeInterval     time.Duration
	WorkSeekerPurgeInterval time.Duration
	WorkClaimPurgeInterval  time.Duration
}

func New(context context.Context, config *Config, database *db.Database) (*Worker, error) {
	var err error
	var worker Worker

	//Register the worker
	worker.ID, err = database.RegisterWorker(context, config.Name, config.Expiration)
	if err != nil {
		return nil, err
	}

	worker.Scheduler = gocron.NewScheduler(time.UTC)

	worker.Scheduler.Every(config.RefreshInterval).Do(func() {
		newTime, err := database.RefreshWorkerRegistration(context, worker.ID, config.ExpirationInterval)
		if err != nil {
			log.Error(err)
			return
		}
		log.Info("new expiration time is %s", newTime.String())
	})

	worker.Scheduler.Every(config.WorkerPurgeInterval).Do(func() {
		numExpired, err := database.PurgeExpiredWorkers(context)
		if err != nil {
			log.Error(err)
			return
		}
		log.Info("purged %d expired workers", numExpired)

		resetClaims, err := database.ResetWorkClaimsForInactiveWorkers(context)
		if err != nil {
			log.Error(err)
			return
		}
		log.Info("reset %d work claims", resetClaims)
	})

	worker.Scheduler.Every(config.WorkSeekerPurgeInterval).Do(func() {
		numExpiredWorkers, err := database.PurgeExpiredWorkSeekers(context)
		if err != nil {
			log.Error(err)
			return
		}
		log.Info("purged %d expired workers", numExpiredWorkers)
	})

	worker.Scheduler.Every(config.WorkClaimPurgeInterval).Do(func() {
		numWorkClaims, err := database.PurgeExpiredWorkClaims(context)
		if err != nil {
			log.Error(err)
			return
		}
		log.Info("purged %d expired work claims", numWorkClaims)

	})

	return &worker, err
}

func (*Worker) Start(context context.Context) {

}
