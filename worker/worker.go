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
	ID                  string
	Scheduler           *gocron.Scheduler
	database            *db.Database
	ClaimLifetime       time.Duration
	WorkSeekingLifetime time.Duration
}

type Config struct {
	Name                    string
	ExpirationInterval      time.Duration
	RefreshInterval         time.Duration
	WorkerPurgeInterval     time.Duration
	WorkSeekerPurgeInterval time.Duration
	WorkClaimPurgeInterval  time.Duration
	ClaimLifetime           time.Duration
	WorkSeekingLifetime     time.Duration
}

func New(context context.Context, config *Config, database *db.Database) (*Worker, error) {
	var err error

	worker := Worker{
		ClaimLifetime:       config.ClaimLifetime,
		WorkSeekingLifetime: config.WorkSeekingLifetime,
	}

	worker.ID, err = database.RegisterWorker(context, config.Name, time.Now().Add(config.ExpirationInterval))
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

func (w *Worker) Start(context context.Context) {
	var err error

	for {
		now := time.Now()

		if err = w.database.GettingWork(context, w.ID, now.Add(w.WorkSeekingLifetime)); err != nil {
			log.Error(err)
			continue
		}

		// Grab all eligible work items from the databse.
		workItems, err := w.database.UnclaimedUnprocessedEvents(context)
		if err != nil {
			log.Error(err)
			if err = w.database.DoneGettingWork(context, w.ID); err != nil {
				log.Error(err)
			}
			continue
		}

		// Can only do something if there's something returned.
		if len(workItems) == 0 {
			if err = w.database.DoneGettingWork(context, w.ID); err != nil {
				log.Error(err)
			}
			continue
		}

		// If multiple items are retrieved to work on, only process the first one
		// this may need to change in the future so that we can process items in a batch.
		workItem := workItems[0]
		if err = w.database.ClaimEvent(context, workItem.ID, w.ID); err != nil {
			log.Error(err)
			if err = w.database.DoneGettingWork(context, w.ID); err != nil {
				log.Error(err)
			}
			continue
		}

		// Record that the worker is finished getting work.
		if err = w.database.DoneGettingWork(context, w.ID); err != nil {
			log.Error(err)
			continue
		}

		// Set the worker as working.
		if err = w.database.SetWorking(context, w.ID, true); err != nil {
			log.Error(err)
			continue
		}

		// Set the work item as being processed.
		if err = w.database.ProcessingEvent(context, workItem.ID); err != nil {
			// Set the worker as not working since something went wrong.
			if err = w.database.SetWorking(context, w.ID, false); err != nil {
				log.Error(err)
			}
			continue
		}

		// TODO: call a synchronous item handler here.

		// Set the workItem as processed
		if err = w.database.FinishedProcessingEvent(context, workItem.ID); err != nil {
			// Set the worker as not working since something went wrong recording that the event was processed.
			if err = w.database.SetWorking(context, w.ID, false); err != nil {
				log.Error(err)
			}
			continue
		}

		// Set the worker as not working.
		if err = w.database.SetWorking(context, w.ID, false); err != nil {
			log.Error(err)
		}
	}
}
