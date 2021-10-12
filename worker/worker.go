package worker

import (
	"context"
	"time"

	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/go-co-op/gocron"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

var log = logging.Log.WithFields(
	logrus.Fields{
		"package": "worker",
	},
)

type Worker struct {
	ID                  string
	Scheduler           *gocron.Scheduler
	db                  *sqlx.DB // needs to be a *sqlx.DB so we can create tranasactions on it.
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

func New(context context.Context, config *Config, dbAccessor *sqlx.DB) (*Worker, error) {
	var (
		err      error
		database *db.Database
	)

	worker := Worker{
		ClaimLifetime:       config.ClaimLifetime,
		db:                  dbAccessor,
		WorkSeekingLifetime: config.WorkSeekingLifetime,
	}

	database = db.New(worker.db)

	worker.ID, err = database.RegisterWorker(context, config.Name, time.Now().Add(config.ExpirationInterval))
	if err != nil {
		return nil, err
	}

	worker.Scheduler = gocron.NewScheduler(time.UTC)

	worker.Scheduler.Every(config.RefreshInterval).Do(func() {
		log := log.WithFields(logrus.Fields{"context": "refreshing worker registration"})
		log.Info("start refreshing worker registrations")

		newTime, err := database.RefreshWorkerRegistration(context, worker.ID, config.ExpirationInterval)
		if err != nil {
			log.Error(err)
			return
		}
		log.Infof("new expiration time is %s", newTime.String())
	})

	worker.Scheduler.Every(config.WorkerPurgeInterval).Do(func() {
		log := log.WithFields(logrus.Fields{"context": "purging expired workers"})
		log.Info("start purging expired workers")

		numExpired, err := database.PurgeExpiredWorkers(context)
		if err != nil {
			log.Error(err)
			return
		}
		log.Infof("purged %d expired workers", numExpired)

		log.Info("resetting work claims for inactive workers")
		resetClaims, err := database.ResetWorkClaimsForInactiveWorkers(context)
		if err != nil {
			log.Error(err)
			return
		}
		log.Infof("reset %d work claims", resetClaims)
	})

	worker.Scheduler.Every(config.WorkSeekerPurgeInterval).Do(func() {
		log := log.WithFields(logrus.Fields{"context": "purging expired work seekers"})
		log.Info("start purging expired work seekers")

		numExpiredWorkers, err := database.PurgeExpiredWorkSeekers(context)
		if err != nil {
			log.Error(err)
			return
		}
		log.Infof("purged %d expired workers", numExpiredWorkers)
	})

	worker.Scheduler.Every(config.WorkClaimPurgeInterval).Do(func() {
		log := log.WithFields(logrus.Fields{"context": "purging expired work claims"})
		log.Info("start purging expired work claims")

		numWorkClaims, err := database.PurgeExpiredWorkClaims(context)
		if err != nil {
			log.Error(err)
			return
		}
		log.Infof("purged %d expired work claims", numWorkClaims)
	})

	worker.Scheduler.StartAsync()

	return &worker, err
}

// Start gets the Worker busy processing work items.
func (w *Worker) Start(context context.Context) {
	var err error

	database := db.New(w.db)

	for {
		now := time.Now()

		if err = database.GettingWork(context, w.ID, now.Add(w.WorkSeekingLifetime)); err != nil {
			log.Error(err)
			continue
		}

		// Grab all eligible work items from the databse.
		workItems, err := database.UnclaimedUnprocessedEvents(context)
		if err != nil {
			log.Error(err)
			if err = database.DoneGettingWork(context, w.ID); err != nil {
				log.Error(err)
			}
			continue
		}

		// Can only do something if there's something returned.
		if len(workItems) == 0 {
			if err = database.DoneGettingWork(context, w.ID); err != nil {
				log.Error(err)
			}
			continue
		}

		// If multiple items are retrieved to work on, only process the first one
		// this may need to change in the future so that we can process items in a batch.
		workItem := workItems[0]
		if err = w.claimWorkItem(context, &workItem); err != nil {
			log.Error(err)
			continue
		}

		if err = w.transitionToWorkingState(context); err != nil {
			log.Error(err)
			continue
		}

		// TODO: call a synchronous item handler here.

		if err = w.finishWorking(context, &workItem); err != nil {
			log.Error(err)
		}

	}
}

// claimWorkItem wraps the logic for claiming an event and marking the worker as processing
// something in a transaction, which will hopefully prevent race conditions with the code that
// purges expired work claims and workers.
func (w *Worker) claimWorkItem(context context.Context, workItem *db.CPUUsageWorkItem) error {
	tx, err := w.db.Beginx()
	if err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	txdb := db.New(tx)

	if err = txdb.ClaimEvent(context, workItem.ID, w.ID); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	// Set the work item as being processed.
	if err = txdb.ProcessingEvent(context, workItem.ID); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	return nil
}

// transitionToWorkingState wraps the logic for finishing off the process of
// getting work starting work in a transaction, which should avoid race conditions
// in the clean up functions that run periodically.
func (w *Worker) transitionToWorkingState(context context.Context) error {
	tx, err := w.db.Beginx()
	if err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	txdb := db.New(tx)

	// Record that the worker is finished getting work.
	if err = txdb.DoneGettingWork(context, w.ID); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	// Set the worker as working.
	if err = txdb.SetWorking(context, w.ID, true); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	return nil
}

func (w *Worker) finishWorking(context context.Context, workItem *db.CPUUsageWorkItem) error {
	// Use a transaction here to avoid causing a race condition that
	// could cause the worker to get purged between steps.
	tx, err := w.db.Beginx()
	if err != nil {
		return err
	}

	txdb := db.New(tx)

	// Set the workItem as processed
	if err = txdb.FinishedProcessingEvent(context, workItem.ID); err != nil {
		// Set the worker as not working since something went wrong recording that the event was processed.
		rerr := tx.Rollback()
		if rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	// Set the worker as not working.
	if err = txdb.SetWorking(context, w.ID, false); err != nil {
		rerr := tx.Rollback()
		if rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		rerr := tx.Rollback()
		if rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	return nil
}
