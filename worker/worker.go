package worker

import (
	"context"
	"time"

	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/go-co-op/gocron"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/multierr"
)

const otelName = "github.com/cyverse-de/resource-usage-api/worker"

var log = logging.Log.WithFields(
	logrus.Fields{
		"package": "worker",
	},
)

// MessageSender - handler for sending update messages based on a work item.
type MessageSender func(*db.CPUUsageWorkItem)

// UsageUpdate contains the info to be sent by MessageSender
type UsageUpdate struct {
	Attribute string `json:"attribute"`
	Value     string `json:"value"`
	Unit      string `json:"unit"`
	Username  string `json:"username"`
	UserID    string `json:"user_id"`
}

type Worker struct {
	ID                   string
	Name                 string
	Scheduler            *gocron.Scheduler
	db                   *sqlx.DB // needs to be a *sqlx.DB so we can create tranasactions on it.
	ClaimLifetime        time.Duration
	WorkSeekingLifetime  time.Duration
	NewUserTotalInterval int64
	MessageSender        MessageSender
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
	NewUserTotalInterval    int64
	MessageSender           MessageSender
}

func New(context context.Context, config *Config, dbAccessor *sqlx.DB) (*Worker, error) {
	var (
		err      error
		database *db.Database
	)

	worker := Worker{
		ClaimLifetime:        config.ClaimLifetime,
		Name:                 config.Name,
		db:                   dbAccessor,
		WorkSeekingLifetime:  config.WorkSeekingLifetime,
		NewUserTotalInterval: config.NewUserTotalInterval,
		MessageSender:        config.MessageSender,
	}

	database = db.New(worker.db)

	worker.ID, err = database.RegisterWorker(context, worker.Name, time.Now().Add(config.ExpirationInterval))
	if err != nil {
		return nil, err
	}

	worker.Scheduler = gocron.NewScheduler(time.UTC)

	worker.Scheduler.Every(config.RefreshInterval).Do(func() { // nolint:errcheck
		log := log.WithFields(logrus.Fields{"context": "refreshing worker registration"})
		log.Info("start refreshing worker registrations")

		newTime, err := database.RefreshWorkerRegistration(context, worker.ID, worker.Name, config.ExpirationInterval)
		if err != nil {
			log.Error(err)
			return
		}
		log.Infof("new expiration time is %s", newTime.String())
	})

	worker.Scheduler.Every(config.WorkerPurgeInterval).Do(func() { // nolint:errcheck
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

	worker.Scheduler.Every(config.WorkSeekerPurgeInterval).Do(func() { // nolint:errcheck
		log := log.WithFields(logrus.Fields{"context": "purging expired work seekers"})
		log.Info("start purging expired work seekers")

		numExpiredWorkers, err := database.PurgeExpiredWorkSeekers(context)
		if err != nil {
			log.Error(err)
			return
		}
		log.Infof("purged %d expired workers", numExpiredWorkers)
	})

	worker.Scheduler.Every(config.WorkClaimPurgeInterval).Do(func() { // nolint:errcheck
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
		iterationCtx, span := otel.Tracer(otelName).Start(context, "worker iteration")
		span.SetAttributes(attribute.String("worker.name", w.Name), attribute.String("worker.id", w.ID))
		now := time.Now()

		log := log.WithFields(logrus.Fields{"context": "processing work items"})
		log.Debugf("start looking for work items")

		if err = database.GettingWork(iterationCtx, w.ID, now.Add(w.WorkSeekingLifetime)); err != nil {
			log.Error(err)
			span.End()
			continue
		}

		log.Infof("worker %s is looking for work", w.ID)

		// Grab all eligible work items from the databse.
		workItems, err := database.UnclaimedUnprocessedEvents(iterationCtx)
		if err != nil {
			log.Error(err)
			if err = database.DoneGettingWork(iterationCtx, w.ID); err != nil {
				log.Error(err)
			}
			time.Sleep(30 * time.Second)
			span.End()
			continue
		}

		log.Debugf("found %d eligible work items from the database.", len(workItems))

		// Can only do something if there's something returned.
		if len(workItems) == 0 {
			if err = database.DoneGettingWork(iterationCtx, w.ID); err != nil {
				log.Error(err)
			}
			time.Sleep(30 * time.Second)
			span.End()
			continue
		}

		// If multiple items are retrieved to work on, only process the first one
		// this may need to change in the future so that we can process items in a batch.
		workItem := workItems[0]
		if err = w.claimWorkItem(iterationCtx, &workItem); err != nil {
			log.Error(err)
			span.End()
			continue
		}

		log.Infof("worker %s has claimed work item %s", w.ID, workItem.ID)

		if err = w.transitionToWorkingState(iterationCtx); err != nil {
			log.Error(err)
			span.End()
			continue
		}

		log.Infof("worker %s is in the working state", w.ID)

		switch workItem.EventType {
		case db.CPUHoursAdd:
			log.Debugf("worker %s is adding to the CPU hours total", w.ID)
			if err = w.AddCPUHours(iterationCtx, &workItem); err != nil {
				log.Error(err)
			}
			log.Debugf("worker %s is done adding to the CPU hours total", w.ID)

		case db.CPUHoursSubtract:
			log.Debugf("worker %s is subtracting from the CPU hours total", w.ID)
			if err = w.SubtractCPUHours(iterationCtx, &workItem); err != nil {
				log.Error(err)
			}
			log.Debugf("worker %s is done subtracting from the CPU hours total", w.ID)

		case db.CPUHoursReset:
			log.Debugf("worker %s is resetting the CPU hours total", w.ID)
			if err = w.ResetCPUHours(iterationCtx, &workItem); err != nil {
				log.Error(err)
			}
			log.Debugf("worker %s is done resetting the CPU hours total", w.ID)

		default:
			log.Errorf("worker %s does not recognize event type %s", w.ID, workItem.EventType)
			span.End()
			continue
		}

		if err = w.finishWorking(iterationCtx, &workItem); err != nil {
			log.Error(err)
		}

		w.MessageSender(&workItem)

		log.Infof("worker %s is finished with work item %s", w.ID, workItem.ID)
		span.End()
	}
}

// claimWorkItem wraps the logic for claiming an event and marking the worker as processing
// something in a transaction, which will hopefully prevent race conditions with the code that
// purges expired work claims and workers.
func (w *Worker) claimWorkItem(context context.Context, workItem *db.CPUUsageWorkItem) error {
	log := log.WithFields(logrus.Fields{"context": "claiming work item"})

	tx, err := w.db.Beginx()
	if err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	log.Debug("began transaction")

	txdb := db.New(tx)

	if err = txdb.ClaimEvent(context, workItem.ID, w.ID); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	log.Debugf("worker %s claimed work item %s", w.ID, workItem.ID)

	// Set the work item as being processed.
	if err = txdb.ProcessingEvent(context, workItem.ID); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	log.Debugf("work item %s is marked as being processed", workItem.ID)

	if err = tx.Commit(); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	log.Debug("committed transaction for claiming work item")

	return nil
}

// transitionToWorkingState wraps the logic for finishing off the process of
// getting work starting work in a transaction, which should avoid race conditions
// in the clean up functions that run periodically.
func (w *Worker) transitionToWorkingState(context context.Context) error {
	log := log.WithFields(logrus.Fields{"context": "claiming work item"})

	tx, err := w.db.Beginx()
	if err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	txdb := db.New(tx)

	log.Debugf("began transaction for moving worker %s to the working state", w.ID)

	// Record that the worker is finished getting work.
	if err = txdb.DoneGettingWork(context, w.ID); err != nil {
		log.Error(err)
		log.Infof("rolling back transaction")
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	log.Debugf("recorded that worker %s is done getting work", w.ID)

	// Set the worker as working.
	if err = txdb.SetWorking(context, w.ID, true); err != nil {
		log.Error(err)
		log.Infof("rolling back transaction")
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		log.Error(err)
		log.Infof("rolling back transaction")
		if rerr := tx.Rollback(); rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	log.Debugf("committed transaction for moving worker %s to the working state", w.ID)

	return nil
}

func (w *Worker) finishWorking(context context.Context, workItem *db.CPUUsageWorkItem) error {
	log := logging.Log.WithFields(logrus.Fields{"context": "mark work item finished"})

	// Use a transaction here to avoid causing a race condition that
	// could cause the worker to get purged between steps.
	tx, err := w.db.Beginx()
	if err != nil {
		return err
	}

	log.Debugf("began transaction for finishing work item %s", workItem.ID)

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

	log.Debugf("work item %s is marked as processed", workItem.ID)

	// Set the worker as not working.
	if err = txdb.SetWorking(context, w.ID, false); err != nil {
		rerr := tx.Rollback()
		if rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	log.Debugf("worker %s is no longer working", w.ID)

	if err = tx.Commit(); err != nil {
		rerr := tx.Rollback()
		if rerr != nil {
			err = multierr.Append(err, rerr)
		}
		return err
	}

	log.Debugf("committed transaction for finishing work item %s", workItem.ID)

	return nil
}
