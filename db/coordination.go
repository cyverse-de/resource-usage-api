package db

import (
	"context"
	"database/sql"

	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/config"
	"github.com/pkg/errors"
)

// BothDatabases coordinates work that spans the DE and ICAT databases. Each transaction is created on
// demand and its commit and rollback functions replace themselves with no-ops once used, so the
// deferred rollback that guards the error paths is harmless after a successful commit.
type BothDatabases struct {
	deconn        DatabaseAccessor
	icatconn      DatabaseAccessor
	configuration *config.Config
	subs          *clients.Subscriptions

	DERollback func()
	DECommit   func() error

	ICATRollback func()
	ICATCommit   func() error

	detx   *DEDatabase
	icattx *ICATDatabase
}

// NewBoth returns a BothDatabases backed by the DE and ICAT connection pools.
func NewBoth(dedb DatabaseAccessor, icatdb DatabaseAccessor, cfg *config.Config, subs *clients.Subscriptions) *BothDatabases {
	return &BothDatabases{deconn: dedb, icatconn: icatdb, configuration: cfg, subs: subs}
}

// DETx returns the open DE transaction, starting one if needed.
func (b *BothDatabases) DETx(ctx context.Context) (*DEDatabase, error) {
	logStats("DE", b.deconn)
	if b.detx != nil {
		return b.detx, nil
	}

	detx, err := b.deconn.BeginTxx(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating DE transaction")
	}

	rb := func() {
		err := detx.Rollback()
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			e := errors.Wrap(err, "Error rolling back DE database transaction")
			log.Error(e)
		}
		b.detx = nil
		b.DERollback = func() {}
		b.DECommit = func() error { return nil }
	}

	commit := func() error {
		err := detx.Commit()
		b.detx = nil
		b.DERollback = func() {}
		b.DECommit = func() error { return nil }
		if err != nil {
			e := errors.Wrap(err, "Error committing DE database transaction")
			log.Error(e)
			return e
		}
		return nil
	}

	b.detx = NewDE(detx, b.configuration)
	b.DERollback = rb
	b.DECommit = commit

	return b.detx, nil
}

// ICATTx returns the open ICAT transaction, starting one if needed.
func (b *BothDatabases) ICATTx(ctx context.Context) (*ICATDatabase, error) {
	logStats("ICAT", b.icatconn)
	if b.icattx != nil {
		return b.icattx, nil
	}

	icattx, err := b.icatconn.BeginTxx(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating ICAT transaction")
	}

	rb := func() {
		err := icattx.Rollback()
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			e := errors.Wrap(err, "Error rolling back ICAT transaction")
			log.Error(e)
		}
		b.icattx = nil
		b.ICATRollback = func() {}
		b.ICATCommit = func() error { return nil }
	}

	commit := func() error {
		err := icattx.Commit()
		b.icattx = nil
		b.ICATRollback = func() {}
		b.ICATCommit = func() error { return nil }
		if err != nil {
			e := errors.Wrap(err, "Error committing ICAT transaction")
			log.Error(e)
			return e
		}
		return nil
	}

	b.icattx = NewICAT(icattx, b.configuration)
	b.ICATRollback = rb
	b.ICATCommit = commit

	return b.icattx, nil
}

// UpdateUserDataUsage recomputes a user's data usage from the ICAT database and records it in QMS.
func (b *BothDatabases) UpdateUserDataUsage(ctx context.Context, username string) (*clients.UserDataUsage, error) {
	dedb, err := b.DETx(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating DE transaction")
	}
	defer b.DERollback()

	icatdb, err := b.ICATTx(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating ICAT transaction")
	}
	defer b.ICATRollback()

	userInfo, err := dedb.GetUserInfo(ctx, username)
	if err != nil {
		return nil, errors.Wrap(err, "error getting user info")
	}

	usagenum, err := icatdb.UserCurrentDataUsage(ctx, username)
	if errors.Is(err, sql.ErrNoRows) {
		usagenum = 0
		log.Infof("No usage information was found for user %s. Attempting to add a usage of 0 anyway", username)
	} else if err != nil {
		return nil, errors.Wrap(err, "Error getting current data usage")
	}
	b.ICATRollback()

	// Both reads are done, so release the DE connection before the call to subscriptions. Holding it
	// across an HTTP request would let concurrent updates exhaust the pool and stall requests that only
	// need to read.
	b.DERollback()

	log.Debugf("username %s; usage value %d", username, usagenum)

	res, err := b.subs.UpdateUsageForUser(ctx, username, float64(usagenum))
	if err != nil {
		e := errors.Wrap(err, "Error adding user data usage")
		log.Error(e)
		return nil, e
	}

	res.UserID = userInfo.ID
	res.Username = userInfo.Username

	return res, nil
}

// UpdateUserDataUsageBatch recomputes data usage for every user from start to end, inclusive, and
// records the results in QMS.
func (b *BothDatabases) UpdateUserDataUsageBatch(ctx context.Context, start, end string) ([]*clients.UserDataUsage, error) {
	// should pass in qualified usernames, icatdb method will strip it as needed
	icatdb, err := b.ICATTx(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating ICAT transaction")
	}
	defer b.ICATRollback()

	usages, err := icatdb.BatchCurrentDataUsage(ctx, start, end)
	if err != nil {
		return nil, err
	}
	b.ICATRollback()

	log.Tracef("usages in batch: %+v", usages)

	us := make([]string, 0, len(usages))
	usagesFixed := make(map[string]float64, len(usages))
	for usr, usg := range usages {
		qualified := b.configuration.FixUsername(usr)
		us = append(us, qualified)
		usagesFixed[qualified] = float64(usg)
	}

	dedb, err := b.DETx(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating DE database transaction")
	}
	defer b.DERollback()

	if len(us) > 0 {
		err = dedb.EnsureUsers(ctx, us)
		if err != nil {
			return nil, errors.Wrap(err, "Error ensuring users exist")
		}
	} else {
		log.Tracef("No users to be ensured in the batch")
	}

	err = b.DECommit()
	if err != nil {
		e := errors.Wrap(err, "Error committing DE transaction")
		log.Error(e)
		return nil, e
	}

	res, err := b.subs.AddUserUpdatesBatch(ctx, usagesFixed)
	if err != nil {
		return nil, errors.Wrap(err, "Error inserting new usage")
	}

	return res, nil
}
