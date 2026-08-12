package db

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/cyverse-de/resource-usage-api/config"
	"github.com/pkg/errors"
)

// UserNotFoundError reports that a username has no row in the DE users table.
type UserNotFoundError struct {
	Username string
}

func (e *UserNotFoundError) Error() string {
	return fmt.Sprintf("no DE user found for %s", e.Username)
}

// StatusCode reports the status a caller should surface for an unknown user.
func (e *UserNotFoundError) StatusCode() int {
	return http.StatusNotFound
}

// UserInfo has information about a user from the DE's database.
type UserInfo struct {
	ID       string `db:"id" json:"id"`
	Username string `db:"username" json:"username"`
}

// DEDatabase reads user records from the DE database.
type DEDatabase struct {
	db            Queryer
	configuration *config.Config
}

// NewDE returns a DEDatabase backed by the given queryer.
func NewDE(db Queryer, cfg *config.Config) *DEDatabase {
	return &DEDatabase{db: db, configuration: cfg}
}

// Table returns a schema-qualified, aliased table name for use in a query.
func (d *DEDatabase) Table(name, alias string) string {
	return fmt.Sprintf("%s.%s AS %s", d.configuration.DBSchema, name, alias)
}

// EnsureUsers inserts any of the given users that the DE database does not already have.
func (d *DEDatabase) EnsureUsers(ctx context.Context, users []string) error {
	log.Tracef("Ensuring users %+v", users)

	// Users passed here should already have the user suffix
	for _, user := range users {
		if !strings.Contains(user, "@") {
			return errors.New("Usernames passed to EnsureUsers should already be domain-qualified")
		}
	}

	query := psql.Insert(d.Table("users", "u")).
		Columns("username").
		Suffix("ON CONFLICT (username) DO NOTHING")
	for _, user := range users {
		query = query.Values(user)
	}

	qs, args, err := query.ToSql()
	if err != nil {
		return errors.Wrap(err, "Error formatting user insert SQL")
	}

	log.Tracef("EnsureUsers SQL: %s, %+v", qs, args)

	_, err = d.db.ExecContext(ctx, qs, args...)
	if err != nil {
		return errors.Wrap(err, "Error inserting users")
	}
	return nil
}

// GetUserInfo returns the DE database's record for a domain-qualified username, reporting a
// UserNotFoundError when there is none.
func (d *DEDatabase) GetUserInfo(ctx context.Context, username string) (*UserInfo, error) {
	log.Tracef("looking up user info for %s", username)

	query, args, err := psql.
		Select("id", "username").
		From(d.Table("users", "u")).
		Where("username = ?", username).
		ToSql()
	if err != nil {
		return nil, err
	}

	var uis []UserInfo
	err = d.db.SelectContext(ctx, &uis, query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "error getting user info")
	}

	if len(uis) < 1 {
		return nil, &UserNotFoundError{Username: username}
	}

	retval := uis[0]
	return &retval, nil
}
