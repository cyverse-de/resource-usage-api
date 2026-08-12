package internal

import (
	"net/http"
	"strconv"

	"github.com/cyverse-de/resource-usage-api/clients"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/cyverse-de/resource-usage-api/logging"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
)

// errorResponse builds a response carrying both the status to send and the matching error code.
func errorResponse(status int, message string) logging.ErrorResponse {
	return logging.ErrorResponse{
		Message:        message,
		ErrorCode:      strconv.Itoa(status),
		HTTPStatusCode: status,
	}
}

// username reads and qualifies the username from the request path.
func (a *App) username(c echo.Context) (string, error) {
	user := c.Param("username")
	if user == "" {
		return "", errorResponse(http.StatusBadRequest, "No username provided")
	}
	return a.config.FixUsername(user), nil
}

// UserCurrentUsageHandler reports the user's current data usage as recorded in QMS.
func (a *App) UserCurrentUsageHandler(c echo.Context) error {
	ctx := c.Request().Context()

	user, err := a.username(c)
	if err != nil {
		return err
	}

	usage, err := a.dataUsage.CurrentForUser(ctx, user)
	if err != nil {
		var unknownUser *db.UserNotFoundError
		var noUsage *clients.NoUsageRecordedError

		switch {
		case errors.As(err, &unknownUser):
			return errorResponse(http.StatusBadRequest, err.Error())
		case errors.As(err, &noUsage):
			// A refresh has been enqueued; a later request will find the reading.
			return errorResponse(http.StatusNotFound, "No data usage information found for user")
		default:
			log.WithContext(ctx).Error(err)
			return errorResponse(http.StatusInternalServerError, err.Error())
		}
	}

	return c.JSON(http.StatusOK, usage)
}

// UpdateUserCurrentUsageHandler recomputes the user's data usage from the ICAT database and records
// the result in QMS.
func (a *App) UpdateUserCurrentUsageHandler(c echo.Context) error {
	ctx := c.Request().Context()

	user, err := a.username(c)
	if err != nil {
		return err
	}

	usage, err := db.NewBoth(a.database, a.icat, a.config, a.subscriptions).UpdateUserDataUsage(ctx, user)
	if err != nil {
		e := errors.Wrap(err, "Failed updating usage information")
		log.WithContext(ctx).Error(e)
		return errorResponse(http.StatusInternalServerError, e.Error())
	}

	return c.JSON(http.StatusOK, usage)
}

// UserDataOverageHandler reports whether the user is over their data quota.
func (a *App) UserDataOverageHandler(c echo.Context) error {
	ctx := c.Request().Context()

	user, err := a.username(c)
	if err != nil {
		return err
	}

	overages, err := a.subscriptions.AllResourceOveragesForUser(ctx, user)
	if err != nil {
		e := errors.Wrap(err, "failed getting all resource overages")
		log.WithContext(ctx).Error(e)
		return errorResponse(http.StatusInternalServerError, e.Error())
	}

	hasDataOverage := false
	for _, overage := range overages.Overages {
		if overage.ResourceName == dataSizeResource {
			hasDataOverage = true
		}
	}

	return c.JSON(http.StatusOK, map[string]bool{"has_data_overage": hasDataOverage})
}
