package internal

import (
	"database/sql"
	"errors"
	"net/http"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cyverse-de/resource-usage-api/cpuhours"
	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
)

// GreetingHandler handles requests that simply need to know if the service is running.
func (a *App) GreetingHandler(context echo.Context) error {
	return context.String(http.StatusOK, "Hello from resource-usage-api.")
}

// CurrentCPUHours looks up the total CPU hours for the current recording period.
func (a *App) CurrentCPUHoursHandler(c echo.Context) error {
	context := c.Request().Context()
	var log = log.WithContext(context)

	user := c.Param("username")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("user was not set"))
	}
	user = a.FixUsername(user)

	log.Debugf("username %s", user)

	d := db.New(a.database)
	results, err := d.CurrentCPUHoursForUser(context, user)
	if err == sql.ErrNoRows {
		return echo.NewHTTPError(http.StatusNotFound, errors.New("no current CPU hours found for user"))
	} else if err != nil {
		log.Error(err)
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	return c.JSON(http.StatusOK, results)
}

// AllCPUHoursHandler returns all of the total CPU hours totals, regardless of recording period.
func (a *App) AllCPUHoursHandler(c echo.Context) error {
	context := c.Request().Context()
	var log = log.WithContext(context)

	user := c.Param("username")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("user was not set"))
	}
	user = a.FixUsername(user)

	d := db.New(a.database)
	results, err := d.AllCPUHoursForUser(context, user)
	if err == sql.ErrNoRows || len(results) == 0 {
		return echo.NewHTTPError(http.StatusNotFound, errors.New("no CPU hours found for user"))
	} else if err != nil {
		log.Error(err)
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	return c.JSON(http.StatusOK, results)
}

/**
	Admin Endpoints
**/

// AdminAllCurrentCPUHoursTotalsHandler looks up all of the total CPU hours totals for all users.
func (a *App) AdminAllCurrentCPUHoursHandler(c echo.Context) error {
	context := c.Request().Context()
	var log = log.WithContext(context)

	d := db.New(a.database)
	results, err := d.AdminAllCurrentCPUHours(context)
	if err == sql.ErrNoRows || len(results) == 0 {
		return echo.NewHTTPError(http.StatusNotFound, errors.New("no current CPU hours found"))
	} else if err != nil {
		log.Error(err)
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, results)
}

// AdminAllCPUHoursTotalsHandler returns all of the total CPU hours totals for all recording periods, regardless of user.
func (a *App) AdminAllCPUHoursTotalsHandler(c echo.Context) error {
	context := c.Request().Context()
	var log = log.WithContext(context)

	d := db.New(a.database)
	results, err := d.AdminAllCPUHours(context)
	if err == sql.ErrNoRows || len(results) == 0 {
		return echo.NewHTTPError(http.StatusNotFound, errors.New("no CPU hours found"))
	} else if err != nil {
		log.Error(err)
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, results)
}

// AdminRecalculateCPUHoursTotalHandler is the echo-based HTTP handler that allows
// callers to trigger a recalculation of a user's CPU hours total. It works by
// zeroing out the current total and then emitting new events from the recaluclated
// totals for each analysis.
func (a *App) AdminRecalculateCPUHoursTotalHandler(c echo.Context) error {
	var (
		analyses []db.CalculableAnalysis
		err      error
	)
	context := c.Request().Context()

	var log = log.WithFields(logrus.Fields{"context": "recalulcating cpu hours total"}).WithContext(context)

	// Make sure the username has the domain suffix attached.
	user := c.Param("username")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("user was not set"))
	}
	user = a.FixUsername(user)

	from := c.QueryParam("from")
	if from == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("from query parameter was not set"))
	}
	fromDate, err := time.Parse("2006-01-02 03:04:05", from)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("from query parameter must be in the format 2006-01-02 03:04:05"))
	}

	to := c.QueryParam("to")
	if to == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("to query parameter was not set"))
	}
	toDate, err := time.Parse("2006-01-02 03:04:05", to)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("to query parameter must be in the format 2006-01-02 03:04:05"))
	}

	// Add the user to the logging context to make the logs easier to read.
	log = log.WithFields(logrus.Fields{"user": user})

	log.Debugf("username set to %s", user)

	d := db.New(a.database)
	ch := cpuhours.New(d)

	// Get the user's ID from the database. If the user doesn't exist there's nothing
	// we can do.
	userID, err := d.UserID(context, user)
	if err == sql.ErrNoRows {
		return echo.NewHTTPError(http.StatusNotFound, errors.New("user not found"))
	} else if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	log.Debugf("userID set to %s", userID)

	// Get the list of the user's analyses that have a millicores_reserved value, a start date, and an end date.
	analyses, err = d.AdminAllCalculableAnalyses(context, userID, fromDate, toDate)
	if err == sql.ErrNoRows || len(analyses) == 0 {
		return echo.NewHTTPError(http.StatusNotFound, errors.New("no calculable analyses found"))
	} else if err != nil {
		log.Error(err)
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	totals := map[string]*apd.Decimal{}

	// For each analysis, get the new CPU hours total.
	for _, analysis := range analyses {
		totals[analysis.ID], _, err = ch.CPUHoursForAnalysis(context, analysis.ID)
		if err != nil {
			log.Error(err)
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		log.Debugf("analysis: %s, CPU hours: %s", analysis.ID, totals[analysis.ID].String())
	}

	now := time.Now()
	zeroedValue := apd.New(0, 0)

	// Emit an event to reset the user's total to 0.
	resetEvent := db.CPUUsageEvent{
		CreatedBy:     userID,
		EffectiveDate: now,
		RecordDate:    now,
		EventType:     db.CPUHoursReset,
		Value:         *zeroedValue,
	}

	if err = d.AddCPUUsageEvent(context, &resetEvent); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	// For each analysis and CPU hours total, emit a new event adding the new analysis
	// total to the user's overall total.
	for _, analysis := range analyses {
		now = time.Now()
		newEvent := db.CPUUsageEvent{
			CreatedBy:     userID,
			EffectiveDate: now,
			RecordDate:    now,
			EventType:     db.CPUHoursAdd,
			Value:         *totals[analysis.ID],
		}
		if err = d.AddCPUUsageEvent(context, &newEvent); err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
		time.Sleep(5 * time.Millisecond) // just to make sure that we don't run afoul of a weird uniqueness constraint
	}

	return nil
}

func (a *App) AdminUsersWithCalculableAnalysesHandler(c echo.Context) error {
	var (
		users []db.User
		err   error
	)
	context := c.Request().Context()

	var log = log.WithFields(logrus.Fields{"context": "users with calculable analyses"}).WithContext(context)

	d := db.New(a.database)

	log.Info("finding users with calculable analyses")

	users, err = d.UsersWithCalculableAnalyses(context)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	log.Infof("found %d users with calculable analyses", len(users))

	return c.JSON(http.StatusOK, users)
}
