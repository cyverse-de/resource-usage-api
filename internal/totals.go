package internal

import (
	"database/sql"
	"errors"
	"net/http"

	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/labstack/echo/v4"
)

// GreetingHandler handles requests that simply need to know if the service is running.
func (a *App) GreetingHandler(context echo.Context) error {
	return context.String(http.StatusOK, "Hello from resource-usage-api.")
}

// CurrentCPUHours looks up the total CPU hours for the current recording period.
func (a *App) CurrentCPUHoursHandler(c echo.Context) error {
	context := c.Request().Context()

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
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	return c.JSON(http.StatusOK, results)
}

// AllCPUHoursHandler returns all of the total CPU hours totals, regardless of recording period.
func (a *App) AllCPUHoursHandler(c echo.Context) error {
	context := c.Request().Context()

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

	d := db.New(a.database)
	results, err := d.AdminAllCurrentCPUHours(context)
	if err == sql.ErrNoRows || len(results) == 0 {
		return echo.NewHTTPError(http.StatusNotFound, errors.New("no current CPU hours found"))
	} else if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, results)
}

// AdminAllCPUHoursTotalsHandler returns all of the total CPU hours totals for all recording periods, regardless of user.
func (a *App) AdminAllCPUHoursTotalsHandler(c echo.Context) error {
	context := c.Request().Context()

	d := db.New(a.database)
	results, err := d.AdminAllCPUHours(context)
	if err == sql.ErrNoRows || len(results) == 0 {
		return echo.NewHTTPError(http.StatusNotFound, errors.New("no CPU hours found"))
	} else if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, results)
}
