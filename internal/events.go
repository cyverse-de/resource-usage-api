package internal

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/labstack/echo/v4"
)

func (a *App) AdminListEvents(c echo.Context) error {
	context := c.Request().Context()
	d := db.New(a.database)
	results, err := d.ListEvents(context)
	if err == sql.ErrNoRows || len(results) == 0 {
		return echo.NewHTTPError(http.StatusNotFound, errors.New("no events found"))
	} else if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}
	return c.JSON(http.StatusOK, results)
}

func (a *App) AdminListAllUserEventsHandler(c echo.Context) error {
	context := c.Request().Context()

	user := c.Param("username")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("user was not set"))
	}
	user = a.FixUsername(user)

	d := db.New(a.database)
	results, err := d.ListAllUserEvents(context, user)
	if err == sql.ErrNoRows || len(results) == 0 {
		return echo.NewHTTPError(http.StatusNotFound, errors.New("no events found"))
	} else if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	return c.JSON(http.StatusOK, results)
}

func (a *App) AdminGetEventHandler(c echo.Context) error {
	context := c.Request().Context()

	id := c.Param("id")
	if id == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("id was not set"))
	}

	d := db.New(a.database)
	result, err := d.Event(context, id)
	if err == sql.ErrNoRows {
		return echo.NewHTTPError(http.StatusNotFound, errors.New("event not found"))
	} else if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	return c.JSON(http.StatusOK, result)
}

func (a *App) AdminUpdateEventHandler(c echo.Context) error {
	context := c.Request().Context()

	id := c.Param("id")
	if id == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("id was not set"))
	}

	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	var workItem db.CPUUsageWorkItem
	if err = json.Unmarshal(body, &workItem); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	if workItem.ID == "" {
		workItem.ID = id
	} else if workItem.ID != id {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("work item IDs %s and %s do not match", workItem.ID, id))
	}

	d := db.New(a.database)
	err = d.UpdateEvent(context, &workItem)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	return c.NoContent(http.StatusOK)
}

func (a *App) AdminDeleteEventHandler(c echo.Context) error {
	context := c.Request().Context()

	id := c.Param("id")
	if id == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("id was not set"))
	}

	d := db.New(a.database)
	err := d.DeleteEvent(context, id)
	if err == sql.ErrNoRows {
		return echo.NewHTTPError(http.StatusNotFound, fmt.Errorf("no event %s found", id))
	} else if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	return c.NoContent(http.StatusOK)
}
