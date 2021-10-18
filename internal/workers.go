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

func (a *App) AdminListWorkersHandler(c echo.Context) error {
	context := c.Request().Context()
	d := db.New(a.database)
	results, err := d.ListWorkers(context)
	if err == sql.ErrNoRows || len(results) == 0 {
		return echo.NewHTTPError(http.StatusNotFound, errors.New("no workers"))
	} else if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}
	return c.JSON(http.StatusOK, results)
}

func (a *App) AdminGetWorkerHandler(c echo.Context) error {
	context := c.Request().Context()

	id := c.Param("id")
	if id == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("id was not set"))
	}

	d := db.New(a.database)
	result, err := d.Worker(context, id)
	if err == sql.ErrNoRows {
		return echo.NewHTTPError(http.StatusNotFound, fmt.Errorf("no worker %s found", id))
	} else if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	return c.JSON(http.StatusOK, result)
}

func (a *App) AdminUpdateWorkerHandler(c echo.Context) error {
	context := c.Request().Context()

	id := c.Param("id")
	if id == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("id was not set"))
	}

	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	var worker db.Worker
	if err = json.Unmarshal(body, &worker); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	if worker.ID == "" {
		worker.ID = id
	} else if worker.ID != id {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("worker IDs %s and %s do not match", worker.ID, id))
	}

	d := db.New(a.database)
	err = d.UpdateWorker(context, &worker)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	return c.NoContent(http.StatusOK)
}

func (a *App) AdminDeleteWorkerHandler(c echo.Context) error {
	context := c.Request().Context()

	id := c.Param("id")
	if id == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("id was not set"))
	}

	d := db.New(a.database)
	err := d.DeleteWorker(context, id)
	if err == sql.ErrNoRows {
		return echo.NewHTTPError(http.StatusNotFound, fmt.Errorf("no worker %s found", id))
	} else if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	return c.NoContent(http.StatusOK)
}
