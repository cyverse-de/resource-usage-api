package internal

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
)

// UserDataUsage contains a user's current data usage, as returned by the
// data-usage-api service.
type UserDataUsage struct {
	ID           string `json:"id"`
	UserID       string `json:"user_id"`
	Username     string `json:"username"`
	Total        int64  `json:"total"`
	Time         string `json:"time"`
	LastModified string `json:"last_modified"`
}

// UserSummary contains the data summarizing the user's current resource
// usages and their current plan.
type UserSummary struct {
	CPUUsage  *db.CPUHours   `json:"cpu_usage"`
	DataUsage *UserDataUsage `json:"data_usage"`
}

// GetUserSummary is an echo request handler for requests to get a user's
// resource usage and current plan (if QMS is enabled).
func (a *App) GetUserSummary(c echo.Context) error {
	log = log.WithFields(logrus.Fields{"context": "get user summary"})
	context := c.Request().Context()

	user := c.Param("username")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("username was not set"))
	}
	user = a.FixUsername(user)

	log = log.WithFields(logrus.Fields{"user": user})

	d := db.New(a.database)

	cpuHours, err := d.CurrentCPUHoursForUser(context, user)
	if err == sql.ErrNoRows {
		return echo.NewHTTPError(http.StatusNotFound, errors.New("no current CPU hours found for user"))
	} else if err != nil {
		log.Error(err)
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	// Put together the URL for the request in to the data-usage-api
	dataUsageURL, err := url.Parse(a.dataUsageBase)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	// The path should be /:username/data/current,
	dataUsageURL.Path = fmt.Sprintf("/%s%s", user, a.dataUsageCurrent)

	// Create the request to to the data-usage-api.
	dataUsageReq, err := http.NewRequest(http.MethodGet, dataUsageURL.String(), nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	// Make the request to the data-usage-api. Close the body when the handler returns.
	resp, err := http.DefaultClient.Do(dataUsageReq)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}
	defer resp.Body.Close()

	// Read the body and parse the JSON into a struct.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	var du UserDataUsage
	if err = json.Unmarshal(body, &du); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	summary := &UserSummary{
		CPUUsage:  cpuHours,
		DataUsage: &du,
	}

	return c.JSON(http.StatusOK, summary)

}
