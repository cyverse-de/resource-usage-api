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

// User is the QMS representation of a user.
type User struct {
	ID       string `json:"id"`
	Username string `json:"username"`
}

// Plan is the representation of a plan.
type Plan struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type ResourceType struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Unit string `json:"description"`
}

type Quota struct {
	ID             string       `json:"id"`
	Quota          float64      `json:"quota"`
	AddedBy        string       `json:"added_by"`
	LastModifiedBy string       `json:"last_modified_by"`
	ResourceType   ResourceType `json:"resource_type"`
}

type Usage struct {
	ID             string       `json:"id"`
	Usage          float64      `json:"usage"`
	AddedBy        string       `json:"added_by"`
	LastModifiedBy string       `json:"last_modified_by"`
	ResourceType   ResourceType `json:"resource_type"`
}

// UserPlan is the representation of a user plan.
type UserPlan struct {
	ID                 string  `json:"id"`
	EffectiveStartDate string  `json:"effective_start_date"`
	EffectiveEndDate   string  `json:"effective_end_date"`
	AddedBy            string  `json:"added_by"`
	LastModifiedBy     string  `json:"last_modified_by"`
	User               User    `json:"user"`
	Plan               Plan    `json:"plan"`
	Quotas             []Quota `json:"quotas"`
	Usages             []Usage `json:"usages"`
}

// UserSummary contains the data summarizing the user's current resource
// usages and their current plan.
type UserSummary struct {
	CPUUsage  *db.CPUHours   `json:"cpu_usage"`
	DataUsage *UserDataUsage `json:"data_usage"`
	UserPlan  *UserPlan      `json:"user_plan"`
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

	// Get the user plan
	userPlanURL, err := url.Parse(a.dataUsageBase)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}
	userPlanURL.Path = fmt.Sprintf("/users/%s/plan", user)

	userPlanReq, err := http.NewRequest(http.MethodGet, userPlanURL.String(), nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	userPlanResp, err := http.DefaultClient.Do(userPlanReq)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	// Read the body and parse the JSON into a struct.
	userPlanBody, err := io.ReadAll(userPlanResp.Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	var up UserPlan
	if err = json.Unmarshal(userPlanBody, &up); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}

	summary := &UserSummary{
		CPUUsage:  cpuHours,
		DataUsage: &du,
		UserPlan:  &up,
	}

	return c.JSON(http.StatusOK, summary)

}
