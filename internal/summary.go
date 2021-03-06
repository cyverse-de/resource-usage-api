package internal

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"

	"github.com/cyverse-de/resource-usage-api/db"
	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
)

var client = http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}

const otelName = "github.com/cyverse-de/resource-usage-api/internal"

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
	ID           string       `json:"id"`
	Quota        float64      `json:"quota"`
	ResourceType ResourceType `json:"resource_type"`
}

type Usage struct {
	ID           string       `json:"id"`
	Usage        float64      `json:"usage"`
	ResourceType ResourceType `json:"resource_type"`
}

// UserPlan is the representation of a user plan.
type UserPlan struct {
	ID                 string  `json:"id"`
	EffectiveStartDate string  `json:"effective_start_date"`
	EffectiveEndDate   string  `json:"effective_end_date"`
	User               User    `json:"users"`
	Plan               Plan    `json:"plan"`
	Quotas             []Quota `json:"quotas"`
	Usages             []Usage `json:"-"`
}

type UserPlanResult struct {
	Result UserPlan `json:"result"`
}

type APIError struct {
	Field     string `json:"field"`
	Message   string `json:"message"`
	ErrorCode int    `json:"error_code"`
}

// UserSummary contains the data summarizing the user's current resource
// usages and their current plan.
type UserSummary struct {
	CPUUsage  *db.CPUHours   `json:"cpu_usage"`
	DataUsage *UserDataUsage `json:"data_usage"`
	UserPlan  *UserPlan      `json:"user_plan"`
	Errors    []APIError     `json:"errors"`
}

// GetUserSummary is an echo request handler for requests to get a user's
// resource usage and current plan (if QMS is enabled).
func (a *App) GetUserSummary(c echo.Context) error {
	var (
		err     error
		summary UserSummary

		duOK   bool
		planOK bool

		userPlanReq  *http.Request
		userPlanResp *http.Response
		userPlanBody []byte

		dataUsageReq *http.Request
		duResp       *http.Response
		duBody       []byte
	)
	duOK = true
	planOK = true

	context := c.Request().Context()

	user := c.Param("username")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("username was not set"))
	}
	user = a.FixUsername(user)

	log = log.WithFields(logrus.Fields{"context": "get user summary", "user": user}).WithContext(context)

	cpuCtx, cpuSpan := otel.Tracer(otelName).Start(context, "summary: CPU hours")

	d := db.New(a.database)

	var cpuHours *db.CPUHours

	cpuHours, err = d.CurrentCPUHoursForUser(cpuCtx, user)
	if err == sql.ErrNoRows {
		cpuHours = &db.CPUHours{}
		cpuHoursError := APIError{
			Field:     "cpu_usage",
			Message:   "no current CPU hours found for user",
			ErrorCode: http.StatusNotFound,
		}
		summary.Errors = append(summary.Errors, cpuHoursError)
	} else if err != nil {
		log.WithContext(cpuCtx).Error(err)
		cpuHours = &db.CPUHours{}
		cpuHoursError := APIError{
			Field:     "cpu_usage",
			Message:   err.Error(),
			ErrorCode: http.StatusInternalServerError,
		}
		summary.Errors = append(summary.Errors, cpuHoursError)
	}
	cpuSpan.End()

	dataUsageCtx, dataUsageSpan := otel.Tracer(otelName).Start(context, "summary: data usage")
	// Put together the URL for the request in to the data-usage-api
	dataUsageURL, err := url.Parse(a.dataUsageBase)
	if err != nil {
		duOK = false
		duError := APIError{
			Field:     "data_usage",
			Message:   err.Error(),
			ErrorCode: http.StatusInternalServerError,
		}
		summary.Errors = append(summary.Errors, duError)
	}

	if duOK {
		// The path should be /:username/data/current,
		dataUsageURL.Path = fmt.Sprintf("/%s%s", user, a.dataUsageCurrent)

		// Create the request to to the data-usage-api.
		dataUsageReq, err = http.NewRequestWithContext(dataUsageCtx, http.MethodGet, dataUsageURL.String(), nil)
		if err != nil {
			duOK = false
			duError := APIError{
				Field:     "data_usage",
				Message:   err.Error(),
				ErrorCode: http.StatusInternalServerError,
			}
			summary.Errors = append(summary.Errors, duError)
		}
	}

	if duOK {
		// Make the request to the data-usage-api. Close the body when the handler returns.
		duResp, err = client.Do(dataUsageReq)
		if err != nil {
			duOK = false
			duError := APIError{
				Field:     "data_usage",
				Message:   err.Error(),
				ErrorCode: http.StatusInternalServerError,
			}
			summary.Errors = append(summary.Errors, duError)
		} else if duResp.StatusCode < 200 || duResp.StatusCode > 299 {
			duOK = false
			duError := APIError{
				Field:     "data_usage",
				Message:   fmt.Sprintf("status code was %d", duResp.StatusCode),
				ErrorCode: duResp.StatusCode,
			}
			summary.Errors = append(summary.Errors, duError)
		}
		defer duResp.Body.Close()
	}

	if duOK {
		// Read the body and parse the JSON into a struct.
		duBody, err = io.ReadAll(duResp.Body)
		if err != nil {
			duOK = false
			duError := APIError{
				Field:     "data_usage",
				Message:   err.Error(),
				ErrorCode: http.StatusInternalServerError,
			}
			summary.Errors = append(summary.Errors, duError)
		}
	}

	var du UserDataUsage

	if duOK {
		if err = json.Unmarshal(duBody, &du); err != nil {
			duError := APIError{
				Field:     "data_usage",
				Message:   err.Error(),
				ErrorCode: http.StatusInternalServerError,
			}
			summary.Errors = append(summary.Errors, duError)
		}
	}
	dataUsageSpan.End()

	if a.qmsEnabled {
		userPlanCtx, userPlanSpan := otel.Tracer(otelName).Start(context, "summary: user plan")
		// Get the user plan
		userPlanURL, err := url.Parse(a.qmsBaseURL)
		if err != nil {
			planOK = false
			planErr := APIError{
				Field:     "user_plan",
				Message:   err.Error(),
				ErrorCode: http.StatusInternalServerError,
			}
			summary.Errors = append(summary.Errors, planErr)
		}
		userPlanURL.Path = fmt.Sprintf(
			"/v1/users/%s/plan",
			strings.TrimSuffix(user, fmt.Sprintf("@%s", a.userSuffix)),
		)

		log.Debug(userPlanURL.String())

		if planOK {
			userPlanReq, err = http.NewRequestWithContext(userPlanCtx, http.MethodGet, userPlanURL.String(), nil)
			if err != nil {
				planOK = false
				planErr := APIError{
					Field:     "user_plan",
					Message:   err.Error(),
					ErrorCode: http.StatusInternalServerError,
				}
				summary.Errors = append(summary.Errors, planErr)
			}
		}

		if planOK {
			userPlanResp, err = client.Do(userPlanReq)
			if err != nil {
				planOK = false
				planErr := APIError{
					Field:     "user_plan",
					Message:   err.Error(),
					ErrorCode: http.StatusInternalServerError,
				}
				summary.Errors = append(summary.Errors, planErr)
				if userPlanResp.Body != nil {
					userPlanResp.Body.Close()
				}
			} else if userPlanResp.StatusCode < 200 || userPlanResp.StatusCode > 299 {
				planOK = false
				planErr := APIError{
					Field:     "user_plan",
					Message:   fmt.Sprintf("status code was %d", userPlanResp.StatusCode),
					ErrorCode: userPlanResp.StatusCode,
				}
				summary.Errors = append(summary.Errors, planErr)
				if userPlanResp.Body != nil {
					userPlanResp.Body.Close()
				}
			}
		}

		if planOK {
			// Read the body and parse the JSON into a struct.
			userPlanBody, err = io.ReadAll(userPlanResp.Body)
			if err != nil {
				planOK = false
				planErr := APIError{
					Field:     "user_plan",
					Message:   err.Error(),
					ErrorCode: http.StatusInternalServerError,
				}
				summary.Errors = append(summary.Errors, planErr)
			}
		}

		var up UserPlanResult

		if planOK {
			if err = json.Unmarshal(userPlanBody, &up); err != nil {
				planErr := APIError{
					Field:     "user_plan",
					Message:   err.Error(),
					ErrorCode: http.StatusInternalServerError,
				}
				summary.Errors = append(summary.Errors, planErr)
			}
		}

		summary.UserPlan = &up.Result
		userPlanSpan.End()
	} else {
		summary.UserPlan = &UserPlan{}
		summary.Errors = append(summary.Errors, APIError{
			Field:     "user_plan",
			Message:   "QMS support is disabled",
			ErrorCode: http.StatusInternalServerError,
		})
	}

	summary.CPUUsage = cpuHours
	summary.DataUsage = &du

	return c.JSON(http.StatusOK, &summary)

}
