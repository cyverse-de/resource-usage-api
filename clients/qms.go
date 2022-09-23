package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/pkg/errors"
)

// QMSAPI represents an instance of a QMS API client.
type QMSAPI struct {
	baseURL *url.URL
}

// QMSAPIClient returns a new QMSAPI instance.
func QMSAPIClient(baseURL string) (*QMSAPI, error) {

	//  Parse the raw base URL.
	url, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	// Ensure that the base URL path doesn't end with a slash.
	url.Path = strings.TrimSuffix(url.Path, "/")

	return &QMSAPI{baseURL: url}, nil
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

// qmsURL returns a URL that can be used to connect to QMS. The URL path is determined by the base URL and the path
// components in the argument list.
func (c QMSAPI) qmsURL(components ...string) *url.URL {
	return BuildURL(c.baseURL, components...)
}

// GetUserPlan retrieves the subscription information for the given user.
func (c *QMSAPI) GetUserPlan(ctx context.Context, username string) (*UserPlan, error) {
	var upr UserPlanResult

	// Build the request.
	requestURL := c.qmsURL("v1", "users", username, "plan")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return &upr.Result, errors.Wrapf(err, "unable to build the request for %s", requestURL)
	}

	// Get the response.
	resp, err := client.Do(req)
	if err != nil {
		return &upr.Result, errors.Wrapf(err, "unable to send the request to %s", requestURL)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &upr.Result, NewHTTPError(resp.StatusCode, fmt.Sprintf("%s returned %d", requestURL, resp.StatusCode))
	}

	// Read the response body.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return &upr.Result, errors.Wrapf(err, "unable to read the response from %s", requestURL)
	}

	// Unmarshal the response body.
	err = json.Unmarshal(body, &upr)
	if err != nil {
		return &upr.Result, errors.Wrapf(err, "unable to parse the response from %s", requestURL)
	}

	return &upr.Result, nil
}