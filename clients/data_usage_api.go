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

type DataUsageAPI struct {
	baseURL *url.URL
}

// DataUsageAPIClient returns a new instance of DataUsageAPI for the given raw base URL.
func DataUsageAPIClient(baseURL string) (*DataUsageAPI, error) {

	//  Parse the raw base URL.
	url, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	// Ensure that the base URL path doesn't end with a slash.
	url.Path = strings.TrimSuffix(url.Path, "/")

	return &DataUsageAPI{baseURL: url}, nil
}

// UserDataUsage contains a user's current data usage, as returned by data-usage-api service.
type UserDataUsage struct {
	ID           string `json:"id"`
	UserID       string `json:"user_id"`
	Username     string `json:"username"`
	Total        int64  `json:"total"`
	Time         string `json:"time"`
	LastModified string `json:"last_modified"`
}

// dataUsageURL returns a URL that can be used to connect to the data-usage-api service. The URL path is determined by
// the base URL and the arguments.
func (c *DataUsageAPI) dataUsageURL(components ...string) *url.URL {
	return BuildURL(c.baseURL, components...)
}

// GetUsageSummary obtains the usage summary information for a user.
func (c *DataUsageAPI) GetUsageSummary(ctx context.Context, username string) (*UserDataUsage, error) {
	var usage UserDataUsage

	// Build the request.
	requestURL := c.dataUsageURL(username, "data", "current")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return &usage, errors.Wrapf(err, "unable to build the request for %s", requestURL)
	}

	// Get the response.
	resp, err := client.Do(req)
	if err != nil {
		return &usage, errors.Wrapf(err, "unable to send the request to %s", requestURL)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &usage, NewHTTPError(resp.StatusCode, fmt.Sprintf("%s returned %d", requestURL, resp.StatusCode))
	}

	// Read the response body.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return &usage, errors.Wrapf(err, "unable to read the response from %s", requestURL)
	}

	// Unmarshal the response body.
	err = json.Unmarshal(body, &usage)
	if err != nil {
		return &usage, errors.Wrapf(err, "unable to parse the response from %s", requestURL)
	}

	return &usage, nil
}
